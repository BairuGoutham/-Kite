import asyncio
import csv
import datetime
import json
import math
import os
import multiprocessing as mp
import logging
import logging.handlers
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo

import redis
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from kiteconnect import KiteConnect, KiteTicker
import uvicorn


# =========================
# TIMEZONE (India / Kolkata)
# =========================
IST = ZoneInfo("Asia/Kolkata")


# =========================
# PATHS + LOGGING
# =========================
BASE_DIR = Path(__file__).resolve().parent
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper().strip()

LOG_DIR = Path(os.environ.get("LOG_DIR", str(BASE_DIR / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

LOG_FORMAT = "%(asctime)s %(levelname)s %(processName)s pid=%(process)d %(name)s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_log_queue: Optional[mp.Queue] = None
_log_listener: Optional[logging.handlers.QueueListener] = None

# main logger (will be configured below)
log = logging.getLogger("kitealgo")


def setup_main_logging() -> mp.Queue:
    """
    Main process owns real handlers (console + rotating file).
    Workers send records to main via multiprocessing queue.
    """
    global _log_queue, _log_listener

    ctx = mp.get_context("spawn")
    _log_queue = ctx.Queue(-1)

    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Remove existing handlers to avoid duplicates
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=25 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    _log_listener = logging.handlers.QueueListener(
        _log_queue, console, file_handler, respect_handler_level=True
    )
    _log_listener.start()

    # Configure root to emit normally in main
    root.addHandler(console)
    root.addHandler(file_handler)

    logging.getLogger("kitealgo").info("Logging initialized level=%s file=%s", LOG_LEVEL, str(LOG_FILE))
    return _log_queue


def setup_worker_logging(log_queue: mp.Queue, worker_id: int):
    """
    Each worker uses QueueHandler only (no file/console handlers directly).
    """
    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    for h in list(root.handlers):
        root.removeHandler(h)

    root.addHandler(qh)
    logging.getLogger("kitealgo").info("Worker logging initialized worker_id=%s", worker_id)


# simple exception throttling (prevents log spam in hot loops)
_exc_throttle_lock = threading.Lock()
_exc_last_ts: Dict[str, float] = {}


def log_exception_throttled(logger: logging.Logger, key: str, msg: str, every_s: float = 5.0):
    now = time.time()
    with _exc_throttle_lock:
        last = _exc_last_ts.get(key, 0.0)
        if (now - last) < every_s:
            return
        _exc_last_ts[key] = now
    logger.exception(msg)


# =========================
# CONFIG
# =========================
API_KEY = os.environ.get("KITE_API_KEY", "eeo1b4qfvxqt7spz")
API_SECRET = os.environ.get("KITE_API_SECRET", "cq7z4ycp4ccezf4k9os2h0i24ba1hh0j")
REDIRECT_URL = os.environ.get("KITE_REDIRECT_URL", "http://127.0.0.1:8000/zerodha/callback")

WORKERS = int(os.environ.get("WORKERS", "6"))
MP_QUEUE_MAX = int(os.environ.get("MP_QUEUE_MAX", "20000"))


# ---- helpers (env parsing) ----
def _parse_hhmm(value: Optional[str], default: datetime.time) -> datetime.time:
    if not value:
        return default
    s = str(value).strip()
    try:
        hh, mm = s.split(":", 1)
        return datetime.time(int(hh), int(mm))
    except Exception:
        return default


# Strategy
NO_NEW_TRADES_AFTER = _parse_hhmm(os.environ.get("NO_NEW_TRADES_AFTER"), datetime.time(9, 30))  # 09:30 AM IST default
RISK_PER_TRADE = float(os.environ.get("RISK_PER_TRADE", "50"))
BREAKOUT_VALUE_MIN = float(os.environ.get("BREAKOUT_VALUE_MIN", "10000000"))  # 1.5 cr
PRODUCT = os.environ.get("PRODUCT", "MIS").upper().strip()
EXCHANGE = os.environ.get("EXCHANGE", "NSE").upper().strip()
BREAKOUT_MODE = os.environ.get("BREAKOUT_MODE", "FIRST_CANDLE").upper().strip()  # FIRST_CANDLE | DAY_HIGH
OPENING_PATTERN_MODE = os.environ.get("OPENING_PATTERN_MODE", "NONE").upper().strip()  # NONE | LEGACY
PENDING_TRIGGER_TIMEOUT_S = int(os.environ.get("PENDING_TRIGGER_TIMEOUT_S", "1800"))  # 30 min

MIN_ENTRY_PRICE = float(os.environ.get("MIN_ENTRY_PRICE", "100"))
MAX_ENTRY_PRICE = float(os.environ.get("MAX_ENTRY_PRICE", "5000"))
MAX_TRADES = int(os.environ.get("MAX_TRADES", "10"))

# ✅ NEW: SL rule
SL_PCT_BELOW_ENTRY = float(os.environ.get("SL_PCT_BELOW_ENTRY", "0.008"))  # 0.8%

# Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
ACCESS_TOKEN_KEY = "access_token"

# Persisted for the day (until next access_token is generated)
DAY_KEY = "day_key"
POSITIONS_SNAPSHOT_KEY = "positions_snapshot_json"
POSITIONS_SNAPSHOT_TS_KEY = "positions_snapshot_ts"
# LTP storage (hash; O(1) update per tick)
LTP_HASH_KEY = "ltp_map"
LTP_MAP_TS_KEY = "ltp_map_ts"
TRADES_DONE_KEY = "trades_done"

# ✅ NEW: completed trades log
TRADES_LOG_KEY = "trades_log"  # Redis list (latest first)
MAX_TRADES_LOG = int(os.environ.get("MAX_TRADES_LOG", "200"))


# Per-symbol keys
def k_in_trade(sym): return f"in_trade:{sym}"
def k_entry(sym): return f"entry_price:{sym}"
def k_sl(sym): return f"sl:{sym}"
def k_target(sym): return f"target:{sym}"
def k_qty(sym): return f"qty:{sym}"
def k_sl_oid(sym): return f"sl_order_id:{sym}"
def k_tgt_oid(sym): return f"tgt_order_id:{sym}"


# =========================
# GLOBAL STATE (for heartbeat)
# =========================
_tick_lock = threading.Lock()
_ticks_total = 0
_last_tick_ts = 0
_last_tick_token = 0
_last_tick_price = 0.0

_ws_connected = False
_ws_connected_ts = 0
_ws_last_event_ts = 0
_ws_last_error = ""
_last_ltp_ts_sec = 0
_ltp_cache_lock = threading.Lock()
_ltp_cache: Dict[str, float] = {}

# heartbeat log throttle
_last_hb_log_ts = 0


# =========================
# REDIS
# =========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def redis_ok() -> bool:
    try:
        r.ping()
        return True
    except Exception:
        return False


def ltp_flush_loop():
    """
    Ultra-low-latency tick path: collect LTP updates in-memory and flush to Redis in batches.
    """
    logger = logging.getLogger("kitealgo.ltp")
    interval_ms = int(os.environ.get("LTP_FLUSH_INTERVAL_MS", "200"))
    interval_s = max(0.05, float(interval_ms) / 1000.0)
    logger.info("LTP flush loop started interval_ms=%s", interval_ms)

    global _last_ltp_ts_sec
    while True:
        time.sleep(interval_s)

        with _ltp_cache_lock:
            if not _ltp_cache:
                continue
            batch = dict(_ltp_cache)
            _ltp_cache.clear()

        try:
            r.hset(LTP_HASH_KEY, mapping=batch)
            now = int(time.time())
            if now != _last_ltp_ts_sec:
                _last_ltp_ts_sec = now
                r.set(LTP_MAP_TS_KEY, str(now))
        except Exception:
            log_exception_throttled(logger, "ltp_flush", f"LTP flush failed batch_size={len(batch)}", every_s=2.0)


# =========================
# FASTAPI + KITE
# =========================
app = FastAPI()
kite = KiteConnect(api_key=API_KEY)
kite.redirect_url = REDIRECT_URL


@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger = logging.getLogger("kitealgo.http")
    t0 = time.perf_counter()
    try:
        resp = await call_next(request)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        logger.info("%s %s -> %s %.1fms", request.method, request.url.path, resp.status_code, dt_ms)
        return resp
    except Exception:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        logger.exception("%s %s -> EXCEPTION %.1fms", request.method, request.url.path, dt_ms)
        raise


def ensure_kite_token_global() -> bool:
    if not redis_ok():
        return False
    at = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not at:
        return False
    try:
        kite.set_access_token(at)
        return True
    except Exception:
        log_exception_throttled(log, "ensure_kite_token_global", "Failed setting global access token", every_s=5.0)
        return False


# =========================
# LOAD UNIVERSE (allowed_stocks.json [+ optional derivative filter])
# =========================
ALLOWED_STOCKS_PATH = Path(os.environ.get("ALLOWED_STOCKS_PATH", str(BASE_DIR / "allowed_stocks.json")))
DERIVATIVE_STOCKS_PATH = Path(os.environ.get("DERIVATIVE_STOCKS_PATH", str(BASE_DIR / "derivative_stocks.txt")))
UNIVERSE_MODE = os.environ.get("UNIVERSE_MODE", "ALL").upper().strip()  # ALL | DERIVATIVES

with open(ALLOWED_STOCKS_PATH, "r", encoding="utf-8") as f:
    allowed_data = json.load(f)

if isinstance(allowed_data, list):
    allowed_stocks: Dict[str, int] = {
        item["symbol"].upper(): int(item["token"])
        for item in allowed_data
        if isinstance(item, dict) and "symbol" in item and "token" in item
    }
elif isinstance(allowed_data, dict):
    allowed_stocks = {k.upper(): int(v) for k, v in allowed_data.items()}
else:
    raise ValueError("allowed_stocks.json format not supported")

if UNIVERSE_MODE in ("DERIVATIVES", "FNO"):
    try:
        deriv = set()
        with open(DERIVATIVE_STOCKS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().upper()
                if s:
                    deriv.add(s)
        before = len(allowed_stocks)
        allowed_stocks = {sym: tok for sym, tok in allowed_stocks.items() if sym in deriv}
        log.info("Universe filtered: mode=%s before=%s after=%s", UNIVERSE_MODE, before, len(allowed_stocks))
    except FileNotFoundError:
        log.warning("Derivative list not found at %s; using full universe", DERIVATIVE_STOCKS_PATH)
    except Exception:
        log_exception_throttled(log, "derivative_filter", "Derivative filter failed; using full universe", every_s=30.0)

token_to_symbol = {v: k for k, v in allowed_stocks.items()}


# =========================
# INSTRUMENT META (tick size) for order price rounding
# =========================
INSTRUMENTS_CSV_PATH = Path(os.environ.get("INSTRUMENTS_CSV_PATH", str(BASE_DIR / "kite_instruments.csv")))
TICK_SIZE_DEFAULT = float(os.environ.get("TICK_SIZE_DEFAULT", "0.05"))
tick_size_by_symbol: Dict[str, float] = {}

try:
    if INSTRUMENTS_CSV_PATH.exists():
        allowed_tokens = set(int(t) for t in allowed_stocks.values())
        with open(INSTRUMENTS_CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    tok = int(row.get("instrument_token") or 0)
                except Exception:
                    continue
                if tok not in allowed_tokens:
                    continue
                sym = token_to_symbol.get(tok) or str(row.get("tradingsymbol", "")).upper()
                try:
                    ts = float(row.get("tick_size") or 0.0)
                except Exception:
                    ts = 0.0
                if ts and ts > 0:
                    tick_size_by_symbol[sym] = ts
        log.info("Loaded tick sizes: %s symbols", len(tick_size_by_symbol))
except Exception:
    log_exception_throttled(log, "tick_size_load", f"Tick size load failed; using default {TICK_SIZE_DEFAULT}", every_s=60.0)


def tick_size(sym: str) -> float:
    try:
        return float(tick_size_by_symbol.get(str(sym).upper(), TICK_SIZE_DEFAULT) or TICK_SIZE_DEFAULT)
    except Exception:
        return TICK_SIZE_DEFAULT


def floor_to_tick(price: float, tick: float) -> float:
    try:
        t = float(tick) if float(tick) > 0 else TICK_SIZE_DEFAULT
        v = math.floor(float(price) / t) * t
        return round(float(v), 2)
    except Exception:
        return float(price)


def ceil_to_tick(price: float, tick: float) -> float:
    try:
        t = float(tick) if float(tick) > 0 else TICK_SIZE_DEFAULT
        v = math.ceil(float(price) / t) * t
        return round(float(v), 2)
    except Exception:
        return float(price)


# =========================
# MULTIPROCESSING ROUTING
# =========================
TOKENS_SORTED = sorted([int(t) for t in allowed_stocks.values()])
TOKEN_TO_WORKER = {tok: (i % WORKERS) for i, tok in enumerate(TOKENS_SORTED)}
_worker_token_counts = [0] * WORKERS
for tok, wid in TOKEN_TO_WORKER.items():
    _worker_token_counts[int(wid)] += 1

_worker_procs: List[mp.Process] = []
_worker_queues: List[Any] = []
_workers_started = False


def _start_workers_if_needed():
    global _workers_started, _worker_procs, _worker_queues, _log_queue
    if _workers_started:
        return

    if _log_queue is None:
        _log_queue = setup_main_logging()

    ctx = mp.get_context("spawn")
    _worker_queues = []
    _worker_procs = []

    for i in range(WORKERS):
        q = ctx.Queue(maxsize=MP_QUEUE_MAX)
        p = ctx.Process(target=worker_main, args=(i, q, _log_queue), daemon=True)
        p.start()
        _worker_queues.append(q)
        _worker_procs.append(p)

    _workers_started = True
    log.info("Started %s worker processes", WORKERS)
    log.info("Token distribution: %s", _worker_token_counts)


def _route_tick_to_worker(tick: dict):
    global _ticks_total, _last_tick_ts, _last_tick_token, _last_tick_price, _ws_last_event_ts

    token = tick.get("instrument_token")
    if token is None:
        return

    wid = TOKEN_TO_WORKER.get(int(token), 0)

    lp = tick.get("last_price")
    now = int(time.time())

    with _tick_lock:
        _ticks_total += 1
        _last_tick_ts = now
        _last_tick_token = int(token)
        _last_tick_price = float(lp) if lp is not None else 0.0
        _ws_last_event_ts = now

    # store LTP for UI (in-memory; flushed to Redis in batches by ltp_flush_loop)
    if lp is not None:
        try:
            with _ltp_cache_lock:
                _ltp_cache[str(int(token))] = float(lp)
        except Exception:
            log_exception_throttled(logging.getLogger("kitealgo.tick"), "ltp_cache", "Failed updating in-memory LTP", every_s=2.0)

    try:
        _worker_queues[int(wid)].put_nowait(tick)
    except Exception:
        # queue full / worker busy; log at warning but throttled
        logger = logging.getLogger("kitealgo.tick")
        with _exc_throttle_lock:
            k = f"qfull:{wid}"
            last = _exc_last_ts.get(k, 0.0)
            if time.time() - last >= 2.0:
                _exc_last_ts[k] = time.time()
                logger.warning("Worker queue put_nowait failed wid=%s token=%s (queue may be full)", wid, token)


# =========================
# STRATEGY HELPERS
# =========================
def breakout_value_ok(close_px: float, vol_1m: float) -> Tuple[bool, float]:
    try:
        val = float(close_px) * float(vol_1m)
        return (val >= float(BREAKOUT_VALUE_MIN)), val
    except Exception:
        return False, 0.0


def risk_qty(entry: float, sl: float, risk: float) -> int:
    diff = float(entry) - float(sl)
    if diff <= 0:
        return 0
    qty = int(float(risk) / diff)
    return max(qty, 0)


def to_ist(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def within_new_trade_window(ts: datetime.datetime) -> bool:
    return to_ist(ts).time() <= NO_NEW_TRADES_AFTER


# =========================
# WORKER PROCESS
# =========================
def worker_main(worker_id: int, q: mp.Queue, log_queue: mp.Queue):
    setup_worker_logging(log_queue, worker_id)
    wlog = logging.getLogger(f"kitealgo.worker.{worker_id}")

    r_local = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

    kite_local = KiteConnect(api_key=API_KEY)
    kite_local.redirect_url = REDIRECT_URL

    TOKEN_REFRESH_INTERVAL_S = float(os.environ.get("TOKEN_REFRESH_INTERVAL_S", "5"))
    _last_token_refresh = 0.0
    _last_access_token = ""

    def refresh_token(force: bool = False):
        nonlocal _last_token_refresh, _last_access_token
        now = time.time()
        if (not force) and (now - _last_token_refresh) < TOKEN_REFRESH_INTERVAL_S:
            return
        _last_token_refresh = now
        try:
            at = (r_local.get(ACCESS_TOKEN_KEY) or "").strip()
        except Exception:
            return
        if at and at != _last_access_token:
            try:
                kite_local.set_access_token(at)
                _last_access_token = at
                wlog.info("Access token refreshed")
            except Exception:
                log_exception_throttled(wlog, "worker_set_token", "Failed to set access token in worker", every_s=10.0)

    def _diag_key(sym: str) -> str:
        return f"diag:{sym}"

    def diag_set(sym: str, **fields: Any):
        try:
            mapping = {k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in fields.items()}
            r_local.hset(_diag_key(sym), mapping=mapping)
            r_local.expire(_diag_key(sym), 60 * 60 * 16)  # trading-day TTL
        except Exception:
            log_exception_throttled(wlog, f"diag_set:{sym}", f"diag_set failed sym={sym}", every_s=10.0)

    # ✅ NEW: persist completed trades
    def trades_log_push(record: dict):
        try:
            r_local.lpush(TRADES_LOG_KEY, json.dumps(record))
            r_local.ltrim(TRADES_LOG_KEY, 0, MAX_TRADES_LOG - 1)
        except Exception:
            log_exception_throttled(wlog, "trades_log_push", "trades_log_push failed", every_s=10.0)

    def wait_for_complete_and_avg(order_id: str, timeout_s: int = 10) -> Tuple[bool, float]:
        t0 = time.time()
        last_avg = 0.0
        while time.time() - t0 < timeout_s:
            try:
                hist = kite_local.order_history(order_id)
                if hist:
                    last = hist[-1]
                    status = str(last.get("status", "")).upper()
                    avg = float(last.get("average_price") or 0.0)
                    if avg > 0:
                        last_avg = avg
                    if status == "COMPLETE":
                        return True, float(avg or last_avg or 0.0)
                    if status in ("REJECTED", "CANCELLED"):
                        return False, 0.0
            except Exception:
                log_exception_throttled(wlog, f"order_hist:{order_id}", f"order_history failed oid={order_id}", every_s=2.0)
            time.sleep(0.4)
        return False, 0.0

    # ==========================================================
    # ✅ OCO MONITOR (CANCEL OTHER EXIT ORDER) + ✅ SAVE COMPLETED TRADE
    # ==========================================================
    def monitor_exit_orders():
        mon_log = logging.getLogger(f"kitealgo.worker.{worker_id}.oco")
        while True:
            try:
                refresh_token()

                for sym in list(mem.keys()):
                    if not r_local.get(k_in_trade(sym)):
                        continue

                    sl_oid = (r_local.get(k_sl_oid(sym)) or "").strip()
                    tgt_oid = (r_local.get(k_tgt_oid(sym)) or "").strip()
                    if not sl_oid or not tgt_oid:
                        continue

                    def status_of(oid: str) -> str:
                        try:
                            h = kite_local.order_history(oid)
                            if not h:
                                return ""
                            return str(h[-1].get("status", "")).upper()
                        except Exception:
                            log_exception_throttled(mon_log, f"status_of:{oid}", f"status_of failed oid={oid}", every_s=2.0)
                            return ""

                    sl_st = status_of(sl_oid)
                    tg_st = status_of(tgt_oid)

                    if sl_st == "COMPLETE" and tg_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=tgt_oid)
                            mon_log.info("OCO cancel target sym=%s tgt_oid=%s", sym, tgt_oid)
                        except Exception:
                            log_exception_throttled(mon_log, f"cancel_tgt:{sym}", f"OCO cancel target failed sym={sym}", every_s=2.0)

                    if tg_st == "COMPLETE" and sl_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=sl_oid)
                            mon_log.info("OCO cancel sl sym=%s sl_oid=%s", sym, sl_oid)
                        except Exception:
                            log_exception_throttled(mon_log, f"cancel_sl:{sym}", f"OCO cancel sl failed sym={sym}", every_s=2.0)

                    if sl_st == "COMPLETE" or tg_st == "COMPLETE":
                        exit_oid = sl_oid if sl_st == "COMPLETE" else tgt_oid
                        exit_type = "SL" if sl_st == "COMPLETE" else "TARGET"

                        try:
                            entry_price = float(r_local.get(k_entry(sym)) or 0.0)
                            qty = int(r_local.get(k_qty(sym)) or "0")
                            sl_v = float(r_local.get(k_sl(sym)) or 0.0)
                            tgt_v = float(r_local.get(k_target(sym)) or 0.0)
                        except Exception:
                            entry_price, qty, sl_v, tgt_v = 0.0, 0, 0.0, 0.0

                        exit_price = 0.0
                        exit_time = ""
                        try:
                            hist = kite_local.order_history(exit_oid)
                            if hist:
                                last = hist[-1]
                                exit_price = float(last.get("average_price") or 0.0)
                                exit_time = str(last.get("order_timestamp") or last.get("exchange_timestamp") or "")
                        except Exception:
                            log_exception_throttled(mon_log, f"exit_hist:{exit_oid}", f"Exit order_history failed oid={exit_oid}", every_s=2.0)

                        pnl = 0.0
                        if entry_price > 0 and exit_price > 0 and qty > 0:
                            pnl = (exit_price - entry_price) * qty

                        trades_log_push({
                            "ts": int(time.time()),
                            "symbol": sym,
                            "qty": qty,
                            "entry": entry_price,
                            "exit": exit_price,
                            "pnl": pnl,
                            "exit_type": exit_type,
                            "sl": sl_v,
                            "target": tgt_v,
                            "exit_time": exit_time,
                            "sl_oid": sl_oid,
                            "tgt_oid": tgt_oid,
                            "exit_oid": exit_oid,
                        })
                        mon_log.info("Trade completed sym=%s exit_type=%s pnl=%.2f", sym, exit_type, pnl)

                        r_local.delete(k_in_trade(sym))
                        r_local.delete(k_entry(sym))
                        r_local.delete(k_sl(sym))
                        r_local.delete(k_target(sym))
                        r_local.delete(k_qty(sym))
                        r_local.delete(k_sl_oid(sym))
                        r_local.delete(k_tgt_oid(sym))

            except Exception:
                log_exception_throttled(mon_log, "monitor_exit_orders", "monitor_exit_orders loop error", every_s=2.0)

            time.sleep(1)

    refresh_token()
    wlog.info("[WORKER %s] started", worker_id)

    candle_1m: Dict[str, dict] = {}
    mem: Dict[str, dict] = {}
    pending_next_open: Dict[str, dict] = {}
    pending_breakout: Dict[str, dict] = {}  # FIRST_CANDLE mode: wait for breakout-candle high to break

    threading.Thread(target=monitor_exit_orders, daemon=True).start()

    while True:
        tick = q.get()
        if tick is None:
            break

        try:
            token = tick.get("instrument_token")
            if token is None:
                continue
            sym = token_to_symbol.get(int(token))
            if not sym:
                continue

            refresh_token()

            if sym not in mem:
                mem[sym] = {
                    "ignored": False,
                    "pattern_ok": False,
                    "day_high": None,
                    "first_high": None,
                    "open_locked": False,

                    # legacy tick-based opening check helper
                    "_legacy_pending": False,
                    "_o915": None,
                    "_h915": None,
                    "_c915": None,
                }

            m = mem[sym]
            if m["ignored"]:
                continue

            price = float(tick.get("last_price", 0.0))
            vol_today = float(tick.get("volume_traded", 0.0))

            ts = tick.get("exchange_timestamp")
            if ts is None:
                ts = datetime.datetime.now(IST)
            elif isinstance(ts, str):
                ts = datetime.datetime.fromisoformat(ts)
            ts = to_ist(ts)

            def maybe_entry_on_breakout_trigger(now_dt: datetime.datetime, ltp: float):
                if BREAKOUT_MODE != "FIRST_CANDLE":
                    return
                if not m.get("open_locked") or not m.get("pattern_ok"):
                    return

                pe = pending_breakout.get(sym)
                if not pe:
                    return

                now_i = int(time.time())
                set_ts = int(pe.get("set_ts", 0) or 0)
                if set_ts and (now_i - set_ts) > int(PENDING_TRIGGER_TIMEOUT_S):
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="trigger_timeout", last_skip_ts=now_i)
                    wlog.info("Trigger timeout sym=%s", sym)
                    return

                trigger = float(pe.get("trigger") or 0.0)
                if trigger <= 0:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="bad_trigger", last_skip_ts=now_i)
                    wlog.info("Bad trigger sym=%s", sym)
                    return

                if float(ltp) <= trigger:
                    return

                try:
                    trades_done = int(r_local.get(TRADES_DONE_KEY) or "0")
                except Exception:
                    trades_done = 0
                if trades_done >= MAX_TRADES:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="max_trades_reached", trades_done=trades_done, last_skip_ts=now_i)
                    wlog.info("Max trades reached sym=%s trades_done=%s", sym, trades_done)
                    return

                if not within_new_trade_window(now_dt):
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="outside_trade_window", last_skip_ts=now_i)
                    wlog.info("Outside trade window sym=%s", sym)
                    return

                if r_local.get(k_in_trade(sym)):
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="already_in_trade", last_skip_ts=now_i)
                    wlog.info("Already in trade sym=%s", sym)
                    return

                entry_ref = float(ltp)
                if entry_ref < MIN_ENTRY_PRICE or entry_ref > MAX_ENTRY_PRICE:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="price_filter", entry=entry_ref, last_skip_ts=now_i)
                    wlog.info("Price filter sym=%s entry=%s", sym, entry_ref)
                    return

                sl_breakout = float(pe.get("sl") or 0.0)
                sl_08 = float(entry_ref) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                sl = max(sl_breakout, sl_08)
                t = tick_size(sym)
                sl = floor_to_tick(sl, t)
                if entry_ref <= 0 or sl <= 0 or entry_ref <= sl:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="invalid_sl", entry=entry_ref, sl=sl, last_skip_ts=now_i)
                    wlog.info("Invalid SL sym=%s entry=%s sl=%s", sym, entry_ref, sl)
                    return

                qty = risk_qty(entry_ref, sl, RISK_PER_TRADE)
                if qty < 1:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="qty_lt_1", entry=entry_ref, sl=sl, last_skip_ts=now_i)
                    wlog.info("Qty < 1 sym=%s entry=%s sl=%s", sym, entry_ref, sl)
                    return

                pe = pending_breakout.pop(sym, None) or pe

                try:
                    refresh_token(force=True)
                    if not _last_access_token:
                        diag_set(sym, last_order_error="missing_access_token", last_order_error_ts=now_i)
                        wlog.warning("Missing access token sym=%s", sym)
                        return

                    diag_set(
                        sym,
                        last_order_attempt_ts=now_i,
                        entry=entry_ref,
                        trigger=trigger,
                        sl_seed=sl_breakout,
                        sl=sl,
                        qty=int(qty),
                    )

                    wlog.info(
                        "ENTRY trigger sym=%s ltp=%s trigger=%s qty=%s sl=%s",
                        sym, entry_ref, trigger, qty, sl
                    )

                    buy_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )
                    wlog.info("Buy order placed sym=%s oid=%s", sym, buy_oid)

                    filled, avg_fill = wait_for_complete_and_avg(buy_oid, timeout_s=10)
                    if not filled or avg_fill <= 0:
                        diag_set(sym, last_order_error="buy_not_filled", buy_order_id=buy_oid, last_order_error_ts=int(time.time()))
                        wlog.warning("Buy not filled sym=%s oid=%s", sym, buy_oid)
                        return

                    sl_final = max(float(pe.get("sl") or 0.0), float(avg_fill) * (1.0 - float(SL_PCT_BELOW_ENTRY)))
                    sl_final = floor_to_tick(sl_final, t)
                    if float(avg_fill) <= float(sl_final):
                        diag_set(sym, last_order_error="invalid_sl_after_fill", avg_fill=avg_fill, sl=sl_final, last_order_error_ts=int(time.time()))
                        wlog.warning("Invalid SL after fill sym=%s avg=%s sl=%s", sym, avg_fill, sl_final)
                        return

                    target = float(avg_fill) + 3.0 * (float(avg_fill) - float(sl_final))
                    target = ceil_to_tick(target, t)

                    sl_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_SLM,
                        trigger_price=float(sl_final),
                    )

                    tgt_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_LIMIT,
                        price=float(target),
                    )

                    r_local.set(k_in_trade(sym), "BUY")
                    r_local.set(k_entry(sym), str(avg_fill))
                    r_local.set(k_sl(sym), str(sl_final))
                    r_local.set(k_target(sym), str(target))
                    r_local.set(k_qty(sym), str(int(qty)))
                    r_local.set(k_sl_oid(sym), str(sl_oid))
                    r_local.set(k_tgt_oid(sym), str(tgt_oid))

                    try:
                        r_local.incr(TRADES_DONE_KEY)
                    except Exception:
                        pass

                    diag_set(
                        sym,
                        last_order_ok_ts=int(time.time()),
                        buy_order_id=buy_oid,
                        sl_order_id=sl_oid,
                        tgt_order_id=tgt_oid,
                        avg_fill=avg_fill,
                        sl=sl_final,
                        target=target,
                    )

                    wlog.info(
                        "Exits placed sym=%s avg=%.2f sl=%.2f target=%.2f sl_oid=%s tgt_oid=%s",
                        sym, avg_fill, sl_final, target, sl_oid, tgt_oid
                    )

                except Exception:
                    log_exception_throttled(wlog, f"entry_trigger:{sym}", f"Entry trigger flow failed sym={sym}", every_s=1.0)
                    diag_set(sym, last_order_error="exception", last_order_error_ts=int(time.time()))

            maybe_entry_on_breakout_trigger(ts, price)

            minute_bucket = ts.replace(second=0, microsecond=0)
            cur = candle_1m.get(sym)

            def maybe_entry_on_open(minute_dt: datetime.datetime, open_price: float):
                pe = pending_next_open.get(sym)
                if not pe or pe["next_minute"] != minute_dt:
                    return

                try:
                    trades_done = int(r_local.get(TRADES_DONE_KEY) or "0")
                except Exception:
                    trades_done = 0
                if trades_done >= MAX_TRADES:
                    diag_set(sym, last_skip_reason="max_trades_reached", trades_done=trades_done, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    wlog.info("Max trades reached sym=%s trades_done=%s", sym, trades_done)
                    return

                if not within_new_trade_window(minute_dt):
                    diag_set(sym, last_skip_reason="outside_trade_window", last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    wlog.info("Outside trade window sym=%s", sym)
                    return

                if r_local.get(k_in_trade(sym)):
                    diag_set(sym, last_skip_reason="already_in_trade", last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    wlog.info("Already in trade sym=%s", sym)
                    return

                entry = float(open_price)

                if entry < MIN_ENTRY_PRICE or entry > MAX_ENTRY_PRICE:
                    diag_set(sym, last_skip_reason="price_filter", entry=entry, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    wlog.info("Price filter sym=%s entry=%s", sym, entry)
                    return

                sl_breakout = float(pe["sl"])
                sl_08 = float(entry) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                sl = max(sl_breakout, sl_08)

                t = tick_size(sym)
                sl = floor_to_tick(sl, t)

                if entry <= 0 or sl <= 0 or entry <= sl:
                    diag_set(sym, last_skip_reason="invalid_sl", entry=entry, sl=sl, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    wlog.info("Invalid SL sym=%s entry=%s sl=%s", sym, entry, sl)
                    return

                qty = risk_qty(entry, sl, RISK_PER_TRADE)
                if qty < 1:
                    diag_set(sym, last_skip_reason="qty_lt_1", entry=entry, sl=sl, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    wlog.info("Qty < 1 sym=%s entry=%s sl=%s", sym, entry, sl)
                    return

                target = entry + 3.0 * (entry - sl)
                target = ceil_to_tick(target, t)

                try:
                    refresh_token(force=True)
                    if not _last_access_token:
                        diag_set(sym, last_order_error="missing_access_token", last_order_error_ts=int(time.time()))
                        pending_next_open.pop(sym, None)
                        wlog.warning("Missing access token sym=%s", sym)
                        return

                    diag_set(sym, last_order_attempt_ts=int(time.time()), entry=entry, sl=sl, target=target, qty=int(qty))

                    wlog.info("ENTRY on open sym=%s entry=%s qty=%s sl=%s target=%s", sym, entry, qty, sl, target)

                    buy_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )

                    filled, avg_fill = wait_for_complete_and_avg(buy_oid, timeout_s=10)
                    if not filled or avg_fill <= 0:
                        diag_set(sym, last_order_error="buy_not_filled", buy_order_id=buy_oid, last_order_error_ts=int(time.time()))
                        pending_next_open.pop(sym, None)
                        wlog.warning("Buy not filled sym=%s oid=%s", sym, buy_oid)
                        return

                    sl_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_SLM,
                        trigger_price=float(sl),
                    )

                    tgt_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_LIMIT,
                        price=float(target),
                    )

                    r_local.set(k_in_trade(sym), "BUY")
                    r_local.set(k_entry(sym), str(avg_fill))
                    r_local.set(k_sl(sym), str(sl))
                    r_local.set(k_target(sym), str(target))
                    r_local.set(k_qty(sym), str(int(qty)))
                    r_local.set(k_sl_oid(sym), str(sl_oid))
                    r_local.set(k_tgt_oid(sym), str(tgt_oid))

                    try:
                        r_local.incr(TRADES_DONE_KEY)
                    except Exception:
                        pass

                    diag_set(
                        sym,
                        last_order_ok_ts=int(time.time()),
                        buy_order_id=buy_oid,
                        sl_order_id=sl_oid,
                        tgt_order_id=tgt_oid,
                        avg_fill=avg_fill,
                    )
                    pending_next_open.pop(sym, None)

                    wlog.info("Exits placed sym=%s avg=%.2f sl_oid=%s tgt_oid=%s", sym, avg_fill, sl_oid, tgt_oid)

                except Exception:
                    log_exception_throttled(wlog, f"entry_open:{sym}", f"Entry on open flow failed sym={sym}", every_s=1.0)
                    diag_set(sym, last_order_error="exception", last_order_error_ts=int(time.time()))
                    pending_next_open.pop(sym, None)

            if cur is None:
                candle_1m[sym] = {
                    "minute": minute_bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "vol_today_start": vol_today,
                    "vol_today_end": vol_today,
                }
                maybe_entry_on_open(minute_bucket, price)
                continue

            if cur["minute"] == minute_bucket:
                cur["high"] = max(cur["high"], price)
                cur["low"] = min(cur["low"], price)
                cur["close"] = price
                cur["vol_today_end"] = vol_today
                continue

            # =========================
            # candle closed (built from websocket ticks)
            # =========================
            closed = cur
            vol_1m = max(0.0, float(closed["vol_today_end"]) - float(closed["vol_today_start"]))

            candle_ts: datetime.datetime = closed["minute"]  # minute start (IST)
            c_open = float(closed.get("open") or 0.0)
            c_high = float(closed["high"])
            c_low = float(closed["low"])
            c_close = float(closed["close"])

            # ==========================================================
            # ✅ LOCK 09:15 CANDLE FROM WEBSOCKET TICKS
            # ==========================================================
            if (not m.get("open_locked")) and (candle_ts.time() == datetime.time(9, 15)):
                m["first_high"] = float(c_high)
                m["day_high"] = float(c_high)
                m["open_locked"] = True

                red1 = (float(c_close) < float(c_open))
                if not red1:
                    m["ignored"] = True
                    m["pattern_ok"] = False
                    m["_legacy_pending"] = False
                    m["ignore_reason"] = "first_candle_not_red"
                    diag_set(
                        sym,
                        open_locked=1,
                        ignored=1,
                        pattern_ok=0,
                        ignore_reason=m.get("ignore_reason", ""),
                        first_high=m["first_high"],
                        day_high=m["day_high"],
                        locked_at=to_ist(candle_ts).isoformat(),
                        opening_source="ticks_0915",
                    )
                    wlog.info("Ignored sym=%s reason=first_candle_not_red", sym)
                else:
                    m["ignored"] = False
                    if OPENING_PATTERN_MODE == "LEGACY":
                        m["pattern_ok"] = False
                        m["_legacy_pending"] = True
                        m["_o915"] = float(c_open)
                        m["_h915"] = float(c_high)
                        m["_c915"] = float(c_close)
                        diag_set(
                            sym,
                            open_locked=1,
                            ignored=0,
                            pattern_ok=0,
                            first_high=m["first_high"],
                            day_high=m["day_high"],
                            locked_at=to_ist(candle_ts).isoformat(),
                            opening_source="ticks_0915",
                            legacy_pending=1,
                        )
                        wlog.info("Opening locked sym=%s mode=LEGACY first_high=%s", sym, m["first_high"])
                    else:
                        m["pattern_ok"] = True
                        diag_set(
                            sym,
                            open_locked=1,
                            ignored=0,
                            pattern_ok=1,
                            first_high=m["first_high"],
                            day_high=m["day_high"],
                            locked_at=to_ist(candle_ts).isoformat(),
                            opening_source="ticks_0915",
                        )
                        wlog.info("Opening locked sym=%s pattern_ok=1 first_high=%s", sym, m["first_high"])

            if m.get("_legacy_pending") and (candle_ts.time() == datetime.time(9, 16)):
                o1 = float(m.get("_o915") or 0.0)
                h1 = float(m.get("_h915") or 0.0)
                cl1 = float(m.get("_c915") or 0.0)

                h2 = float(c_high)
                red1 = (cl1 < o1)
                ignored = (not red1) or (h2 >= h1)

                m["_legacy_pending"] = False
                m["ignored"] = bool(ignored)
                m["pattern_ok"] = bool(not ignored)

                m["day_high"] = float(max(float(m.get("day_high") or 0.0), h2))

                if ignored:
                    m["ignore_reason"] = "opening_pattern_failed"
                    wlog.info("Ignored sym=%s reason=opening_pattern_failed", sym)
                else:
                    wlog.info("Legacy opening passed sym=%s", sym)

                diag_set(
                    sym,
                    legacy_pending=0,
                    open_locked=1,
                    ignored=int(bool(m["ignored"])),
                    pattern_ok=int(bool(m["pattern_ok"])),
                    day_high=m.get("day_high"),
                    ignore_reason=m.get("ignore_reason", ""),
                    legacy_0916_high=h2,
                )

            if (not m.get("open_locked")) or (not m.get("pattern_ok")):
                candle_1m[sym] = {
                    "minute": minute_bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "vol_today_start": vol_today,
                    "vol_today_end": vol_today,
                }
                maybe_entry_on_open(minute_bucket, price)
                continue

            if m.get("ignored"):
                continue

            prev_day_high = float(m["day_high"] or c_high)
            m["day_high"] = max(prev_day_high, c_high)

            if within_new_trade_window(candle_ts) and m["pattern_ok"] and (not r_local.get(k_in_trade(sym))):
                if BREAKOUT_MODE == "DAY_HIGH":
                    if sym not in pending_next_open:
                        if c_high > prev_day_high:
                            ok, val = breakout_value_ok(c_close, float(vol_1m))
                            if ok:
                                pending_next_open[sym] = {"next_minute": minute_bucket, "sl": float(c_low)}
                                diag_set(
                                    sym,
                                    pending_next_open=minute_bucket.isoformat(),
                                    pending_sl=float(c_low),
                                    pending_set_ts=int(time.time()),
                                    breakout_close=c_close,
                                    breakout_vol_1m=float(vol_1m),
                                )
                                wlog.info("Breakout DAY_HIGH set sym=%s val=%.2f sl=%s", sym, val, c_low)

                elif BREAKOUT_MODE == "FIRST_CANDLE":
                    if sym not in pending_breakout:
                        first_high = float(m.get("first_high") or 0.0)
                        if first_high > 0 and candle_ts.time() >= datetime.time(9, 16):
                            if c_open < first_high and c_close > first_high:
                                ok, val = breakout_value_ok(c_close, float(vol_1m))
                                if ok:
                                    pending_breakout[sym] = {
                                        "trigger": float(c_high),
                                        "sl": float(c_low),
                                        "set_ts": int(time.time()),
                                        "breakout_minute": candle_ts.isoformat(),
                                        "first_high": first_high,
                                        "open": c_open,
                                        "close": c_close,
                                    }
                                    diag_set(
                                        sym,
                                        pending_break_trigger=float(c_high),
                                        pending_sl=float(c_low),
                                        pending_set_ts=int(time.time()),
                                        first_high=first_high,
                                        breakout_open=c_open,
                                        breakout_close=c_close,
                                        breakout_vol_1m=float(vol_1m),
                                    )
                                    wlog.info(
                                        "Breakout FIRST_CANDLE set sym=%s val=%.2f trigger=%s sl=%s first_high=%s",
                                        sym, val, c_high, c_low, first_high
                                    )

            candle_1m[sym] = {
                "minute": minute_bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "vol_today_start": vol_today,
                "vol_today_end": vol_today,
            }
            maybe_entry_on_open(minute_bucket, price)

        except Exception:
            log_exception_throttled(wlog, f"worker_loop:{worker_id}", f"Worker loop error worker_id={worker_id}", every_s=1.0)

    wlog.info("[WORKER %s] stopped", worker_id)


# =========================
# KITE TICKER (FULL MODE)
# =========================
ticker_started = False
ticker_running = False
_ticker_lock = threading.Lock()
_tkr: Optional[KiteTicker] = None


async def run_ticker():
    global ticker_running, _tkr
    global _ws_connected, _ws_connected_ts, _ws_last_event_ts, _ws_last_error

    tlog = logging.getLogger("kitealgo.ticker")

    if not redis_ok():
        tlog.warning("Redis not ok; ticker not started")
        return

    access_token = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not access_token:
        tlog.warning("No access_token in Redis; ticker not started")
        return

    with _ticker_lock:
        if ticker_running:
            return
        ticker_running = True

        try:
            if _tkr is not None:
                _tkr.close()
        except Exception:
            log_exception_throttled(tlog, "tkr_close", "Failed closing existing KiteTicker", every_s=10.0)

        _tkr = KiteTicker(
            API_KEY,
            access_token,
            reconnect=True,
            reconnect_max_tries=300,
            reconnect_max_delay=60,
            connect_timeout=30,
        )

    def on_ticks(ws, ticks):
        for t in ticks:
            _route_tick_to_worker(t)

    def on_connect(ws, response):
        global _ws_connected, _ws_connected_ts, _ws_last_event_ts, _ws_last_error
        now = int(time.time())
        with _tick_lock:
            _ws_connected = True
            _ws_connected_ts = now
            _ws_last_event_ts = now
            _ws_last_error = ""
        tokens = list(allowed_stocks.values())
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        tlog.info("Ticker connected; subscribed tokens=%s", len(tokens))

    def on_close(ws, code, reason):
        global _ws_connected, _ws_last_error
        with _tick_lock:
            _ws_connected = False
            _ws_last_error = f"close {code}: {reason}"
        tlog.warning("Ticker closed code=%s reason=%s", code, reason)

    def on_reconnect(ws, attempts_count):
        global _ws_last_event_ts
        with _tick_lock:
            _ws_last_event_ts = int(time.time())
        tlog.info("Ticker reconnect attempts=%s", attempts_count)

    def on_noreconnect(ws):
        global ticker_running, _ws_connected
        with _tick_lock:
            _ws_connected = False
        ticker_running = False
        tlog.error("Ticker no-reconnect; ticker_running=False")

    def on_error(ws, code, reason):
        global ticker_running, _ws_connected, _ws_last_error
        msg = f"{code} - {reason}"
        with _tick_lock:
            _ws_last_error = msg
        tlog.error("Ticker error %s", msg)
        if "403" in msg or str(code) == "403":
            ticker_running = False
            with _tick_lock:
                _ws_connected = False
            try:
                ws.close()
            except Exception:
                log_exception_throttled(tlog, "ws_close_on_403", "Failed ws.close() after 403", every_s=10.0)

    _tkr.on_ticks = on_ticks
    _tkr.on_connect = on_connect
    _tkr.on_close = on_close
    _tkr.on_error = on_error
    _tkr.on_reconnect = on_reconnect
    _tkr.on_noreconnect = on_noreconnect

    tlog.info("Ticker connect(threaded=True) starting")
    _tkr.connect(threaded=True)


def start_ticker_background():
    tlog = logging.getLogger("kitealgo.ticker")

    def runner():
        try:
            asyncio.run(run_ticker())
        except RuntimeError:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(run_ticker())
                loop.close()
            except Exception:
                log_exception_throttled(tlog, "ticker_runner", "Ticker runner failed", every_s=5.0)
        except Exception:
            log_exception_throttled(tlog, "ticker_runner2", "Ticker runner failed", every_s=5.0)

    threading.Thread(target=runner, daemon=True).start()
    tlog.info("Ticker background thread started")


# =========================
# POSITIONS SNAPSHOT UPDATER (for UI, low latency)
# =========================
def positions_snapshot_loop():
    logger = logging.getLogger("kitealgo.positions")
    logger.info("Positions snapshot loop started")
    global _last_hb_log_ts

    while True:
        try:
            # heartbeat log every 10s
            now = int(time.time())
            if now - _last_hb_log_ts >= 10:
                _last_hb_log_ts = now
                with _tick_lock:
                    tick_age = (now - _last_tick_ts) if _last_tick_ts else None
                    ws_age = (now - _ws_last_event_ts) if _ws_last_event_ts else None
                    logger.info(
                        "HB ticks_total=%s tick_age_s=%s ws_connected=%s ws_age_s=%s ws_err=%s",
                        _ticks_total, tick_age, _ws_connected, ws_age, _ws_last_error
                    )

            if not redis_ok():
                time.sleep(1)
                continue
            if not ensure_kite_token_global():
                time.sleep(1)
                continue

            pos = kite.positions()
            net = pos.get("net", [])
            rows = []
            total_pnl = 0.0

            tok_list: List[str] = []
            for p in net:
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                sym = str(p.get("tradingsymbol", "")).upper()
                tok = allowed_stocks.get(sym)
                if tok is None:
                    continue
                tok_list.append(str(int(tok)))

            ltp_by_tok: Dict[str, float] = {}
            if tok_list:
                try:
                    vals = r.hmget(LTP_HASH_KEY, tok_list)
                    for i, t in enumerate(tok_list):
                        v = vals[i]
                        if v is None or v == "":
                            continue
                        try:
                            ltp_by_tok[t] = float(v)
                        except Exception:
                            continue
                except Exception:
                    log_exception_throttled(logger, "hmget_ltp", "Failed hmget LTP map", every_s=5.0)
                    ltp_by_tok = {}

            for p in net:
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                sym = str(p.get("tradingsymbol", "")).upper()
                avg = float(p.get("average_price") or 0.0)

                tok = allowed_stocks.get(sym)
                ltp = float(p.get("last_price") or 0.0)
                if tok is not None:
                    ltp = float(ltp_by_tok.get(str(int(tok)), ltp) or ltp)

                unreal = (ltp - avg) * qty
                realised = float(p.get("realised") or 0.0)
                pnl = float(unreal) + float(realised)
                total_pnl += pnl

                rows.append({
                    "symbol": sym,
                    "qty": qty,
                    "avg": avg,
                    "ltp": ltp,
                    "pnl": pnl,
                    "sl": (r.get(k_sl(sym)) or ""),
                    "target": (r.get(k_target(sym)) or ""),
                    "in_trade": bool(r.get(k_in_trade(sym)) or ""),
                })

            snap = {
                "rows": rows,
                "total_pnl": total_pnl,
                "ts": int(time.time()),
                "trades_done": int(r.get(TRADES_DONE_KEY) or "0"),
                "max_trades": MAX_TRADES,
            }
            r.set(POSITIONS_SNAPSHOT_KEY, json.dumps(snap))
            r.set(POSITIONS_SNAPSHOT_TS_KEY, str(snap["ts"]))

        except Exception:
            log_exception_throttled(logger, "positions_snapshot_loop", "positions_snapshot_loop error", every_s=2.0)

        time.sleep(1)


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    now = int(time.time())
    redis_up = redis_ok()
    with _tick_lock:
        last_ts = int(_last_tick_ts or 0)
        ticks_total = int(_ticks_total)
        last_token = int(_last_tick_token or 0)
        last_price = float(_last_tick_price or 0.0)

        ws_connected = bool(_ws_connected)
        ws_connected_ts = int(_ws_connected_ts or 0)
        ws_last_event_ts = int(_ws_last_event_ts or 0)
        ws_last_error = str(_ws_last_error or "")

    tick_age = (now - last_ts) if last_ts else None
    ws_age = (now - ws_last_event_ts) if ws_last_event_ts else None
    ws_conn_age = (now - ws_connected_ts) if ws_connected_ts else None

    has_token = False
    if redis_up:
        try:
            has_token = bool((r.get(ACCESS_TOKEN_KEY) or "").strip())
        except Exception:
            has_token = False

    return {
        "status": "ok",
        "redis": redis_up,
        "workers": WORKERS,
        "tokens": len(allowed_stocks),
        "universe_mode": UNIVERSE_MODE,
        "breakout_mode": BREAKOUT_MODE,
        "opening_pattern_mode": OPENING_PATTERN_MODE,
        "token_distribution": _worker_token_counts,
        "has_access_token": has_token,
        "exchange": EXCHANGE,
        "product": PRODUCT,
        "no_new_trades_after": NO_NEW_TRADES_AFTER.isoformat(),
        "ltp_backend": "redis_hash",
        "ticker_running": ticker_running,
        "ticks_total": ticks_total,
        "last_tick_ts": last_ts,
        "last_tick_age_s": tick_age,
        "last_tick_token": last_token,
        "last_tick_price": last_price,
        "ws_connected": ws_connected,
        "ws_connected_age_s": ws_conn_age,
        "ws_last_event_age_s": ws_age,
        "ws_last_error": ws_last_error,
    }


@app.get("/state")
def state():
    if not redis_ok():
        return {"ok": False, "error": "Redis not running"}

    snap = None
    raw = r.get(POSITIONS_SNAPSHOT_KEY)
    ts = r.get(POSITIONS_SNAPSHOT_TS_KEY)
    if raw:
        try:
            snap = json.loads(raw)
        except Exception:
            snap = None

    return {
        "ok": True,
        "positions": snap or {"rows": [], "total_pnl": 0.0, "ts": None, "trades_done": 0, "max_trades": MAX_TRADES},
        "positions_ts": ts,
    }


@app.get("/trades")
def trades():
    if not redis_ok():
        return {"ok": False, "error": "Redis not running"}
    try:
        raw = r.lrange(TRADES_LOG_KEY, 0, MAX_TRADES_LOG - 1)
        out = []
        for x in raw or []:
            try:
                out.append(json.loads(x))
            except Exception:
                continue
        return {"ok": True, "trades": out}
    except Exception as e:
        logging.getLogger("kitealgo.http").exception("Failed reading trades")
        return {"ok": False, "error": str(e)}


@app.get("/universe")
def universe():
    return {
        "ok": True,
        "mode": UNIVERSE_MODE,
        "count": len(allowed_stocks),
        "allowed_stocks_path": str(ALLOWED_STOCKS_PATH),
        "derivative_stocks_path": str(DERIVATIVE_STOCKS_PATH),
        "exchange": EXCHANGE,
        "product": PRODUCT,
        "breakout_mode": BREAKOUT_MODE,
        "opening_pattern_mode": OPENING_PATTERN_MODE,
        "pending_trigger_timeout_s": PENDING_TRIGGER_TIMEOUT_S,
        "no_new_trades_after": NO_NEW_TRADES_AFTER.isoformat(),
        "min_entry_price": MIN_ENTRY_PRICE,
        "max_entry_price": MAX_ENTRY_PRICE,
        "max_trades": MAX_TRADES,
    }


@app.get("/diag/{symbol}")
def diag(symbol: str):
    if not redis_ok():
        return {"ok": False, "error": "Redis not running"}
    sym = symbol.strip().upper()
    try:
        d = r.hgetall(f"diag:{sym}") or {}
    except Exception as e:
        logging.getLogger("kitealgo.http").exception("diag fetch failed sym=%s", sym)
        return {"ok": False, "error": str(e)}
    return {"ok": True, "symbol": sym, "diag": d}


@app.get("/login")
def login():
    if redis_ok() and (r.get(ACCESS_TOKEN_KEY) or "").strip():
        return RedirectResponse(url="/", status_code=303)
    return RedirectResponse(kite.login_url())


@app.get("/zerodha/callback")
async def callback(request: Request):
    clog = logging.getLogger("kitealgo.auth")
    request_token = request.query_params.get("request_token")
    if not request_token:
        clog.warning("Callback missing request_token")
        return JSONResponse({"ok": False, "error": "Missing request_token"}, status_code=400)

    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
    except Exception:
        clog.exception("generate_session failed")
        return JSONResponse({"ok": False, "error": "Session generation failed"}, status_code=500)

    if redis_ok():
        try:
            r.set(ACCESS_TOKEN_KEY, access_token)
            r.set(DAY_KEY, str(int(time.time())))
            r.set(TRADES_DONE_KEY, "0")
            r.delete(TRADES_LOG_KEY)
        except Exception:
            clog.exception("Failed writing access token to Redis")

    try:
        kite.set_access_token(access_token)
    except Exception:
        clog.exception("Failed setting access token on global kite")

    global ticker_started
    if not ticker_started:
        ticker_started = True
        start_ticker_background()

    clog.info("Login success; ticker_started=%s", ticker_started)
    return RedirectResponse(url="/", status_code=303)


@app.post("/override")
def set_override(symbol: str = Form(...), sl: str = Form(...), target: str = Form(...)):
    olog = logging.getLogger("kitealgo.override")

    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)
    if not ensure_kite_token_global():
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    sym = symbol.strip().upper()
    try:
        sl_v = float(sl)
        t_v = float(target)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid SL/Target"}, status_code=400)

    t = tick_size(sym)
    sl_v = floor_to_tick(sl_v, t)
    t_v = ceil_to_tick(t_v, t)

    try:
        sl_oid = (r.get(k_sl_oid(sym)) or "").strip()
        tgt_oid = (r.get(k_tgt_oid(sym)) or "").strip()

        if sl_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_oid)
                olog.info("Cancelled old SL sym=%s oid=%s", sym, sl_oid)
            except Exception:
                olog.exception("Cancel old SL failed sym=%s oid=%s", sym, sl_oid)

        if tgt_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
                olog.info("Cancelled old TARGET sym=%s oid=%s", sym, tgt_oid)
            except Exception:
                olog.exception("Cancel old TARGET failed sym=%s oid=%s", sym, tgt_oid)

        qty = int(r.get(k_qty(sym)) or "0")
        if qty <= 0:
            pos = kite.positions().get("net", [])
            for p in pos:
                if str(p.get("tradingsymbol", "")).upper() == sym:
                    qty = abs(int(p.get("quantity", 0)))
                    break

        if qty <= 0:
            return JSONResponse({"ok": False, "error": "No quantity found for symbol"}, status_code=400)

        new_sl_oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=PRODUCT,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=float(sl_v),
        )
        new_tgt_oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=PRODUCT,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=float(t_v),
        )

        r.set(k_sl(sym), str(sl_v))
        r.set(k_target(sym), str(t_v))
        r.set(k_qty(sym), str(int(qty)))
        r.set(k_sl_oid(sym), str(new_sl_oid))
        r.set(k_tgt_oid(sym), str(new_tgt_oid))

        olog.info("Override applied sym=%s sl=%s target=%s qty=%s", sym, sl_v, t_v, qty)
        return {"ok": True, "symbol": sym, "sl": sl_v, "target": t_v, "qty": qty}

    except Exception as e:
        olog.exception("Override failed sym=%s", sym)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/exit/{symbol}")
def exit_symbol(symbol: str):
    elog = logging.getLogger("kitealgo.exit")

    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)
    if not ensure_kite_token_global():
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    sym = symbol.strip().upper()

    try:
        sl_oid = (r.get(k_sl_oid(sym)) or "").strip()
        tgt_oid = (r.get(k_tgt_oid(sym)) or "").strip()
        if sl_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_oid)
            except Exception:
                elog.exception("Cancel SL failed sym=%s oid=%s", sym, sl_oid)
        if tgt_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
            except Exception:
                elog.exception("Cancel TARGET failed sym=%s oid=%s", sym, tgt_oid)

        qty = 0
        pos = kite.positions().get("net", [])
        for p in pos:
            if str(p.get("tradingsymbol", "")).upper() == sym:
                qty = int(p.get("quantity", 0))
                break
        if qty == 0:
            elog.info("Exit requested but no position sym=%s", sym)
            return {"ok": True, "message": "No position"}

        txn = kite.TRANSACTION_TYPE_SELL if qty > 0 else kite.TRANSACTION_TYPE_BUY
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=txn,
            quantity=abs(int(qty)),
            product=PRODUCT,
            order_type=kite.ORDER_TYPE_MARKET,
        )

        r.delete(k_in_trade(sym))
        r.delete(k_entry(sym))
        r.delete(k_sl(sym))
        r.delete(k_target(sym))
        r.delete(k_qty(sym))
        r.delete(k_sl_oid(sym))
        r.delete(k_tgt_oid(sym))

        elog.info("Exit placed sym=%s qty=%s", sym, qty)
        return {"ok": True, "message": "Exit placed"}

    except Exception as e:
        elog.exception("Exit failed sym=%s", sym)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ----------- Dashboard UI -----------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    token_present = bool((r.get(ACCESS_TOKEN_KEY) or "").strip()) if redis_ok() else False
    login_btn = (
        '<button disabled style="opacity:0.6; cursor:not-allowed;">Logged in ✅</button>'
        if token_present
        else '<button onclick="window.location.href=\'/login\'">Login to Zerodha</button>'
    )

    html = """
    <html>
    <head>
      <title>FASTAPI Kite Algotrading</title>
      <style>
        body { font-family: Arial, sans-serif; padding: 18px; }
        .row { display:flex; gap:12px; flex-wrap:wrap; margin: 10px 0; }
        .card { border:1px solid #ddd; border-radius:10px; padding:12px; min-width:280px; }
        .pnl-pos { color: green; font-weight: bold; }
        .pnl-neg { color: red; font-weight: bold; }
        table { border-collapse: collapse; width: 100%; margin-top: 10px; }
        th, td { border: 1px solid #999; padding: 8px; text-align: left; }
        th { background: #f3f3f3; }
        input { width: 110px; padding:4px; }
        button { padding: 6px 12px; cursor: pointer; }
        .tiny { font-size: 12px; opacity: 0.9; margin-top: 8px; }
        .badge {
          display: inline-block;
          padding: 2px 8px;
          border-radius: 999px;
          font-size: 11px;
          font-weight: bold;
          margin: 0 6px;
          border: 1px solid #ddd;
        }
        .badge-live { background: #e8f7ee; color: #157a3d; border-color: #bfe9cf; }
        .badge-idle { background: #fff7dd; color: #7a5a00; border-color: #ffe29a; }
        .badge-off { background: #f2f2f2; color: #555; border-color: #ddd; }
        .badge-bad { background: #fdecec; color: #a32121; border-color: #f4bcbc; }
      </style>
    </head>
    <body>
      <h1>FASTAPI Kite Algotrading</h1>

      <div class="row">
        <div class="card">
          __LOGIN_BTN__
          <div class="tiny" style="margin-top:10px;">
            <b>Tick heartbeat:</b>
            <span id="tickBadge" class="badge badge-off">OFF</span>
            <span id="tickHb">-</span>
          </div>
        </div>

        <div class="card">
          <div><b>Active P&amp;L (updates every 1s):</b></div>
          <div style="font-size:22px; margin-top:8px;">
            <span id="totalPnl" class="pnl-pos">0.00</span>
          </div>
          <div class="tiny" style="margin-top:8px;">
            <b>Risk per trade:</b> 50 (Qty = 50 / (Entry - SL))<br>
            <b>SL rule:</b> max(BreakoutLow, Entry - 0.8%) (nearer to entry)<br>
            <b>Entry price range:</b> 100 to 5000<br>
            <b>Max trades:</b> 6<br>
            <b>No new trades after:</b> 09:30 AM (Asia/Kolkata)<br>
            <b>Breakout value:</b> LTP * 1mVolume >= 1.5cr
          </div>
        </div>
      </div>

      <h2>Positions (MIS)</h2>
      <div>Change SL / Target from UI. Changes apply immediately (replaces exit orders).</div>

      <table>
        <thead>
          <tr>
            <th>Symbol</th><th>Qty</th><th>Avg</th><th>LTP</th><th>P&amp;L</th>
            <th>SL</th><th>Target</th><th>Update</th><th>Exit</th>
          </tr>
        </thead>
        <tbody id="posBody">
          <tr><td colspan="9">Loading...</td></tr>
        </tbody>
      </table>

      <h2>Completed Trades</h2>
      <table>
        <thead>
          <tr>
            <th>Time</th><th>Symbol</th><th>Qty</th><th>Entry</th><th>Exit</th><th>Exit Type</th><th>P&amp;L</th>
          </tr>
        </thead>
        <tbody id="tradesBody">
          <tr><td colspan="7">Loading...</td></tr>
        </tbody>
      </table>

      <script>
        function pnlClass(v){ return (Number(v||0) >= 0) ? 'pnl-pos' : 'pnl-neg'; }

        function statusBadgeGlobal(h){
          if (!h.ticker_running) return {cls:"badge badge-off", txt:"OFF"};
          if (!h.ws_connected) return {cls:"badge badge-bad", txt:"DISCONNECTED"};
          const tickAge = (h.last_tick_age_s === null || h.last_tick_age_s === undefined) ? null : Number(h.last_tick_age_s);
          if (tickAge !== null && tickAge <= 2) return {cls:"badge badge-live", txt:"LIVE"};
          return {cls:"badge badge-idle", txt:"CONNECTED (NO TICKS)"};
        }

        async function refreshHeartbeat(){
          const res = await fetch("/health?ts=" + Date.now(), { cache: "no-store" });
          const h = await res.json();

          const hb = document.getElementById("tickHb");
          const badgeEl = document.getElementById("tickBadge");
          if (!hb || !badgeEl) return;

          const b = statusBadgeGlobal(h);
          badgeEl.className = b.cls;
          badgeEl.textContent = b.txt;

          hb.textContent =
            "ticks=" + (h.ticks_total ?? 0) +
            ", tick_age_s=" + (h.last_tick_age_s ?? "-") +
            ", ws_age_s=" + (h.ws_last_event_age_s ?? "-") +
            (h.ws_last_error ? (", ws_err=" + h.ws_last_error) : "");
        }

        async function updateSlTarget(sym){
          const sl = document.getElementById("sl_" + sym).value;
          const target = document.getElementById("t_" + sym).value;
          await fetch("/override", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: "symbol=" + encodeURIComponent(sym) + "&sl=" + encodeURIComponent(sl) + "&target=" + encodeURIComponent(target)
          });
        }

        async function exitSymbol(sym){
          await fetch("/exit/" + encodeURIComponent(sym), { method: "POST" });
        }

        async function refreshPositions(){
          const res = await fetch("/state?ts=" + Date.now(), { cache: "no-store" });
          const data = await res.json();

          const total = Number((data.positions && data.positions.total_pnl) || 0);
          const pnlEl = document.getElementById("totalPnl");
          pnlEl.textContent = total.toFixed(2);
          pnlEl.className = pnlClass(total);

          const body = document.getElementById("posBody");
          body.innerHTML = "";

          const rows = (data.positions && data.positions.rows) ? data.positions.rows : [];
          if (!rows.length){
            body.innerHTML = "<tr><td colspan='9'>No positions</td></tr>";
            return;
          }

          for (const rr of rows){
            const sym = String(rr.symbol || "").toUpperCase();
            const pnl = Number(rr.pnl || 0);
            const qty = Number(rr.qty || 0);

            const slVal = (rr.sl ?? "");
            const tVal  = (rr.target ?? "");

            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td>${sym}</td>
              <td>${qty}</td>
              <td>${Number(rr.avg||0).toFixed(2)}</td>
              <td>${Number(rr.ltp||0).toFixed(2)}</td>
              <td class="${pnlClass(pnl)}">${pnl.toFixed(2)}</td>
              <td><input id="sl_${sym}" value="${slVal}"></td>
              <td><input id="t_${sym}" value="${tVal}"></td>
              <td><button onclick="updateSlTarget('${sym}')">Update</button></td>
              <td><button onclick="exitSymbol('${sym}')">Exit</button></td>
            `;
            body.appendChild(tr);
          }
        }

        async function refreshTrades(){
          const res = await fetch("/trades?ts=" + Date.now(), { cache: "no-store" });
          const data = await res.json();

          const body = document.getElementById("tradesBody");
          body.innerHTML = "";

          const rows = (data.trades || []);
          if (!rows.length){
            body.innerHTML = "<tr><td colspan='7'>No completed trades</td></tr>";
            return;
          }

          for (const rr of rows){
            const pnl = Number(rr.pnl || 0);
            const dt = rr.exit_time || (rr.ts ? new Date(rr.ts * 1000).toLocaleTimeString() : "");
            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td>${dt}</td>
              <td>${String(rr.symbol||"")}</td>
              <td>${Number(rr.qty||0)}</td>
              <td>${Number(rr.entry||0).toFixed(2)}</td>
              <td>${Number(rr.exit||0).toFixed(2)}</td>
              <td>${String(rr.exit_type||"")}</td>
              <td class="${pnlClass(pnl)}">${pnl.toFixed(2)}</td>
            `;
            body.appendChild(tr);
          }
        }

        refreshHeartbeat();
        setInterval(refreshHeartbeat, 1000);

        refreshPositions();
        setInterval(refreshPositions, 1000);

        refreshTrades();
        setInterval(refreshTrades, 1000);
      </script>
    </body>
    </html>
    """

    html = html.replace("__LOGIN_BTN__", login_btn)
    return HTMLResponse(html)


@app.on_event("startup")
async def startup_event():
    # initialize logging once (if not started by workers starter)
    global _log_queue
    if _log_queue is None:
        _log_queue = setup_main_logging()

    if not redis_ok():
        log.warning("Redis not running. Start Redis on %s:%s", REDIS_HOST, REDIS_PORT)
        return

    if not r.get(TRADES_DONE_KEY):
        r.set(TRADES_DONE_KEY, "0")

    _start_workers_if_needed()

    global ticker_started
    if (r.get(ACCESS_TOKEN_KEY) or "").strip() and not ticker_started:
        ticker_started = True
        start_ticker_background()

    threading.Thread(target=ltp_flush_loop, daemon=True).start()
    threading.Thread(target=positions_snapshot_loop, daemon=True).start()

    log.info("Startup done. workers=%s tokens=%s breakout_mode=%s opening_mode=%s",
             WORKERS, len(allowed_stocks), BREAKOUT_MODE, OPENING_PATTERN_MODE)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # optional: start logging early when running directly
    if _log_queue is None:
        _log_queue = setup_main_logging()

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
