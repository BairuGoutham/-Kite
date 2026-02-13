import asyncio
import datetime
import json
import os
import multiprocessing as mp
import threading
import time
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
# CONFIG
# =========================
API_KEY = os.environ.get("KITE_API_KEY", "eeo1b4qfvxqt7spz")
API_SECRET = os.environ.get("KITE_API_SECRET", "cq7z4ycp4ccezf4k9os2h0i24ba1hh0j")
REDIRECT_URL = os.environ.get("KITE_REDIRECT_URL", "http://127.0.0.1:8000/zerodha/callback")

WORKERS = int(os.environ.get("WORKERS", "6"))
MP_QUEUE_MAX = int(os.environ.get("MP_QUEUE_MAX", "20000"))

# Strategy
NO_NEW_TRADES_AFTER = datetime.time(9, 30)  # ✅ 09:30 AM IST
RISK_PER_TRADE = 50.0
BREAKOUT_VALUE_MIN = 1.5e7  # 1.5 cr
PRODUCT = "MIS"
EXCHANGE = "NSE"

MIN_ENTRY_PRICE = 100.0
MAX_ENTRY_PRICE = 5000.0
MAX_TRADES = 6

# ✅ NEW: SL rule
SL_PCT_BELOW_ENTRY = 0.008  # 0.8%

# Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
ACCESS_TOKEN_KEY = "access_token"

# Persisted for the day (until next access_token is generated)
DAY_KEY = "day_key"
POSITIONS_SNAPSHOT_KEY = "positions_snapshot_json"
POSITIONS_SNAPSHOT_TS_KEY = "positions_snapshot_ts"
LTP_MAP_KEY = "ltp_map_json"
LTP_MAP_TS_KEY = "ltp_map_ts"
TRADES_DONE_KEY = "trades_done"

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


# =========================
# FASTAPI + KITE
# =========================
app = FastAPI()
kite = KiteConnect(api_key=API_KEY)
kite.redirect_url = REDIRECT_URL


def ensure_kite_token_global() -> bool:
    if not redis_ok():
        return False
    at = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not at:
        return False
    kite.set_access_token(at)
    return True


# =========================
# LOAD UNIVERSE (allowed_stocks.json)
# =========================
with open("allowed_stocks.json", "r", encoding="utf-8") as f:
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

token_to_symbol = {v: k for k, v in allowed_stocks.items()}


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
    global _workers_started, _worker_procs, _worker_queues
    if _workers_started:
        return

    ctx = mp.get_context("spawn")
    _worker_queues = []
    _worker_procs = []

    for i in range(WORKERS):
        q = ctx.Queue(maxsize=MP_QUEUE_MAX)
        p = ctx.Process(target=worker_main, args=(i, q), daemon=True)
        p.start()
        _worker_queues.append(q)
        _worker_procs.append(p)

    _workers_started = True
    print(f"✅ Started {WORKERS} worker processes")
    print(f"✅ Token distribution: {_worker_token_counts}")


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

    # store LTP in redis (shared for UI)
    try:
        if redis_ok() and lp is not None:
            raw = r.get(LTP_MAP_KEY)
            m = json.loads(raw) if raw else {}
            m[str(int(token))] = float(lp)
            r.set(LTP_MAP_KEY, json.dumps(m))
            r.set(LTP_MAP_TS_KEY, str(now))
    except Exception:
        pass

    try:
        _worker_queues[int(wid)].put_nowait(tick)
    except Exception:
        pass


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
def worker_main(worker_id: int, q: mp.Queue):
    r_local = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

    def redis_ok_local():
        try:
            r_local.ping()
            return True
        except Exception:
            return False

    kite_local = KiteConnect(api_key=API_KEY)
    kite_local.redirect_url = REDIRECT_URL

    def refresh_token():
        if not redis_ok_local():
            return
        at = (r_local.get(ACCESS_TOKEN_KEY) or "").strip()
        if at:
            kite_local.set_access_token(at)

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
                pass
            time.sleep(0.4)
        return False, 0.0

    # ==========================================================
    # ✅ REST OPENING CANDLES (ONE CALL, IST-NORMALIZED)
    # ==========================================================
    _rest_opening: Dict[str, dict] = {}  # sym -> {"c915":..., "c916":...}

    def fetch_opening_candles(sym: str) -> Optional[dict]:
        """
        Fetch 09:15 and 09:16 candles in ONE REST call.
        Range: 09:15 -> 09:18 (small buffer), then extract exact minutes by IST.
        """
        try:
            tok = allowed_stocks.get(sym)
            if not tok:
                return None

            today = datetime.datetime.now(IST).date()
            start_dt = datetime.datetime.combine(today, datetime.time(9, 15), tzinfo=IST)
            end_dt = datetime.datetime.combine(today, datetime.time(9, 18), tzinfo=IST)

            data = kite_local.historical_data(
                instrument_token=int(tok),
                from_date=start_dt,
                to_date=end_dt,
                interval="minute"
            )

            c915 = None
            c916 = None

            for c in data or []:
                dt = c.get("date")
                if dt is None:
                    continue
                if isinstance(dt, str):
                    try:
                        dt = datetime.datetime.fromisoformat(dt)
                    except Exception:
                        continue

                dt = to_ist(dt)
                if dt.date() != today:
                    continue

                if dt.hour == 9 and dt.minute == 15:
                    c915 = c
                elif dt.hour == 9 and dt.minute == 16:
                    c916 = c

            if not c915 or not c916:
                return None

            return {
                "c915": {
                    "open": float(c915["open"]),
                    "high": float(c915["high"]),
                    "low": float(c915["low"]),
                    "close": float(c915["close"]),
                },
                "c916": {
                    "open": float(c916["open"]),
                    "high": float(c916["high"]),
                    "low": float(c916["low"]),
                    "close": float(c916["close"]),
                },
            }
        except Exception:
            return None

    def try_lock_opening_from_rest(sym: str, ts: datetime.datetime, m: dict):
        """
        Lock opening candles once time >= 09:17 IST (so 09:16 is complete).
        Single attempt only.
        """
        if m.get("open_locked") or m.get("ignored"):
            return

        ts = to_ist(ts)

        if not (ts.hour == 9 and ts.minute >= 17):
            return

        if m.get("_rest_done"):
            return
        m["_rest_done"] = True

        got = _rest_opening.get(sym)
        if not got:
            got = fetch_opening_candles(sym)
            if got:
                _rest_opening[sym] = got

        if not got:
            m["ignored"] = True
            return

        c915 = got["c915"]
        c916 = got["c916"]

        o1, h1, l1, cl1 = c915["open"], c915["high"], c915["low"], c915["close"]
        o2, h2, l2, cl2 = c916["open"], c916["high"], c916["low"], c916["close"]

        red1 = (cl1 < o1)
        ignored = (not red1) or (h2 >= h1)

        m["open_locked"] = True
        m["ignored"] = bool(ignored)
        m["pattern_ok"] = bool(not ignored)

        # ✅ day_high includes BOTH 09:15 and 09:16 (09:15 is never skipped)
        m["day_high"] = float(max(h1, h2))

    # ==========================================================
    # ✅ OCO MONITOR (CANCEL OTHER EXIT ORDER)
    # ==========================================================
    def monitor_exit_orders():
        while True:
            try:
                refresh_token()
                if not redis_ok_local():
                    time.sleep(1)
                    continue

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
                            return ""

                    sl_st = status_of(sl_oid)
                    tg_st = status_of(tgt_oid)

                    if sl_st == "COMPLETE" and tg_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=tgt_oid)
                        except Exception:
                            pass

                    if tg_st == "COMPLETE" and sl_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=sl_oid)
                        except Exception:
                            pass

                    if sl_st == "COMPLETE" or tg_st == "COMPLETE":
                        r_local.delete(k_in_trade(sym))
                        r_local.delete(k_entry(sym))
                        r_local.delete(k_sl(sym))
                        r_local.delete(k_target(sym))
                        r_local.delete(k_qty(sym))
                        r_local.delete(k_sl_oid(sym))
                        r_local.delete(k_tgt_oid(sym))

            except Exception:
                pass

            time.sleep(1)

    refresh_token()
    print(f"[WORKER {worker_id}] started")

    candle_1m: Dict[str, dict] = {}
    mem: Dict[str, dict] = {}
    pending_next_open: Dict[str, dict] = {}

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
            if not redis_ok_local():
                continue

            if sym not in mem:
                mem[sym] = {
                    "ignored": False,
                    "pattern_ok": False,
                    "day_high": None,
                    "open_locked": False,
                    "_rest_done": False,
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

            # ✅ lock opening candles (REST one-call)
            try_lock_opening_from_rest(sym, ts, m)
            if m["ignored"]:
                continue

            minute_bucket = ts.replace(second=0, microsecond=0)
            cur = candle_1m.get(sym)

            def maybe_entry_on_open(minute_dt: datetime.datetime, open_price: float):
                pe = pending_next_open.get(sym)
                if not pe or pe["next_minute"] != minute_dt:
                    return

                # max trades
                try:
                    trades_done = int(r_local.get(TRADES_DONE_KEY) or "0")
                except Exception:
                    trades_done = 0
                if trades_done >= MAX_TRADES:
                    pending_next_open.pop(sym, None)
                    return

                # no new trades after 09:30 IST
                if not within_new_trade_window(minute_dt):
                    pending_next_open.pop(sym, None)
                    return

                # avoid duplicate entry
                if r_local.get(k_in_trade(sym)):
                    pending_next_open.pop(sym, None)
                    return

                entry = float(open_price)

                # ✅ price filter
                if entry < MIN_ENTRY_PRICE or entry > MAX_ENTRY_PRICE:
                    pending_next_open.pop(sym, None)
                    return

                # ✅ NEW SL RULE:
                # SL = max( breakout_low , entry*(1-0.8%) ) for BUY (higher SL is nearer to entry)
                sl_breakout = float(pe["sl"])
                sl_08 = float(entry) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                sl = max(sl_breakout, sl_08)

                if entry <= 0 or sl <= 0 or entry <= sl:
                    pending_next_open.pop(sym, None)
                    return

                qty = risk_qty(entry, sl, RISK_PER_TRADE)
                if qty < 1:
                    pending_next_open.pop(sym, None)
                    return

                target = entry + 3.0 * (entry - sl)

                try:
                    buy_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=kite_local.EXCHANGE_NSE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(qty),
                        product=kite_local.PRODUCT_MIS,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )

                    filled, avg_fill = wait_for_complete_and_avg(buy_oid, timeout_s=10)
                    if not filled or avg_fill <= 0:
                        pending_next_open.pop(sym, None)
                        return

                    # Place exits based on computed SL and target
                    sl_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=kite_local.EXCHANGE_NSE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=kite_local.PRODUCT_MIS,
                        order_type=kite_local.ORDER_TYPE_SLM,
                        trigger_price=float(sl),
                    )

                    tgt_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=kite_local.EXCHANGE_NSE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=kite_local.PRODUCT_MIS,
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

                    pending_next_open.pop(sym, None)

                except Exception:
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

            # candle closed
            closed = cur
            vol_1m = max(0.0, float(closed["vol_today_end"]) - float(closed["vol_today_start"]))

            candle_ts: datetime.datetime = closed["minute"]
            c_high = float(closed["high"])
            c_low = float(closed["low"])
            c_close = float(closed["close"])

            # do nothing until opening pattern is locked
            if not m.get("open_locked"):
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

            prev_day_high = float(m["day_high"] or c_high)
            m["day_high"] = max(prev_day_high, c_high)

            # breakout detection at candle close -> entry next candle open
            if within_new_trade_window(candle_ts):
                if m["pattern_ok"] and (not r_local.get(k_in_trade(sym))) and (sym not in pending_next_open):
                    if c_high > prev_day_high:
                        ok, _val = breakout_value_ok(c_close, float(vol_1m))
                        if ok:
                            pending_next_open[sym] = {
                                "next_minute": minute_bucket,
                                "sl": float(c_low),  # breakout candle low (we will compare vs 0.8% at entry)
                            }

            # start new candle
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
            pass

    print(f"[WORKER {worker_id}] stopped")


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

    if not redis_ok():
        return

    access_token = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not access_token:
        return

    with _ticker_lock:
        if ticker_running:
            return
        ticker_running = True

        try:
            if _tkr is not None:
                _tkr.close()
        except Exception:
            pass

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

    def on_close(ws, code, reason):
        global _ws_connected, _ws_last_error
        with _tick_lock:
            _ws_connected = False
            _ws_last_error = f"close {code}: {reason}"

    def on_reconnect(ws, attempts_count):
        global _ws_last_event_ts
        with _tick_lock:
            _ws_last_event_ts = int(time.time())

    def on_noreconnect(ws):
        global ticker_running, _ws_connected
        with _tick_lock:
            _ws_connected = False
        ticker_running = False

    def on_error(ws, code, reason):
        global ticker_running, _ws_connected, _ws_last_error
        msg = f"{code} - {reason}"
        with _tick_lock:
            _ws_last_error = msg
        if "403" in msg or str(code) == "403":
            ticker_running = False
            with _tick_lock:
                _ws_connected = False
            try:
                ws.close()
            except Exception:
                pass

    _tkr.on_ticks = on_ticks
    _tkr.on_connect = on_connect
    _tkr.on_close = on_close
    _tkr.on_error = on_error
    _tkr.on_reconnect = on_reconnect
    _tkr.on_noreconnect = on_noreconnect

    _tkr.connect(threaded=True)


def start_ticker_background():
    def runner():
        try:
            asyncio.run(run_ticker())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run_ticker())
            loop.close()

    threading.Thread(target=runner, daemon=True).start()


# =========================
# POSITIONS SNAPSHOT UPDATER (for UI, low latency)
# =========================
def positions_snapshot_loop():
    while True:
        try:
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

            ltp_map = {}
            try:
                raw = r.get(LTP_MAP_KEY)
                ltp_map = json.loads(raw) if raw else {}
            except Exception:
                ltp_map = {}

            for p in net:
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                sym = str(p.get("tradingsymbol", "")).upper()
                avg = float(p.get("average_price") or 0.0)

                tok = allowed_stocks.get(sym)
                ltp = float(p.get("last_price") or 0.0)
                if tok is not None:
                    ltp = float(ltp_map.get(str(int(tok)), ltp) or ltp)

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
            pass

        time.sleep(1)


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    now = int(time.time())
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

    return {
        "status": "ok",
        "redis": redis_ok(),
        "workers": WORKERS,
        "tokens": len(allowed_stocks),
        "token_distribution": _worker_token_counts,
        "has_access_token": bool((r.get(ACCESS_TOKEN_KEY) or "").strip()) if redis_ok() else False,
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
    ts = None
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


@app.get("/login")
def login():
    if redis_ok() and (r.get(ACCESS_TOKEN_KEY) or "").strip():
        return RedirectResponse(url="/", status_code=303)
    return RedirectResponse(kite.login_url())


@app.get("/zerodha/callback")
async def callback(request: Request):
    request_token = request.query_params.get("request_token")
    data = kite.generate_session(request_token, api_secret=API_SECRET)
    access_token = data["access_token"]

    if redis_ok():
        r.set(ACCESS_TOKEN_KEY, access_token)
        r.set(DAY_KEY, str(int(time.time())))
        r.set(TRADES_DONE_KEY, "0")

    kite.set_access_token(access_token)

    global ticker_started
    if not ticker_started:
        ticker_started = True
        start_ticker_background()

    return RedirectResponse(url="/", status_code=303)


@app.post("/override")
def set_override(symbol: str = Form(...), sl: str = Form(...), target: str = Form(...)):
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

    try:
        sl_oid = (r.get(k_sl_oid(sym)) or "").strip()
        tgt_oid = (r.get(k_tgt_oid(sym)) or "").strip()

        if sl_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_oid)
            except Exception:
                pass
        if tgt_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
            except Exception:
                pass

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
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=float(sl_v),
        )
        new_tgt_oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=float(t_v),
        )

        r.set(k_sl(sym), str(sl_v))
        r.set(k_target(sym), str(t_v))
        r.set(k_qty(sym), str(int(qty)))
        r.set(k_sl_oid(sym), str(new_sl_oid))
        r.set(k_tgt_oid(sym), str(new_tgt_oid))

        return {"ok": True, "symbol": sym, "sl": sl_v, "target": t_v, "qty": qty}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/exit/{symbol}")
def exit_symbol(symbol: str):
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
                pass
        if tgt_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
            except Exception:
                pass

        qty = 0
        pos = kite.positions().get("net", [])
        for p in pos:
            if str(p.get("tradingsymbol", "")).upper() == sym:
                qty = int(p.get("quantity", 0))
                break
        if qty == 0:
            return {"ok": True, "message": "No position"}

        txn = kite.TRANSACTION_TYPE_SELL if qty > 0 else kite.TRANSACTION_TYPE_BUY
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=sym,
            transaction_type=txn,
            quantity=abs(int(qty)),
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
        )

        r.delete(k_in_trade(sym))
        r.delete(k_entry(sym))
        r.delete(k_sl(sym))
        r.delete(k_target(sym))
        r.delete(k_qty(sym))
        r.delete(k_sl_oid(sym))
        r.delete(k_tgt_oid(sym))

        return {"ok": True, "message": "Exit placed"}
    except Exception as e:
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

        refreshHeartbeat();
        setInterval(refreshHeartbeat, 1000);

        refreshPositions();
        setInterval(refreshPositions, 1000);
      </script>
    </body>
    </html>
    """

    html = html.replace("__LOGIN_BTN__", login_btn)
    return HTMLResponse(html)


@app.on_event("startup")
async def startup_event():
    if not redis_ok():
        print("⚠️ Redis not running. Start Redis on localhost:6379")
        return

    if not r.get(TRADES_DONE_KEY):
        r.set(TRADES_DONE_KEY, "0")

    _start_workers_if_needed()

    global ticker_started
    if (r.get(ACCESS_TOKEN_KEY) or "").strip() and not ticker_started:
        ticker_started = True
        start_ticker_background()

    threading.Thread(target=positions_snapshot_loop, daemon=True).start()

    print("Startup done.")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
