import asyncio
import datetime
import json
import os
import multiprocessing as mp
import redis
import threading
import time  # ✅ added

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from kiteconnect import KiteConnect, KiteTicker
import uvicorn


# =========================
# CONFIG (MOVE TO ENV IN REAL USE)
# =========================
API_KEY = os.environ.get("KITE_API_KEY", "eeo1b4qfvxqt7spz")
API_SECRET = os.environ.get("KITE_API_SECRET", "cq7z4ycp4ccezf4k9os2h0i24ba1hh0j")
REDIRECT_URL = os.environ.get("KITE_REDIRECT_URL", "http://127.0.0.1:8000/zerodha/callback")

# Strategy constraints
WORKERS = int(os.environ.get("WORKERS", "6"))  # use 6 cores
MP_QUEUE_MAX = int(os.environ.get("MP_QUEUE_MAX", "20000"))

MAX_TRADES = 6
NO_NEW_TRADES_AFTER = datetime.time(9, 30)
MAX_DAILY_LOSS = -300.0
MAX_DAILY_PROFIT = 900.0

# ✅ changed default risk per trade to 50
DEFAULT_RISK_PER_TRADE = 50.0
BREAKOUT_VALUE_MIN = 1.5e7

# ✅ allowed entry price range
MIN_ENTRY_PRICE = 100.0
MAX_ENTRY_PRICE = 5000.0

# ✅ NEW: hard rupee exits (per trade) and tick size
HARD_MAX_LOSS_RUPEES = 50.0
HARD_MAX_PROFIT_RUPEES = 150.0
PRICE_TICK_SIZE = 0.05

# Redis keys
REDIS_HOST = "localhost"
REDIS_PORT = 6379
ACCESS_TOKEN_KEY = "access_token"
RISK_KEY = "risk_per_trade"
TRADES_DONE_KEY = "trades_done"
TRADING_BLOCKED_KEY = "trading_blocked"
PNL_KEY = "pnl_total"

# ✅ NEW: snapshot keys (to persist last known positions/pnl)
POS_SNAPSHOT_KEY = "pos_snapshot_json"
POS_SNAPSHOT_TS_KEY = "pos_snapshot_ts"

# Per-symbol trade keys
def k_in_trade(sym): return f"in_trade:{sym}"
def k_entry(sym): return f"entry_price:{sym}"
def k_sl(sym): return f"sl:{sym}"
def k_target(sym): return f"target:{sym}"
def k_qty(sym): return f"qty:{sym}"
def k_override(sym): return f"override:{sym}"  # {"sl":..., "target":...}

# ✅ NEW: exit order ids (SL-M + Target LIMIT)
def k_sl_oid(sym): return f"sl_order_id:{sym}"
def k_tgt_oid(sym): return f"tgt_order_id:{sym}"


# =========================
# Tick heartbeat (in-memory) + per-worker status
# =========================
_ticks_total = 0
_last_tick_ts = 0
_last_tick_token = 0
_last_tick_price = 0.0

_worker_ticks_total = [0] * WORKERS
_worker_last_tick_ts = [0] * WORKERS
_worker_last_tick_token = [0] * WORKERS
_worker_last_tick_price = [0.0] * WORKERS

# Websocket status (separate from ticks)
_ws_connected = False
_ws_connected_ts = 0
_ws_last_event_ts = 0
_ws_last_error = ""

_tick_lock = threading.Lock()


# =========================
# Redis
# =========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def redis_ok() -> bool:
    try:
        r.ping()
        return True
    except Exception:
        return False


# =========================
# FastAPI + Kite
# =========================
app = FastAPI()
kite = KiteConnect(api_key=API_KEY)
kite.redirect_url = REDIRECT_URL


# =========================
# Load Universe (1700 stocks)
# =========================
with open("allowed_stocks.json", "r", encoding="utf-8") as f:
    allowed_data = json.load(f)

if isinstance(allowed_data, list):
    allowed_stocks = {
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
# Multiprocessing setup
# =========================
_worker_procs = []
_worker_queues = []
_workers_started = False

TOKENS_SORTED = sorted([int(t) for t in allowed_stocks.values()])
TOKEN_TO_WORKER = {tok: (i % WORKERS) for i, tok in enumerate(TOKENS_SORTED)}
_worker_token_counts = [0] * WORKERS


def ensure_kite_token_global():
    if not redis_ok():
        return
    at = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if at:
        kite.set_access_token(at)

def invalidate_access_token(reason: str):
    """If KiteTicker gives 403, token is unusable for streaming. Force re-login."""
    if not redis_ok():
        return
    print(f"⚠️ Invalidating access token: {reason}")
    r.delete(ACCESS_TOKEN_KEY)
    r.set(TRADING_BLOCKED_KEY, f"LOGIN_REQUIRED ({reason})")

def _start_workers_if_needed():
    global _workers_started, _worker_procs, _worker_queues, _worker_token_counts
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

    _worker_token_counts[:] = [0] * WORKERS
    for tok, wid in TOKEN_TO_WORKER.items():
        _worker_token_counts[int(wid)] += 1

    _workers_started = True
    print(f"✅ Started {WORKERS} worker processes")
    print(f"✅ Token distribution: {_worker_token_counts}")

def _route_tick_to_worker(tick: dict):
    global _ticks_total, _last_tick_ts, _last_tick_token, _last_tick_price
    global _worker_ticks_total, _worker_last_tick_ts, _worker_last_tick_token, _worker_last_tick_price
    global _ws_last_event_ts

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

        _ws_last_event_ts = now  # ✅ any tick counts as WS activity

        if 0 <= int(wid) < WORKERS:
            _worker_ticks_total[int(wid)] += 1
            _worker_last_tick_ts[int(wid)] = now
            _worker_last_tick_token[int(wid)] = int(token)
            _worker_last_tick_price[int(wid)] = float(lp) if lp is not None else 0.0

    try:
        _worker_queues[int(wid)].put_nowait(tick)
    except Exception:
        pass


# =========================
# Strategy Helpers
# =========================
def is_red(open_px: float, close_px: float) -> bool:
    return float(close_px) < float(open_px)

def breakout_value_ok(close_px: float, vol_1m: float):
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

def within_new_trade_window(ts: datetime.datetime) -> bool:
    return ts.time() <= NO_NEW_TRADES_AFTER

def first_candle_ts_ok(ts: datetime.datetime) -> bool:
    return ts.hour == 9 and ts.minute == 15

def second_candle_ts_ok(ts: datetime.datetime) -> bool:
    return ts.hour == 9 and ts.minute == 16


# =========================
# Worker Strategy State (per process)
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

    # ✅ NEW: helpers for hard rupee exits via SL-M + target LIMIT
    def round_to_tick(price: float, tick: float = PRICE_TICK_SIZE) -> float:
        try:
            return round(round(float(price) / float(tick)) * float(tick), 2)
        except Exception:
            return float(price)

    def wait_for_complete_and_avg(order_id: str, timeout_s: int = 10):
        """
        Poll order history until COMPLETE or timeout.
        Returns (is_complete: bool, avg_price: float)
        """
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
            time.sleep(0.5)
        return False, 0.0

    refresh_token()
    print(f"[WORKER {worker_id}] started")

    candle_1m = {}
    mem = {}
    pending_next_open = {}

    # ==========================================================
    # ✅ REST-ONLY OPENING CANDLES (NO FALLBACK)
    # ==========================================================
    _rest_c915 = {}  # sym -> candle dict
    _rest_c916 = {}  # sym -> candle dict

    def _extract_minute_candle(data, hh: int, mm: int):
        found = None
        for c in data or []:
            dt = c.get("date")
            if dt is None:
                continue
            if isinstance(dt, str):
                try:
                    dt = datetime.datetime.fromisoformat(dt)
                except Exception:
                    continue
            tt = dt.time()
            if tt.hour == hh and tt.minute == mm:
                found = c
        return found

    def fetch_single_candle(sym: str, hh: int, mm: int, window_end: datetime.time):
        """
        Fetch ONLY one target minute candle (hh:mm) using a tiny historical window.
        Returns: candle dict or None
        """
        try:
            tok = allowed_stocks.get(sym)
            if not tok:
                return None

            today = datetime.date.today()

            # buffer start 1 minute before target
            start_dt = datetime.datetime.combine(today, datetime.time(hh, max(mm - 1, 0), 0))
            end_dt = datetime.datetime.combine(today, window_end)

            data = kite_local.historical_data(
                instrument_token=int(tok),
                from_date=start_dt,
                to_date=end_dt,
                interval="minute"
            )
            c = _extract_minute_candle(data, hh, mm)
            if not c:
                return None

            return {
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
            }
        except Exception:
            return None

    def try_lock_opening_from_rest(sym: str, ts: datetime.datetime, m: dict):
        """
        Called on every tick. Will:
          - fetch 09:15 after 09:16 begins (single attempt)
          - fetch 09:16 after 09:17 begins (single attempt)
          - when both present, lock pattern
          - if missing after attempts -> ignore symbol (REST-only)
        """
        if m.get("open_locked") or m.get("ignored"):
            return

        now_epoch = time.time()

        # throttle (prevents hammering REST)
        next_try = float(m.get("_rest_next_try", 0.0) or 0.0)
        if now_epoch < next_try:
            return

        # ✅ max attempts (CHANGED TO 1)
        a1 = int(m.get("_rest_a915", 0) or 0)
        a2 = int(m.get("_rest_a916", 0) or 0)
        MAX_ATTEMPTS_EACH = 1  # ✅ CHANGED FROM 2 -> 1

        # 1) After 09:16 starts, fetch 09:15 candle if not cached
        if (ts.hour == 9 and ts.minute >= 16) and (sym not in _rest_c915) and a1 < MAX_ATTEMPTS_EACH:
            m["_rest_a915"] = a1 + 1
            c = fetch_single_candle(sym, 9, 15, window_end=datetime.time(9, 16, 30))
            if c:
                _rest_c915[sym] = c
            else:
                # keep throttle but we won't retry due to MAX_ATTEMPTS_EACH=1
                m["_rest_next_try"] = now_epoch + 0.7
                return

        # 2) After 09:17 starts, fetch 09:16 candle if not cached
        if (ts.hour == 9 and ts.minute >= 17) and (sym not in _rest_c916) and a2 < MAX_ATTEMPTS_EACH:
            m["_rest_a916"] = a2 + 1
            c = fetch_single_candle(sym, 9, 16, window_end=datetime.time(9, 17, 30))
            if c:
                _rest_c916[sym] = c
            else:
                m["_rest_next_try"] = now_epoch + 0.7
                return

        # 3) If both are available -> lock
        c915 = _rest_c915.get(sym)
        c916 = _rest_c916.get(sym)
        if c915 and c916:
            o1, h1, l1, cl1 = c915["open"], c915["high"], c915["low"], c915["close"]
            o2, h2, l2, cl2 = c916["open"], c916["high"], c916["low"], c916["close"]

            red1 = (cl1 < o1)
            red2 = (cl2 < o2)

            ignored = (not red1) or (not red2) or (h2 >= h1)
            pattern_ok = (not ignored)
            day_high = max(h1, h2)

            m["open_locked"] = True
            m["ignored"] = bool(ignored)
            m["c1"] = {"high": float(h1), "low": float(l1)}
            m["c2"] = {"high": float(h2), "low": float(l2)}
            m["pattern_ok"] = bool(pattern_ok)
            m["day_high"] = float(day_high)
            return

        # 4) If reached attempts and still missing -> ignore (after 09:17)
        if ts.hour == 9 and ts.minute >= 17:
            if (sym not in _rest_c915 and int(m.get("_rest_a915", 0) or 0) >= MAX_ATTEMPTS_EACH) or \
               (sym not in _rest_c916 and int(m.get("_rest_a916", 0) or 0) >= MAX_ATTEMPTS_EACH):
                m["ignored"] = True
                return

    # ✅ NEW: monitor exit orders (OCO via cancel-other) without manual involvement
    def monitor_exit_orders():
        while True:
            try:
                refresh_token()
                if not redis_ok_local():
                    time.sleep(1)
                    continue

                # check only symbols that worker has seen (mem keys)
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

                    # If one is COMPLETE -> cancel the other
                    if sl_st == "COMPLETE" and tg_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=tgt_oid)
                            print(f"{sym} SL hit -> cancelled target {tgt_oid}")
                        except Exception as e:
                            print(f"{sym} cancel target failed: {e}")

                    if tg_st == "COMPLETE" and sl_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=sl_oid)
                            print(f"{sym} TARGET hit -> cancelled SL {sl_oid}")
                        except Exception as e:
                            print(f"{sym} cancel SL failed: {e}")

                    # If either complete, clear redis tracking for this symbol
                    if sl_st == "COMPLETE" or tg_st == "COMPLETE":
                        r_local.delete(k_in_trade(sym))
                        r_local.delete(k_entry(sym))
                        r_local.delete(k_sl(sym))
                        r_local.delete(k_target(sym))
                        r_local.delete(k_qty(sym))
                        r_local.delete(k_override(sym))
                        r_local.delete(k_sl_oid(sym))
                        r_local.delete(k_tgt_oid(sym))

            except Exception as e:
                print(f"[WORKER {worker_id}] monitor_exit_orders error: {e}")

            time.sleep(1)

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
                    "c1": None,
                    "c2": None,
                    "pattern_ok": False,
                    "day_high": None,
                    "open_locked": False,

                    # REST fetch controls
                    "_rest_a915": 0,
                    "_rest_a916": 0,
                    "_rest_next_try": 0.0,
                }

            m = mem[sym]
            if m["ignored"]:
                continue

            price = float(tick.get("last_price", 0.0))
            vol_today = float(tick.get("volume_traded", 0.0))

            ts = tick.get("exchange_timestamp")
            if ts is None:
                ts = datetime.datetime.now()
            elif isinstance(ts, str):
                ts = datetime.datetime.fromisoformat(ts)

            # ✅ REST-only: stage fetch 09:15 after 09:16 starts, 09:16 after 09:17 starts
            try_lock_opening_from_rest(sym, ts, m)
            if m["ignored"]:
                continue

            minute_bucket = ts.replace(second=0, microsecond=0)
            cur = candle_1m.get(sym)

            def maybe_entry_on_open(minute_dt: datetime.datetime, open_price: float):
                pe = pending_next_open.get(sym)
                if not pe or pe["next_minute"] != minute_dt:
                    return

                if not within_new_trade_window(minute_dt):
                    pending_next_open.pop(sym, None)
                    return

                trades_done = int(r_local.get(TRADES_DONE_KEY) or 0)
                pnl_total = float(r_local.get(PNL_KEY) or 0.0)
                if trades_done >= MAX_TRADES:
                    pending_next_open.pop(sym, None)
                    return
                if pnl_total <= MAX_DAILY_LOSS or pnl_total >= MAX_DAILY_PROFIT:
                    r_local.set(TRADING_BLOCKED_KEY, "DAILY_LIMIT")
                    pending_next_open.pop(sym, None)
                    return
                if r_local.get(TRADING_BLOCKED_KEY):
                    pending_next_open.pop(sym, None)
                    return
                if r_local.get(k_in_trade(sym)):
                    pending_next_open.pop(sym, None)
                    return

                risk = float(r_local.get(RISK_KEY) or DEFAULT_RISK_PER_TRADE)
                entry = float(open_price)

                # ✅ PRICE FILTER: allow entries only if entry is 100-5000
                if entry < MIN_ENTRY_PRICE or entry > MAX_ENTRY_PRICE:
                    pending_next_open.pop(sym, None)
                    return

                # SL = closer to entry among:
                #   1) Low of breakout candle (pe["sl"])
                #   2) 0.9% below entry (entry * 0.991)
                # For BUY, "closer" => higher SL => max()
                sl_breakout = float(pe["sl"])
                sl_09 = float(entry) * (1.0 - 0.009)
                sl = max(sl_breakout, sl_09)

                if entry <= 0 or sl <= 0 or entry <= sl:
                    pending_next_open.pop(sym, None)
                    return

                qty = risk_qty(entry, sl, risk)
                if qty < 1:
                    pending_next_open.pop(sym, None)
                    return

                # ✅ FIX: True 1:3 target from ENTRY price
                target = entry + 3.0 * (entry - sl)

                try:
                    # ✅ BUY first (market)
                    buy_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=kite_local.EXCHANGE_NSE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(qty),
                        product=kite_local.PRODUCT_MIS,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )

                    # ✅ wait for actual average fill
                    filled, avg_fill = wait_for_complete_and_avg(buy_oid, timeout_s=10)
                    if not filled or avg_fill <= 0:
                        pending_next_open.pop(sym, None)
                        print(f"{sym} BUY NOT COMPLETE / FAILED oid={buy_oid}")
                        return

                    # ✅ compute HARD ₹ SL/Target prices from avg fill + qty
                    sl_trigger = float(avg_fill) - (float(HARD_MAX_LOSS_RUPEES) / float(qty))
                    tgt_price = float(avg_fill) + (float(HARD_MAX_PROFIT_RUPEES) / float(qty))

                    sl_trigger = round_to_tick(sl_trigger)
                    tgt_price = round_to_tick(tgt_price)

                    if sl_trigger <= 0 or tgt_price <= 0:
                        pending_next_open.pop(sym, None)
                        print(f"{sym} BAD SL/TGT computed avg={avg_fill} qty={qty}")
                        return

                    # ✅ place SL-M (trigger only)
                    sl_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=kite_local.EXCHANGE_NSE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=kite_local.PRODUCT_MIS,
                        order_type=kite_local.ORDER_TYPE_SLM,
                        trigger_price=float(sl_trigger),
                    )

                    # ✅ place Target LIMIT
                    tgt_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=kite_local.EXCHANGE_NSE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=kite_local.PRODUCT_MIS,
                        order_type=kite_local.ORDER_TYPE_LIMIT,
                        price=float(tgt_price),
                    )

                    # ✅ store tracking (entry stored as avg fill for correct ₹ exits)
                    r_local.set(k_in_trade(sym), "BUY")
                    r_local.set(k_entry(sym), str(avg_fill))
                    r_local.set(k_sl(sym), str(sl_trigger))
                    r_local.set(k_target(sym), str(tgt_price))
                    r_local.set(k_qty(sym), str(int(qty)))
                    r_local.set(k_sl_oid(sym), str(sl_oid))
                    r_local.set(k_tgt_oid(sym), str(tgt_oid))
                    r_local.incr(TRADES_DONE_KEY)

                    pending_next_open.pop(sym, None)
                    print(f"{sym} ENTRY avg={avg_fill:.2f} qty={qty} SLTrig={sl_trigger:.2f} TGT={tgt_price:.2f}")

                except Exception as e:
                    pending_next_open.pop(sym, None)
                    print(f"{sym} ENTRY FAILED: {e}")

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

            closed = cur
            vol_1m = max(0.0, float(closed["vol_today_end"]) - float(closed["vol_today_start"]))

            candle = {
                "ts": closed["minute"],
                "open": float(closed["open"]),
                "high": float(closed["high"]),
                "low": float(closed["low"]),
                "close": float(closed["close"]),
                "vol_1m": float(vol_1m),
            }

            c_ts = candle["ts"]
            c_high = candle["high"]
            c_low = candle["low"]
            c_close = candle["close"]

            # ✅ REST-only rule: do NOTHING until open_locked is True
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

            # ✅ from here onward: opening candles are REST-locked
            prev_day_high = float(m["day_high"] or c_high)
            m["day_high"] = max(prev_day_high, c_high)

            if within_new_trade_window(c_ts):
                if m["pattern_ok"] and (not r_local.get(k_in_trade(sym))) and (sym not in pending_next_open):
                    if c_high > prev_day_high:
                        ok, _val = breakout_value_ok(c_close, candle["vol_1m"])
                        if ok:
                            entry_minute = minute_bucket
                            sl = float(c_low)

                            pending_next_open[sym] = {
                                "next_minute": entry_minute,
                                "sl": sl,
                            }

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

        except Exception as e:
            print(f"[WORKER {worker_id}] error: {e}")

    print(f"[WORKER {worker_id}] stopped")


# =========================
# KiteTicker runner (thread-safe for --reload)
# =========================
ticker_started = False
ticker_running = False
_ticker_lock = threading.Lock()
_tkr = None

def _token_is_valid_for_rest():
    try:
        ensure_kite_token_global()
        kite.profile()
        return True
    except Exception as e:
        print(f"⚠️ Token sanity check failed (kite.profile): {e}")
        return False

async def run_ticker():
    global ticker_running, _tkr
    global _ws_connected, _ws_connected_ts, _ws_last_event_ts, _ws_last_error

    if not redis_ok():
        print("Redis not running. Cannot start ticker.")
        return

    access_token = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not access_token:
        print("No access token in Redis. Login first.")
        return

    if not _token_is_valid_for_rest():
        invalidate_access_token("REST_TOKEN_INVALID")
        return

    with _ticker_lock:
        if ticker_running:
            print("ℹ️ Ticker already running.")
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
        print(f"✅ KiteTicker connected. Subscribing FULL to {len(tokens)} tokens...")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
        global _ws_connected, _ws_last_error
        with _tick_lock:
            _ws_connected = False
            _ws_last_error = f"close {code}: {reason}"
        print(f"⚠️ KiteTicker closed: code={code}, reason={reason}")

    def on_reconnect(ws, attempts_count):
        global _ws_last_event_ts
        with _tick_lock:
            _ws_last_event_ts = int(time.time())
        print(f"↻ KiteTicker reconnecting... attempt={attempts_count}")

    def on_noreconnect(ws):
        global ticker_running, _ws_connected
        with _tick_lock:
            _ws_connected = False
        print("❌ KiteTicker: no more reconnect attempts (max reached).")
        ticker_running = False

    def on_error(ws, code, reason):
        global ticker_running, _ws_connected, _ws_last_error
        msg = f"{code} - {reason}"
        with _tick_lock:
            _ws_last_error = msg
        print(f"❌ KiteTicker error: {msg}")
        if "403" in msg or str(code) == "403":
            ticker_running = False
            with _tick_lock:
                _ws_connected = False
            invalidate_access_token("WS_403_FORBIDDEN")
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
    """Start ticker safely even under uvicorn --reload (Windows)."""
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
# Routes
# =========================
@app.get("/health")
def health():
    now = int(time.time())
    with _tick_lock:
        last_ts = int(_last_tick_ts or 0)
        ticks_total = int(_ticks_total)
        last_token = int(_last_tick_token or 0)
        last_price = float(_last_tick_price or 0.0)

        w_ticks = list(_worker_ticks_total)
        w_ts = list(_worker_last_tick_ts)
        w_tok = list(_worker_last_tick_token)
        w_px = list(_worker_last_tick_price)

        ws_connected = bool(_ws_connected)
        ws_connected_ts = int(_ws_connected_ts or 0)
        ws_last_event_ts = int(_ws_last_event_ts or 0)
        ws_last_error = str(_ws_last_error or "")

    tick_age = (now - last_ts) if last_ts else None
    ws_age = (now - ws_last_event_ts) if ws_last_event_ts else None
    ws_conn_age = (now - ws_connected_ts) if ws_connected_ts else None

    worker_stats = []
    for i in range(WORKERS):
        w_age = (now - int(w_ts[i])) if int(w_ts[i]) else None
        worker_stats.append({
            "worker": i,
            "ticks_total": int(w_ticks[i]),
            "last_tick_ts": int(w_ts[i] or 0),
            "last_tick_age_s": w_age,
            "last_tick_token": int(w_tok[i] or 0),
            "last_tick_price": float(w_px[i] or 0.0),
            "tokens_assigned": int(_worker_token_counts[i]) if i < len(_worker_token_counts) else 0,
        })

    return {
        "status": "ok",
        "redis": redis_ok(),
        "workers": WORKERS,
        "tokens": len(allowed_stocks),
        "token_distribution": _worker_token_counts,
        "has_access_token": bool((r.get(ACCESS_TOKEN_KEY) or "").strip()) if redis_ok() else False,
        "ticker_running": ticker_running,

        # ticks (global)
        "ticks_total": ticks_total,
        "last_tick_ts": last_ts,
        "last_tick_age_s": tick_age,
        "last_tick_token": last_token,
        "last_tick_price": last_price,

        # websocket status (separate)
        "ws_connected": ws_connected,
        "ws_connected_age_s": ws_conn_age,
        "ws_last_event_age_s": ws_age,
        "ws_last_error": ws_last_error,

        # per-worker
        "worker_stats": worker_stats,
    }

@app.post("/start-ticker")
def start_ticker_api():
    start_ticker_background()
    return {"ok": True, "message": "ticker start requested"}

@app.post("/stop-ticker")
def stop_ticker_api():
    global ticker_running, _tkr
    global _ws_connected
    try:
        if _tkr is not None:
            _tkr.close()
    except Exception:
        pass
    with _tick_lock:
        _ws_connected = False
    ticker_running = False
    return {"ok": True, "message": "ticker stop requested"}

# ✅ LOGOUT ROUTE (CHANGED MINIMALLY: stop ticker but DO NOT delete access token)
@app.post("/logout")
def logout():
    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)

    # Stop ticker if running (safe to call)
    global ticker_running, _tkr
    global _ws_connected
    try:
        if _tkr is not None:
            _tkr.close()
    except Exception:
        pass
    with _tick_lock:
        _ws_connected = False
    ticker_running = False

    # ✅ CHANGED: do NOT delete ACCESS_TOKEN_KEY (keeps positions/pnl available)
    r.set(TRADING_BLOCKED_KEY, "LOGGED_OUT_UI_ONLY")

    return {"ok": True, "message": "Ticker stopped. Positions/P&L remain visible. Login again only if you want to trade."}

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

        r.set(RISK_KEY, str(DEFAULT_RISK_PER_TRADE))  # ✅ force 50 every login

        if not r.get(TRADES_DONE_KEY):
            r.set(TRADES_DONE_KEY, "0")
        if not r.get(PNL_KEY):
            r.set(PNL_KEY, "0")
        r.delete(TRADING_BLOCKED_KEY)

    kite.set_access_token(access_token)

    global ticker_started
    if not ticker_started:
        ticker_started = True
        start_ticker_background()

    return RedirectResponse(url="/", status_code=303)

@app.on_event("startup")
async def startup_event():
    if not redis_ok():
        print("⚠️ Redis not running. Start Redis on localhost:6379")
        return

    # ✅ force 50 on every restart (prevents old Redis value like 75)
    r.set(RISK_KEY, str(DEFAULT_RISK_PER_TRADE))

    if not r.get(TRADES_DONE_KEY):
        r.set(TRADES_DONE_KEY, "0")
    if not r.get(PNL_KEY):
        r.set(PNL_KEY, "0")
    r.delete(TRADING_BLOCKED_KEY)

    _start_workers_if_needed()

    global ticker_started
    if (r.get(ACCESS_TOKEN_KEY) or "").strip() and not ticker_started:
        if _token_is_valid_for_rest():
            ticker_started = True
            start_ticker_background()
            print("✅ access_token found -> requested ticker start")
        else:
            invalidate_access_token("REST_TOKEN_INVALID_ON_STARTUP")

    print("Startup done.")


# ----------- Dashboard UI -----------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    token_present = bool((r.get(ACCESS_TOKEN_KEY) or "").strip()) if redis_ok() else False
    login_btn = (
        '<button disabled style="opacity:0.6; cursor:not-allowed;">Logged in ✅</button>'
        if token_present
        else '<button onclick="window.location.href=\'/login\'">Login to Zerodha</button>'
    )

    risk = (r.get(RISK_KEY) if redis_ok() else str(DEFAULT_RISK_PER_TRADE)) or str(DEFAULT_RISK_PER_TRADE)
    trades_done = (r.get(TRADES_DONE_KEY) if redis_ok() else "0") or "0"
    blocked = (r.get(TRADING_BLOCKED_KEY) if redis_ok() else "") or ""

    html = """
    <html>
    <head>
      <title>Goutham's_custard_apple</title>

      <style>
        body { font-family: Arial, sans-serif; padding: 18px; }
        .row { display:flex; gap:12px; flex-wrap:wrap; margin: 10px 0; }
        .card { border:1px solid #ddd; border-radius:10px; padding:12px; min-width:240px; }
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

        .wtable { margin-top: 10px; }
        .wtable th, .wtable td { font-size: 12px; padding: 6px; }
      </style>
    </head>
    <body>
      <h1>Goutham's custard apple</h1>


      <div class="row">
        <div class="card">
          __LOGIN_BTN__
          <div style="margin-top:10px;">
            <button onclick="resetDay()">Reset Day</button>
            <button onclick="squareoffAll()">Square Off All</button>
            <button onclick="startTicker()">Start Ticker</button>
            <button onclick="stopTicker()">Stop Ticker</button>
            <button onclick="logout()">Logout</button>
          </div>

          <div class="tiny">
            <b>Tick heartbeat:</b>
            <span id="tickBadge" class="badge badge-off">OFF</span>
            <span id="tickHb">-</span>
          </div>

          <table class="wtable" style="width:100%;">
            <thead>
              <tr>
                <th>Worker</th>
                <th>Status</th>
                <th>ticks</th>
                <th>age_s</th>
                <th>token</th>
                <th>price</th>
              </tr>
            </thead>
            <tbody id="workersBody">
              <tr><td colspan="6">Loading...</td></tr>
            </tbody>
          </table>
        </div>

        <div class="card">
          <div><b>Risk per trade:</b></div>
          <div style="margin-top:6px;">
            <input id="risk" value="__RISK__">
            <button onclick="updateRisk()">Update</button>
          </div>
          <div style="margin-top:10px;"><b>Trades done:</b> <span id="tradesDone">__TRADES_DONE__</span> / __MAX_TRADES__</div>
          <div style="margin-top:6px;"><b>Trading blocked:</b> <span id="blocked">__BLOCKED__</span></div>
          <div style="margin-top:6px;"><b>No new trades after:</b> 09:30</div>
          <div class="tiny" style="margin-top:10px;"><b>Entry price filter:</b> 100 to 5000</div>
        </div>

        <div class="card">
          <div><b>Active P&amp;L (updates every 3s):</b></div>
          <div style="font-size:22px; margin-top:8px;">
            <span id="totalPnl" class="pnl-pos">0.00</span>
          </div>
        </div>
      </div>

      <h2>Positions (MIS)</h2>
      <div>Change SL / Target from UI. Changes apply immediately.</div>

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
          if (tickAge !== null && tickAge <= 3) return {cls:"badge badge-live", txt:"LIVE"};
          return {cls:"badge badge-idle", txt:"CONNECTED (NO TICKS)"};
        }

        function statusBadgeWorker(h, age){
          if (!h.ticker_running) return {cls:"badge badge-off", txt:"OFF"};
          if (!h.ws_connected) return {cls:"badge badge-bad", txt:"DISCONNECTED"};
          if (age !== null && age !== undefined && Number(age) <= 3) return {cls:"badge badge-live", txt:"LIVE"};
          return {cls:"badge badge-idle", txt:"IDLE"};
        }

        async function startTicker(){ await fetch("/start-ticker", { method: "POST" }); }
        async function stopTicker(){ await fetch("/stop-ticker", { method: "POST" }); }

        // ✅ CHANGED text only (logic same): logout stops ticker but keeps positions/pnl visible
        async function logout(){
          const ok = confirm("Stop ticker? (Positions/P&L will still remain visible)");
          if(!ok) return;
          await fetch("/logout", { method: "POST" });
          window.location.reload();
        }

        async function updateRisk(){
          const risk = document.getElementById("risk").value;
          await fetch("/settings/risk", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: "risk=" + encodeURIComponent(risk)
          });
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

        async function squareoffAll(){
          const ok = confirm("Square off ALL positions?");
          if(!ok) return;
          await fetch("/squareoff", { method: "POST" });
        }

        async function resetDay(){
          const ok = confirm("Reset day counters (pnl, trades_done, unblock)?");
          if(!ok) return;
          await fetch("/reset-day", { method: "POST" });
        }

        async function refreshHeartbeat(){
          const res = await fetch("/health?ts=" + Date.now(), { cache: "no-store" });
          const h = await res.json();

          const hb = document.getElementById("tickHb");
          const badgeEl = document.getElementById("tickBadge");
          const workersBody = document.getElementById("workersBody");
          if (!hb || !badgeEl || !workersBody) return;

          const b = statusBadgeGlobal(h);
          badgeEl.className = b.cls;
          badgeEl.textContent = b.txt;

          hb.textContent =
            "ticks=" + (h.ticks_total ?? 0) +
            ", tick_age_s=" + (h.last_tick_age_s ?? "-") +
            ", ws_age_s=" + (h.ws_last_event_age_s ?? "-") +
            (h.ws_last_error ? (", ws_err=" + h.ws_last_error) : "");

          workersBody.innerHTML = "";
          const ws = h.worker_stats || [];
          if (!ws.length){
            workersBody.innerHTML = "<tr><td colspan='6'>No worker stats</td></tr>";
            return;
          }

          for (const w of ws){
            const age = (w.last_tick_age_s === null || w.last_tick_age_s === undefined) ? null : Number(w.last_tick_age_s);
            const wb = statusBadgeWorker(h, age);

            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td>${Number(w.worker)}</td>
              <td><span class="${wb.cls}">${wb.txt}</span></td>
              <td>${Number(w.ticks_total || 0)}</td>
              <td>${(age === null) ? "-" : age}</td>
              <td>${Number(w.last_tick_token || 0)}</td>
              <td>${Number(w.last_tick_price || 0).toFixed(2)}</td>
            `;
            workersBody.appendChild(tr);
          }
        }

        async function refreshPositions(){
          const res = await fetch("/positions-table?ts=" + Date.now(), { cache: "no-store" });
          const data = await res.json();

          // ✅ CHANGED MINIMALLY: allow cached snapshot even if needs_login is true
          if (data.needs_login && !(data.rows && data.rows.length)){
            document.getElementById("posBody").innerHTML = '<tr><td colspan="9">Please login</td></tr>';
            return;
          }

          document.getElementById("tradesDone").textContent = String(data.trades_done ?? 0);
          document.getElementById("blocked").textContent = String(data.blocked ?? "-");

          const total = Number(data.total_pnl || 0);
          const pnlEl = document.getElementById("totalPnl");
          pnlEl.textContent = total.toFixed(2);
          pnlEl.className = pnlClass(total);

          const body = document.getElementById("posBody");
          body.innerHTML = "";

          if (!data.rows || data.rows.length === 0){
            body.innerHTML = "<tr><td colspan='9'>No positions</td></tr>";
            return;
          }

          for (const rr of data.rows){
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
        setInterval(refreshPositions, 3000);
      </script>
    </body>
    </html>
    """

    html = (
        html.replace("__LOGIN_BTN__", login_btn)
            .replace("__RISK__", str(risk))
            .replace("__TRADES_DONE__", str(trades_done))
            .replace("__BLOCKED__", str(blocked or "-"))
            .replace("__MAX_TRADES__", str(MAX_TRADES))
    )
    return HTMLResponse(html)


@app.post("/settings/risk")
def update_risk(risk: str = Form(...)):
    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)

    # ✅ HARD LOCK: Risk is ALWAYS 50 (cannot become 75)
    r.set(RISK_KEY, str(DEFAULT_RISK_PER_TRADE))
    return {"ok": True, "risk": DEFAULT_RISK_PER_TRADE}


@app.post("/override")
def set_override(symbol: str = Form(...), sl: str = Form(...), target: str = Form(...)):
    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)

    sym = symbol.strip().upper()
    try:
        sl_v = float(sl) if sl.strip() else None
        t_v = float(target) if target.strip() else None
        r.set(k_override(sym), json.dumps({"sl": sl_v, "target": t_v}))
        return {"ok": True, "symbol": sym, "sl": sl_v, "target": t_v}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

@app.get("/positions-table")
def positions_table():
    # ✅ NEW: return cached snapshot if Kite call fails
    def cached_snapshot(needs_login=False, error_msg=""):
        snap = None
        ts = None
        if redis_ok():
            raw = r.get(POS_SNAPSHOT_KEY)
            ts = r.get(POS_SNAPSHOT_TS_KEY)
            if raw:
                try:
                    snap = json.loads(raw)
                except Exception:
                    snap = None

        if snap:
            snap["ok"] = True
            snap["cached"] = True
            snap["cached_ts"] = ts
            snap["needs_login"] = needs_login
            if error_msg:
                snap["warning"] = error_msg
            return snap

        return {
            "ok": False,
            "cached": False,
            "needs_login": needs_login,
            "error": error_msg or "No cached snapshot available"
        }

    try:
        ensure_kite_token_global()
        pos = kite.positions()
    except Exception as e:
        msg = str(e)
        if "TokenException" in msg or "access_token" in msg:
            return JSONResponse(cached_snapshot(needs_login=True, error_msg=msg), status_code=200)
        return JSONResponse(cached_snapshot(needs_login=False, error_msg=msg), status_code=200)

    net = pos.get("net", [])
    rows = []
    total_pnl = 0.0

    for p in net:
        qty = int(p.get("quantity", 0))
        if qty == 0:
            continue

        sym = str(p.get("tradingsymbol", "")).upper()
        avg_price = float(p.get("average_price") or 0.0)
        ltp = float(p.get("last_price") or 0.0)
        pnl = float(p.get("unrealised") or 0.0) + float(p.get("realised") or 0.0)
        total_pnl += pnl

        sl = r.get(k_sl(sym)) if redis_ok() else None
        target = r.get(k_target(sym)) if redis_ok() else None

        rows.append({
            "symbol": sym,
            "qty": qty,
            "avg": avg_price,
            "ltp": ltp,
            "pnl": pnl,
            "sl": sl,
            "target": target,
        })

    if redis_ok():
        r.set(PNL_KEY, str(total_pnl))
        if total_pnl <= MAX_DAILY_LOSS:
            r.set(TRADING_BLOCKED_KEY, "MAX_DAILY_LOSS")
        elif total_pnl >= MAX_DAILY_PROFIT:
            r.set(TRADING_BLOCKED_KEY, "MAX_DAILY_PROFIT")

        # ✅ NEW: store snapshot so UI can show it even after logout/temp Kite errors
        snapshot = {
            "rows": rows,
            "total_pnl": total_pnl,
            "trades_done": int(r.get(TRADES_DONE_KEY) or 0),
            "blocked": (r.get(TRADING_BLOCKED_KEY) or ""),
        }
        r.set(POS_SNAPSHOT_KEY, json.dumps(snapshot))
        r.set(POS_SNAPSHOT_TS_KEY, str(int(time.time())))

    return {
        "ok": True,
        "cached": False,
        "rows": rows,
        "total_pnl": total_pnl,
        "trades_done": int(r.get(TRADES_DONE_KEY) or 0) if redis_ok() else 0,
        "blocked": (r.get(TRADING_BLOCKED_KEY) or "") if redis_ok() else "",
    }

@app.post("/squareoff")
def squareoff_all():
    ensure_kite_token_global()
    try:
        positions = kite.positions().get("net", [])
        for p in positions:
            qty = int(p.get("quantity", 0))
            if qty == 0:
                continue
            sym = str(p.get("tradingsymbol", "")).upper()
            txn = kite.TRANSACTION_TYPE_SELL if qty > 0 else kite.TRANSACTION_TYPE_BUY

            kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=p.get("exchange", kite.EXCHANGE_NSE),
                tradingsymbol=sym,
                transaction_type=txn,
                quantity=abs(qty),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            if redis_ok():
                r.delete(k_in_trade(sym)); r.delete(k_entry(sym)); r.delete(k_sl(sym))
                r.delete(k_target(sym)); r.delete(k_qty(sym)); r.delete(k_override(sym))
                r.delete(k_sl_oid(sym)); r.delete(k_tgt_oid(sym))

        return {"ok": True, "message": "All positions squared off"}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/reset-day")
def reset_day():
    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)
    r.set(PNL_KEY, "0")
    r.set(TRADES_DONE_KEY, "0")
    r.delete(TRADING_BLOCKED_KEY)

    # ✅ reset risk back to 50 on reset-day also
    r.set(RISK_KEY, str(DEFAULT_RISK_PER_TRADE))

    # ✅ NEW: clear cached snapshot on reset-day
    r.delete(POS_SNAPSHOT_KEY)
    r.delete(POS_SNAPSHOT_TS_KEY)

    return {"ok": True, "message": "Reset done: pnl=0, trades_done=0, trading unblocked"}


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
