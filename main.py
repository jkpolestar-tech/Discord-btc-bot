import os
import time
import json
from datetime import datetime, timezone, timedelta

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COIN = os.getenv("COIN", "BTC").strip().upper()
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
API_RETRIES = int(os.getenv("API_RETRIES", "2"))
API_RETRY_SLEEP = float(os.getenv("API_RETRY_SLEEP", "10"))

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_STATUS_REPORT = os.getenv("SEND_STATUS_REPORT", "false").lower() == "true"
STATUS_INTERVAL_MINUTES = int(os.getenv("STATUS_INTERVAL_MINUTES", "180"))
DISCORD_DEDUPE_SECONDS = int(os.getenv("DISCORD_DEDUPE_SECONDS", "120"))

ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))
SIGNAL_LOCK_HOURS = int(os.getenv("SIGNAL_LOCK_HOURS", "8"))
COIN_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("COIN_COOLDOWN_AFTER_SL_HOURS", "8"))
LONG_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("LONG_COOLDOWN_AFTER_SL_HOURS", "6"))
SHORT_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("SHORT_COOLDOWN_AFTER_SL_HOURS", "6"))

ENABLE_DAILY_PAUSE = os.getenv("ENABLE_DAILY_PAUSE", "true").lower() == "true"
MAX_DAILY_SL = int(os.getenv("MAX_DAILY_SL", "2"))

ENTRY_CONFIRMATION = os.getenv("ENTRY_CONFIRMATION", "true").lower() == "true"
USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "true").lower() == "true"
USE_BTC_BIAS = os.getenv("USE_BTC_BIAS", "false").lower() == "true"

BASE_MIN_SCORE = int(os.getenv("BASE_MIN_SCORE", "8"))
PULLBACK_LOOKBACK = int(os.getenv("PULLBACK_LOOKBACK", "20"))
ENTRY_ZONE_PCT = float(os.getenv("ENTRY_ZONE_PCT", "0.0025"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.012"))
VOLUME_BOOST_MIN = float(os.getenv("VOLUME_BOOST_MIN", "1.05"))

BREAK_EVEN_ENABLED = os.getenv("BREAK_EVEN_ENABLED", "true").lower() == "true"
BREAK_EVEN_R = float(os.getenv("BREAK_EVEN_R", "1.0"))
PARTIAL_TP_ENABLED = os.getenv("PARTIAL_TP_ENABLED", "true").lower() == "true"
PARTIAL_TP_R = float(os.getenv("PARTIAL_TP_R", "1.0"))
FINAL_TP_R = float(os.getenv("FINAL_TP_R", "2.0"))

TRAILING_ENABLED = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
TRAILING_EMA = int(os.getenv("TRAILING_EMA", "20"))
TRAILING_BUFFER_PCT = float(os.getenv("TRAILING_BUFFER_PCT", "0.0025"))

CHOP_FILTER_ENABLED = os.getenv("CHOP_FILTER_ENABLED", "true").lower() == "true"
MIN_1H_EMA_SPREAD_PCT = float(os.getenv("MIN_1H_EMA_SPREAD_PCT", "0.20"))
MAX_15M_ATR_PCT_FOR_PULLBACK = float(os.getenv("MAX_15M_ATR_PCT_FOR_PULLBACK", "1.80"))

TREND_REVALIDATION_ENABLED = os.getenv("TREND_REVALIDATION_ENABLED", "true").lower() == "true"
TREND_BREAK_CLOSE_FILTER = os.getenv("TREND_BREAK_CLOSE_FILTER", "true").lower() == "true"
TREND_BREAK_USE_PULLBACK_INVALIDATION = os.getenv("TREND_BREAK_USE_PULLBACK_INVALIDATION", "true").lower() == "true"

ENABLE_LONGS = os.getenv("ENABLE_LONGS", "true").lower() == "true"
ENABLE_SHORTS = os.getenv("ENABLE_SHORTS", "true").lower() == "true"
MIN_SIGNAL_GAP_MINUTES = int(os.getenv("MIN_SIGNAL_GAP_MINUTES", "60"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "1.20"))
ATR_STOP_MULTIPLIER = float(os.getenv("ATR_STOP_MULTIPLIER", "0.35"))
RETEST_TOLERANCE_PCT = float(os.getenv("RETEST_TOLERANCE_PCT", "0.0018"))
LIQUIDITY_SWEEP_LOOKBACK = int(os.getenv("LIQUIDITY_SWEEP_LOOKBACK", "8"))
BREAKOUT_LOOKBACK = int(os.getenv("BREAKOUT_LOOKBACK", "20"))

MAX_OPEN_TRADES = 1


def now_utc():
    return datetime.now(timezone.utc)


def utc_date_str():
    return now_utc().strftime("%Y-%m-%d")


def log(msg: str):
    print(f"[{now_utc().isoformat()}] {msg}", flush=True)


def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def format_price(x: float) -> str:
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 100:
        return f"{x:.3f}"
    return f"{x:.4f}"


def pct_change(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def post_info(payload):
    last_error = None
    for attempt in range(API_RETRIES):
        try:
            r = requests.post(INFO_URL, json=payload, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            log(f"API request failed ({attempt + 1}/{API_RETRIES}): {e}")
            if attempt < API_RETRIES - 1:
                time.sleep(API_RETRY_SLEEP)
    raise last_error


def candles(coin, tf="15m", limit=120):
    now_ms = int(time.time() * 1000)
    interval_ms_map = {
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
    }
    interval_ms = interval_ms_map[tf]

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": tf,
            "startTime": now_ms - (limit + 10) * interval_ms,
            "endTime": now_ms,
        },
    }

    data = post_info(payload)
    out = []
    for x in data[-limit:]:
        out.append({
            "o": float(x["o"]),
            "h": float(x["h"]),
            "l": float(x["l"]),
            "c": float(x["c"]),
            "v": float(x.get("v", 0)),
        })
    return out


def fetch_contexts():
    data = post_info({"type": "metaAndAssetCtxs"})
    meta = data[0]
    ctxs = data[1]
    universe = meta.get("universe", [])

    result = {}
    for i, asset in enumerate(universe):
        name = (asset.get("name") or asset.get("coin") or "").upper()
        if not name or i >= len(ctxs):
            continue
        ctx = ctxs[i] or {}
        result[name] = {
            "markPx": _to_float(ctx.get("markPx", ctx.get("midPx", 0))),
            "funding": _to_float(ctx.get("funding", 0)),
            "openInterest": _to_float(ctx.get("openInterest", ctx.get("oi", 0))),
        }
    return result


def ema(arr, n):
    if len(arr) < n:
        return None
    k = 2 / (n + 1)
    e = sum(arr[:n]) / n
    for v in arr[n:]:
        e = v * k + e * (1 - k)
    return e


def rsi(arr, n=14):
    if len(arr) < n + 1:
        return None

    gains = []
    losses = []

    for i in range(1, n + 1):
        d = arr[i] - arr[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))

    avg_gain = sum(gains) / n
    avg_loss = sum(losses) / n

    for i in range(n + 1, len(arr)):
        d = arr[i] - arr[i - 1]
        gain = max(d, 0)
        loss = max(-d, 0)
        avg_gain = ((avg_gain * (n - 1)) + gain) / n
        avg_loss = ((avg_loss * (n - 1)) + loss) / n

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles_list, period=14):
    if len(candles_list) < period + 1:
        return None

    trs = []
    for i in range(1, len(candles_list)):
        h = candles_list[i]["h"]
        l = candles_list[i]["l"]
        pc = candles_list[i - 1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


def avg_volume(candle_list, n=20):
    vals = [x["v"] for x in candle_list[-n:]]
    return sum(vals) / len(vals) if vals else 0.0


def candle_body_strength(c):
    rng = max(c["h"] - c["l"], 1e-9)
    return abs(c["c"] - c["o"]) / rng


def upper_wick_ratio(c):
    rng = max(c["h"] - c["l"], 1e-9)
    wick = c["h"] - max(c["o"], c["c"])
    return wick / rng


def lower_wick_ratio(c):
    rng = max(c["h"] - c["l"], 1e-9)
    wick = min(c["o"], c["c"]) - c["l"]
    return wick / rng


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        state = {}

    state.setdefault("open", {})
    state.setdefault("last_alerts", {})
    state.setdefault("discord_dedupe", {})
    state.setdefault("market_ctx_prev", {})
    state.setdefault("last_status_time", 0)
    state.setdefault("direction_cooldown_until", {"LONG": "", "SHORT": ""})
    state.setdefault("coin_cooldown_until", {})
    state.setdefault("signal_lock_until", {})
    state.setdefault("last_signal_at", {"LONG": "", "SHORT": ""})
    state.setdefault("daily_stats", {"date": utc_date_str(), "sl_count": 0, "pause_until": ""})
    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def refresh_daily_state(state):
    today = utc_date_str()
    ds = state.setdefault("daily_stats", {"date": today, "sl_count": 0, "pause_until": ""})
    if ds.get("date") != today:
        state["daily_stats"] = {"date": today, "sl_count": 0, "pause_until": ""}


def is_daily_pause_active(state):
    refresh_daily_state(state)
    raw = state["daily_stats"].get("pause_until", "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def bump_daily_sl(state):
    refresh_daily_state(state)
    state["daily_stats"]["sl_count"] += 1
    if ENABLE_DAILY_PAUSE and state["daily_stats"]["sl_count"] >= MAX_DAILY_SL:
        tomorrow = datetime.combine(
            (now_utc() + timedelta(days=1)).date(),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        state["daily_stats"]["pause_until"] = tomorrow.isoformat()


def dedupe_ok(state, key: str):
    last = state["discord_dedupe"].get(key, 0)
    now_ts = time.time()
    if now_ts - last < DISCORD_DEDUPE_SECONDS:
        return False
    state["discord_dedupe"][key] = now_ts
    return True


def post_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt")
    r = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()


def safe_discord(state, message: str, dedupe_key=None):
    try:
        if dedupe_key and not dedupe_ok(state, dedupe_key):
            return
        post_discord(message)
    except Exception as e:
        log(f"Discord send failed: {e}")


def cooldown_ok(state, key: str):
    last = state["last_alerts"].get(key, 0)
    return (time.time() - last) > ALERT_COOLDOWN_MINUTES * 60


def mark_alert(state, key: str):
    state["last_alerts"][key] = time.time()


def is_direction_cooldown_active(state, side: str):
    raw = state.get("direction_cooldown_until", {}).get(side, "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def set_direction_cooldown(state, side: str):
    hours = LONG_COOLDOWN_AFTER_SL_HOURS if side == "LONG" else SHORT_COOLDOWN_AFTER_SL_HOURS
    until = now_utc() + timedelta(hours=hours)
    state["direction_cooldown_until"][side] = until.isoformat()


def is_coin_cooldown_active(state, coin: str):
    raw = state.get("coin_cooldown_until", {}).get(coin, "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def set_coin_cooldown(state, coin: str):
    until = now_utc() + timedelta(hours=COIN_COOLDOWN_AFTER_SL_HOURS)
    state["coin_cooldown_until"][coin] = until.isoformat()


def signal_lock_key(coin: str, side: str):
    return f"{coin}_{side}"


def is_signal_locked(state, coin: str, side: str):
    raw = state.get("signal_lock_until", {}).get(signal_lock_key(coin, side), "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def set_signal_lock(state, coin: str, side: str):
    until = now_utc() + timedelta(hours=SIGNAL_LOCK_HOURS)
    state["signal_lock_until"][signal_lock_key(coin, side)] = until.isoformat()


def mark_signal_time(state, side: str):
    state["last_signal_at"][side] = now_utc().isoformat()


def min_signal_gap_ok(state, side: str):
    raw = state.get("last_signal_at", {}).get(side, "")
    if not raw:
        return True
    try:
        return now_utc() >= datetime.fromisoformat(raw) + timedelta(minutes=MIN_SIGNAL_GAP_MINUTES)
    except Exception:
        return True


def get_trend_bias():
    c1 = candles(COIN, "1h", 220)
    closes = [x["c"] for x in c1]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    if not e20 or not e50 or not e200:
        return "NEUTRAL"

    price = closes[-1]
    if price > e20 > e50 > e200:
        return "LONG"
    if price < e20 < e50 < e200:
        return "SHORT"
    return "NEUTRAL"


def entry_confirmation_ok(c5, side):
    if len(c5) < 3:
        return False

    last = c5[-1]
    prev = c5[-2]

    if side == "LONG":
        return (
            last["c"] > last["o"]
            and candle_body_strength(last) >= 0.35
            and last["c"] > prev["h"]
        )

    return (
        last["c"] < last["o"]
        and candle_body_strength(last) >= 0.35
        and last["c"] < prev["l"]
    )


def chop_filter_ok(price, e20_1h, e50_1h, atr15):
    if not CHOP_FILTER_ENABLED:
        return True

    if price <= 0 or e20_1h is None or e50_1h is None or atr15 is None:
        return False

    ema_spread_pct = abs(e20_1h - e50_1h) / price * 100.0
    atr_pct = atr15 / price * 100.0

    if ema_spread_pct < MIN_1H_EMA_SPREAD_PCT:
        return False

    if atr_pct > MAX_15M_ATR_PCT_FOR_PULLBACK:
        return False

    return True


def previous_swing_low(candles_list, lookback=8):
    vals = [x["l"] for x in candles_list[-lookback:]]
    return min(vals) if vals else None


def previous_swing_high(candles_list, lookback=8):
    vals = [x["h"] for x in candles_list[-lookback:]]
    return max(vals) if vals else None


def analyze_btc(market_ctx, prev_ctx):
    c5 = candles(COIN, "5m", 180)
    time.sleep(0.4)
    c15 = candles(COIN, "15m", 200)
    time.sleep(0.4)
    c1 = candles(COIN, "1h", 220)

    closes5 = [x["c"] for x in c5]
    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]

    e20_5 = ema(closes5, 20)
    e20_15 = ema(closes15, 20)
    e50_15 = ema(closes15, 50)
    e20_1h = ema(closes1, 20)
    e50_1h = ema(closes1, 50)
    e200_1h = ema(closes1, 200)
    r15 = rsi(closes15)
    atr15 = atr(c15, 14)

    if None in (e20_5, e20_15, e50_15, e20_1h, e50_1h, e200_1h, r15, atr15):
        return None

    price = closes5[-1]
    if not chop_filter_ok(price, e20_1h, e50_1h, atr15):
        return None

    bullish_1h = closes1[-1] > e20_1h > e50_1h > e200_1h
    bearish_1h = closes1[-1] < e20_1h < e50_1h < e200_1h

    if bullish_1h and not ENABLE_LONGS:
        return None
    if bearish_1h and not ENABLE_SHORTS:
        return None

    recent_15_lows = [x["l"] for x in c15[-PULLBACK_LOOKBACK:]]
    recent_15_highs = [x["h"] for x in c15[-PULLBACK_LOOKBACK:]]
    range_low_15 = min(recent_15_lows)
    range_high_15 = max(recent_15_highs)

    last5 = c5[-1]
    prev5 = c5[-2]
    prev2_5 = c5[-3]
    last15 = c15[-1]
    prev15 = c15[-2]

    fib_long_low = range_low_15 + (range_high_15 - range_low_15) * 0.50
    fib_long_high = range_low_15 + (range_high_15 - range_low_15) * 0.618
    fib_short_low = range_high_15 - (range_high_15 - range_low_15) * 0.618
    fib_short_high = range_high_15 - (range_high_15 - range_low_15) * 0.50

    in_pullback_long_zone = fib_long_low <= price <= fib_long_high
    in_pullback_short_zone = fib_short_low <= price <= fib_short_high

    swept_low = last5["l"] < min(x["l"] for x in c5[-(LIQUIDITY_SWEEP_LOOKBACK + 1):-1]) and last5["c"] > prev5["l"]
    swept_high = last5["h"] > max(x["h"] for x in c5[-(LIQUIDITY_SWEEP_LOOKBACK + 1):-1]) and last5["c"] < prev5["h"]

    breakout_high = max(x["h"] for x in c15[-(BREAKOUT_LOOKBACK + 1):-1])
    breakout_low = min(x["l"] for x in c15[-(BREAKOUT_LOOKBACK + 1):-1])
    broke_out_up = prev15["c"] > breakout_high
    broke_out_down = prev15["c"] < breakout_low
    retest_up = abs(price - breakout_high) / max(price, 1e-9) <= RETEST_TOLERANCE_PCT and price >= breakout_high * (1 - RETEST_TOLERANCE_PCT)
    retest_down = abs(price - breakout_low) / max(price, 1e-9) <= RETEST_TOLERANCE_PCT and price <= breakout_low * (1 + RETEST_TOLERANCE_PCT)

    ctx = market_ctx.get(COIN, {})
    funding = _to_float(ctx.get("funding", 0))
    oi = _to_float(ctx.get("openInterest", 0))
    prev_oi = _to_float(prev_ctx.get(COIN, {}).get("openInterest", 0))

    score = 0
    side = None
    trigger = None
    invalidation_level = None
    setup_note = []

    # Long setup selection
    if bullish_1h:
        score += 2
        if closes1[-1] > e50_1h:
            score += 1
        if 45 <= r15 <= 68:
            score += 1

        if in_pullback_long_zone and price >= e20_15 * 0.998 and price <= e20_15 * 1.006:
            score += 2
            side = "LONG"
            trigger = "Pullback Long"
            invalidation_level = previous_swing_low(c5, 10)
            setup_note.append("15m Pullback in Value Zone")

        if swept_low:
            if side is None or score < 8:
                side = "LONG"
                trigger = "Liquidity Sweep Long"
                invalidation_level = min(last5["l"], prev5["l"], prev2_5["l"])
            score += 3
            setup_note.append("5m Liquidity Sweep")

        if broke_out_up and retest_up:
            if side is None or score < 8:
                side = "LONG"
                trigger = "Breakout Retest Long"
                invalidation_level = breakout_high
            score += 2
            setup_note.append("15m Breakout Retest")

        if side == "LONG" and closes5[-1] > e20_5:
            score += 1

        if side == "LONG" and ENTRY_CONFIRMATION and entry_confirmation_ok(c5, "LONG"):
            score += 2
            setup_note.append("5m Confirmation")
        elif side == "LONG" and ENTRY_CONFIRMATION:
            return None

        if side == "LONG" and funding < 0.0008:
            score += 1
        if side == "LONG" and prev_oi > 0 and oi >= prev_oi:
            score += 1

    # Short setup selection
    if side is None and bearish_1h:
        score += 2
        if closes1[-1] < e50_1h:
            score += 1
        if 32 <= r15 <= 55:
            score += 1

        if in_pullback_short_zone and price <= e20_15 * 1.002 and price >= e20_15 * 0.994:
            score += 2
            side = "SHORT"
            trigger = "Pullback Short"
            invalidation_level = previous_swing_high(c5, 10)
            setup_note.append("15m Pullback in Value Zone")

        if swept_high:
            if side is None or score < 8:
                side = "SHORT"
                trigger = "Liquidity Sweep Short"
                invalidation_level = max(last5["h"], prev5["h"], prev2_5["h"])
            score += 3
            setup_note.append("5m Liquidity Sweep")

        if broke_out_down and retest_down:
            if side is None or score < 8:
                side = "SHORT"
                trigger = "Breakout Retest Short"
                invalidation_level = breakout_low
            score += 2
            setup_note.append("15m Breakout Retest")

        if side == "SHORT" and closes5[-1] < e20_5:
            score += 1

        if side == "SHORT" and ENTRY_CONFIRMATION and entry_confirmation_ok(c5, "SHORT"):
            score += 2
            setup_note.append("5m Confirmation")
        elif side == "SHORT" and ENTRY_CONFIRMATION:
            return None

        if side == "SHORT" and funding > -0.0008:
            score += 1
        if side == "SHORT" and prev_oi > 0 and oi >= prev_oi:
            score += 1

    if side is None:
        return None

    if USE_VOLUME_FILTER:
        current_vol = c5[-1]["v"]
        ref_vol = avg_volume(c5[:-1], 20)
        if ref_vol > 0 and current_vol >= ref_vol * VOLUME_BOOST_MIN:
            score += 1
            setup_note.append("Volume Confirmed")
        else:
            return None

    if USE_BTC_BIAS:
        trend_bias = get_trend_bias()
        if trend_bias == side:
            score += 1
        else:
            return None

    if score < BASE_MIN_SCORE:
        return None

    atr_buffer = atr15 * ATR_STOP_MULTIPLIER
    if side == "LONG":
        swing_low = previous_swing_low(c5, 12)
        if swing_low is None:
            return None
        base_stop = min(swing_low, invalidation_level if invalidation_level is not None else swing_low)
        stop = base_stop - atr_buffer
        risk = price - stop
        if risk <= 0:
            return None
        stop_pct = risk / price * 100.0
        if stop_pct > MAX_STOP_PCT:
            return None

        tp1 = price + risk * PARTIAL_TP_R
        tp2_candidate_a = range_high_15
        tp2_candidate_b = price + risk * FINAL_TP_R
        tp2 = max(tp2_candidate_a, tp2_candidate_b)
    else:
        swing_high = previous_swing_high(c5, 12)
        if swing_high is None:
            return None
        base_stop = max(swing_high, invalidation_level if invalidation_level is not None else swing_high)
        stop = base_stop + atr_buffer
        risk = stop - price
        if risk <= 0:
            return None
        stop_pct = risk / price * 100.0
        if stop_pct > MAX_STOP_PCT:
            return None

        tp1 = price - risk * PARTIAL_TP_R
        tp2_candidate_a = range_low_15
        tp2_candidate_b = price - risk * FINAL_TP_R
        tp2 = min(tp2_candidate_a, tp2_candidate_b)

    entry_low = price * (1 - ENTRY_ZONE_PCT)
    entry_high = price * (1 + ENTRY_ZONE_PCT)
    rr = abs(tp1 - price) / abs(price - stop) if abs(price - stop) > 0 else 0

    return {
        "coin": COIN,
        "side": side,
        "score": score,
        "trigger": trigger,
        "entry": price,
        "entry_zone": (entry_low, entry_high),
        "stop": stop,
        "initial_stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "funding": funding,
        "oi": oi,
        "tp1_hit": False,
        "be_moved": False,
        "partial_taken": False,
        "created_at": now_utc().isoformat(),
        "entry_e20_1h": e20_1h,
        "entry_e50_1h": e50_1h,
        "invalidation_level": invalidation_level,
        "stop_pct": stop_pct,
        "notes": setup_note,
    }


def trend_still_valid(trade):
    try:
        c1 = candles(COIN, "1h", 220)
        closes1 = [x["c"] for x in c1]
        e20_1h = ema(closes1, 20)
        e50_1h = ema(closes1, 50)
        e200_1h = ema(closes1, 200)

        if e20_1h is None or e50_1h is None or e200_1h is None:
            return True, "insufficient data"

        last_close = closes1[-1]
        side = trade["side"]
        invalidation_level = trade.get("invalidation_level", None)

        if side == "LONG":
            if not (last_close > e50_1h and e20_1h > e50_1h > e200_1h):
                return False, "1H Trend verloren"
            if TREND_BREAK_CLOSE_FILTER and last_close < e20_1h:
                return False, "1H Close unter EMA20"
            if TREND_BREAK_USE_PULLBACK_INVALIDATION and invalidation_level and last_close < invalidation_level:
                return False, "unter Invalidation geschlossen"
            return True, "Trend ok"

        if not (last_close < e50_1h and e20_1h < e50_1h < e200_1h):
            return False, "1H Trend verloren"
        if TREND_BREAK_CLOSE_FILTER and last_close > e20_1h:
            return False, "1H Close über EMA20"
        if TREND_BREAK_USE_PULLBACK_INVALIDATION and invalidation_level and last_close > invalidation_level:
            return False, "über Invalidation geschlossen"
        return True, "Trend ok"

    except Exception as e:
        log(f"TREND REVALIDATION ERROR -> {e}")
        return True, "revalidation error"


def format_signal(sig):
    ez_low, ez_high = sig["entry_zone"]
    notes = " | ".join(sig.get("notes", [])) if sig.get("notes") else "-"
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Score: {sig['score']}\n"
        f"Trigger: {sig['trigger']}\n"
        f"Zeit: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"Entry: {format_price(sig['entry'])}\n"
        f"Entry Zone: {format_price(ez_low)} - {format_price(ez_high)}\n"
        f"Stop: {format_price(sig['stop'])} ({sig.get('stop_pct', 0):.2f}%)\n"
        f"TP1: {format_price(sig['tp1'])}\n"
        f"TP2: {format_price(sig['tp2'])}\n"
        f"RR: {sig['rr']:.2f}\n"
        f"Funding: {sig['funding'] * 100:.4f}%\n"
        f"OI: {sig['oi']:.2f}\n"
        f"Setup: {notes}"
    )


def store_trade(state, sig):
    state["open"][sig["coin"]] = sig


def manage(state, price_data):
    to_delete = []

    for coin, trade in list(state["open"].items()):
        if coin != COIN:
            to_delete.append(coin)
            continue

        price = price_data.get(coin)
        if not price:
            continue

        if TREND_REVALIDATION_ENABLED:
            valid, reason = trend_still_valid(trade)
            if not valid:
                safe_discord(
                    state,
                    f"🟡 **{coin} {trade['side']} Trendbruch** | Preis: {format_price(price)}\nGrund: {reason}",
                    dedupe_key=f"trend_break_{coin}_{trade['side']}"
                )
                set_signal_lock(state, coin, trade["side"])
                to_delete.append(coin)
                continue

        side = trade["side"]
        entry = trade["entry"]
        initial_stop = trade.get("initial_stop", trade["stop"])
        tp1 = trade["tp1"]
        tp2 = trade["tp2"]

        if side == "LONG":
            one_r = max(entry - initial_stop, 1e-9)
            profit_r = (price - entry) / one_r
        else:
            one_r = max(initial_stop - entry, 1e-9)
            profit_r = (entry - price) / one_r

        if BREAK_EVEN_ENABLED and not trade.get("be_moved", False) and profit_r >= BREAK_EVEN_R:
            if side == "LONG":
                trade["stop"] = max(trade["stop"], entry)
            else:
                trade["stop"] = min(trade["stop"], entry)
            trade["be_moved"] = True

        if side == "LONG" and not trade.get("tp1_hit", False) and price >= tp1:
            trade["tp1_hit"] = True
            if PARTIAL_TP_ENABLED and not trade.get("partial_taken", False):
                trade["partial_taken"] = True
                safe_discord(
                    state,
                    f"✅ **{coin} LONG TP1 erreicht** | Preis: {format_price(price)}",
                    dedupe_key=f"tp1_{coin}_LONG"
                )

        if side == "SHORT" and not trade.get("tp1_hit", False) and price <= tp1:
            trade["tp1_hit"] = True
            if PARTIAL_TP_ENABLED and not trade.get("partial_taken", False):
                trade["partial_taken"] = True
                safe_discord(
                    state,
                    f"✅ **{coin} SHORT TP1 erreicht** | Preis: {format_price(price)}",
                    dedupe_key=f"tp1_{coin}_SHORT"
                )

        if TRAILING_ENABLED and trade.get("tp1_hit", False):
            try:
                c15 = candles(coin, "15m", 60)
                trail_closes = [x["c"] for x in c15]
                trail_ema = ema(trail_closes, TRAILING_EMA)
                if trail_ema:
                    if side == "LONG":
                        new_stop = max(trade["stop"], trail_ema * (1 - TRAILING_BUFFER_PCT))
                        if new_stop > trade["stop"]:
                            trade["stop"] = new_stop
                    else:
                        new_stop = min(trade["stop"], trail_ema * (1 + TRAILING_BUFFER_PCT))
                        if new_stop < trade["stop"]:
                            trade["stop"] = new_stop
            except Exception:
                pass

        if side == "LONG":
            if price <= trade["stop"]:
                if not trade.get("tp1_hit", False):
                    safe_discord(
                        state,
                        f"❌ **{coin} LONG SL getroffen** | Preis: {format_price(price)}",
                        dedupe_key=f"sl_{coin}_LONG"
                    )
                    set_direction_cooldown(state, "LONG")
                    set_coin_cooldown(state, coin)
                    set_signal_lock(state, coin, "LONG")
                    bump_daily_sl(state)
                to_delete.append(coin)
                continue

            if price >= tp2:
                safe_discord(
                    state,
                    f"🎯 **{coin} LONG TP2 erreicht** | Preis: {format_price(price)}",
                    dedupe_key=f"tp2_{coin}_LONG"
                )
                set_signal_lock(state, coin, "LONG")
                to_delete.append(coin)
                continue

        if side == "SHORT":
            if price >= trade["stop"]:
                if not trade.get("tp1_hit", False):
                    safe_discord(
                        state,
                        f"❌ **{coin} SHORT SL getroffen** | Preis: {format_price(price)}",
                        dedupe_key=f"sl_{coin}_SHORT"
                    )
                    set_direction_cooldown(state, "SHORT")
                    set_coin_cooldown(state, coin)
                    set_signal_lock(state, coin, "SHORT")
                    bump_daily_sl(state)
                to_delete.append(coin)
                continue

            if price <= tp2:
                safe_discord(
                    state,
                    f"🎯 **{coin} SHORT TP2 erreicht** | Preis: {format_price(price)}",
                    dedupe_key=f"tp2_{coin}_SHORT"
                )
                set_signal_lock(state, coin, "SHORT")
                to_delete.append(coin)
                continue

    for coin in to_delete:
        state["open"].pop(coin, None)


def send_status_report(state, trend_bias):
    if not SEND_STATUS_REPORT:
        return

    now_ts = time.time()
    last = state.get("last_status_time", 0)
    if now_ts - last < STATUS_INTERVAL_MINUTES * 60:
        return

    refresh_daily_state(state)

    msg = (
        f"📊 **BTC CLEAN RECOVERY STATUS**\n"
        f"Trend Bias: {trend_bias}\n"
        f"Open Trades: {len(state['open'])}\n"
        f"Daily SL Count: {state['daily_stats']['sl_count']}/{MAX_DAILY_SL}\n"
        f"Daily Pause: {is_daily_pause_active(state)}\n"
        f"Short Cooldown: {is_direction_cooldown_active(state, 'SHORT')}\n"
        f"Long Cooldown: {is_direction_cooldown_active(state, 'LONG')}\n"
        f"Coin Cooldown: {is_coin_cooldown_active(state, COIN)}"
    )
    safe_discord(state, msg, dedupe_key=f"status_{int(now_ts // 60)}")
    state["last_status_time"] = now_ts


def passes_filters(sig, state):
    side = sig["side"]
    coin = sig["coin"]

    if is_daily_pause_active(state):
        return False, "daily pause aktiv"

    if coin in state["open"]:
        return False, "coin bereits offen"

    if is_coin_cooldown_active(state, coin):
        return False, f"{coin} cooldown aktiv"

    if is_signal_locked(state, coin, side):
        return False, f"{coin} {side} signal lock aktiv"

    if is_direction_cooldown_active(state, side):
        return False, f"{side} cooldown aktiv"

    if len(state["open"]) >= MAX_OPEN_TRADES:
        return False, "max offene Trades erreicht"

    if not min_signal_gap_ok(state, side):
        return False, f"min signal gap {side} aktiv"

    return True, ""


def main():
    print("BOT STARTING...", flush=True)

    if not DISCORD_WEBHOOK_URL:
        print("NO WEBHOOK", flush=True)
        return

    time.sleep(15)

    state = load_state()
    refresh_daily_state(state)

    for coin in list(state["open"].keys()):
        if coin != COIN:
            state["open"].pop(coin, None)

    if SEND_STARTUP_MESSAGE:
        safe_discord(state, "✅ BTC CLEAN RECOVERY BOT AKTIV", dedupe_key="startup")

    save_state(state)

    while True:
        try:
            state = load_state()
            refresh_daily_state(state)

            for coin in list(state["open"].keys()):
                if coin != COIN:
                    state["open"].pop(coin, None)

            market_ctx = fetch_contexts()
            prices = {COIN: market_ctx.get(COIN, {}).get("markPx", 0)}

            manage(state, prices)

            trend_bias = get_trend_bias()
            prev_ctx = state.get("market_ctx_prev", {})

            send_status_report(state, trend_bias)

            sig = None
            if COIN not in state["open"]:
                try:
                    sig = analyze_btc(market_ctx, prev_ctx)
                except Exception as e:
                    log(f"{COIN} ANALYZE ERROR -> {e}")

            if sig:
                ok, reason = passes_filters(sig, state)
                if ok:
                    key = f"{sig['coin']}_{sig['side']}"
                    if cooldown_ok(state, key):
                        safe_discord(state, format_signal(sig), dedupe_key=f"sig_{sig['coin']}_{sig['side']}")
                        store_trade(state, sig)
                        mark_alert(state, key)
                        mark_signal_time(state, sig["side"])
                        set_signal_lock(state, sig["coin"], sig["side"])
                    else:
                        log(f"{COIN} BLOCKED -> alert cooldown")
                else:
                    log(f"{COIN} BLOCKED -> {reason}")

            state["market_ctx_prev"] = {
                COIN: {"openInterest": market_ctx.get(COIN, {}).get("openInterest", 0)}
            }

            save_state(state)

        except Exception as e:
            log(f"ERR {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
