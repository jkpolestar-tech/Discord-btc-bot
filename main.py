import os
import time
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COIN = os.getenv("COIN", "BTC").strip().upper()
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "600"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_RETRY_SLEEP = float(os.getenv("API_RETRY_SLEEP", "12"))
REQUEST_GAP_SECONDS = float(os.getenv("REQUEST_GAP_SECONDS", "1.35"))
RATE_LIMIT_EXTRA_SLEEP = float(os.getenv("RATE_LIMIT_EXTRA_SLEEP", "18"))

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

BASE_MIN_SCORE = int(os.getenv("BASE_MIN_SCORE", "7"))
PULLBACK_LOOKBACK = int(os.getenv("PULLBACK_LOOKBACK", "16"))
ENTRY_ZONE_PCT = float(os.getenv("ENTRY_ZONE_PCT", "0.0025"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.012"))
VOLUME_BOOST_MIN = float(os.getenv("VOLUME_BOOST_MIN", "1.10"))

BREAK_EVEN_ENABLED = os.getenv("BREAK_EVEN_ENABLED", "true").lower() == "true"
BREAK_EVEN_R = float(os.getenv("BREAK_EVEN_R", "1.0"))
PARTIAL_TP_ENABLED = os.getenv("PARTIAL_TP_ENABLED", "true").lower() == "true"
PARTIAL_TP_R = float(os.getenv("PARTIAL_TP_R", "1.4"))
FINAL_TP_R = float(os.getenv("FINAL_TP_R", "2.4"))

TRAILING_ENABLED = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
TRAILING_EMA = int(os.getenv("TRAILING_EMA", "20"))
TRAILING_BUFFER_PCT = float(os.getenv("TRAILING_BUFFER_PCT", "0.0025"))

CHOP_FILTER_ENABLED = os.getenv("CHOP_FILTER_ENABLED", "true").lower() == "true"
MIN_1H_EMA_SPREAD_PCT = float(os.getenv("MIN_1H_EMA_SPREAD_PCT", "0.20"))
MAX_15M_ATR_PCT = float(os.getenv("MAX_15M_ATR_PCT", "1.80"))

TREND_REVALIDATION_ENABLED = os.getenv("TREND_REVALIDATION_ENABLED", "true").lower() == "true"
TREND_BREAK_CLOSE_FILTER = os.getenv("TREND_BREAK_CLOSE_FILTER", "true").lower() == "true"

ENABLE_PULLBACK_SETUPS = os.getenv("ENABLE_PULLBACK_SETUPS", "true").lower() == "true"
ENABLE_SWEEP_SETUPS = os.getenv("ENABLE_SWEEP_SETUPS", "true").lower() == "true"
ENABLE_BREAKOUT_RETEST = os.getenv("ENABLE_BREAKOUT_RETEST", "true").lower() == "true"

MAX_OPEN_TRADES = 1

LAST_API_CALL_TS = 0.0


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_date_str() -> str:
    return now_utc().strftime("%Y-%m-%d")


def log(msg: str) -> None:
    print(f"[{now_utc().isoformat()}] {msg}", flush=True)


def _to_float(x: Any, default: float = 0.0) -> float:
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


def sleep_for_rate_limit() -> None:
    global LAST_API_CALL_TS
    elapsed = time.time() - LAST_API_CALL_TS
    if elapsed < REQUEST_GAP_SECONDS:
        time.sleep(REQUEST_GAP_SECONDS - elapsed)


def post_info(payload: Dict[str, Any]) -> Any:
    global LAST_API_CALL_TS

    last_error: Optional[Exception] = None

    for attempt in range(API_RETRIES):
        try:
            sleep_for_rate_limit()
            r = requests.post(INFO_URL, json=payload, timeout=REQUEST_TIMEOUT)
            LAST_API_CALL_TS = time.time()

            if r.status_code == 429:
                wait_s = RATE_LIMIT_EXTRA_SLEEP + (attempt * 3)
                log(f"API 429 Too Many Requests -> sleeping {wait_s:.1f}s")
                time.sleep(wait_s)
                continue

            r.raise_for_status()
            return r.json()

        except Exception as e:
            last_error = e
            log(f"API request failed ({attempt + 1}/{API_RETRIES}): {e}")
            if attempt < API_RETRIES - 1:
                time.sleep(API_RETRY_SLEEP + attempt * 2)

    raise last_error if last_error else RuntimeError("Unknown API error")


def candles(coin: str, tf: str = "15m", limit: int = 120) -> List[Dict[str, float]]:
    now_ms = int(time.time() * 1000)
    interval_ms_map = {
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
    }
    if tf not in interval_ms_map:
        raise ValueError(f"Unsupported timeframe: {tf}")

    interval_ms = interval_ms_map[tf]

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": tf,
            "startTime": now_ms - (limit + 12) * interval_ms,
            "endTime": now_ms,
        },
    }

    data = post_info(payload)
    out: List[Dict[str, float]] = []

    for x in data[-limit:]:
        out.append({
            "o": float(x["o"]),
            "h": float(x["h"]),
            "l": float(x["l"]),
            "c": float(x["c"]),
            "v": float(x.get("v", 0)),
        })
    return out


def fetch_contexts() -> Dict[str, Dict[str, float]]:
    data = post_info({"type": "metaAndAssetCtxs"})
    meta = data[0]
    ctxs = data[1]
    universe = meta.get("universe", [])

    result: Dict[str, Dict[str, float]] = {}
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


def ema(arr: List[float], n: int) -> Optional[float]:
    if len(arr) < n:
        return None
    k = 2 / (n + 1)
    e = sum(arr[:n]) / n
    for v in arr[n:]:
        e = v * k + e * (1 - k)
    return e


def rsi(arr: List[float], n: int = 14) -> Optional[float]:
    if len(arr) < n + 1:
        return None

    gains: List[float] = []
    losses: List[float] = []

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


def atr(candles_list: List[Dict[str, float]], period: int = 14) -> Optional[float]:
    if len(candles_list) < period + 1:
        return None

    trs: List[float] = []
    for i in range(1, len(candles_list)):
        h = candles_list[i]["h"]
        l = candles_list[i]["l"]
        pc = candles_list[i - 1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


def avg_volume(candle_list: List[Dict[str, float]], n: int = 20) -> float:
    vals = [x["v"] for x in candle_list[-n:]]
    return sum(vals) / len(vals) if vals else 0.0


def candle_body_strength(c: Dict[str, float]) -> float:
    rng = max(c["h"] - c["l"], 1e-9)
    return abs(c["c"] - c["o"]) / rng


def upper_wick_ratio(c: Dict[str, float]) -> float:
    rng = max(c["h"] - c["l"], 1e-9)
    wick = c["h"] - max(c["o"], c["c"])
    return wick / rng


def lower_wick_ratio(c: Dict[str, float]) -> float:
    rng = max(c["h"] - c["l"], 1e-9)
    wick = min(c["o"], c["c"]) - c["l"]
    return wick / rng


def highest_high(candle_list: List[Dict[str, float]], lookback: int) -> float:
    return max(x["h"] for x in candle_list[-lookback:])


def lowest_low(candle_list: List[Dict[str, float]], lookback: int) -> float:
    return min(x["l"] for x in candle_list[-lookback:])


def load_state() -> Dict[str, Any]:
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
    state.setdefault("daily_stats", {"date": utc_date_str(), "sl_count": 0, "pause_until": ""})
    return state


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def refresh_daily_state(state: Dict[str, Any]) -> None:
    today = utc_date_str()
    ds = state.setdefault("daily_stats", {"date": today, "sl_count": 0, "pause_until": ""})
    if ds.get("date") != today:
        state["daily_stats"] = {"date": today, "sl_count": 0, "pause_until": ""}


def is_daily_pause_active(state: Dict[str, Any]) -> bool:
    refresh_daily_state(state)
    raw = state["daily_stats"].get("pause_until", "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def bump_daily_sl(state: Dict[str, Any]) -> None:
    refresh_daily_state(state)
    state["daily_stats"]["sl_count"] += 1
    if ENABLE_DAILY_PAUSE and state["daily_stats"]["sl_count"] >= MAX_DAILY_SL:
        tomorrow = datetime.combine(
            (now_utc() + timedelta(days=1)).date(),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        state["daily_stats"]["pause_until"] = tomorrow.isoformat()


def dedupe_ok(state: Dict[str, Any], key: str) -> bool:
    last = state["discord_dedupe"].get(key, 0)
    now_ts = time.time()
    if now_ts - last < DISCORD_DEDUPE_SECONDS:
        return False
    state["discord_dedupe"][key] = now_ts
    return True


def post_discord(message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt")
    r = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()


def safe_discord(state: Dict[str, Any], message: str, dedupe_key: Optional[str] = None) -> None:
    try:
        if dedupe_key and not dedupe_ok(state, dedupe_key):
            return
        post_discord(message)
    except Exception as e:
        log(f"Discord send failed: {e}")


def cooldown_ok(state: Dict[str, Any], key: str) -> bool:
    last = state["last_alerts"].get(key, 0)
    return (time.time() - last) > ALERT_COOLDOWN_MINUTES * 60


def mark_alert(state: Dict[str, Any], key: str) -> None:
    state["last_alerts"][key] = time.time()


def is_direction_cooldown_active(state: Dict[str, Any], side: str) -> bool:
    raw = state.get("direction_cooldown_until", {}).get(side, "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def set_direction_cooldown(state: Dict[str, Any], side: str) -> None:
    hours = LONG_COOLDOWN_AFTER_SL_HOURS if side == "LONG" else SHORT_COOLDOWN_AFTER_SL_HOURS
    until = now_utc() + timedelta(hours=hours)
    state["direction_cooldown_until"][side] = until.isoformat()


def is_coin_cooldown_active(state: Dict[str, Any], coin: str) -> bool:
    raw = state.get("coin_cooldown_until", {}).get(coin, "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def set_coin_cooldown(state: Dict[str, Any], coin: str) -> None:
    until = now_utc() + timedelta(hours=COIN_COOLDOWN_AFTER_SL_HOURS)
    state["coin_cooldown_until"][coin] = until.isoformat()


def signal_lock_key(coin: str, side: str) -> str:
    return f"{coin}_{side}"


def is_signal_locked(state: Dict[str, Any], coin: str, side: str) -> bool:
    raw = state.get("signal_lock_until", {}).get(signal_lock_key(coin, side), "")
    if not raw:
        return False
    try:
        return now_utc() < datetime.fromisoformat(raw)
    except Exception:
        return False


def set_signal_lock(state: Dict[str, Any], coin: str, side: str) -> None:
    until = now_utc() + timedelta(hours=SIGNAL_LOCK_HOURS)
    state["signal_lock_until"][signal_lock_key(coin, side)] = until.isoformat()


def get_trend_bias() -> str:
    c1 = candles(COIN, "1h", 240)
    closes = [x["c"] for x in c1]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    if e20 is None or e50 is None or e200 is None:
        return "NEUTRAL"

    price = closes[-1]
    if price > e20 > e50 > e200:
        return "LONG"
    if price < e20 < e50 < e200:
        return "SHORT"
    return "NEUTRAL"


def entry_confirmation_ok(c5: List[Dict[str, float]], side: str) -> bool:
    if len(c5) < 3:
        return False

    last = c5[-1]
    prev = c5[-2]

    if side == "LONG":
        return (
            last["c"] > last["o"]
            and last["c"] > prev["h"]
            and candle_body_strength(last) >= 0.40
            and lower_wick_ratio(last) <= 0.45
        )

    return (
        last["c"] < last["o"]
        and last["c"] < prev["l"]
        and candle_body_strength(last) >= 0.40
        and upper_wick_ratio(last) <= 0.45
    )


def chop_filter_ok(price: float, e20_1h: float, e50_1h: float, atr15: float) -> bool:
    if not CHOP_FILTER_ENABLED:
        return True

    if price <= 0 or e20_1h is None or e50_1h is None or atr15 is None:
        return False

    ema_spread_pct = abs(e20_1h - e50_1h) / price * 100.0
    atr_pct = atr15 / price * 100.0

    if ema_spread_pct < MIN_1H_EMA_SPREAD_PCT:
        return False

    if atr_pct > MAX_15M_ATR_PCT:
        return False

    return True


def detect_pullback_setup(
    side: str,
    c15: List[Dict[str, float]],
    price: float,
    e20_15: float,
    e50_15: float,
) -> Tuple[bool, Optional[float]]:
    recent = c15[-PULLBACK_LOOKBACK:]
    recent_lows = [x["l"] for x in recent]
    recent_highs = [x["h"] for x in recent]
    last = c15[-1]
    prev = c15[-2]

    if side == "LONG":
        ok = (
            min(recent_lows) <= e20_15 * 1.003
            and price > e20_15 > e50_15
            and last["c"] > prev["c"]
            and last["c"] > e20_15
        )
        return ok, min(recent_lows) if ok else None

    ok = (
        max(recent_highs) >= e20_15 * 0.997
        and price < e20_15 < e50_15
        and last["c"] < prev["c"]
        and last["c"] < e20_15
    )
    return ok, max(recent_highs) if ok else None


def detect_sweep_setup(
    side: str,
    c15: List[Dict[str, float]],
    c5: List[Dict[str, float]],
) -> Tuple[bool, Optional[float]]:
    if len(c15) < 8 or len(c5) < 8:
        return False, None

    recent15 = c15[-8:-1]
    recent_low = min(x["l"] for x in recent15)
    recent_high = max(x["h"] for x in recent15)
    last5 = c5[-1]
    prev5 = c5[-2]

    if side == "LONG":
        swept = prev5["l"] < recent_low and prev5["c"] > recent_low
        confirmed = last5["c"] > prev5["h"]
        return (swept and confirmed), (prev5["l"] if swept and confirmed else None)

    swept = prev5["h"] > recent_high and prev5["c"] < recent_high
    confirmed = last5["c"] < prev5["l"]
    return (swept and confirmed), (prev5["h"] if swept and confirmed else None)


def detect_breakout_retest_setup(
    side: str,
    c15: List[Dict[str, float]],
) -> Tuple[bool, Optional[float]]:
    if len(c15) < 12:
        return False, None

    prior = c15[-12:-3]
    breakout_candle = c15[-3]
    retest_candle = c15[-2]
    confirm_candle = c15[-1]

    prior_high = max(x["h"] for x in prior)
    prior_low = min(x["l"] for x in prior)

    if side == "LONG":
        broke = breakout_candle["c"] > prior_high
        retested = retest_candle["l"] <= prior_high and retest_candle["c"] >= prior_high
        confirmed = confirm_candle["c"] > retest_candle["h"]
        return (broke and retested and confirmed), (prior_high if broke and retested and confirmed else None)

    broke = breakout_candle["c"] < prior_low
    retested = retest_candle["h"] >= prior_low and retest_candle["c"] <= prior_low
    confirmed = confirm_candle["c"] < retest_candle["l"]
    return (broke and retested and confirmed), (prior_low if broke and retested and confirmed else None)


def build_stop_and_targets(
    side: str,
    price: float,
    atr15: float,
    c5: List[Dict[str, float]],
    c15: List[Dict[str, float]],
    invalidation_level: float,
) -> Optional[Tuple[float, float, float, float]]:
    atr_buffer = atr15 * 0.35
    price_cap = price * MAX_STOP_PCT

    if side == "LONG":
        recent_5m_low = min(x["l"] for x in c5[-8:])
        stop_raw = min(recent_5m_low, invalidation_level) - atr_buffer
        risk = price - stop_raw
        if risk <= 0 or risk > price_cap:
            return None
        tp1 = price + risk * PARTIAL_TP_R
        tp2 = price + risk * FINAL_TP_R
        rr = abs(tp1 - price) / max(abs(price - stop_raw), 1e-9)
        return stop_raw, tp1, tp2, rr

    recent_5m_high = max(x["h"] for x in c5[-8:])
    stop_raw = max(recent_5m_high, invalidation_level) + atr_buffer
    risk = stop_raw - price
    if risk <= 0 or risk > price_cap:
        return None
    tp1 = price - risk * PARTIAL_TP_R
    tp2 = price - risk * FINAL_TP_R
    rr = abs(tp1 - price) / max(abs(price - stop_raw), 1e-9)
    return stop_raw, tp1, tp2, rr


def analyze_btc(market_ctx: Dict[str, Dict[str, float]], prev_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    c5 = candles(COIN, "5m", 140)
    c15 = candles(COIN, "15m", 180)
    c1 = candles(COIN, "1h", 240)

    closes5 = [x["c"] for x in c5]
    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]

    e20_15 = ema(closes15, 20)
    e50_15 = ema(closes15, 50)
    e20_1h = ema(closes1, 20)
    e50_1h = ema(closes1, 50)
    e200_1h = ema(closes1, 200)
    r = rsi(closes15)
    atr15 = atr(c15, 14)

    if None in (e20_15, e50_15, e20_1h, e50_1h, e200_1h, r, atr15):
        return None

    price = closes15[-1]
    if not chop_filter_ok(price, e20_1h, e50_1h, atr15):
        return None

    bullish_1h = closes1[-1] > e20_1h > e50_1h > e200_1h
    bearish_1h = closes1[-1] < e20_1h < e50_1h < e200_1h
    bullish_15m = price > e20_15 > e50_15
    bearish_15m = price < e20_15 < e50_15

    ctx = market_ctx.get(COIN, {})
    funding = _to_float(ctx.get("funding", 0))
    oi = _to_float(ctx.get("openInterest", 0))
    prev_oi = _to_float(prev_ctx.get(COIN, {}).get("openInterest", 0))

    candidate_setups: List[Dict[str, Any]] = []

    for side in ("LONG", "SHORT"):
        if side == "LONG" and not (bullish_1h and bullish_15m):
            continue
        if side == "SHORT" and not (bearish_1h and bearish_15m):
            continue

        if ENABLE_PULLBACK_SETUPS:
            ok, invalidation = detect_pullback_setup(side, c15, price, e20_15, e50_15)
            if ok and invalidation is not None:
                candidate_setups.append({
                    "side": side,
                    "trigger": "Pullback Long" if side == "LONG" else "Pullback Short",
                    "setup_type": "pullback",
                    "base_score": 3,
                    "invalidation_level": invalidation,
                })

        if ENABLE_SWEEP_SETUPS:
            ok, invalidation = detect_sweep_setup(side, c15, c5)
            if ok and invalidation is not None:
                candidate_setups.append({
                    "side": side,
                    "trigger": "Liquidity Sweep Long" if side == "LONG" else "Liquidity Sweep Short",
                    "setup_type": "sweep",
                    "base_score": 4,
                    "invalidation_level": invalidation,
                })

        if ENABLE_BREAKOUT_RETEST:
            ok, invalidation = detect_breakout_retest_setup(side, c15)
            if ok and invalidation is not None:
                candidate_setups.append({
                    "side": side,
                    "trigger": "Breakout Retest Long" if side == "LONG" else "Breakout Retest Short",
                    "setup_type": "breakout_retest",
                    "base_score": 4,
                    "invalidation_level": invalidation,
                })

    if not candidate_setups:
        return None

    best_sig: Optional[Dict[str, Any]] = None
    best_score = -1

    for setup in candidate_setups:
        side = setup["side"]
        score = setup["base_score"]

        if side == "LONG":
            score += 2 if bullish_1h else 0
            score += 1 if 45 < r < 68 else 0
            if funding <= 0.0008:
                score += 1
        else:
            score += 2 if bearish_1h else 0
            score += 1 if 32 < r < 58 else 0
            if funding >= -0.0008:
                score += 1

        if USE_VOLUME_FILTER:
            current_vol = c15[-1]["v"]
            ref_vol = avg_volume(c15[:-1], 20)
            if ref_vol > 0 and current_vol >= ref_vol * VOLUME_BOOST_MIN:
                score += 1
            else:
                continue

        if prev_oi > 0 and oi >= prev_oi:
            score += 1

        if ENTRY_CONFIRMATION and not entry_confirmation_ok(c5, side):
            continue

        levels = build_stop_and_targets(
            side=side,
            price=price,
            atr15=atr15,
            c5=c5,
            c15=c15,
            invalidation_level=setup["invalidation_level"],
        )
        if levels is None:
            continue

        stop, tp1, tp2, rr = levels

        if score < BASE_MIN_SCORE:
            continue

        entry_low = price * (1 - ENTRY_ZONE_PCT)
        entry_high = price * (1 + ENTRY_ZONE_PCT)

        sig = {
            "coin": COIN,
            "side": side,
            "score": score,
            "trigger": setup["trigger"],
            "setup_type": setup["setup_type"],
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
            "entry_e200_1h": e200_1h,
            "invalidation_level": setup["invalidation_level"],
        }

        if score > best_score:
            best_score = score
            best_sig = sig

    return best_sig


def trend_still_valid(trade: Dict[str, Any]) -> Tuple[bool, str]:
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

        if side == "LONG":
            if e20_1h <= e50_1h or e50_1h <= e200_1h:
                return False, "1H Trend verloren"
            if TREND_BREAK_CLOSE_FILTER and last_close < e20_1h:
                return False, "1H Close unter EMA20"
            return True, "Trend ok"

        if e20_1h >= e50_1h or e50_1h >= e200_1h:
            return False, "1H Trend verloren"
        if TREND_BREAK_CLOSE_FILTER and last_close > e20_1h:
            return False, "1H Close über EMA20"
        return True, "Trend ok"

    except Exception as e:
        log(f"TREND REVALIDATION ERROR -> {e}")
        return True, "revalidation error"


def format_signal(sig: Dict[str, Any]) -> str:
    ez_low, ez_high = sig["entry_zone"]
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Score: {sig['score']}\n"
        f"Trigger: {sig['trigger']}\n"
        f"Typ: {sig['setup_type']}\n"
        f"Zeit: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"Entry: {format_price(sig['entry'])}\n"
        f"Entry Zone: {format_price(ez_low)} - {format_price(ez_high)}\n"
        f"Stop: {format_price(sig['stop'])}\n"
        f"TP1: {format_price(sig['tp1'])}\n"
        f"TP2: {format_price(sig['tp2'])}\n"
        f"RR: {sig['rr']:.2f}\n"
        f"Funding: {sig['funding'] * 100:.4f}%\n"
        f"OI: {sig['oi']:.2f}"
    )


def store_trade(state: Dict[str, Any], sig: Dict[str, Any]) -> None:
    state["open"][sig["coin"]] = sig


def manage(state: Dict[str, Any], price_data: Dict[str, float]) -> None:
    to_delete: List[str] = []

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
            except Exception as e:
                log(f"Trailing update failed: {e}")

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


def send_status_report(state: Dict[str, Any], trend_bias: str) -> None:
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


def passes_filters(sig: Dict[str, Any], state: Dict[str, Any]) -> Tuple[bool, str]:
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

    return True, ""


def main() -> None:
    print("BOT STARTING...", flush=True)

    if not DISCORD_WEBHOOK_URL:
        print("NO WEBHOOK", flush=True)
        return

    time.sleep(8)

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

            sig: Optional[Dict[str, Any]] = None
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
