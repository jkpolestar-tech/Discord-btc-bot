import os
import time
import json
from datetime import datetime, timezone, timedelta

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COINS = [c.strip().upper() for c in os.getenv(
    "COINS",
    "BTC,ETH,SOL,XRP,DOGE,ADA,LINK,AVAX,SUI,BNB"
).split(",") if c.strip()]

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

# ===== Modes / risk =====
SETUP_MODE = os.getenv("SETUP_MODE", "SAFE").upper()
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "2"))
MAX_TRADES_PER_DIRECTION = int(os.getenv("MAX_TRADES_PER_DIRECTION", "1"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))

if SETUP_MODE == "AGGRESSIVE":
    MIN_SCORE = 5
elif SETUP_MODE == "NORMAL":
    MIN_SCORE = 6
else:
    MIN_SCORE = 7

# ===== Entry / filters =====
ENTRY_CONFIRMATION = os.getenv("ENTRY_CONFIRMATION", "true").lower() == "true"
REQUIRE_BTC_ALIGNMENT_FOR_ALTS = os.getenv("REQUIRE_BTC_ALIGNMENT_FOR_ALTS", "true").lower() == "true"
NEUTRAL_BIAS_BLOCK_WEAK_TRADES = os.getenv("NEUTRAL_BIAS_BLOCK_WEAK_TRADES", "true").lower() == "true"
FLIP_BLOCK_ENABLED = os.getenv("FLIP_BLOCK_ENABLED", "true").lower() == "true"

USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "true").lower() == "true"
USE_OI_FILTER = os.getenv("USE_OI_FILTER", "true").lower() == "true"
USE_FUNDING_FILTER = os.getenv("USE_FUNDING_FILTER", "true").lower() == "true"

VOLUME_BOOST_MIN = float(os.getenv("VOLUME_BOOST_MIN", "1.20"))
MAX_LONG_FUNDING = float(os.getenv("MAX_LONG_FUNDING", "0.0005"))
MIN_SHORT_FUNDING = float(os.getenv("MIN_SHORT_FUNDING", "-0.0005"))

# ===== Smart-money / structure =====
LOOKBACK_BREAK = int(os.getenv("LOOKBACK_BREAK", "10"))
SWEEP_LOOKBACK = int(os.getenv("SWEEP_LOOKBACK", "18"))
SWING_LOOKBACK = int(os.getenv("SWING_LOOKBACK", "5"))
FAKE_BREAKOUT_FILTER = os.getenv("FAKE_BREAKOUT_FILTER", "true").lower() == "true"
SMART_MONEY_FILTER = os.getenv("SMART_MONEY_FILTER", "true").lower() == "true"
REGIME_FILTER_ENABLED = os.getenv("REGIME_FILTER_ENABLED", "true").lower() == "true"

# ===== Regime =====
REGIME_RANGE_ATR_PCT_MAX = float(os.getenv("REGIME_RANGE_ATR_PCT_MAX", "0.35"))
REGIME_RANGE_SPREAD_PCT_MAX = float(os.getenv("REGIME_RANGE_SPREAD_PCT_MAX", "1.20"))
REGIME_CHAOS_ATR_PCT_MIN = float(os.getenv("REGIME_CHAOS_ATR_PCT_MIN", "1.10"))
REGIME_CHAOS_1H_MOVE_PCT_MIN = float(os.getenv("REGIME_CHAOS_1H_MOVE_PCT_MIN", "2.20"))

# ===== Trade management =====
ENTRY_ZONE_PCT = float(os.getenv("ENTRY_ZONE_PCT", "0.0015"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.02"))

BREAK_EVEN_ENABLED = os.getenv("BREAK_EVEN_ENABLED", "true").lower() == "true"
BREAK_EVEN_R = float(os.getenv("BREAK_EVEN_R", "1.0"))

PARTIAL_TP_ENABLED = os.getenv("PARTIAL_TP_ENABLED", "true").lower() == "true"
PARTIAL_TP_R = float(os.getenv("PARTIAL_TP_R", "1.5"))
FINAL_TP_R = float(os.getenv("FINAL_TP_R", "3.0"))

TRAILING_ENABLED = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
TRAILING_EMA = int(os.getenv("TRAILING_EMA", "20"))
TRAILING_BUFFER_PCT = float(os.getenv("TRAILING_BUFFER_PCT", "0.0025"))

# ===== Direction cooldown =====
SHORT_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("SHORT_COOLDOWN_AFTER_SL_HOURS", "4"))
LONG_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("LONG_COOLDOWN_AFTER_SL_HOURS", "4"))

# ===== Status =====
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_STATUS_REPORT = os.getenv("SEND_STATUS_REPORT", "true").lower() == "true"
STATUS_INTERVAL_MINUTES = int(os.getenv("STATUS_INTERVAL_MINUTES", "180"))
DISCORD_DEDUPE_SECONDS = int(os.getenv("DISCORD_DEDUPE_SECONDS", "90"))

MAJORS = {"BTC", "ETH", "SOL"}


def now_utc():
    return datetime.now(timezone.utc)


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
    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


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
    r = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=20)
    r.raise_for_status()


def safe_discord(state, message: str, dedupe_key: str | None = None):
    try:
        if dedupe_key and not dedupe_ok(state, dedupe_key):
            return
        post_discord(message)
        save_state(state)
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
    hours = SHORT_COOLDOWN_AFTER_SL_HOURS if side == "SHORT" else LONG_COOLDOWN_AFTER_SL_HOURS
    until = now_utc() + timedelta(hours=hours)
    state["direction_cooldown_until"][side] = until.isoformat()


def candles(coin, tf="15m", limit=120):
    now_ms = int(time.time() * 1000)
    interval_ms_map = {
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

    r = requests.post(INFO_URL, json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()

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
    r = requests.post(INFO_URL, json={"type": "metaAndAssetCtxs"}, timeout=20)
    r.raise_for_status()
    data = r.json()

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


def entry_confirmation_ok(c15, side):
    if len(c15) < 3:
        return False

    prev = c15[-2]
    last = c15[-1]

    if side == "LONG":
        return last["c"] > last["o"] and prev["c"] > prev["o"] and candle_body_strength(last) >= 0.35
    return last["c"] < last["o"] and prev["c"] < prev["o"] and candle_body_strength(last) >= 0.35


def detect_swings(candles_list, left_right=5):
    swing_highs = []
    swing_lows = []

    for i in range(left_right, len(candles_list) - left_right):
        h = candles_list[i]["h"]
        l = candles_list[i]["l"]

        is_high = all(h >= candles_list[j]["h"] for j in range(i - left_right, i + left_right + 1) if j != i)
        is_low = all(l <= candles_list[j]["l"] for j in range(i - left_right, i + left_right + 1) if j != i)

        if is_high:
            swing_highs.append((i, h))
        if is_low:
            swing_lows.append((i, l))

    return swing_highs, swing_lows


def get_structure_bias(candles_list):
    highs, lows = detect_swings(candles_list, SWING_LOOKBACK)
    highs = highs[-3:]
    lows = lows[-3:]

    if len(highs) < 2 or len(lows) < 2:
        return "NEUTRAL"

    hh = highs[-1][1] > highs[-2][1]
    hl = lows[-1][1] > lows[-2][1]
    lh = highs[-1][1] < highs[-2][1]
    ll = lows[-1][1] < lows[-2][1]

    if hh and hl:
        return "BULLISH"
    if lh and ll:
        return "BEARISH"
    return "NEUTRAL"


def detect_bos(candles_list, atr_value):
    highs, lows = detect_swings(candles_list[-40:], SWING_LOOKBACK)
    if not highs or not lows or atr_value is None:
        return "NEUTRAL"

    last_close = candles_list[-1]["c"]
    prev_close = candles_list[-2]["c"]

    recent_high = highs[-1][1]
    recent_low = lows[-1][1]
    buffer = atr_value * 0.10

    if prev_close <= recent_high and last_close > recent_high + buffer:
        return "BULLISH_BOS"
    if prev_close >= recent_low and last_close < recent_low - buffer:
        return "BEARISH_BOS"
    return "NEUTRAL"


def detect_liquidity_sweep(candles_list):
    if len(candles_list) < SWEEP_LOOKBACK + 3:
        return (False, False)

    recent = candles_list[-(SWEEP_LOOKBACK + 2):-2]
    prev = candles_list[-2]
    last = candles_list[-1]

    local_high = max(x["h"] for x in recent)
    local_low = min(x["l"] for x in recent)

    bullish_sweep = (
        prev["l"] < local_low and
        prev["c"] > local_low and
        lower_wick_ratio(prev) >= 0.35 and
        last["c"] > prev["c"]
    )

    bearish_sweep = (
        prev["h"] > local_high and
        prev["c"] < local_high and
        upper_wick_ratio(prev) >= 0.35 and
        last["c"] < prev["c"]
    )

    return bullish_sweep, bearish_sweep


def fake_breakout_filter_ok(side, c15, breakout_level):
    if not FAKE_BREAKOUT_FILTER:
        return True

    last = c15[-1]
    prev = c15[-2]

    if side == "LONG":
        # kein Long, wenn direkt unter Level zurückgefallen
        if last["c"] <= breakout_level:
            return False
        if upper_wick_ratio(last) > 0.45 and last["c"] < last["h"] * 0.999:
            return False
        if prev["c"] > breakout_level and last["c"] < breakout_level:
            return False
    else:
        if last["c"] >= breakout_level:
            return False
        if lower_wick_ratio(last) > 0.45 and last["c"] > last["l"] * 1.001:
            return False
        if prev["c"] < breakout_level and last["c"] > breakout_level:
            return False

    return True


def get_market_bias():
    c1 = candles("BTC", "1h", 150)
    c4 = candles("BTC", "4h", 100)

    close1 = [x["c"] for x in c1]
    close4 = [x["c"] for x in c4]

    e20_1 = ema(close1, 20)
    e50_1 = ema(close1, 50)
    e20_4 = ema(close4, 20)

    if not e20_1 or not e50_1 or not e20_4:
        return "NEUTRAL"

    if close1[-1] > e20_1 > e50_1 and close4[-1] > e20_4:
        return "LONG"
    if close1[-1] < e20_1 < e50_1 and close4[-1] < e20_4:
        return "SHORT"
    return "NEUTRAL"


def get_flip_state():
    c15 = candles("BTC", "15m", 120)
    c1 = candles("BTC", "1h", 120)

    close15 = [x["c"] for x in c15]
    close1 = [x["c"] for x in c1]

    e20_15 = ema(close15, 20)
    e50_15 = ema(close15, 50)
    e20_1 = ema(close1, 20)

    if not e20_15 or not e50_15 or not e20_1:
        return "NEUTRAL"

    price = close15[-1]
    high_break = max(close15[-12:-1])
    low_break = min(close15[-12:-1])

    bullish_flip = price > e20_15 and e20_15 > e50_15 and close1[-1] > e20_1 and price > high_break
    bearish_flip = price < e20_15 and e20_15 < e50_15 and close1[-1] < e20_1 and price < low_break

    if bullish_flip:
        return "LONG"
    if bearish_flip:
        return "SHORT"
    return "NEUTRAL"


def get_market_regime():
    c15 = candles("BTC", "15m", 80)
    c1 = candles("BTC", "1h", 40)

    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]
    price = closes15[-1]

    atr15 = atr(c15, 14)
    if atr15 is None:
        return "RANGE"

    atr_pct = atr15 / price * 100 if price else 0
    spread_pct = pct_change(max(closes15[-24:]), min(closes15[-24:]))
    one_hour_move_pct = abs(pct_change(closes1[-1], closes1[-2]))

    if atr_pct >= REGIME_CHAOS_ATR_PCT_MIN or one_hour_move_pct >= REGIME_CHAOS_1H_MOVE_PCT_MIN:
        return "CHAOS"

    if atr_pct <= REGIME_RANGE_ATR_PCT_MAX and spread_pct <= REGIME_RANGE_SPREAD_PCT_MAX:
        return "RANGE"

    return "TREND"


def count_open_exposure(open_trades):
    total_longs = 0
    total_shorts = 0

    for _, trade in open_trades.items():
        if trade["side"] == "LONG":
            total_longs += 1
        if trade["side"] == "SHORT":
            total_shorts += 1

    return total_longs, total_shorts


def oi_is_supportive(prev_oi, oi):
    return prev_oi > 0 and oi > prev_oi


def analyze(coin, market_ctx, prev_ctx, market_bias, flip_state, regime):
    c15 = candles(coin, "15m", 160)
    c1 = candles(coin, "1h", 120)
    c4 = candles(coin, "4h", 80)

    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]
    closes4 = [x["c"] for x in c4]

    e20 = ema(closes15, 20)
    e50 = ema(closes15, 50)
    e20_1h = ema(closes1, 20)
    e20_4h = ema(closes4, 20)
    r = rsi(closes15)
    atr15 = atr(c15, 14)

    if not e20 or not e50 or not e20_1h or not e20_4h or r is None or atr15 is None:
        return None

    price = closes15[-1]
    prev_close = closes15[-2]
    high_break = max(closes15[-LOOKBACK_BREAK:-1])
    low_break = min(closes15[-LOOKBACK_BREAK:-1])

    ctx = market_ctx.get(coin, {})
    funding = _to_float(ctx.get("funding", 0))
    oi = _to_float(ctx.get("openInterest", 0))
    prev_oi = _to_float(prev_ctx.get(coin, {}).get("openInterest", 0))

    structure = get_structure_bias(c15[-80:])
    bos = detect_bos(c15[-80:], atr15)
    bullish_sweep, bearish_sweep = detect_liquidity_sweep(c15[-60:])

    score = 0
    side = None
    trigger = None

    bullish_trend = price > e20 > e50 and closes1[-1] > e20_1h and closes4[-1] > e20_4h
    bearish_trend = price < e20 < e50 and closes1[-1] < e20_1h and closes4[-1] < e20_4h

    breakout_long = prev_close <= high_break and price > high_break
    breakdown_short = prev_close >= low_break and price < low_break

    if bullish_trend:
        side = "LONG"
        score += 3
    elif bearish_trend:
        side = "SHORT"
        score += 3
    else:
        # in range nur sweep/reclaim handeln
        if regime == "RANGE":
            if bullish_sweep:
                side = "LONG"
                score += 3
            elif bearish_sweep:
                side = "SHORT"
                score += 3
            else:
                return None
        else:
            return None

    if SMART_MONEY_FILTER:
        if side == "LONG":
            if structure == "BULLISH":
                score += 1
            if bos == "BULLISH_BOS":
                score += 1
            if bullish_sweep:
                score += 2
                trigger = "Liquidity Sweep + Reclaim"
            elif breakout_long:
                score += 2
                trigger = "Breakout + Hold"
        else:
            if structure == "BEARISH":
                score += 1
            if bos == "BEARISH_BOS":
                score += 1
            if bearish_sweep:
                score += 2
                trigger = "Liquidity Sweep + Reject"
            elif breakdown_short:
                score += 2
                trigger = "Breakdown + Hold"
    else:
        if side == "LONG" and breakout_long:
            score += 2
            trigger = "Breakout + Hold"
        if side == "SHORT" and breakdown_short:
            score += 2
            trigger = "Breakdown + Hold"

    if trigger is None:
        if side == "LONG" and bullish_sweep:
            trigger = "Liquidity Sweep + Reclaim"
        elif side == "SHORT" and bearish_sweep:
            trigger = "Liquidity Sweep + Reject"
        elif side == "LONG":
            trigger = "Trend Long"
        else:
            trigger = "Trend Short"

    # fake breakout filter
    if side == "LONG":
        level_ok = fake_breakout_filter_ok("LONG", c15, high_break if breakout_long else e20)
    else:
        level_ok = fake_breakout_filter_ok("SHORT", c15, low_break if breakdown_short else e20)

    if not level_ok:
        return None

    # entry confirmation
    if ENTRY_CONFIRMATION:
        if entry_confirmation_ok(c15, side):
            score += 1
        else:
            return None

    # RSI window
    if side == "LONG" and 45 < r < 72:
        score += 1
    if side == "SHORT" and 28 < r < 55:
        score += 1

    # volume
    if USE_VOLUME_FILTER:
        current_vol = c15[-1]["v"]
        ref_vol = avg_volume(c15[:-1], 20)
        if ref_vol > 0 and current_vol >= ref_vol * VOLUME_BOOST_MIN:
            score += 1
        else:
            return None

    # funding
    if USE_FUNDING_FILTER:
        if side == "LONG" and funding <= MAX_LONG_FUNDING:
            score += 1
        elif side == "SHORT" and funding >= MIN_SHORT_FUNDING:
            score += 1
        else:
            return None

    # oi
    if USE_OI_FILTER:
        if oi_is_supportive(prev_oi, oi):
            score += 1
        else:
            return None

    # bias
    if market_bias == side:
        score += 1

    # neutral protection
    if NEUTRAL_BIAS_BLOCK_WEAK_TRADES and market_bias == "NEUTRAL":
        score -= 1

    # regime filter
    if REGIME_FILTER_ENABLED:
        if regime == "CHAOS":
            return None
        if regime == "RANGE":
            # in range lieber sweeps handeln, nicht simple breakouts
            if side == "LONG" and not bullish_sweep:
                return None
            if side == "SHORT" and not bearish_sweep:
                return None

    # flip protection
    if FLIP_BLOCK_ENABLED:
        if flip_state == "LONG" and side == "SHORT":
            return None
        if flip_state == "SHORT" and side == "LONG":
            return None

    # BTC alignment for alts
    if REQUIRE_BTC_ALIGNMENT_FOR_ALTS and coin not in MAJORS:
        if market_bias != side:
            return None

    if score < MIN_SCORE:
        return None

    if side == "LONG":
        entry_low = price * (1 - ENTRY_ZONE_PCT)
        entry_high = price * (1 + ENTRY_ZONE_PCT)
        stop = min(price * (1 - STOP_PCT), min(x["l"] for x in c15[-8:]))
        risk = price - stop
        if risk <= 0:
            return None
        tp1 = price + risk * PARTIAL_TP_R
        tp2 = price + risk * FINAL_TP_R
    else:
        entry_low = price * (1 - ENTRY_ZONE_PCT)
        entry_high = price * (1 + ENTRY_ZONE_PCT)
        stop = max(price * (1 + STOP_PCT), max(x["h"] for x in c15[-8:]))
        risk = stop - price
        if risk <= 0:
            return None
        tp1 = price - risk * PARTIAL_TP_R
        tp2 = price - risk * FINAL_TP_R

    rr = abs(tp1 - price) / abs(price - stop) if abs(price - stop) > 0 else 0

    return {
        "coin": coin,
        "side": side,
        "score": score,
        "trigger": trigger,
        "regime": regime,
        "structure": structure,
        "bos": bos,
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
    }


def format_signal(sig):
    ez_low, ez_high = sig["entry_zone"]
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Score: {sig['score']}\n"
        f"Trigger: {sig['trigger']}\n"
        f"Regime: {sig['regime']}\n"
        f"Structure: {sig['structure']}\n"
        f"BOS: {sig['bos']}\n"
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


def store_trade(state, sig):
    state["open"][sig["coin"]] = sig


def manage(state, price_data):
    to_delete = []

    for coin, trade in list(state["open"].items()):
        price = price_data.get(coin)
        if not price:
            continue

        side = trade["side"]
        entry = trade["entry"]
        stop = trade["stop"]
        initial_stop = trade.get("initial_stop", stop)
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
            safe_discord(
                state,
                f"🛡 **{coin} {side} Break-Even aktiviert**\n"
                f"Preis: {format_price(price)}\n"
                f"Neuer Stop: {format_price(trade['stop'])}",
                dedupe_key=f"be_{coin}_{side}"
            )

        if side == "LONG" and not trade.get("tp1_hit", False) and price >= tp1:
            trade["tp1_hit"] = True
            if PARTIAL_TP_ENABLED and not trade.get("partial_taken", False):
                trade["partial_taken"] = True
                safe_discord(
                    state,
                    f"✅ **{coin} LONG TP1 erreicht** | Preis: {format_price(price)}\n"
                    f"💰 Teilgewinn mitnehmen\n"
                    f"🛡 Rest auf BE/Trail laufen lassen",
                    dedupe_key=f"tp1_{coin}_LONG"
                )

        if side == "SHORT" and not trade.get("tp1_hit", False) and price <= tp1:
            trade["tp1_hit"] = True
            if PARTIAL_TP_ENABLED and not trade.get("partial_taken", False):
                trade["partial_taken"] = True
                safe_discord(
                    state,
                    f"✅ **{coin} SHORT TP1 erreicht** | Preis: {format_price(price)}\n"
                    f"💰 Teilgewinn mitnehmen\n"
                    f"🛡 Rest auf BE/Trail laufen lassen",
                    dedupe_key=f"tp1_{coin}_SHORT"
                )

        if TRAILING_ENABLED and trade.get("tp1_hit", False):
            try:
                c15 = candles(coin, "15m", 80)
                closes = [x["c"] for x in c15]
                trail_ema = ema(closes, TRAILING_EMA)
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
                if trade.get("tp1_hit", False):
                    safe_discord(
                        state,
                        f"🟡 **{coin} LONG Restposition beendet** | Preis: {format_price(price)}",
                        dedupe_key=f"rest_{coin}_LONG"
                    )
                else:
                    safe_discord(
                        state,
                        f"❌ **{coin} LONG SL getroffen** | Preis: {format_price(price)}",
                        dedupe_key=f"sl_{coin}_LONG"
                    )
                    set_direction_cooldown(state, "LONG")
                to_delete.append(coin)
                continue

            if price >= tp2:
                safe_discord(
                    state,
                    f"🎯 **{coin} LONG TP2 erreicht** | Preis: {format_price(price)}",
                    dedupe_key=f"tp2_{coin}_LONG"
                )
                to_delete.append(coin)
                continue

        if side == "SHORT":
            if price >= trade["stop"]:
                if trade.get("tp1_hit", False):
                    safe_discord(
                        state,
                        f"🟡 **{coin} SHORT Restposition beendet** | Preis: {format_price(price)}",
                        dedupe_key=f"rest_{coin}_SHORT"
                    )
                else:
                    safe_discord(
                        state,
                        f"❌ **{coin} SHORT SL getroffen** | Preis: {format_price(price)}",
                        dedupe_key=f"sl_{coin}_SHORT"
                    )
                    set_direction_cooldown(state, "SHORT")
                to_delete.append(coin)
                continue

            if price <= tp2:
                safe_discord(
                    state,
                    f"🎯 **{coin} SHORT TP2 erreicht** | Preis: {format_price(price)}",
                    dedupe_key=f"tp2_{coin}_SHORT"
                )
                to_delete.append(coin)
                continue

    for coin in to_delete:
        state["open"].pop(coin, None)


def send_status_report(state, market_bias, flip_state, regime):
    if not SEND_STATUS_REPORT:
        return

    now_ts = time.time()
    last = state.get("last_status_time", 0)
    if now_ts - last < STATUS_INTERVAL_MINUTES * 60:
        return

    msg = (
        f"📊 **BOT STATUS REPORT**\n"
        f"Bias: {market_bias}\n"
        f"Flip: {flip_state}\n"
        f"Regime: {regime}\n"
        f"Open Trades: {len(state['open'])}\n"
        f"Short Cooldown: {is_direction_cooldown_active(state, 'SHORT')}\n"
        f"Long Cooldown: {is_direction_cooldown_active(state, 'LONG')}"
    )
    safe_discord(state, msg, dedupe_key=f"status_{int(now_ts // 60)}")
    state["last_status_time"] = now_ts


def passes_filters(sig, state, market_bias):
    total_longs, total_shorts = count_open_exposure(state["open"])
    side = sig["side"]

    if is_direction_cooldown_active(state, side):
        return False, f"{side} cooldown aktiv"

    same_direction = [t for t in state["open"].values() if t["side"] == side]
    if len(same_direction) >= MAX_TRADES_PER_DIRECTION:
        return False, "zu viele Trades gleiche Richtung"

    if len(state["open"]) >= MAX_OPEN_TRADES:
        return False, "max offene Trades erreicht"

    if side == "LONG" and total_longs >= MAX_TRADES_PER_DIRECTION:
        return False, "zu viele Longs"
    if side == "SHORT" and total_shorts >= MAX_TRADES_PER_DIRECTION:
        return False, "zu viele Shorts"

    if NEUTRAL_BIAS_BLOCK_WEAK_TRADES and market_bias == "NEUTRAL" and sig["score"] < MIN_SCORE + 1:
        return False, "neutral bias block"

    return True, ""


def main():
    print("BOT STARTING...", flush=True)

    if not DISCORD_WEBHOOK_URL:
        print("NO WEBHOOK", flush=True)
        return

    state = load_state()

    if SEND_STARTUP_MESSAGE:
        safe_discord(state, "✅ V11 SMART MONEY BOT AKTIV", dedupe_key="startup")

    while True:
        try:
            state = load_state()
            market_ctx = fetch_contexts()
            prices = {coin: market_ctx.get(coin, {}).get("markPx", 0) for coin in COINS}

            manage(state, prices)

            market_bias = get_market_bias()
            flip_state = get_flip_state()
            regime = get_market_regime()
            prev_ctx = state.get("market_ctx_prev", {})

            send_status_report(state, market_bias, flip_state, regime)

            for coin in COINS:
                if coin in state["open"]:
                    continue

                sig = analyze(coin, market_ctx, prev_ctx, market_bias, flip_state, regime)
                if not sig:
                    continue

                ok, reason = passes_filters(sig, state, market_bias)
                if not ok:
                    log(f"{coin}: blocked ({reason})")
                    continue

                key = f"{sig['coin']}_{sig['side']}"
                if not cooldown_ok(state, key):
                    continue

                safe_discord(state, format_signal(sig), dedupe_key=f"sig_{sig['coin']}_{sig['side']}")
                store_trade(state, sig)
                mark_alert(state, key)

            state["market_ctx_prev"] = {
                coin: {"openInterest": market_ctx.get(coin, {}).get("openInterest", 0)}
                for coin in COINS
            }

            save_state(state)

        except Exception as e:
            print("ERR", e, flush=True)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
