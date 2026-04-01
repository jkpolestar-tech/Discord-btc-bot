import os
import time
import json
from datetime import datetime, timezone, timedelta

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COINS = [c.strip().upper() for c in os.getenv(
    "COINS",
    "BTC,ETH,SOL"
).split(",") if c.strip()]

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
API_RETRIES = int(os.getenv("API_RETRIES", "2"))
API_RETRY_SLEEP = float(os.getenv("API_RETRY_SLEEP", "10"))

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_STATUS_REPORT = os.getenv("SEND_STATUS_REPORT", "false").lower() == "true"
STATUS_INTERVAL_MINUTES = int(os.getenv("STATUS_INTERVAL_MINUTES", "180"))
DISCORD_DEDUPE_SECONDS = int(os.getenv("DISCORD_DEDUPE_SECONDS", "120"))

MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "2"))
MAX_TRADES_PER_DIRECTION = int(os.getenv("MAX_TRADES_PER_DIRECTION", "1"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "120"))

USE_BTC_BIAS = os.getenv("USE_BTC_BIAS", "true").lower() == "true"
REQUIRE_BTC_ALIGNMENT_FOR_ALTS = os.getenv("REQUIRE_BTC_ALIGNMENT_FOR_ALTS", "true").lower() == "true"

USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "true").lower() == "true"
VOLUME_BOOST_MIN = float(os.getenv("VOLUME_BOOST_MIN", "1.10"))

ENTRY_CONFIRMATION = os.getenv("ENTRY_CONFIRMATION", "true").lower() == "true"
ENTRY_ZONE_PCT = float(os.getenv("ENTRY_ZONE_PCT", "0.003"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.025"))

PULLBACK_LOOKBACK = int(os.getenv("PULLBACK_LOOKBACK", "14"))
BASE_MIN_SCORE = int(os.getenv("BASE_MIN_SCORE", "5"))

BREAK_EVEN_ENABLED = os.getenv("BREAK_EVEN_ENABLED", "true").lower() == "true"
BREAK_EVEN_R = float(os.getenv("BREAK_EVEN_R", "1.0"))

PARTIAL_TP_ENABLED = os.getenv("PARTIAL_TP_ENABLED", "true").lower() == "true"
PARTIAL_TP_R = float(os.getenv("PARTIAL_TP_R", "2.0"))
FINAL_TP_R = float(os.getenv("FINAL_TP_R", "4.0"))

TRAILING_ENABLED = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
TRAILING_EMA = int(os.getenv("TRAILING_EMA", "20"))
TRAILING_BUFFER_PCT = float(os.getenv("TRAILING_BUFFER_PCT", "0.003"))

LONG_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("LONG_COOLDOWN_AFTER_SL_HOURS", "4"))
SHORT_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("SHORT_COOLDOWN_AFTER_SL_HOURS", "4"))
COIN_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("COIN_COOLDOWN_AFTER_SL_HOURS", "8"))
SIGNAL_LOCK_HOURS = int(os.getenv("SIGNAL_LOCK_HOURS", "6"))

MAX_DAILY_SL = int(os.getenv("MAX_DAILY_SL", "3"))
ENABLE_DAILY_PAUSE = os.getenv("ENABLE_DAILY_PAUSE", "true").lower() == "true"

ENABLE_COIN_OF_DAY = os.getenv("ENABLE_COIN_OF_DAY", "false").lower() == "true"
COIN_OF_DAY_HOUR_UTC = int(os.getenv("COIN_OF_DAY_HOUR_UTC", "7"))
COIN_OF_DAY_MIN_SCORE = int(os.getenv("COIN_OF_DAY_MIN_SCORE", "6"))
COIN_OF_DAY_PRIORITY_BOOST = int(os.getenv("COIN_OF_DAY_PRIORITY_BOOST", "1"))

CHOP_FILTER_ENABLED = os.getenv("CHOP_FILTER_ENABLED", "true").lower() == "true"
MIN_1H_EMA_SPREAD_PCT = float(os.getenv("MIN_1H_EMA_SPREAD_PCT", "0.20"))
MAX_15M_ATR_PCT_FOR_PULLBACK = float(os.getenv("MAX_15M_ATR_PCT_FOR_PULLBACK", "2.50"))

MAJORS = {"BTC", "ETH", "SOL"}


def now_utc():
    return datetime.now(timezone.utc)


def log(msg: str):
    print(f"[{now_utc().isoformat()}] {msg}", flush=True)


def utc_date_str():
    return now_utc().strftime("%Y-%m-%d")


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
    state.setdefault("coin_of_day", {"date": "", "coin": "", "score": 0, "reason": "", "side": ""})
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


def get_btc_bias():
    c1 = candles("BTC", "1h", 120)
    closes = [x["c"] for x in c1]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    if not e20 or not e50:
        return "NEUTRAL"

    price = closes[-1]
    if price > e20 > e50:
        return "LONG"
    if price < e20 < e50:
        return "SHORT"
    return "NEUTRAL"


def entry_confirmation_ok(c15, side):
    if len(c15) < 2:
        return False
    last = c15[-1]
    prev = c15[-2]

    if side == "LONG":
        return (
            last["c"] > last["o"]
            and lower_wick_ratio(last) < 0.50
            and (last["c"] > prev["c"] or candle_body_strength(last) >= 0.35)
        )

    return (
        last["c"] < last["o"]
        and upper_wick_ratio(last) < 0.50
        and (last["c"] < prev["c"] or candle_body_strength(last) >= 0.35)
    )


def count_open_exposure(open_trades):
    total_longs = 0
    total_shorts = 0

    for trade in open_trades.values():
        if trade["side"] == "LONG":
            total_longs += 1
        elif trade["side"] == "SHORT":
            total_shorts += 1

    return total_longs, total_shorts


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


def analyze(coin, market_ctx, prev_ctx, btc_bias, state):
    c15 = candles(coin, "15m", 140)
    time.sleep(0.7)
    c1 = candles(coin, "1h", 120)

    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]

    e20_15 = ema(closes15, 20)
    e50_15 = ema(closes15, 50)
    e20_1h = ema(closes1, 20)
    e50_1h = ema(closes1, 50)
    r = rsi(closes15)
    atr15 = atr(c15, 14)

    if None in (e20_15, e50_15, e20_1h, e50_1h, r, atr15):
        return None

    price = closes15[-1]
    ctx = market_ctx.get(coin, {})
    funding = _to_float(ctx.get("funding", 0))
    oi = _to_float(ctx.get("openInterest", 0))
    prev_oi = _to_float(prev_ctx.get(coin, {}).get("openInterest", 0))

    if not chop_filter_ok(price, e20_1h, e50_1h, atr15):
        return None

    bullish_1h = closes1[-1] > e20_1h > e50_1h
    bearish_1h = closes1[-1] < e20_1h < e50_1h
    bullish_15m = price > e20_15 > e50_15
    bearish_15m = price < e20_15 < e50_15

    recent_lows = [x["l"] for x in c15[-PULLBACK_LOOKBACK:]]
    recent_highs = [x["h"] for x in c15[-PULLBACK_LOOKBACK:]]

    last = c15[-1]
    prev = c15[-2]

    pullback_long = (
        bullish_1h
        and bullish_15m
        and min(recent_lows) <= e20_15 * 1.003
        and last["c"] > e20_15
        and last["c"] > prev["c"]
    )

    pullback_short = (
        bearish_1h
        and bearish_15m
        and max(recent_highs) >= e20_15 * 0.997
        and last["c"] < e20_15
        and last["c"] < prev["c"]
    )

    side = None
    trigger = None
    score = 0

    if pullback_long:
        side = "LONG"
        trigger = "Pullback Long"
        score += 3
    elif pullback_short:
        side = "SHORT"
        trigger = "Pullback Short"
        score += 3
    else:
        return None

    if side == "LONG" and bullish_1h:
        score += 1
    if side == "SHORT" and bearish_1h:
        score += 1

    if side == "LONG" and 42 < r < 72:
        score += 1
    if side == "SHORT" and 28 < r < 58:
        score += 1

    if USE_VOLUME_FILTER:
        current_vol = c15[-1]["v"]
        ref_vol = avg_volume(c15[:-1], 20)
        if ref_vol > 0 and current_vol >= ref_vol * VOLUME_BOOST_MIN:
            score += 1
        else:
            return None

    if prev_oi > 0 and oi > prev_oi:
        score += 1

    if USE_BTC_BIAS and btc_bias == side:
        score += 1

    if REQUIRE_BTC_ALIGNMENT_FOR_ALTS and coin not in MAJORS:
        if btc_bias != side:
            return None

    if ENTRY_CONFIRMATION and not entry_confirmation_ok(c15, side):
        return None

    cod = state.get("coin_of_day", {})
    if cod.get("date") == utc_date_str() and cod.get("coin") == coin:
        score += COIN_OF_DAY_PRIORITY_BOOST

    if score < BASE_MIN_SCORE:
        return None

    if side == "LONG":
        stop_ref = min(x["l"] for x in c15[-10:])
        stop = min(price * (1 - STOP_PCT), stop_ref)
        risk = price - stop
        if risk <= 0:
            return None
        tp1 = price + risk * PARTIAL_TP_R
        tp2 = price + risk * FINAL_TP_R
    else:
        stop_ref = max(x["h"] for x in c15[-10:])
        stop = max(price * (1 + STOP_PCT), stop_ref)
        risk = stop - price
        if risk <= 0:
            return None
        tp1 = price - risk * PARTIAL_TP_R
        tp2 = price - risk * FINAL_TP_R

    entry_low = price * (1 - ENTRY_ZONE_PCT)
    entry_high = price * (1 + ENTRY_ZONE_PCT)
    rr = abs(tp1 - price) / abs(price - stop) if abs(price - stop) > 0 else 0

    return {
        "coin": coin,
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
    }


def analyze_coin_of_day(coin, market_ctx, prev_ctx, btc_bias):
    c15 = candles(coin, "15m", 100)
    time.sleep(0.6)
    c1 = candles(coin, "1h", 70)

    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]

    e20_15 = ema(closes15, 20)
    e50_15 = ema(closes15, 50)
    e20_1h = ema(closes1, 20)
    e50_1h = ema(closes1, 50)
    if None in (e20_15, e50_15, e20_1h, e50_1h):
        return None

    price = closes15[-1]
    current_vol = c15[-1]["v"]
    avg_vol15 = avg_volume(c15[:-1], 20)
    vol_ratio = current_vol / avg_vol15 if avg_vol15 > 0 else 0

    ctx = market_ctx.get(coin, {})
    oi = _to_float(ctx.get("openInterest", 0))
    prev_oi = _to_float(prev_ctx.get(coin, {}).get("openInterest", 0))

    bullish = price > e20_15 > e50_15 and closes1[-1] > e20_1h > e50_1h
    bearish = price < e20_15 < e50_15 and closes1[-1] < e20_1h < e50_1h

    if not bullish and not bearish:
        return None

    side = "LONG" if bullish else "SHORT"
    score = 2
    reasons = ["MTF Trend"]

    if vol_ratio >= 1.2:
        score += 2
        reasons.append(f"Volume x{vol_ratio:.2f}")

    if prev_oi > 0 and oi > prev_oi:
        score += 1
        reasons.append("OI steigt")

    if btc_bias == side:
        score += 1
        reasons.append("BTC Alignment")

    return {
        "coin": coin,
        "score": score,
        "side": side,
        "price": price,
        "reasons": reasons[:5],
    }


def maybe_post_coin_of_day(state, market_ctx, prev_ctx, btc_bias):
    if not ENABLE_COIN_OF_DAY:
        return

    today = utc_date_str()

    if state["coin_of_day"].get("date") == today:
        return
    if now_utc().hour < COIN_OF_DAY_HOUR_UTC:
        return

    best = None
    for coin in COINS:
        try:
            item = analyze_coin_of_day(coin, market_ctx, prev_ctx, btc_bias)
            time.sleep(0.6)
        except Exception:
            item = None
        if not item:
            continue
        if best is None or item["score"] > best["score"]:
            best = item

    if not best or best["score"] < COIN_OF_DAY_MIN_SCORE:
        state["coin_of_day"] = {"date": today, "coin": "", "score": 0, "reason": "Kein klarer Kandidat", "side": ""}
        return

    reason_text = "\n".join([f"• {r}" for r in best["reasons"]])

    msg = (
        f"🚀 **COIN OF THE DAY**\n"
        f"Coin: {best['coin']}\n"
        f"Richtung: {best['side']}\n"
        f"Score: {best['score']}\n"
        f"Preis: {format_price(best['price'])}\n\n"
        f"Gründe:\n{reason_text}"
    )

    safe_discord(state, msg, dedupe_key=f"cod_{today}")
    state["coin_of_day"] = {
        "date": today,
        "coin": best["coin"],
        "score": best["score"],
        "reason": ", ".join(best["reasons"]),
        "side": best["side"],
    }


def format_signal(sig):
    ez_low, ez_high = sig["entry_zone"]
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Score: {sig['score']}\n"
        f"Trigger: {sig['trigger']}\n"
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
        if coin not in COINS:
            to_delete.append(coin)
            continue

        price = price_data.get(coin)
        if not price:
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
                c15 = candles(coin, "15m", 50)
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


def send_status_report(state, btc_bias):
    if not SEND_STATUS_REPORT:
        return

    now_ts = time.time()
    last = state.get("last_status_time", 0)
    if now_ts - last < STATUS_INTERVAL_MINUTES * 60:
        return

    refresh_daily_state(state)
    cod = state.get("coin_of_day", {})
    cod_text = cod.get("coin", "-") if cod.get("date") == utc_date_str() else "-"
    blocked = [c for c in COINS if is_coin_cooldown_active(state, c)]

    msg = (
        f"📊 **BOT STATUS REPORT**\n"
        f"BTC Bias: {btc_bias}\n"
        f"Coin of the Day: {cod_text}\n"
        f"Open Trades: {len(state['open'])}\n"
        f"Daily SL Count: {state['daily_stats']['sl_count']}/{MAX_DAILY_SL}\n"
        f"Daily Pause: {is_daily_pause_active(state)}\n"
        f"Short Cooldown: {is_direction_cooldown_active(state, 'SHORT')}\n"
        f"Long Cooldown: {is_direction_cooldown_active(state, 'LONG')}\n"
        f"Coin Cooldowns: {', '.join(blocked) if blocked else '-'}"
    )
    safe_discord(state, msg, dedupe_key=f"status_{int(now_ts // 60)}")
    state["last_status_time"] = now_ts


def passes_filters(sig, state):
    total_longs, total_shorts = count_open_exposure(state["open"])
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

    same_direction = [t for t in state["open"].values() if t["side"] == side]
    if len(same_direction) >= MAX_TRADES_PER_DIRECTION:
        return False, "zu viele Trades gleiche Richtung"

    if side == "LONG" and total_longs >= MAX_TRADES_PER_DIRECTION:
        return False, "zu viele Longs"
    if side == "SHORT" and total_shorts >= MAX_TRADES_PER_DIRECTION:
        return False, "zu viele Shorts"

    return True, ""


def main():
    print("BOT STARTING...", flush=True)

    if not DISCORD_WEBHOOK_URL:
        print("NO WEBHOOK", flush=True)
        return

    time.sleep(15)

    state = load_state()
    refresh_daily_state(state)

    if SEND_STARTUP_MESSAGE:
        safe_discord(state, "✅ RECOVERY BOT AKTIV | Coin Cooldown + Signal Lock aktiv", dedupe_key="startup")
    save_state(state)

    while True:
        try:
            state = load_state()
            refresh_daily_state(state)

            market_ctx = fetch_contexts()
            prices = {coin: market_ctx.get(coin, {}).get("markPx", 0) for coin in COINS}

            manage(state, prices)

            btc_bias = get_btc_bias()
            prev_ctx = state.get("market_ctx_prev", {})

            maybe_post_coin_of_day(state, market_ctx, prev_ctx, btc_bias)
            send_status_report(state, btc_bias)

            candidates = []

            for coin in COINS:
                if coin in state["open"]:
                    continue

                time.sleep(1.0)

                try:
                    sig = analyze(coin, market_ctx, prev_ctx, btc_bias, state)
                except Exception as e:
                    log(f"{coin} ANALYZE ERROR -> {e}")
                    continue

                if not sig:
                    continue

                ok, reason = passes_filters(sig, state)
                if not ok:
                    log(f"{coin} BLOCKED -> {reason}")
                    continue

                candidates.append(sig)

            if candidates:
                cod_coin = state.get("coin_of_day", {}).get("coin", "")
                candidates.sort(
                    key=lambda x: (x["coin"] == cod_coin, x["score"], x["rr"]),
                    reverse=True
                )

            for sig in candidates:
                if len(state["open"]) >= MAX_OPEN_TRADES:
                    break

                key = f"{sig['coin']}_{sig['side']}"
                if not cooldown_ok(state, key):
                    continue

                ok, reason = passes_filters(sig, state)
                if not ok:
                    log(f"{sig['coin']} FINAL BLOCK -> {reason}")
                    continue

                safe_discord(state, format_signal(sig), dedupe_key=f"sig_{sig['coin']}_{sig['side']}")
                store_trade(state, sig)
                mark_alert(state, key)
                set_signal_lock(state, sig["coin"], sig["side"])

            state["market_ctx_prev"] = {
                coin: {"openInterest": market_ctx.get(coin, {}).get("openInterest", 0)}
                for coin in COINS
            }

            save_state(state)

        except Exception as e:
            log(f"ERR {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
