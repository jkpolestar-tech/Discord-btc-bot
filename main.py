import os
import time
import json
from datetime import datetime, timezone, timedelta

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COINS = [c.strip() for c in os.getenv(
    "COINS",
    "BTC,SOL,ETH,XRP,DOGE,ADA,LINK,AVAX,SUI,BNB"
).split(",") if c.strip()]

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
STATE_FILE = "state.json"

# ===== Setup mode =====
SETUP_MODE = os.getenv("SETUP_MODE", "NORMAL").upper()

if SETUP_MODE == "SAFE":
    BASE_MIN_SIGNAL_SCORE_A = 8
    BASE_MIN_SIGNAL_SCORE_B = 6
    BASE_RISK_REWARD_MIN_A = 2.0
    BASE_RISK_REWARD_MIN_B = 1.6
elif SETUP_MODE == "AGGRESSIVE":
    BASE_MIN_SIGNAL_SCORE_A = 6
    BASE_MIN_SIGNAL_SCORE_B = 4
    BASE_RISK_REWARD_MIN_A = 1.4
    BASE_RISK_REWARD_MIN_B = 1.2
else:
    BASE_MIN_SIGNAL_SCORE_A = 7
    BASE_MIN_SIGNAL_SCORE_B = 5
    BASE_RISK_REWARD_MIN_A = 1.8
    BASE_RISK_REWARD_MIN_B = 1.4

# ===== Risk controls =====
MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "6"))
MAX_A_SIGNALS_PER_SCAN = int(os.getenv("MAX_A_SIGNALS_PER_SCAN", "1"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))

MAX_TOTAL_SHORTS = int(os.getenv("MAX_TOTAL_SHORTS", "2"))
MAX_TOTAL_LONGS = int(os.getenv("MAX_TOTAL_LONGS", "2"))
MAX_MAJOR_SHORTS = int(os.getenv("MAX_MAJOR_SHORTS", "1"))
MAX_MAJOR_LONGS = int(os.getenv("MAX_MAJOR_LONGS", "1"))
LOSS_STREAK_PAUSE_HOURS = int(os.getenv("LOSS_STREAK_PAUSE_HOURS", "6"))
MAX_LOSS_STREAK = int(os.getenv("MAX_LOSS_STREAK", "2"))

# ===== Funding / OI =====
MAX_LONG_FUNDING = float(os.getenv("MAX_LONG_FUNDING", "0.00035"))
MIN_SHORT_FUNDING = float(os.getenv("MIN_SHORT_FUNDING", "-0.00005"))
OI_RISE_THRESHOLD = float(os.getenv("OI_RISE_THRESHOLD", "0.0015"))
ALT_SHORT_FUNDING_FLOOR = float(os.getenv("ALT_SHORT_FUNDING_FLOOR", "-0.0020"))
ALT_LONG_FUNDING_CEIL = float(os.getenv("ALT_LONG_FUNDING_CEIL", "0.0020"))

# ===== Regime detection =====
REGIME_RANGE_ATR_PCT_MAX = float(os.getenv("REGIME_RANGE_ATR_PCT_MAX", "0.35"))
REGIME_RANGE_SPREAD_PCT_MAX = float(os.getenv("REGIME_RANGE_SPREAD_PCT_MAX", "1.20"))
REGIME_CHAOS_ATR_PCT_MIN = float(os.getenv("REGIME_CHAOS_ATR_PCT_MIN", "1.10"))
REGIME_CHAOS_1H_MOVE_PCT_MIN = float(os.getenv("REGIME_CHAOS_1H_MOVE_PCT_MIN", "2.20"))

# ===== News block =====
NEWS_BLOCK_ENABLED = os.getenv("NEWS_BLOCK_ENABLED", "true").lower() == "true"
HIGH_RISK_UTC_HOURS = {18, 19, 20, 21}

# ===== Status / debug =====
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_HEARTBEAT = os.getenv("SEND_HEARTBEAT", "true").lower() == "true"
HEARTBEAT_HOUR_UTC = int(os.getenv("HEARTBEAT_HOUR_UTC", "6"))
SEND_STATUS_REPORT = os.getenv("SEND_STATUS_REPORT", "true").lower() == "true"
STATUS_INTERVAL_MINUTES = int(os.getenv("STATUS_INTERVAL_MINUTES", "180"))
DEBUG_TO_DISCORD = os.getenv("DEBUG_TO_DISCORD", "false").lower() == "true"

MAJORS = {"BTC", "SOL"}


def log(msg: str):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        state = {}

    state.setdefault("last_alerts", {})
    state.setdefault("daily_count", {"date": "", "count": 0})
    state.setdefault("last_heartbeat_date", "")
    state.setdefault("last_status_time", 0)
    state.setdefault("market_ctx", {})
    state.setdefault("open_trades", {})
    state.setdefault("loss_streak", 0)
    state.setdefault("paused_until", "")
    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def _sanitize_error(e: Exception) -> str:
    txt = str(e)
    if "discord.com/api/webhooks/" in txt:
        return "Discord webhook request failed"
    return txt[:300]


def post_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt in Railway Variables")
    r = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=20)
    r.raise_for_status()


def safe_discord(message: str):
    try:
        post_discord(message)
    except Exception as e:
        log(f"Discord send failed: {_sanitize_error(e)}")


def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def fetch_candles(coin: str, interval: str, limit: int):
    now_ms = int(time.time() * 1000)
    interval_ms = {
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
    }[interval]

    start_ms = now_ms - (limit + 10) * interval_ms
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": start_ms,
            "endTime": now_ms,
        },
    }

    r = requests.post(INFO_URL, json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()

    candles = []
    for c in data[-limit:]:
        candles.append({
            "t": c.get("t") or c.get("T"),
            "o": float(c["o"]),
            "h": float(c["h"]),
            "l": float(c["l"]),
            "c": float(c["c"]),
            "v": float(c.get("v", 0)),
        })
    return candles


def fetch_asset_contexts():
    payload = {"type": "metaAndAssetCtxs"}
    r = requests.post(INFO_URL, json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()

    meta = data[0]
    ctxs = data[1]
    universe = meta.get("universe", [])

    result = {}
    for idx, asset in enumerate(universe):
        name = asset.get("name") or asset.get("coin")
        if not name or idx >= len(ctxs):
            continue
        ctx = ctxs[idx] or {}
        result[name] = {
            "markPx": _to_float(ctx.get("markPx", ctx.get("midPx", ctx.get("oraclePx", 0)))),
            "funding": _to_float(ctx.get("funding", ctx.get("currentFunding", 0))),
            "openInterest": _to_float(ctx.get("openInterest", ctx.get("oi", 0))),
        }
    return result


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains, losses = [], []
    for i in range(1, period + 1):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period + 1, len(values)):
        diff = values[i] - values[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["h"]
        l = candles[i]["l"]
        pc = candles[i - 1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def highest_high(candles, lookback):
    return max(c["h"] for c in candles[-lookback:])


def lowest_low(candles, lookback):
    return min(c["l"] for c in candles[-lookback:])


def pct_change(a, b):
    if b == 0:
        return 0
    return (a - b) / b * 100


def format_price(x):
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 100:
        return f"{x:.3f}"
    return f"{x:.4f}"


def candle_body_strength(candle):
    rng = max(candle["h"] - candle["l"], 1e-9)
    body = abs(candle["c"] - candle["o"])
    return body / rng


def upper_wick_ratio(candle):
    rng = max(candle["h"] - candle["l"], 1e-9)
    wick = candle["h"] - max(candle["o"], candle["c"])
    return wick / rng


def lower_wick_ratio(candle):
    rng = max(candle["h"] - candle["l"], 1e-9)
    wick = min(candle["o"], candle["c"]) - candle["l"]
    return wick / rng


def in_news_block():
    if not NEWS_BLOCK_ENABLED:
        return False
    return datetime.now(timezone.utc).hour in HIGH_RISK_UTC_HOURS


def maybe_send_heartbeat(state):
    if not SEND_HEARTBEAT:
        return
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour == HEARTBEAT_HOUR_UTC and state.get("last_heartbeat_date") != today:
        safe_discord("💓 Bot läuft. Regime + Risk Control aktiv.")
        state["last_heartbeat_date"] = today
        save_state(state)


def send_status_report(state, market_bias, market_regime):
    if not SEND_STATUS_REPORT:
        return

    now = time.time()
    last = state.get("last_status_time", 0)
    if now - last < STATUS_INTERVAL_MINUTES * 60:
        return

    open_trades = state.get("open_trades", {})
    msg = "📊 **BOT STATUS REPORT**\n"
    msg += f"🧭 Bias: {market_bias.upper()}\n"
    msg += f"🌡 Regime: {market_regime.upper()}\n"
    msg += f"📂 Open Trades: {len(open_trades)}\n"
    msg += f"📉 Loss Streak: {state.get('loss_streak', 0)}\n"

    paused_until = state.get("paused_until", "")
    if paused_until:
        msg += f"⏸ Pause bis: {paused_until}\n"

    if open_trades:
        msg += "\n"
        for coin, t in open_trades.items():
            msg += (
                f"• {coin} {t['side']} | Entry: {format_price(t['entry'])} | "
                f"TP1: {format_price(t['tp1'])} | Stop: {format_price(t['stop'])}\n"
            )

    safe_discord(msg.strip())
    state["last_status_time"] = now
    save_state(state)


def oi_is_rising(prev_oi, current_oi):
    if prev_oi is None or prev_oi <= 0 or current_oi <= 0:
        return False
    return current_oi >= prev_oi * (1 + OI_RISE_THRESHOLD)


def make_entry_zone(entry, atr_value):
    pad = atr_value * 0.25
    return (entry - pad, entry + pad)


def risk_pct_for_grade(score):
    if score >= 9:
        return 0.75
    if score >= 8:
        return 0.50
    if score >= 7:
        return 0.35
    return 0.25


def get_market_bias():
    candles_1h = fetch_candles("BTC", "1h", 150)
    candles_4h = fetch_candles("BTC", "4h", 100)

    closes_1h = [c["c"] for c in candles_1h]
    closes_4h = [c["c"] for c in candles_4h]

    ema20_1h = ema(closes_1h, 20)
    ema50_1h = ema(closes_1h, 50)
    ema20_4h = ema(closes_4h, 20)

    if ema20_1h is None or ema50_1h is None or ema20_4h is None:
        return "neutral"

    bull = closes_1h[-1] > ema20_1h and ema20_1h > ema50_1h and closes_4h[-1] > ema20_4h
    bear = closes_1h[-1] < ema20_1h and ema20_1h < ema50_1h and closes_4h[-1] < ema20_4h

    if bull:
        return "long"
    if bear:
        return "short"
    return "neutral"


def get_market_regime():
    candles_15m = fetch_candles("BTC", "15m", 80)
    candles_1h = fetch_candles("BTC", "1h", 40)

    closes_15m = [c["c"] for c in candles_15m]
    closes_1h = [c["c"] for c in candles_1h]
    last_15m = candles_15m[-1]["c"]
    prev_1h_close = candles_1h[-2]["c"]

    atr_15m = atr(candles_15m, 14)
    if atr_15m is None:
        return "range"

    atr_pct = atr_15m / last_15m * 100 if last_15m else 0
    spread_pct = pct_change(max(closes_15m[-24:]), min(closes_15m[-24:]))
    one_hour_move_pct = abs(pct_change(closes_1h[-1], prev_1h_close))

    ema20_15m = ema(closes_15m, 20)
    ema50_1h = ema(closes_1h, 20)

    if atr_pct >= REGIME_CHAOS_ATR_PCT_MIN or one_hour_move_pct >= REGIME_CHAOS_1H_MOVE_PCT_MIN:
        return "chaos"

    if atr_pct <= REGIME_RANGE_ATR_PCT_MAX and spread_pct <= REGIME_RANGE_SPREAD_PCT_MAX:
        return "range"

    if ema20_15m and ema50_1h:
        if abs(last_15m - ema20_15m) / last_15m < 0.02:
            return "trend"

    return "range"


def get_adjusted_thresholds(market_regime):
    a = BASE_MIN_SIGNAL_SCORE_A
    b = BASE_MIN_SIGNAL_SCORE_B
    rr_a = BASE_RISK_REWARD_MIN_A
    rr_b = BASE_RISK_REWARD_MIN_B

    if market_regime == "range":
        a += 1
        b += 1
        rr_a += 0.2
        rr_b += 0.2
    elif market_regime == "chaos":
        a = 999
        b = 999
        rr_a = 99
        rr_b = 99

    return a, b, rr_a, rr_b


def get_btc_structure_ok_for_alts(market_bias):
    candles_15m = fetch_candles("BTC", "15m", 60)
    closes = [c["c"] for c in candles_15m]
    e20 = ema(closes, 20)
    if e20 is None:
        return False
    px = closes[-1]
    return (market_bias == "short" and px < e20) or (market_bias == "long" and px > e20)


def count_open_exposure(open_trades):
    total_shorts = total_longs = major_shorts = major_longs = 0

    for coin, t in open_trades.items():
        if t["side"] == "SHORT":
            total_shorts += 1
            if coin in MAJORS:
                major_shorts += 1
        if t["side"] == "LONG":
            total_longs += 1
            if coin in MAJORS:
                major_longs += 1

    return total_shorts, total_longs, major_shorts, major_longs


def is_paused(state):
    paused_until = state.get("paused_until", "")
    if not paused_until:
        return False
    try:
        dt = datetime.fromisoformat(paused_until)
        return datetime.now(timezone.utc) < dt
    except Exception:
        return False


def register_loss(state):
    state["loss_streak"] = state.get("loss_streak", 0) + 1
    if state["loss_streak"] >= MAX_LOSS_STREAK:
        pause_until = datetime.now(timezone.utc) + timedelta(hours=LOSS_STREAK_PAUSE_HOURS)
        state["paused_until"] = pause_until.isoformat()
        safe_discord(
            f"⏸ **Bot Pause aktiviert** nach {state['loss_streak']} SLs in Folge "
            f"bis {pause_until.strftime('%Y-%m-%d %H:%M UTC')}"
        )
    save_state(state)


def register_win(state):
    state["loss_streak"] = 0
    state["paused_until"] = ""
    save_state(state)


def analyze_coin(coin: str, market_ctx: dict, state: dict, market_bias: str, market_regime: str):
    min_signal_score_a, min_signal_score_b, risk_reward_min_a, risk_reward_min_b = get_adjusted_thresholds(market_regime)

    candles_15m = fetch_candles(coin, "15m", 150)
    candles_1h = fetch_candles(coin, "1h", 150)
    candles_4h = fetch_candles(coin, "4h", 100)

    closes_15m = [c["c"] for c in candles_15m]
    closes_1h = [c["c"] for c in candles_1h]
    closes_4h = [c["c"] for c in candles_4h]

    last_15m = candles_15m[-1]
    prev_15m = candles_15m[-2]
    prev2_15m = candles_15m[-3]

    ema20_15m = ema(closes_15m, 20)
    ema20_1h = ema(closes_1h, 20)
    ema50_1h = ema(closes_1h, 50)
    ema20_4h = ema(closes_4h, 20)
    rsi_15m = rsi(closes_15m, 14)
    atr_15m = atr(candles_15m, 14)

    if None in [ema20_15m, ema20_1h, ema50_1h, ema20_4h, rsi_15m, atr_15m]:
        return None

    price = last_15m["c"]

    asset_ctx = market_ctx.get(coin, {})
    funding = _to_float(asset_ctx.get("funding", 0))
    open_interest = _to_float(asset_ctx.get("openInterest", 0))
    mark_px = _to_float(asset_ctx.get("markPx", price))

    previous_ctx = state.get("market_ctx", {}).get(coin, {})
    prev_oi = _to_float(previous_ctx.get("openInterest", 0), 0)

    local_high_20 = highest_high(candles_15m[-21:-1], 20)
    local_low_20 = lowest_low(candles_15m[-21:-1], 20)

    recent_range_high = highest_high(candles_15m[-25:-1], 24)
    recent_range_low = lowest_low(candles_15m[-25:-1], 24)
    range_pct = pct_change(recent_range_high, recent_range_low)
    atr_pct = atr_15m / price * 100 if price else 0

    bullish_4h = closes_4h[-1] > ema20_4h
    bearish_4h = closes_4h[-1] < ema20_4h
    bullish_1h = ema20_1h > ema50_1h and closes_1h[-1] > ema20_1h
    bearish_1h = ema20_1h < ema50_1h and closes_1h[-1] < ema20_1h

    not_chop = range_pct >= 0.75 and atr_pct >= 0.20
    near_ema_long = price >= ema20_15m and (price - ema20_15m) <= atr_15m * 1.1
    near_ema_short = price <= ema20_15m and (ema20_15m - price) <= atr_15m * 1.1

    breakout_long = prev_15m["c"] <= local_high_20 and price > local_high_20
    breakdown_short = prev_15m["c"] >= local_low_20 and price < local_low_20

    sweep_reclaim_long = (
        prev_15m["l"] < local_low_20 and
        prev_15m["c"] > local_low_20 and
        lower_wick_ratio(prev_15m) >= 0.35 and
        price > prev_15m["c"]
    )

    sweep_reject_short = (
        prev_15m["h"] > local_high_20 and
        prev_15m["c"] < local_high_20 and
        upper_wick_ratio(prev_15m) >= 0.35 and
        price < prev_15m["c"]
    )

    failed_breakout_short = (
        prev2_15m["c"] > local_high_20 and
        prev_15m["c"] < local_high_20 and
        upper_wick_ratio(prev_15m) >= 0.30 and
        price < prev_15m["c"]
    )

    failed_breakdown_long = (
        prev2_15m["c"] < local_low_20 and
        prev_15m["c"] > local_low_20 and
        lower_wick_ratio(prev_15m) >= 0.30 and
        price > prev_15m["c"]
    )

    rsi_long_ok = 48 <= rsi_15m <= 68
    rsi_short_ok = 32 <= rsi_15m <= 52

    momentum_long = candle_body_strength(last_15m) >= 0.40 and last_15m["c"] > last_15m["o"]
    momentum_short = candle_body_strength(last_15m) >= 0.40 and last_15m["c"] < last_15m["o"]

    funding_long_ok = funding <= MAX_LONG_FUNDING
    funding_short_ok = funding >= MIN_SHORT_FUNDING
    oi_supportive = oi_is_rising(prev_oi, open_interest)

    if coin not in MAJORS:
        if funding < ALT_SHORT_FUNDING_FLOOR:
            funding_short_ok = False
        if funding > ALT_LONG_FUNDING_CEIL:
            funding_long_ok = False

    score_long = 0
    score_short = 0

    if bullish_4h:
        score_long += 1
    if bullish_1h:
        score_long += 1
    if not_chop:
        score_long += 1
    if near_ema_long:
        score_long += 1
    if breakout_long or sweep_reclaim_long or failed_breakdown_long:
        score_long += 1
    if rsi_long_ok:
        score_long += 1
    if momentum_long:
        score_long += 1
    if funding_long_ok:
        score_long += 1
    if oi_supportive:
        score_long += 1
    if market_bias == "long":
        score_long += 1

    if bearish_4h:
        score_short += 1
    if bearish_1h:
        score_short += 1
    if not_chop:
        score_short += 1
    if near_ema_short:
        score_short += 1
    if breakdown_short or sweep_reject_short or failed_breakout_short:
        score_short += 1
    if rsi_short_ok:
        score_short += 1
    if momentum_short:
        score_short += 1
    if funding_short_ok:
        score_short += 1
    if oi_supportive:
        score_short += 1
    if market_bias == "short":
        score_short += 1

    candidates = []

    if score_long >= min_signal_score_b and (breakout_long or sweep_reclaim_long or failed_breakdown_long):
        entry = price
        stop = min(lowest_low(candles_15m[-10:], 10), ema20_15m - atr_15m * 0.35)
        risk = entry - stop
        if risk > 0:
            rr_target = 2.0 if score_long >= min_signal_score_a else 1.5
            tp1 = entry + risk * rr_target
            tp2 = entry + risk * (rr_target + 1.0)
            rr = (tp1 - entry) / risk
            trigger = "Breakout + Hold" if breakout_long else ("Sweep + Reclaim" if sweep_reclaim_long else "Failed Breakdown Reclaim")

            if score_long >= min_signal_score_a and rr >= risk_reward_min_a:
                candidates.append({
                    "coin": coin,
                    "side": "LONG",
                    "entry": entry,
                    "entry_zone": make_entry_zone(entry, atr_15m),
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "score": score_long,
                    "grade": "A",
                    "trigger": trigger,
                    "reason": f"Trend-Regime {market_regime}, reclaim/breakout bestätigt, Funding/OI passen.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                    "risk_pct": risk_pct_for_grade(score_long),
                })
            elif rr >= risk_reward_min_b:
                candidates.append({
                    "coin": coin,
                    "side": "LONG",
                    "entry": entry,
                    "entry_zone": make_entry_zone(entry, atr_15m),
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "score": score_long,
                    "grade": "B",
                    "trigger": trigger,
                    "reason": f"Long-Setup im Regime {market_regime}.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                    "risk_pct": risk_pct_for_grade(score_long),
                })

    if score_short >= min_signal_score_b and (breakdown_short or sweep_reject_short or failed_breakout_short):
        entry = price
        stop = max(highest_high(candles_15m[-10:], 10), ema20_15m + atr_15m * 0.35)
        risk = stop - entry
        if risk > 0:
            rr_target = 2.0 if score_short >= min_signal_score_a else 1.5
            tp1 = entry - risk * rr_target
            tp2 = entry - risk * (rr_target + 1.0)
            rr = (entry - tp1) / risk
            trigger = "Breakdown + Hold" if breakdown_short else ("Sweep + Reject" if sweep_reject_short else "Failed Breakout Reject")

            if score_short >= min_signal_score_a and rr >= risk_reward_min_a:
                candidates.append({
                    "coin": coin,
                    "side": "SHORT",
                    "entry": entry,
                    "entry_zone": make_entry_zone(entry, atr_15m),
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "score": score_short,
                    "grade": "A",
                    "trigger": trigger,
                    "reason": f"Trend-Regime {market_regime}, reject/breakdown bestätigt, Funding/OI passen.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                    "risk_pct": risk_pct_for_grade(score_short),
                })
            elif rr >= risk_reward_min_b:
                candidates.append({
                    "coin": coin,
                    "side": "SHORT",
                    "entry": entry,
                    "entry_zone": make_entry_zone(entry, atr_15m),
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "score": score_short,
                    "grade": "B",
                    "trigger": trigger,
                    "reason": f"Short-Setup im Regime {market_regime}.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                    "risk_pct": risk_pct_for_grade(score_short),
                })

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["grade"] == "A", x["score"], x["rr"]), reverse=True)
    return candidates[0]


def format_signal(sig):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    funding_pct = sig["funding"] * 100
    ez_low, ez_high = sig["entry_zone"]
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Qualität: {sig['grade']} | Score: {sig['score']}\n"
        f"Trigger: {sig['trigger']}\n"
        f"Zeit: {now}\n"
        f"Mark: {format_price(sig['mark_px'])}\n"
        f"Entry Zone: {format_price(ez_low)} - {format_price(ez_high)}\n"
        f"Stop: {format_price(sig['stop'])}\n"
        f"TP1: {format_price(sig['tp1'])}\n"
        f"TP2: {format_price(sig['tp2'])}\n"
        f"RR: {sig['rr']:.2f}\n"
        f"Funding: {funding_pct:.4f}%\n"
        f"OI: {sig['open_interest']:.2f}\n"
        f"Empf. Risiko: {sig['risk_pct']:.2f}%\n"
        f"Grund: {sig['reason']}"
    )


def store_open_trade(state, sig):
    state["open_trades"][sig["coin"]] = {
        "coin": sig["coin"],
        "side": sig["side"],
        "entry": sig["entry"],
        "entry_zone": sig["entry_zone"],
        "stop": sig["stop"],
        "tp1": sig["tp1"],
        "tp2": sig["tp2"],
        "score": sig["score"],
        "grade": sig["grade"],
        "trigger": sig["trigger"],
        "risk_pct": sig["risk_pct"],
        "tp1_hit": False,
        "tp2_hit": False,
        "closed": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def manage_open_trades(state, market_ctx):
    if not state["open_trades"]:
        return

    to_remove = []

    for coin, trade in list(state["open_trades"].items()):
        if trade.get("closed"):
            to_remove.append(coin)
            continue
        if coin not in market_ctx:
            continue

        price = _to_float(market_ctx[coin].get("markPx", 0))
        if price <= 0:
            continue

        side = trade["side"]
        stop = trade["stop"]
        tp1 = trade["tp1"]
        tp2 = trade["tp2"]

        candles_15m = fetch_candles(coin, "15m", 60)
        closes_15m = [c["c"] for c in candles_15m]
        ema20_15m = ema(closes_15m, 20)
        if ema20_15m is None:
            continue

        if side == "LONG":
            if price <= stop:
                safe_discord(f"❌ **{coin} LONG SL getroffen** | Preis: {format_price(price)}")
                trade["closed"] = True
                to_remove.append(coin)
                register_loss(state)
                continue

            if (not trade["tp1_hit"]) and price >= tp1:
                safe_discord(f"✅ **{coin} LONG TP1 erreicht** | Preis: {format_price(price)}")
                trade["tp1_hit"] = True
                register_win(state)

            if (not trade["tp2_hit"]) and price >= tp2:
                safe_discord(f"🎯 **{coin} LONG TP2 erreicht** | Preis: {format_price(price)}")
                trade["tp2_hit"] = True
                trade["closed"] = True
                to_remove.append(coin)
                register_win(state)
                continue

            if trade["tp1_hit"] and price < ema20_15m:
                safe_discord(f"⚠️ **{coin} CLOSE EMPFOHLEN** | Nach TP1 unter 15m EMA gefallen @ {format_price(price)}")
                trade["closed"] = True
                to_remove.append(coin)
                continue

        if side == "SHORT":
            if price >= stop:
                safe_discord(f"❌ **{coin} SHORT SL getroffen** | Preis: {format_price(price)}")
                trade["closed"] = True
                to_remove.append(coin)
                register_loss(state)
                continue

            if (not trade["tp1_hit"]) and price <= tp1:
                safe_discord(f"✅ **{coin} SHORT TP1 erreicht** | Preis: {format_price(price)}")
                trade["tp1_hit"] = True
                register_win(state)

            if (not trade["tp2_hit"]) and price <= tp2:
                safe_discord(f"🎯 **{coin} SHORT TP2 erreicht** | Preis: {format_price(price)}")
                trade["tp2_hit"] = True
                trade["closed"] = True
                to_remove.append(coin)
                register_win(state)
                continue

            if trade["tp1_hit"] and price > ema20_15m:
                safe_discord(f"⚠️ **{coin} CLOSE EMPFOHLEN** | Nach TP1 über 15m EMA gestiegen @ {format_price(price)}")
                trade["closed"] = True
                to_remove.append(coin)
                continue

    for coin in to_remove:
        state["open_trades"].pop(coin, None)

    save_state(state)


def passes_exposure_filters(sig, state, market_bias, btc_structure_ok):
    total_shorts, total_longs, major_shorts, major_longs = count_open_exposure(state["open_trades"])
    side = sig["side"]
    coin = sig["coin"]

    if side == "SHORT":
        if total_shorts >= MAX_TOTAL_SHORTS:
            return False, "zu viele Shorts offen"
        if coin in MAJORS and major_shorts >= MAX_MAJOR_SHORTS:
            return False, "Major-Short Limit erreicht"
        if coin not in MAJORS:
            if market_bias != "short":
                return False, "BTC Bias nicht short"
            if not btc_structure_ok:
                return False, "BTC Struktur nicht bearish genug"
    else:
        if total_longs >= MAX_TOTAL_LONGS:
            return False, "zu viele Longs offen"
        if coin in MAJORS and major_longs >= MAX_MAJOR_LONGS:
            return False, "Major-Long Limit erreicht"
        if coin not in MAJORS:
            if market_bias != "long":
                return False, "BTC Bias nicht long"
            if not btc_structure_ok:
                return False, "BTC Struktur nicht bullish genug"

    return True, ""


def main():
    print("BOT STARTING...", flush=True)

    try:
        if not DISCORD_WEBHOOK_URL:
            print("❌ WEBHOOK FEHLT", flush=True)
            return

        state = load_state()
        print("STATE LOADED", flush=True)
        log("Bot started successfully.")

        if SEND_STARTUP_MESSAGE:
            safe_discord("✅ Regime Bot gestartet. Trend/Range/Chaos Filter aktiv.")

        while True:
            try:
                maybe_send_heartbeat(state)

                market_ctx = fetch_asset_contexts()

                manage_open_trades(state, market_ctx)

                if in_news_block():
                    print("News block active", flush=True)
                    time.sleep(POLL_SECONDS)
                    continue

                if is_paused(state):
                    print("Bot paused after loss streak", flush=True)
                    time.sleep(POLL_SECONDS)
                    continue

                market_bias = get_market_bias()
                market_regime = get_market_regime()
                btc_structure_ok = get_btc_structure_ok_for_alts(market_bias)

                print(f"Market bias: {market_bias}", flush=True)
                print(f"Market regime: {market_regime}", flush=True)

                if market_regime == "chaos":
                    print("Chaos regime: no new entries", flush=True)
                    send_status_report(state, market_bias, market_regime)
                    time.sleep(POLL_SECONDS)
                    continue

                send_status_report(state, market_bias, market_regime)

                candidates = []

                for coin in COINS:
                    if coin not in market_ctx:
                        continue
                    if coin in state["open_trades"]:
                        continue

                    sig = analyze_coin(coin, market_ctx, state, market_bias, market_regime)
                    if not sig:
                        continue

                    ok, reason = passes_exposure_filters(sig, state, market_bias, btc_structure_ok)
                    if not ok:
                        print(f"{coin}: blocked ({reason})", flush=True)
                        continue

                    candidates.append(sig)

                state["market_ctx"] = {
                    c: {
                        "funding": market_ctx.get(c, {}).get("funding", 0),
                        "openInterest": market_ctx.get(c, {}).get("openInterest", 0),
                        "markPx": market_ctx.get(c, {}).get("markPx", 0),
                    }
                    for c in COINS if c in market_ctx
                }
                save_state(state)

                if not candidates:
                    print("No valid setup after regime/risk filters", flush=True)
                    time.sleep(POLL_SECONDS)
                    continue

                for c in candidates:
                    if c["grade"] == "B":
                        print(f"{c['coin']}: B-setup ({c['trigger']})", flush=True)

                a_candidates = [c for c in candidates if c["grade"] == "A"]
                a_candidates.sort(key=lambda x: (x["score"], x["rr"]), reverse=True)

                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if state["daily_count"]["date"] != today:
                    state["daily_count"] = {"date": today, "count": 0}
                if state["daily_count"]["count"] >= MAX_SIGNALS_PER_DAY:
                    print("Daily signal limit reached", flush=True)
                    save_state(state)
                    time.sleep(POLL_SECONDS)
                    continue

                sent = 0
                for sig in a_candidates:
                    if sent >= MAX_A_SIGNALS_PER_SCAN:
                        break

                    key = f"{sig['coin']}_{sig['side']}"
                    last = state["last_alerts"].get(key, 0)
                    if (time.time() - last) < ALERT_COOLDOWN_MINUTES * 60:
                        continue

                    post_discord(format_signal(sig))
                    print(f"{sig['coin']}: A-SIGNAL SENT", flush=True)

                    store_open_trade(state, sig)
                    state["last_alerts"][key] = time.time()
                    state["daily_count"]["count"] += 1
                    save_state(state)
                    sent += 1

            except Exception as e:
                print(f"INNER ERROR: {_sanitize_error(e)}", flush=True)
                log(f"Bot error: {_sanitize_error(e)}")
                if DEBUG_TO_DISCORD:
                    safe_discord(f"⚠️ Bot-Fehler: {_sanitize_error(e)}")

            time.sleep(POLL_SECONDS)

    except Exception as e:
        print(f"FATAL ERROR: {_sanitize_error(e)}", flush=True)


if __name__ == "__main__":
    main()
