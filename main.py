import os
import time
import json
from datetime import datetime, timezone

import requests

print("BOT STARTING...", flush=True)

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COINS = ["BTC", "SOL"]
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
STATE_FILE = "state.json"

# Signalqualität
RISK_REWARD_MIN_A = float(os.getenv("RISK_REWARD_MIN_A", "2.0"))
RISK_REWARD_MIN_B = float(os.getenv("RISK_REWARD_MIN_B", "1.6"))
MIN_SIGNAL_SCORE_A = int(os.getenv("MIN_SIGNAL_SCORE_A", "6"))
MIN_SIGNAL_SCORE_B = int(os.getenv("MIN_SIGNAL_SCORE_B", "5"))

ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "240"))
MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "4"))

# Funding / OI
MAX_LONG_FUNDING = float(os.getenv("MAX_LONG_FUNDING", "0.00025"))
MIN_SHORT_FUNDING = float(os.getenv("MIN_SHORT_FUNDING", "0.0"))
OI_RISE_THRESHOLD = float(os.getenv("OI_RISE_THRESHOLD", "0.002"))  # 0.2%

# News / Event Schutz (UTC)
NEWS_BLOCK_ENABLED = os.getenv("NEWS_BLOCK_ENABLED", "true").lower() == "true"
HIGH_RISK_UTC_HOURS = {18, 19, 20, 21}

# Bot-Verhalten
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_HEARTBEAT = os.getenv("SEND_HEARTBEAT", "true").lower() == "true"
HEARTBEAT_HOUR_UTC = int(os.getenv("HEARTBEAT_HOUR_UTC", "6"))
DEBUG_TO_DISCORD = os.getenv("DEBUG_TO_DISCORD", "false").lower() == "true"


def log(msg: str):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "last_alerts": {},
            "daily_count": {"date": "", "count": 0},
            "last_heartbeat_date": "",
            "market_ctx": {}
        }


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def post_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt in Railway Variables")
    r = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=20)
    r.raise_for_status()


def safe_discord(message: str):
    try:
        post_discord(message)
    except Exception as e:
        log(f"Discord send failed: {e}")


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


def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def fetch_asset_contexts():
    """
    Hyperliquid metaAndAssetCtxs:
    response is typically [meta, assetCtxs]
    meta contains universe
    assetCtxs contains markPx / funding / openInterest style fields
    """
    payload = {"type": "metaAndAssetCtxs"}
    r = requests.post(INFO_URL, json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, list) or len(data) < 2:
        raise RuntimeError(f"Unexpected metaAndAssetCtxs response: {type(data)}")

    meta = data[0]
    ctxs = data[1]

    universe = meta.get("universe", [])
    result = {}

    for idx, asset in enumerate(universe):
        name = asset.get("name") or asset.get("coin")
        if not name or idx >= len(ctxs):
            continue

        ctx = ctxs[idx] or {}

        mark_px = _to_float(
            ctx.get("markPx", ctx.get("mark_price", ctx.get("midPx", ctx.get("oraclePx", 0))))
        )
        funding = _to_float(
            ctx.get("funding", ctx.get("currentFunding", ctx.get("premium", 0)))
        )
        open_interest = _to_float(
            ctx.get("openInterest", ctx.get("open_interest", ctx.get("oi", 0)))
        )

        result[name] = {
            "markPx": mark_px,
            "funding": funding,
            "openInterest": open_interest,
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

    gains = []
    losses = []

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
    now = datetime.now(timezone.utc)
    return now.hour in HIGH_RISK_UTC_HOURS


def daily_limit_ok(state):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state["daily_count"]["date"] != today:
        state["daily_count"] = {"date": today, "count": 0}
        save_state(state)
    return state["daily_count"]["count"] < MAX_SIGNALS_PER_DAY


def increment_daily_count(state):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state["daily_count"]["date"] != today:
        state["daily_count"] = {"date": today, "count": 0}
    state["daily_count"]["count"] += 1


def cooldown_ok(state, key):
    now = time.time()
    last = state["last_alerts"].get(key, 0)
    return (now - last) > ALERT_COOLDOWN_MINUTES * 60


def mark_alert(state, key):
    state["last_alerts"][key] = time.time()


def maybe_send_heartbeat(state):
    if not SEND_HEARTBEAT:
        return

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    if now.hour == HEARTBEAT_HOUR_UTC and state.get("last_heartbeat_date") != today:
        safe_discord("💓 Bot läuft. Markt-Scan für BTC und SOL aktiv.")
        state["last_heartbeat_date"] = today
        save_state(state)


def oi_is_rising(prev_oi, current_oi):
    if prev_oi is None or prev_oi <= 0 or current_oi <= 0:
        return False
    return current_oi >= prev_oi * (1 + OI_RISE_THRESHOLD)


def analyze_coin(coin: str, market_ctx: dict, state: dict):
    candles_15m = fetch_candles(coin, "15m", 150)
    candles_1h = fetch_candles(coin, "1h", 150)
    candles_4h = fetch_candles(coin, "4h", 100)

    closes_15m = [c["c"] for c in candles_15m]
    closes_1h = [c["c"] for c in candles_1h]
    closes_4h = [c["c"] for c in candles_4h]

    last_15m = candles_15m[-1]
    prev_15m = candles_15m[-2]

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

    not_chop = range_pct >= 0.85 and atr_pct >= 0.22

    near_ema_long = price >= ema20_15m and (price - ema20_15m) <= atr_15m * 0.9
    near_ema_short = price <= ema20_15m and (ema20_15m - price) <= atr_15m * 0.9

    breakout_long = prev_15m["c"] <= local_high_20 and price > local_high_20
    breakdown_short = prev_15m["c"] >= local_low_20 and price < local_low_20

    reclaim_long = (
        prev_15m["l"] < local_low_20 and
        prev_15m["c"] > local_low_20 and
        lower_wick_ratio(prev_15m) >= 0.35 and
        price > prev_15m["c"]
    )

    reject_short = (
        prev_15m["h"] > local_high_20 and
        prev_15m["c"] < local_high_20 and
        upper_wick_ratio(prev_15m) >= 0.35 and
        price < prev_15m["c"]
    )

    rsi_long_ok = 50 <= rsi_15m <= 66
    rsi_short_ok = 34 <= rsi_15m <= 50

    momentum_long = candle_body_strength(last_15m) >= 0.45 and last_15m["c"] > last_15m["o"]
    momentum_short = candle_body_strength(last_15m) >= 0.45 and last_15m["c"] < last_15m["o"]

    funding_long_ok = funding <= MAX_LONG_FUNDING
    funding_short_ok = funding >= MIN_SHORT_FUNDING
    oi_supportive = oi_is_rising(prev_oi, open_interest)

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
    if breakout_long or reclaim_long:
        score_long += 1
    if rsi_long_ok:
        score_long += 1
    if momentum_long:
        score_long += 1
    if funding_long_ok:
        score_long += 1
    if oi_supportive:
        score_long += 1

    if bearish_4h:
        score_short += 1
    if bearish_1h:
        score_short += 1
    if not_chop:
        score_short += 1
    if near_ema_short:
        score_short += 1
    if breakdown_short or reject_short:
        score_short += 1
    if rsi_short_ok:
        score_short += 1
    if momentum_short:
        score_short += 1
    if funding_short_ok:
        score_short += 1
    if oi_supportive:
        score_short += 1

    # A-Setups
    if score_long >= MIN_SIGNAL_SCORE_A and (breakout_long or reclaim_long):
        entry = price
        stop = min(lowest_low(candles_15m[-10:], 10), ema20_15m - atr_15m * 0.35)
        risk = entry - stop
        if risk > 0:
            tp1 = entry + risk * 2.0
            tp2 = entry + risk * 3.0
            rr = (tp1 - entry) / risk
            if rr >= RISK_REWARD_MIN_A:
                trigger = "Breakout + Hold" if breakout_long else "Sweep + Reclaim"
                return {
                    "coin": coin,
                    "side": "LONG",
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "score": score_long,
                    "grade": "A",
                    "trigger": trigger,
                    "reason": "Trend stimmt, Funding nicht überhitzt, OI unterstützt den Move.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                }

    if score_short >= MIN_SIGNAL_SCORE_A and (breakdown_short or reject_short):
        entry = price
        stop = max(highest_high(candles_15m[-10:], 10), ema20_15m + atr_15m * 0.35)
        risk = stop - entry
        if risk > 0:
            tp1 = entry - risk * 2.0
            tp2 = entry - risk * 3.0
            rr = (entry - tp1) / risk
            if rr >= RISK_REWARD_MIN_A:
                trigger = "Breakdown + Hold" if breakdown_short else "Sweep + Reject"
                return {
                    "coin": coin,
                    "side": "SHORT",
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "score": score_short,
                    "grade": "A",
                    "trigger": trigger,
                    "reason": "Trend stimmt, Funding begünstigt Short, OI unterstützt den Move.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                }

    # B-Setups nur Logs
    if score_long >= MIN_SIGNAL_SCORE_B and (breakout_long or reclaim_long):
        entry = price
        stop = min(lowest_low(candles_15m[-10:], 10), ema20_15m - atr_15m * 0.35)
        risk = entry - stop
        if risk > 0:
            tp1 = entry + risk * 1.6
            rr = (tp1 - entry) / risk
            if rr >= RISK_REWARD_MIN_B:
                trigger = "Breakout + Hold" if breakout_long else "Sweep + Reclaim"
                return {
                    "coin": coin,
                    "side": "LONG",
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp1,
                    "rr": rr,
                    "score": score_long,
                    "grade": "B",
                    "trigger": trigger,
                    "reason": "Ordentliches Setup, aber nicht stark genug für Discord.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                }

    if score_short >= MIN_SIGNAL_SCORE_B and (breakdown_short or reject_short):
        entry = price
        stop = max(highest_high(candles_15m[-10:], 10), ema20_15m + atr_15m * 0.35)
        risk = stop - entry
        if risk > 0:
            tp1 = entry - risk * 1.6
            rr = (entry - tp1) / risk
            if rr >= RISK_REWARD_MIN_B:
                trigger = "Breakdown + Hold" if breakdown_short else "Sweep + Reject"
                return {
                    "coin": coin,
                    "side": "SHORT",
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp1,
                    "rr": rr,
                    "score": score_short,
                    "grade": "B",
                    "trigger": trigger,
                    "reason": "Ordentliches Setup, aber nicht stark genug für Discord.",
                    "funding": funding,
                    "open_interest": open_interest,
                    "mark_px": mark_px,
                }

    return None


def format_signal(sig):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    funding_pct = sig["funding"] * 100
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Qualität: {sig['grade']} | Score: {sig['score']}\n"
        f"Trigger: {sig['trigger']}\n"
        f"Zeit: {now}\n"
        f"Mark: {format_price(sig['mark_px'])}\n"
        f"Entry: {format_price(sig['entry'])}\n"
        f"Stop: {format_price(sig['stop'])}\n"
        f"TP1: {format_price(sig['tp1'])}\n"
        f"TP2: {format_price(sig['tp2'])}\n"
        f"RR: {sig['rr']:.2f}\n"
        f"Funding: {funding_pct:.4f}%\n"
        f"OI: {sig['open_interest']:.2f}\n"
        f"Grund: {sig['reason']}"
    )


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
            safe_discord("✅ Safe Pro Signal Bot V4 gestartet. BTC und SOL werden überwacht.")

        while True:
            print("Scanning market...", flush=True)

            try:
                maybe_send_heartbeat(state)

                if in_news_block():
                    print("News block active", flush=True)
                    time.sleep(POLL_SECONDS)
                    continue

                if not daily_limit_ok(state):
                    print("Daily signal limit reached", flush=True)
                    time.sleep(POLL_SECONDS)
                    continue

                market_ctx = fetch_asset_contexts()

                for coin in COINS:
                    print(f"Checking {coin}", flush=True)

                    sig = analyze_coin(coin, market_ctx, state)

                    if not sig:
                        print(f"{coin}: no setup", flush=True)
                        continue

                    if sig["grade"] == "B":
                        print(
                            f"{coin}: B-setup only ({sig['trigger']}) | funding={sig['funding']:.6f} | oi={sig['open_interest']:.2f}",
                            flush=True
                        )
                        continue

                    state_key = f"{coin}_{sig['side']}"
                    if not cooldown_ok(state, state_key):
                        print(f"{coin}: cooldown active for {sig['side']}", flush=True)
                        continue

                    msg = format_signal(sig)
                    post_discord(msg)
                    print(f"{coin}: A-SIGNAL SENT", flush=True)

                    mark_alert(state, state_key)
                    increment_daily_count(state)
                    save_state(state)

                # aktuellen Markt-Kontext für nächsten Scan merken
                state["market_ctx"] = {
                    c: {
                        "funding": market_ctx.get(c, {}).get("funding", 0),
                        "openInterest": market_ctx.get(c, {}).get("openInterest", 0),
                        "markPx": market_ctx.get(c, {}).get("markPx", 0),
                    }
                    for c in COINS
                }
                save_state(state)

            except Exception as e:
                print(f"INNER ERROR: {e}", flush=True)
                log(f"Bot error: {e}")
                if DEBUG_TO_DISCORD:
                    safe_discord(f"⚠️ Bot-Fehler: {str(e)[:1200]}")

            time.sleep(POLL_SECONDS)

    except Exception as e:
        print(f"FATAL ERROR: {e}", flush=True)


if __name__ == "__main__":
    main()
