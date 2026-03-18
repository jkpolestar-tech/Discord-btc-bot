import os
import time
import json
from datetime import datetime, timezone

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COINS = ["BTC", "SOL"]
POLL_SECONDS = 300
STATE_FILE = "state.json"

ALERT_COOLDOWN_MINUTES = 180
RISK_REWARD_MIN = 1.8


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def cooldown_ok(state, key):
    now = time.time()
    last = state.get(key, 0)
    return (now - last) > ALERT_COOLDOWN_MINUTES * 60


def mark_alert(state, key):
    state[key] = time.time()


def post_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt")
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=20)


def fetch_candles(coin: str, interval: str, limit: int):
    now_ms = int(time.time() * 1000)

    interval_ms = {
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
    }[interval]

    start_ms = now_ms - (limit + 5) * interval_ms

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


def analyze_coin(coin: str):
    candles_15m = fetch_candles(coin, "15m", 120)
    candles_1h = fetch_candles(coin, "1h", 120)
    candles_4h = fetch_candles(coin, "4h", 80)

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

    local_high_20 = highest_high(candles_15m[-21:-1], 20)
    local_low_20 = lowest_low(candles_15m[-21:-1], 20)

    recent_range_high = highest_high(candles_15m[-25:-1], 24)
    recent_range_low = lowest_low(candles_15m[-25:-1], 24)
    range_pct = pct_change(recent_range_high, recent_range_low)
    atr_pct = atr_15m / price * 100 if price else 0

    # Trendfilter wie konservative Trader
    bullish_4h = closes_4h[-1] > ema20_4h
    bearish_4h = closes_4h[-1] < ema20_4h

    bullish_1h = ema20_1h > ema50_1h and closes_1h[-1] > ema20_1h
    bearish_1h = ema20_1h < ema50_1h and closes_1h[-1] < ema20_1h

    # Chop vermeiden
    not_chop = range_pct >= 0.8 and atr_pct >= 0.22

    # Pullback nicht zu weit weg
    near_ema_long = price >= ema20_15m and (price - ema20_15m) <= atr_15m * 0.8
    near_ema_short = price <= ema20_15m and (ema20_15m - price) <= atr_15m * 0.8

    # Break + Bestätigung
    breakout_long = prev_15m["c"] <= local_high_20 and price > local_high_20
    breakdown_short = prev_15m["c"] >= local_low_20 and price < local_low_20

    # RSI Filter
    rsi_long_ok = 52 <= rsi_15m <= 68
    rsi_short_ok = 32 <= rsi_15m <= 48

    # LONG
    if bullish_4h and bullish_1h and not_chop and near_ema_long and breakout_long and rsi_long_ok:
        entry = price
        stop = min(lowest_low(candles_15m[-10:], 10), ema20_15m - atr_15m * 0.35)
        risk = entry - stop
        if risk > 0:
            tp1 = entry + risk * 1.8
            tp2 = entry + risk * 2.6
            rr = (tp1 - entry) / risk
            if rr >= RISK_REWARD_MIN:
                return {
                    "coin": coin,
                    "side": "LONG",
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "reason": "4h und 1h bullish, 15m Breakout über lokales Hoch, Pullback nicht überdehnt, Markt nicht in Chop.",
                }

    # SHORT
    if bearish_4h and bearish_1h and not_chop and near_ema_short and breakdown_short and rsi_short_ok:
        entry = price
        stop = max(highest_high(candles_15m[-10:], 10), ema20_15m + atr_15m * 0.35)
        risk = stop - entry
        if risk > 0:
            tp1 = entry - risk * 1.8
            tp2 = entry - risk * 2.6
            rr = (entry - tp1) / risk
            if rr >= RISK_REWARD_MIN:
                return {
                    "coin": coin,
                    "side": "SHORT",
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr": rr,
                    "reason": "4h und 1h bearish, 15m Breakdown unter lokales Tief, Pullback nicht überdehnt, Markt nicht in Chop.",
                }

    return None


def format_signal(sig):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"📢 **{sig['coin']} {sig['side']} SIGNAL**\n"
        f"Zeit: {now}\n"
        f"Entry: {format_price(sig['entry'])}\n"
        f"Stop: {format_price(sig['stop'])}\n"
        f"TP1: {format_price(sig['tp1'])}\n"
        f"TP2: {format_price(sig['tp2'])}\n"
        f"RR: {sig['rr']:.2f}\n"
        f"Grund: {sig['reason']}"
    )


def main():
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt in Railway Variables")

    state = load_state()
    post_discord("✅ Safe Signal Bot gestartet. BTC und SOL werden überwacht.")

    while True:
        try:
            for coin in COINS:
                sig = analyze_coin(coin)
                if not sig:
                    continue

                state_key = f"{coin}_{sig['side']}"
                if cooldown_ok(state, state_key):
                    post_discord(format_signal(sig))
                    mark_alert(state, state_key)
                    save_state(state)

        except Exception as e:
            try:
                post_discord(f"⚠️ Bot-Fehler: {str(e)[:1500]}")
            except Exception:
                pass

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
