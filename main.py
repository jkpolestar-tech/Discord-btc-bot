import os
import time
import json
import math
from datetime import datetime, timezone

import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

COINS = ["BTC", "SOL"]
POLL_SECONDS = 300
STATE_FILE = "state.json"

# Konservative Parameter
RISK_REWARD_MIN = 1.8
ALERT_COOLDOWN_MINUTES = 180
CHOP_LOOKBACK_15M = 24
TREND_LOOKBACK_1H = 50


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def post_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt in Railway Variables")
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=15)


def fetch_candles(coin: str, interval: str, limit: int):
    now_ms = int(time.time() * 1000)

    interval_ms = {
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000,
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


def pct(a, b):
    if b == 0:
        return 0
    return (a - b) / b * 100


def cooldown_ok(state, key):
    now = time.time()
    last = state.get(key, 0)
    return (now - last) > ALERT_COOLDOWN_MINUTES * 60


def mark_alert(state, key):
    state[key] = time.time()


def analyze_coin(coin: str):
    candles_15m = fetch_candles(coin, "15m", 120)
    candles_1h = fetch_candles(coin, "1h", 120)

    closes_15m = [c["c"] for c in candles_15m]
    closes_1h = [c["c"] for c in candles_1h]

    last_15m = candles_15m[-1]
    prev_15m = candles_15m[-2]
    last_1h = candles_1h[-1]

    ema20_1h = ema(closes_1h, 20)
    ema50_1h = ema(closes_1h, 50)
    ema20_15m = ema(closes_15m, 20)
    atr_15m = atr(candles_15m, 14)
    rsi_15m = rsi(closes_15m, 14)

    if None in [ema20_1h, ema50_1h, ema20_15m, atr_15m, rsi_15m]:
        return None

    price = last_15m["c"]
    atr_pct = atr_15m / price * 100

    # Chop vermeiden: zu wenig Bewegung
    range_high = highest_high(candles_15m[:-1], CHOP_LOOKBACK_15M)
    range_low = lowest_low(candles_15m[:-1], CHOP_LOOKBACK_15M)
    range_pct = pct(range_high, range_low)

    trend_up = ema20_1h > ema50_1h and last_1h["c"] > ema20_1h
    trend_down = ema20_1h < ema50_1h and last_1h["c"] < ema20_1h

    # Momentum / relative Stärke
    breakout_high = highest_high(candles_15m[-21:-1], 20)
    breakdown_low = lowest_low(candles_15m[-21:-1], 20)

    # Pullback-Logik: nicht mitten im Spike
    pullback_ok_long = price >= ema20_15m and (price - ema20_15m) <= atr_15m * 0.8
    pullback_ok_short = price <= ema20_15m and (ema20_15m - price) <= atr_15m * 0.8

    # Conservative filters
    not_chop = range_pct >= 0.9 and atr_pct >= 0.25
    rsi_long_ok = 52 <= rsi_15m <= 68
    rsi_short_ok = 32 <= rsi_15m <= 48

    # Long-Setup
    if (
        trend_up
        and not_chop
        and pullback_ok_long
        and rsi_long_ok
        and prev_15m["c"] <= breakout_high
        and price > breakout_high
    ):
        entry = price
        stop = min(lowest_low(candles_15m[-10:], 10), ema20_15m - atr_15m * 0.3)
        tp1 = entry + (entry - stop) * 1.8
        tp2 = entry + (entry - stop) * 2.5
        rr = (tp1 - entry) / max(entry - stop, 1e-9)
        if rr >= RISK_REWARD_MIN:
            return {
                "coin": coin,
                "side": "LONG",
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "rr": rr,
                "reason": (
                    "1h Trend bullish, 15m Breakout über lokales Hoch, "
                    "Pullback nicht überdehnt, RSI im gesunden Bereich."
                ),
            }

    # Short-Setup
    if (
        trend_down
        and not_chop
        and pullback_ok_short
        and rsi_short_ok
        and prev_15m["c"] >= breakdown_low
        and price < breakdown_low
    ):
        entry = price
        stop = max(highest_high(candles_15m[-10:], 10), ema20_15m + atr_15m * 0.3)
        tp1 = entry - (stop - entry) * 1.8
        tp2 = entry - (stop - entry) * 2.5
        rr = (entry - tp1) / max(stop - entry, 1e-9)
        if rr >= RISK_REWARD_MIN:
            return {
                "coin": coin,
                "side": "SHORT",
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "rr": rr,
                "reason": (
                    "1h Trend bearish, 15m Breakdown unter lokales Tief, "
                    "Pullback nicht überdehnt, RSI im gesunden Bereich."
                ),
            }

    return None


def format_signal(sig):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"**{sig['coin']} {sig['side']}**\n"
        f"Zeit: {now}\n"
        f"Entry: {sig['entry']:.2f}\n"
        f"Stop: {sig['stop']:.2f}\n"
        f"TP1: {sig['tp1']:.2f}\n"
        f"TP2: {sig['tp2']:.2f}\n"
        f"RR: {sig['rr']:.2f}\n"
        f"Grund: {sig['reason']}"
    )


def main():
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL fehlt")

    state = load_state()
    post_discord("✅ Safe Hyperliquid Signal Bot gestartet. Scan für BTC & SOL läuft.")

    while True:
        try:
            for coin in COINS:
                sig = analyze_coin(coin)
                if not sig:
                    continue

                key = f"{coin}_{sig['side']}"
                if cooldown_ok(state, key):
                    post_discord(format_signal(sig))
                    mark_alert(state, key)
                    save_state(state)

        except Exception as e:
            try:
                post_discord(f"⚠️ Bot-Fehler: {str(e)[:1500]}")
            except Exception:
                pass

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
