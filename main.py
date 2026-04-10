import os
import time
import requests
from datetime import datetime, timezone

INFO_URL = "https://api.hyperliquid.xyz/info"

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
COIN = os.getenv("COIN", "BTC")

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "600"))
BASE_MIN_SCORE = int(os.getenv("BASE_MIN_SCORE", "7"))

REQUEST_GAP = float(os.getenv("REQUEST_GAP", "1.3"))

LAST_REQUEST = 0


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


# -------------------------
# RATE LIMITED REQUEST
# -------------------------

def api_post(payload):

    global LAST_REQUEST

    diff = time.time() - LAST_REQUEST
    if diff < REQUEST_GAP:
        time.sleep(REQUEST_GAP - diff)

    try:

        r = requests.post(INFO_URL, json=payload, timeout=20)

        if r.status_code == 429:
            log("RATE LIMIT -> waiting 10s")
            time.sleep(10)
            r = requests.post(INFO_URL, json=payload, timeout=20)

        r.raise_for_status()

        LAST_REQUEST = time.time()

        return r.json()

    except Exception as e:
        log(f"API ERROR {e}")
        time.sleep(5)
        return None


# -------------------------
# DISCORD
# -------------------------

def send_discord(msg):

    if not DISCORD_WEBHOOK_URL:
        return

    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=10)
    except:
        pass


# -------------------------
# MARKET DATA
# -------------------------

def fetch_context():

    data = api_post({"type": "metaAndAssetCtxs"})

    if not data:
        return None

    meta = data[0]
    ctx = data[1]

    for i, asset in enumerate(meta["universe"]):

        if asset["name"] == COIN:

            return {
                "price": float(ctx[i]["markPx"]),
                "oi": float(ctx[i]["openInterest"]),
                "funding": float(ctx[i]["funding"])
            }

    return None


def candles(tf="15m", limit=120):

    now = int(time.time() * 1000)

    intervals = {
        "15m": 15 * 60 * 1000,
        "1h": 60 * 60 * 1000
    }

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": COIN,
            "interval": tf,
            "startTime": now - intervals[tf] * (limit + 10),
            "endTime": now
        }
    }

    data = api_post(payload)

    if not data:
        return []

    out = []

    for c in data[-limit:]:

        out.append({
            "o": float(c["o"]),
            "h": float(c["h"]),
            "l": float(c["l"]),
            "c": float(c["c"]),
            "v": float(c["v"])
        })

    return out


# -------------------------
# INDICATORS
# -------------------------

def ema(data, length):

    if len(data) < length:
        return None

    k = 2 / (length + 1)

    e = sum(data[:length]) / length

    for price in data[length:]:
        e = price * k + e * (1 - k)

    return e


def rsi(data, length=14):

    if len(data) < length + 1:
        return None

    gains = []
    losses = []

    for i in range(1, length + 1):

        diff = data[i] - data[i - 1]

        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / length
    avg_loss = sum(losses) / length

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# -------------------------
# SIGNAL LOGIC
# -------------------------

def analyze():

    c15 = candles("15m", 120)
    time.sleep(1)

    c1 = candles("1h", 120)

    if not c15 or not c1:
        return None

    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]

    e20 = ema(closes15, 20)
    e50 = ema(closes15, 50)

    e20h = ema(closes1, 20)
    e50h = ema(closes1, 50)

    r = rsi(closes15)

    if None in (e20, e50, e20h, e50h, r):
        return None

    price = closes15[-1]

    score = 0

    if price > e20 > e50:
        side = "LONG"
        score += 2
    elif price < e20 < e50:
        side = "SHORT"
        score += 2
    else:
        return None

    if side == "LONG" and closes1[-1] > e20h > e50h:
        score += 2

    if side == "SHORT" and closes1[-1] < e20h < e50h:
        score += 2

    if 40 < r < 70:
        score += 1

    if score < BASE_MIN_SCORE:
        return None

    stop_pct = 0.02

    if side == "LONG":

        stop = price * (1 - stop_pct)
        tp1 = price + (price - stop) * 2
        tp2 = price + (price - stop) * 4

    else:

        stop = price * (1 + stop_pct)
        tp1 = price - (stop - price) * 2
        tp2 = price - (stop - price) * 4

    return {
        "side": side,
        "price": price,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "score": score
    }


# -------------------------
# MAIN LOOP
# -------------------------

def main():

    print("BOT STARTING...", flush=True)

    time.sleep(10)

    send_discord("✅ BTC CLEAN RECOVERY BOT AKTIV")

    while True:

        try:

            ctx = fetch_context()

            signal = analyze()

            if signal:

                msg = f"""📢 BTC {signal['side']} SIGNAL

Score: {signal['score']}

Entry: {signal['price']:.2f}
Stop: {signal['stop']:.2f}

TP1: {signal['tp1']:.2f}
TP2: {signal['tp2']:.2f}
"""

                send_discord(msg)

        except Exception as e:
            log(f"ERROR {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
