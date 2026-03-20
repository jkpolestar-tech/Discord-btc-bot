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

# ===== Core settings =====
SETUP_MODE = os.getenv("SETUP_MODE", "NORMAL").upper()
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "2"))
MAX_TRADES_PER_DIRECTION = int(os.getenv("MAX_TRADES_PER_DIRECTION", "2"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))

# ===== Quality thresholds =====
if SETUP_MODE == "SAFE":
    MIN_SCORE = 8
elif SETUP_MODE == "AGGRESSIVE":
    MIN_SCORE = 5
else:
    MIN_SCORE = 6

# ===== Entry / confirmation =====
ENTRY_CONFIRMATION = os.getenv("ENTRY_CONFIRMATION", "true").lower() == "true"
REQUIRE_BTC_ALIGNMENT_FOR_ALTS = os.getenv("REQUIRE_BTC_ALIGNMENT_FOR_ALTS", "true").lower() == "true"
NEUTRAL_BIAS_BLOCK_WEAK_TRADES = os.getenv("NEUTRAL_BIAS_BLOCK_WEAK_TRADES", "true").lower() == "true"

# ===== Flip / cooldown =====
FLIP_BLOCK_ENABLED = os.getenv("FLIP_BLOCK_ENABLED", "true").lower() == "true"
SHORT_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("SHORT_COOLDOWN_AFTER_SL_HOURS", "4"))
LONG_COOLDOWN_AFTER_SL_HOURS = int(os.getenv("LONG_COOLDOWN_AFTER_SL_HOURS", "4"))

# ===== Trade management =====
BREAK_EVEN_ENABLED = os.getenv("BREAK_EVEN_ENABLED", "true").lower() == "true"
BREAK_EVEN_R = float(os.getenv("BREAK_EVEN_R", "1.0"))
PARTIAL_TP_ENABLED = os.getenv("PARTIAL_TP_ENABLED", "true").lower() == "true"
PARTIAL_TP_R = float(os.getenv("PARTIAL_TP_R", "1.5"))
FINAL_TP_R = float(os.getenv("FINAL_TP_R", "3.0"))
TRAILING_ENABLED = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
TRAILING_EMA = int(os.getenv("TRAILING_EMA", "20"))
TRAILING_BUFFER_PCT = float(os.getenv("TRAILING_BUFFER_PCT", "0.0025"))

# ===== Structure / breakout =====
ENTRY_ZONE_PCT = float(os.getenv("ENTRY_ZONE_PCT", "0.0015"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.02"))
LOOKBACK_BREAK = int(os.getenv("LOOKBACK_BREAK", "10"))
USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "true").lower() == "true"
VOLUME_BOOST_MIN = float(os.getenv("VOLUME_BOOST_MIN", "1.15"))

# ===== Funding / OI =====
USE_OI_FILTER = os.getenv("USE_OI_FILTER", "true").lower() == "true"
USE_FUNDING_FILTER = os.getenv("USE_FUNDING_FILTER", "true").lower() == "true"
MAX_LONG_FUNDING = float(os.getenv("MAX_LONG_FUNDING", "0.0005"))
MIN_SHORT_FUNDING = float(os.getenv("MIN_SHORT_FUNDING", "-0.0005"))

# ===== Status / dedupe =====
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
    state.setdefault("flip_bias_block", {"LONG": False, "SHORT": False})
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


def avg_volume(candle_list, n=20):
    vals = [x["v"] for x in candle_list[-n:]]
    return sum(vals) / len(vals) if vals else 0.0


def entry_confirmation_ok(c15, side):
    if len(c15) < 3:
        return False

    prev = c15[-2]
    last = c15[-1]

    if side == "LONG":
        return last["c"] > last["o"] and prev["c"] > prev["o"]
    return last["c"] < last["o"] and prev["c"] < prev["o"]


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


def count_open_exposure(open_trades):
    total_longs = 0
    total_shorts = 0

    for _, trade in open_trades.items():
        if trade["side"] == "LONG":
            total_longs += 1
        if trade["side"] == "SHORT":
            total_shorts += 1

    return total_longs, total_shorts


def analyze(coin, market_ctx, prev_ctx, market_bias, flip_state):
    c15 = candles(coin, "15m", 120)
    c1 = candles(coin, "1h", 120)

    closes15 = [x["c"] for x in c15]
    closes1 = [x["c"] for x in c1]

    e20 = ema(closes15, 20)
    e50 = ema(closes15, 50)
    e20_1h = ema(closes1, 20)
    r = rsi(closes15)

    if not e20 or not e50 or not e20_1h or r is None:
        return None

    price = closes15[-1]
    prev_close = closes15[-2]
    high_break = max(closes15[-LOOKBACK_BREAK:-1])
    low_break = min(closes15[-LOOKBACK_BREAK:-1])

    ctx = market_ctx.get(coin, {})
    funding = _to_float(ctx.get("funding", 0))
    oi = _to_float(ctx.get("openInterest", 0))
    prev_oi = _to_float(prev_ctx.get(coin, {}).get("openInterest", 0))

    score = 0
    side = None
    trigger = None

    # Trend side
    if price > e20 > e50 and closes1[-1] > e20_1h:
        side = "LONG"
        score += 3
    elif price < e20 < e50 and closes1[-1] < e20_1h:
        side = "SHORT"
        score += 3
    else:
        return None

    # Confirmed breakout / breakdown
    breakout_long = prev_close <= high_break and price > high_break
    breakdown_short = prev_close >= low_break and price < low_break

    if side == "LONG" and breakout_long:
        score += 2
        trigger = "Breakout + Hold"
    if side == "SHORT" and breakdown_short:
        score += 2
        trigger = "Breakdown + Hold"

    # Entry confirmation
    if ENTRY_CONFIRMATION and entry_confirmation_ok(c15, side):
        score += 1
    elif ENTRY_CONFIRMATION:
        return None

    # RSI window
    if side == "LONG" and 45 < r < 72:
        score += 1
    if side == "SHORT" and 28 < r < 55:
        score += 1

    # Volume confirmation
    if USE_VOLUME_FILTER:
        current_vol = c15[-1]["v"]
        ref_vol = avg_volume(c15[:-1], 20)
        if ref_vol > 0 and current_vol >= ref_vol * VOLUME_BOOST_MIN:
            score += 1
        else:
            return None

    # Funding
    if USE_FUNDING_FILTER:
        if side == "LONG" and funding <= MAX_LONG_FUNDING:
            score += 1
        elif side == "SHORT" and funding >= MIN_SHORT_FUNDING:
            score += 1
        else:
            return None

    # OI
    if USE_OI_FILTER:
        if prev_oi > 0 and oi > prev_oi:
            score += 1
        else:
            return None

    # Bias
    if market_bias == side:
        score += 1

    # Neutral protection
    if NEUTRAL_BIAS_BLOCK_WEAK_TRADES and market_bias == "NEUTRAL":
        score -= 1

    # Flip protection
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
        stop = price * (1 - STOP_PCT)
        risk = price - stop
        tp1 = price + risk * PARTIAL_TP_R
        tp2 = price + risk * FINAL_TP_R
    else:
        entry_low = price * (1 - ENTRY_ZONE_PCT)
        entry_high = price * (1 + ENTRY_ZONE_PCT)
        stop = price * (1 + STOP_PCT)
        risk = stop - price
        tp1 = price - risk * PARTIAL_TP_R
        tp2 = price - risk * FINAL_TP_R

    rr = abs(tp1 - price) / abs(price - stop) if abs(price - stop) > 0 else 0

    return {
        "coin": coin,
        "side": side,
        "score": score,
        "trigger": trigger or ("Trend Long" if side == "LONG" else "Trend Short"),
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


def format_close_message(coin, trade, reason, price):
    return (
        f"⚠️ **{coin} CLOSE EMPFOHLEN**\n"
        f"Richtung: {trade['side']}\n"
        f"Aktueller Preis: {format_price(price)}\n"
        f"Stop: {format_price(trade['stop'])}\n"
        f"Grund: {reason}"
    )


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

        # Break-even
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

        # TP1
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

        # Trailing
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

        # Exit logic
        if side == "LONG":
            if price <= trade["stop"]:
                if trade.get("tp1_hit", False):
                    safe_discord(state, f"🟡 **{coin} LONG Restposition beendet** | Preis: {format_price(price)}",
                                 dedupe_key=f"rest_{coin}_LONG")
                else:
                    safe_discord(state, f"❌ **{coin} LONG SL getroffen** | Preis: {format_price(price)}",
                                 dedupe_key=f"sl_{coin}_LONG")
                    set_direction_cooldown(state, "LONG")
                to_delete.append(coin)
                continue

            if price >= tp2:
                safe_discord(state, f"🎯 **{coin} LONG TP2 erreicht** | Preis: {format_price(price)}",
                             dedupe_key=f"tp2_{coin}_LONG")
                to_delete.append(coin)
                continue

        if side == "SHORT":
            if price >= trade["stop"]:
                if trade.get("tp1_hit", False):
                    safe_discord(state, f"🟡 **{coin} SHORT Restposition beendet** | Preis: {format_price(price)}",
                                 dedupe_key=f"rest_{coin}_SHORT")
                else:
                    safe_discord(state, f"❌ **{coin} SHORT SL getroffen** | Preis: {format_price(price)}",
                                 dedupe_key=f"sl_{coin}_SHORT")
                    set_direction_cooldown(state, "SHORT")
                to_delete.append(coin)
                continue

            if price <= tp2:
                safe_discord(state, f"🎯 **{coin} SHORT TP2 erreicht** | Preis: {format_price(price)}",
                             dedupe_key=f"tp2_{coin}_SHORT")
                to_delete.append(coin)
                continue

    for coin in to_delete:
        state["open"].pop(coin, None)


def send_status_report(state, market_bias, flip_state):
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
        safe_discord(state, "✅ PRO FINAL BOT AKTIV", dedupe_key="startup")

    while True:
        try:
            state = load_state()
            market_ctx = fetch_contexts()
            prices = {coin: market_ctx.get(coin, {}).get("markPx", 0) for coin in COINS}

            manage(state, prices)

            market_bias = get_market_bias()
            flip_state = get_flip_state()
            prev_ctx = state.get("market_ctx_prev", {})

            send_status_report(state, market_bias, flip_state)

            for coin in COINS:
                if coin in state["open"]:
                    continue

                sig = analyze(coin, market_ctx, prev_ctx, market_bias, flip_state)
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
