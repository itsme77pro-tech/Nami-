import os
import asyncio
import aiohttp
import pandas as pd
import logging
import random

# ================= CONFIG =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

WATCHLIST = [
    "EUR/USD:FXCM",
    "GBP/USD:FXCM",
    "USD/JPY:FXCM",
    "BTC/USDT:BINANCE",
    "ETH/USDT:BINANCE"
]

SCAN_INTERVAL = 1800
MIN_SCORE = 3

logging.basicConfig(level=logging.INFO)

# ================= NAMI PERSONALITY =================

NAMI_LINES = [
    "💰 Smart money is active.",
    "🌊 Liquidity just shifted.",
    "🔥 Structure confirms bias.",
    "⚡ Institutional momentum detected.",
    "💎 High probability setup."
]

def nami_line():
    return random.choice(NAMI_LINES)

# ================= TELEGRAM =================

async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logging.error("Telegram credentials missing")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload) as response:
                result = await response.json()
                logging.info(result)
    except Exception as e:
        logging.error(f"Telegram Error: {e}")

# ================= DATA =================

async def fetch_data(session, symbol, interval):
    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": 300,
        "apikey": TWELVEDATA_KEY
    }

    try:
        async with session.get(url, params=params) as response:
            data = await response.json()

        if "values" not in data:
            logging.error(f"No data for {symbol}")
            return None

        df = pd.DataFrame(data["values"])

        # Convert safely
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col].astype(float)

        # Fix volume crash (Forex safe)
        if "volume" in df.columns:
            df["volume"] = df["volume"].astype(float)
        else:
            df["volume"] = 0

        df.index = pd.to_datetime(df["datetime"])
        df = df.sort_index()

        return df

    except Exception as e:
        logging.error(f"Fetch error: {e}")
        return None

# ================= STRATEGIES =================

def ema_trend(df):
    ema20 = df.close.ewm(span=20).mean()
    ema50 = df.close.ewm(span=50).mean()
    return "BUY" if ema20.iloc[-1] > ema50.iloc[-1] else "SELL"

def breakout(df):
    high = df.high.tail(20).max()
    low = df.low.tail(20).min()
    close = df.close.iloc[-1]

    if close > high:
        return "BUY"
    if close < low:
        return "SELL"
    return None

def bull_flag(df):
    if len(df) < 20:
        return None

    impulse = df.close.iloc[-15:-10]
    flag = df.close.iloc[-5:]

    if impulse.iloc[-1] > impulse.iloc[0]:
        if flag.max() - flag.min() < impulse.mean() * 0.01:
            return "BUY"

    if impulse.iloc[-1] < impulse.iloc[0]:
        if flag.max() - flag.min() < abs(impulse.mean()) * 0.01:
            return "SELL"

    return None

def double_top_bottom(df):
    highs = df.high.tail(10)
    lows = df.low.tail(10)

    if abs(highs.iloc[-1] - highs.iloc[-3]) < highs.mean() * 0.001:
        return "SELL"

    if abs(lows.iloc[-1] - lows.iloc[-3]) < lows.mean() * 0.001:
        return "BUY"

    return None

def head_shoulders(df):
    highs = df.high.tail(20)
    head_idx = highs.idxmax()

    left = highs.loc[:head_idx]
    right = highs.loc[head_idx:]

    if len(left) > 0 and len(right) > 0:
        if left.max() < highs.max() and right.max() < highs.max():
            return "SELL"

    return None

def triangle(df):
    highs = df.high.tail(20)
    lows = df.low.tail(20)

    if highs.std() < highs.mean() * 0.001:
        return "BUY"

    if lows.std() < lows.mean() * 0.001:
        return "SELL"

    return None

def fair_value_gap(df):
    if len(df) < 3:
        return None

    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    body = abs(c2.close - c2.open)
    rng = c2.high - c2.low

    if rng == 0 or body < 0.6 * rng:
        return None

    if c3.low > c1.high:
        return "BUY"

    if c3.high < c1.low:
        return "SELL"

    return None

def volume_spike(df):
    if "volume" not in df.columns:
        return False
    return df.volume.iloc[-1] > df.volume.tail(20).mean()

# ================= SCORING =================

def score_signal(df15, df1h):
    score = 0
    direction = ema_trend(df15)

    if ema_trend(df1h) == direction:
        score += 1

    patterns = [
        breakout(df15),
        bull_flag(df15),
        double_top_bottom(df15),
        head_shoulders(df15),
        triangle(df15),
        fair_value_gap(df15)
    ]

    for p in patterns:
        if p == direction:
            score += 1

    if volume_spike(df15):
        score += 1

    return score, direction

# ================= RISK =================

def risk_reward(df, direction):
    price = df.close.iloc[-1]

    if direction == "BUY":
        sl = df.low.tail(10).min()
        tp = price + (price - sl) * 2
    else:
        sl = df.high.tail(10).max()
        tp = price - (sl - price) * 2

    rr = abs((tp - price) / (price - sl)) if price != sl else 0
    return round(price,5), round(sl,5), round(tp,5), round(rr,2)

def confidence(score):
    return min(95, 55 + score * 8)

# ================= MAIN LOOP =================

async def run_bot():
    await send_telegram("🧭 Nami Institutional Engine LIVE.")

    last_signal = {}

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                for symbol in WATCHLIST:

                    df15 = await fetch_data(session, symbol, "15min")
                    df1h = await fetch_data(session, symbol, "1h")

                    if df15 is None or df1h is None:
                        continue

                    score, direction = score_signal(df15, df1h)

                    if score < MIN_SCORE:
                        continue

                    if last_signal.get(symbol) == direction:
                        continue

                    last_signal[symbol] = direction

                    entry, sl, tp, rr = risk_reward(df15, direction)
                    conf = confidence(score)

                    message = f"""
🎯 NAMI INSTITUTIONAL SIGNAL

Symbol: {symbol}
Direction: {direction}
Score: {score}
Confidence: {conf}%

Entry: {entry}
Stop: {sl}
Target: {tp}
RR: 1:{rr}

{nami_line()}
"""

                    await send_telegram(message)

                await asyncio.sleep(SCAN_INTERVAL)

            except Exception as e:
                logging.error(f"Main Loop Error: {e}")
                await asyncio.sleep(10)

# ================= START =================

if __name__ == "__main__":
    asyncio.run(run_bot())
