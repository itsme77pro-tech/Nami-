import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone
from telegram import Bot

# ================= CONFIG =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

WATCHLIST = ["EUR/USD", "BTC/USD"]
SCAN_INTERVAL = 1800  # 30 min


# ================= DATA FETCH =================

async def fetch_data(session, symbol):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "15min",
        "outputsize": 100,
        "apikey": TWELVEDATA_KEY
    }

    async with session.get(url, params=params) as response:
        data = await response.json()

    if "values" not in data:
        return None

    df = pd.DataFrame(data["values"])
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
    df.index = pd.to_datetime(df["datetime"])
    return df.sort_index()


# ================= SIMPLE SIGNAL =================

def simple_signal(df):
    ema20 = df.close.ewm(span=20).mean()
    ema50 = df.close.ewm(span=50).mean()

    if ema20.iloc[-1] > ema50.iloc[-1]:
        return "BUY"
    elif ema20.iloc[-1] < ema50.iloc[-1]:
        return "SELL"
    return None


# ================= BOT =================

async def run_bot():
    bot = Bot(token=TELEGRAM_TOKEN)

    await bot.send_message(chat_id=CHAT_ID, text="🧭 Nami Bot is LIVE on Railway.")

    async with aiohttp.ClientSession() as session:
        while True:
            for symbol in WATCHLIST:
                df = await fetch_data(session, symbol)
                if df is None:
                    continue

                signal = simple_signal(df)
                if not signal:
                    continue

                price = round(df.close.iloc[-1], 5)

                message = f"""
🎯 NAMI SIGNAL

Symbol: {symbol}
Direction: {signal}
Price: {price}

Navigator says: Trust the trend.
"""

                await bot.send_message(chat_id=CHAT_ID, text=message)

            await asyncio.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run_bot())
