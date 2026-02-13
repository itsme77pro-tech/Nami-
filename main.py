import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
from dataclasses import dataclass
from typing import Optional, Tuple, List
from datetime import datetime
import time

# ================= CONFIG =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

# Verified working symbols for TwelveData
WATCHLIST = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "USD/CHF",
    "AUD/USD",
    "USD/CAD"
]
SCAN_INTERVAL = 1800  # 30 minutes
MIN_SCORE = 3
MAX_RETRIES = 3
RETRY_DELAY = 5

# RATE LIMITING: 8 calls per minute max (free tier)
# With 2 timeframes per symbol, we can handle 4 symbols per minute safely
# Adding buffer: 6 calls per minute to be safe
MAX_CALLS_PER_MINUTE = 6
CALL_DELAY = 60 / MAX_CALLS_PER_MINUTE  # 10 seconds between calls

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ================= DATA CLASSES =================

@dataclass
class Signal:
    symbol: str
    direction: str
    score: int
    confidence: int
    entry: float
    stop_loss: float
    take_profit: float
    risk_reward: float
    timestamp: datetime

# ================= NAMI PERSONALITY =================

NAMI_LINES = [
    "💰 Smart money is active — follow the liquidity.",
    "🌊 Liquidity shift detected — institutional footprint confirmed.",
    "🔥 Structure confirms directional bias — high conviction.",
    "⚡ Institutional momentum building — volatility incoming.",
    "💎 High probability setup — edge is present.",
    "🎯 Order block respected — smart money accumulation zone.",
    "📊 Volume profile supports the move — conviction validated."
]

def get_nami_line() -> str:
    return random.choice(NAMI_LINES)

# ================= TELEGRAM =================

class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
    
    async def send_message(self, message: str) -> bool:
        if not self.token or not self.chat_id:
            logging.error("Telegram credentials not configured")
            return False
        
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=payload, timeout=10) as response:
                    result = await response.json()
                    if not result.get("ok"):
                        logging.error(f"Telegram API error: {result}")
                        return False
                    return True
        except asyncio.TimeoutError:
            logging.error("Telegram request timed out")
            return False
        except Exception as e:
            logging.error(f"Telegram error: {e}")
            return False
    
    def format_signal(self, signal: Signal) -> str:
        emoji = "🟢" if signal.direction == "BUY" else "🔴"
        
        return f"""
{emoji} <b>NAMI INSTITUTIONAL SIGNAL</b>

<b>Symbol:</b> <code>{signal.symbol}</code>
<b>Direction:</b> {signal.direction}
<b>Score:</b> {signal.score}/7
<b>Confidence:</b> {signal.confidence}%

<b>Entry:</b> <code>{signal.entry:.5f}</code>
<b>Stop Loss:</b> <code>{signal.stop_loss:.5f}</code>
<b>Take Profit:</b> <code>{signal.take_profit:.5f}</code>
<b>Risk:Reward:</b> 1:{signal.risk_reward:.1f}

<i>{get_nami_line()}</i>
"""

# ================= RATE LIMITED DATA FETCHER =================

class RateLimitedDataFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_call_time = 0
        self.call_count = 0
        self.minute_start = time.time()
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"Accept": "application/json"}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _rate_limit(self):
        """Enforce rate limiting: max 6 calls per minute."""
        now = time.time()
        
        # Reset counter every minute
        if now - self.minute_start >= 60:
            self.call_count = 0
            self.minute_start = now
        
        # If at limit, wait until next minute
        if self.call_count >= MAX_CALLS_PER_MINUTE:
            sleep_time = 60 - (now - self.minute_start) + 1
            logging.warning(f"Rate limit reached. Sleeping {sleep_time:.0f}s...")
            await asyncio.sleep(sleep_time)
            self.call_count = 0
            self.minute_start = time.time()
        
        # Ensure minimum delay between calls
        time_since_last = now - self.last_call_time
        if time_since_last < CALL_DELAY:
            sleep_time = CALL_DELAY - time_since_last
            await asyncio.sleep(sleep_time)
        
        self.last_call_time = time.time()
        self.call_count += 1
    
    async def fetch_time_series(
        self, 
        symbol: str, 
        interval: str, 
        outputsize: int = 300
    ) -> Optional[pd.DataFrame]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        # Wait for rate limit
        await self._rate_limit()
        
        url = f"{self.base_url}/time_series"
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": self.api_key
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logging.warning(f"Rate limited by API. Waiting {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        continue
                    
                    if response.status != 200:
                        logging.warning(f"HTTP {response.status} for {symbol}")
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        return None
                    
                    data = await response.json()
                    
                    if "code" in data and data["code"] != 200:
                        error_msg = data.get('message', 'Unknown error')
                        # Don't retry on rate limit errors, wait instead
                        if "run out of API credits" in error_msg:
                            logging.error(f"Rate limit hit for {symbol}: {error_msg}")
                            await asyncio.sleep(60)  # Wait a full minute
                            continue
                        logging.error(f"API error for {symbol}: {error_msg}")
                        return None
                    
                    if "values" not in data or not data["values"]:
                        logging.warning(f"No data returned for {symbol} ({interval})")
                        return None
                    
                    return self._process_data(data["values"])
                    
            except aiohttp.ClientError as e:
                logging.warning(f"Network error for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            except Exception as e:
                logging.error(f"Unexpected error fetching {symbol}: {e}")
                return None
        
        return None
    
    def _process_data(self, values: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame(values)
        
        numeric_cols = ["open", "high", "low", "close"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        else:
            df["volume"] = 0
        
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df.dropna(subset=["open", "high", "low", "close"], inplace=True)
        
        return df

# ================= TECHNICAL ANALYSIS =================

class TechnicalAnalyzer:
    @staticmethod
    def ema_trend(df: pd.DataFrame, fast: int = 20, slow: int = 50) -> str:
        if len(df) < slow:
            return "NEUTRAL"
        
        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        
        return "BUY" if ema_fast.iloc[-1] > ema_slow.iloc[-1] else "SELL"
    
    @staticmethod
    def breakout(df: pd.DataFrame, lookback: int = 20) -> Optional[str]:
        if len(df) < lookback:
            return None
        
        recent_high = df["high"].tail(lookback).max()
        recent_low = df["low"].tail(lookback).min()
        current_close = df["close"].iloc[-1]
        
        if current_close > recent_high * 0.999:
            return "BUY"
        elif current_close < recent_low * 1.001:
            return "SELL"
        return None
    
    @staticmethod
    def bull_flag(df: pd.DataFrame) -> Optional[str]:
        if len(df) < 20:
            return None
        
        impulse = df["close"].iloc[-15:-10]
        flag = df["close"].iloc[-5:]
        
        impulse_range = impulse.iloc[-1] - impulse.iloc[0]
        flag_range = flag.max() - flag.min()
        
        if impulse_range > 0 and flag_range < abs(impulse.mean()) * 0.015:
            return "BUY"
        
        if impulse_range < 0 and flag_range < abs(impulse.mean()) * 0.015:
            return "SELL"
        
        return None
    
    @staticmethod
    def double_top_bottom(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        if len(df) < lookback + 2:
            return None
        
        highs = df["high"].tail(lookback)
        lows = df["low"].tail(lookback)
        
        current_high = highs.iloc[-1]
        prev_high = highs.iloc[-3]
        high_threshold = highs.mean() * 0.002
        
        current_low = lows.iloc[-1]
        prev_low = lows.iloc[-3]
        low_threshold = lows.mean() * 0.002
        
        if abs(current_high - prev_high) < high_threshold:
            if current_high > highs.mean():
                return "SELL"
        
        if abs(current_low - prev_low) < low_threshold:
            if current_low < lows.mean():
                return "BUY"
        
        return None
    
    @staticmethod
    def head_and_shoulders(df: pd.DataFrame) -> Optional[str]:
        if len(df) < 20:
            return None
        
        highs = df["high"].tail(20)
        head_idx = highs.idxmax()
        head_val = highs.max()
        
        left_shoulder = highs.loc[:head_idx]
        right_shoulder = highs.loc[head_idx:]
        
        if len(left_shoulder) < 3 or len(right_shoulder) < 3:
            return None
        
        if (left_shoulder.max() < head_val and 
            right_shoulder.max() < head_val and
            abs(left_shoulder.max() - right_shoulder.max()) < head_val * 0.02):
            return "SELL"
        
        return None
    
    @staticmethod
    def fair_value_gap(df: pd.DataFrame) -> Optional[str]:
        if len(df) < 3:
            return None
        
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        body = abs(c2["close"] - c2["open"])
        range_val = c2["high"] - c2["low"]
        
        if range_val == 0 or body < 0.5 * range_val:
            return None
        
        if c3["low"] > c1["high"]:
            return "BUY"
        
        if c3["high"] < c1["low"]:
            return "SELL"
        
        return None
    
    @staticmethod
    def volume_spike(df: pd.DataFrame, lookback: int = 20, threshold: float = 1.5) -> bool:
        if "volume" not in df.columns or df["volume"].sum() == 0:
            return False
        
        if len(df) < lookback:
            return False
        
        current_vol = df["volume"].iloc[-1]
        avg_vol = df["volume"].tail(lookback).mean()
        
        return current_vol > avg_vol * threshold

# ================= SIGNAL SCORING =================

class SignalScorer:
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
    
    def calculate_score(
        self, 
        df_15m: pd.DataFrame, 
        df_1h: pd.DataFrame
    ) -> Tuple[int, str]:
        direction = self.analyzer.ema_trend(df_15m)
        if direction == "NEUTRAL":
            return 0, "NEUTRAL"
        
        score = 0
        
        if self.analyzer.ema_trend(df_1h) == direction:
            score += 1
        
        patterns = [
            self.analyzer.breakout(df_15m),
            self.analyzer.bull_flag(df_15m),
            self.analyzer.double_top_bottom(df_15m),
            self.analyzer.head_and_shoulders(df_15m),
            self.analyzer.fair_value_gap(df_15m)
        ]
        
        for pattern in patterns:
            if pattern == direction:
                score += 1
        
        if self.analyzer.volume_spike(df_15m):
            score += 1
        
        return score, direction

# ================= RISK MANAGEMENT =================

class RiskManager:
    @staticmethod
    def calculate_levels(
        df: pd.DataFrame, 
        direction: str,
        risk_mult: float = 2.0,
        sl_lookback: int = 10
    ) -> Tuple[float, float, float, float]:
        price = df["close"].iloc[-1]
        
        if direction == "BUY":
            stop = df["low"].tail(sl_lookback).min()
            risk = price - stop
            target = price + (risk * risk_mult)
        else:
            stop = df["high"].tail(sl_lookback).max()
            risk = stop - price
            target = price - (risk * risk_mult)
        
        rr = abs((target - price) / risk) if risk != 0 else 0
        
        return price, stop, target, rr
    
    @staticmethod
    def calculate_confidence(score: int) -> int:
        base = 50
        per_point = 7
        return min(95, base + score * per_point)

# ================= MAIN BOT =================

class NamiBot:
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.scorer = SignalScorer()
        self.risk_manager = RiskManager()
        self.last_signals: dict = {}
        self.stats = {"scanned": 0, "signals": 0, "errors": 0}
    
    async def scan_symbol(
        self, 
        fetcher: RateLimitedDataFetcher, 
        symbol: str
    ) -> Optional[Signal]:
        self.stats["scanned"] += 1
        
        # Fetch 15m data
        df_15m = await fetcher.fetch_time_series(symbol, "15min", 300)
        if df_15m is None:
            return None
        
        # Fetch 1h data  
        df_1h = await fetcher.fetch_time_series(symbol, "1h", 200)
        if df_1h is None:
            return None
        
        if len(df_15m) < 50 or len(df_1h) < 50:
            logging.warning(f"Insufficient data for {symbol}")
            return None
        
        score, direction = self.scorer.calculate_score(df_15m, df_1h)
        
        if score < MIN_SCORE:
            return None
        
        signal_key = f"{symbol}_{direction}"
        if self.last_signals.get(symbol) == direction:
            return None
        
        entry, sl, tp, rr = self.risk_manager.calculate_levels(df_15m, direction)
        confidence = self.risk_manager.calculate_confidence(score)
        
        if rr < 1.0:
            logging.info(f"{symbol}: R:R too low ({rr:.2f})")
            return None
        
        signal = Signal(
            symbol=symbol,
            direction=direction,
            score=score,
            confidence=confidence,
            entry=entry,
            stop_loss=sl,
            take_profit=tp,
            risk_reward=rr,
            timestamp=datetime.utcnow()
        )
        
        self.last_signals[symbol] = direction
        self.stats["signals"] += 1
        
        return signal
    
    async def run(self):
        logging.info("🧭 Nami Institutional Engine starting...")
        
        await self.telegram.send_message(
            "🧭 <b>Nami Institutional Engine</b> is now LIVE.\n\n"
            f"Monitoring {len(WATCHLIST)} instruments every {SCAN_INTERVAL//60} minutes.\n"
            f"Rate limit: {MAX_CALLS_PER_MINUTE} calls/minute."
        )
        
        while True:
            try:
                async with RateLimitedDataFetcher(TWELVEDATA_KEY) as fetcher:
                    for symbol in WATCHLIST:
                        try:
                            signal = await self.scan_symbol(fetcher, symbol)
                            
                            if signal:
                                message = self.telegram.format_signal(signal)
                                success = await self.telegram.send_message(message)
                                
                                if success:
                                    logging.info(f"Signal sent: {signal.symbol} {signal.direction}")
                                else:
                                    logging.error(f"Failed to send signal for {signal.symbol}")
                            
                        except Exception as e:
                            self.stats["errors"] += 1
                            logging.error(f"Error scanning {symbol}: {e}")
                            continue
                    
                    logging.info(
                        f"Scan complete. "
                        f"Scanned: {self.stats['scanned']}, "
                        f"Signals: {self.stats['signals']}, "
                        f"Errors: {self.stats['errors']}"
                    )
                
                await asyncio.sleep(SCAN_INTERVAL)
                
            except Exception as e:
                self.stats["errors"] += 1
                logging.error(f"Main loop error: {e}")
                await asyncio.sleep(60)

# ================= ENTRY POINT =================

if __name__ == "__main__":
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not CHAT_ID:
        missing.append("CHAT_ID")
    if not TWELVEDATA_KEY:
        missing.append("TWELVEDATA_KEY")
    
    if missing:
        logging.error(f"Missing environment variables: {', '.join(missing)}")
        exit(1)
    
    bot = NamiBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise
