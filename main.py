import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
import json
from dataclasses import dataclass, field, asdict
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timedelta
from enum import Enum
import time
from pathlib import Path

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
MAX_CALLS_PER_MINUTE = 6
CALL_DELAY = 60 / MAX_CALLS_PER_MINUTE  # 10 seconds between calls

# Daily Report Configuration
REPORT_HOUR = 21  # 9:00 PM
REPORT_MINUTE = 0
CSV_EXPORT_DIR = os.getenv("CSV_EXPORT_DIR", "./reports")  # Set to empty string to disable CSV export

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ================= DATA CLASSES =================

class TradeStatus(Enum):
    PENDING = "PENDING"
    WIN = "WIN"
    LOSS = "LOSS"
    EXPIRED = "EXPIRED"  # Neither SL nor TP hit within reasonable time

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
    trade_id: str = field(default_factory=lambda: f"TRD_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{random.randint(1000, 9999)}")

@dataclass
class TradeLog:
    trade_id: str
    symbol: str
    direction: str
    entry_price: float
    stop_loss: float
    take_profit: float
    timestamp: datetime
    status: TradeStatus = TradeStatus.PENDING
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    pnl_rr: float = 0.0  # Profit/Loss in Risk:Reward terms
    notes: str = ""

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

REPORT_LINES = [
    "📈 Another day of institutional precision.",
    "🧠 Data-driven decisions, algorithmic execution.",
    "⚡ Edge captured, alpha extracted.",
    "🎯 Discipline pays — consistency compounds.",
    "📊 Markets whispered, we listened.",
    "💎 Quality over quantity, always."
]

def get_nami_line() -> str:
    return random.choice(NAMI_LINES)

def get_report_line() -> str:
    return random.choice(REPORT_LINES)

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

<b>Trade ID:</b> <code>{signal.trade_id}</code>
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

    def format_daily_report(self, stats: Dict, date_str: str, filename: Optional[str] = None) -> str:
        total = stats['total_signals']
        wins = stats['wins']
        losses = stats['losses']
        pending = stats['pending']
        win_rate = stats['win_rate']
        total_profit = stats['total_profit_rr']
        total_loss = stats['total_loss_rr']
        net_performance = stats['net_performance_rr']
        
        # Determine emoji based on performance
        performance_emoji = "🟢" if net_performance > 0 else "🔴" if net_performance < 0 else "⚪"
        
        report = f"""
📊 <b>NAMI DAILY PERFORMANCE REPORT</b>
📅 <code>{date_str}</code>

<b>Signal Statistics:</b>
• Total Signals: <code>{total}</code>
• Wins: <code>{wins}</code> 🟢
• Losses: <code>{losses}</code> 🔴
• Pending/Expired: <code>{pending}</code> ⏳
• Win Rate: <code>{win_rate:.1f}%</code>

<b>Performance (R:R Model):</b>
• Total Profit: <code>+{total_profit:.2f}R</code>
• Total Loss: <code>{total_loss:.2f}R</code>
• Net Performance: <code>{net_performance:+.2f}R</code> {performance_emoji}

<b>Risk Metrics:</b>
• Avg R per Trade: <code>{stats['avg_r_per_trade']:.2f}R</code>
• Profit Factor: <code>{stats['profit_factor']:.2f}</code>

<i>{get_report_line()}</i>"""
        
        if filename:
            report += f"\n\n📁 CSV exported: <code>{filename}</code>"
        
        return report

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
        
        if now - self.minute_start >= 60:
            self.call_count = 0
            self.minute_start = now
        
        if self.call_count >= MAX_CALLS_PER_MINUTE:
            sleep_time = 60 - (now - self.minute_start) + 1
            logging.warning(f"Rate limit reached. Sleeping {sleep_time:.0f}s...")
            await asyncio.sleep(sleep_time)
            self.call_count = 0
            self.minute_start = time.time()
        
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
                    if response.status == 429:
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
                        if "run out of API credits" in error_msg:
                            logging.error(f"Rate limit hit for {symbol}: {error_msg}")
                            await asyncio.sleep(60)
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

# ================= TRADE TRACKING =================

class TradeTracker:
    def __init__(self):
        self.trades: List[TradeLog] = []
        self.pending_trades: Dict[str, TradeLog] = {}  # trade_id -> TradeLog
        self.daily_stats = {
            "date": datetime.utcnow().date(),
            "total_signals": 0,
            "wins": 0,
            "losses": 0,
            "pending": 0,
            "expired": 0,
            "total_profit_rr": 0.0,
            "total_loss_rr": 0.0
        }
        self._lock = asyncio.Lock()
    
    async def add_signal(self, signal: Signal):
        """Log a new signal as a pending trade."""
        async with self._lock:
            trade = TradeLog(
                trade_id=signal.trade_id,
                symbol=signal.symbol,
                direction=signal.direction,
                entry_price=signal.entry,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                timestamp=signal.timestamp,
                status=TradeStatus.PENDING
            )
            self.trades.append(trade)
            self.pending_trades[signal.trade_id] = trade
            self.daily_stats["total_signals"] += 1
            logging.info(f"Trade logged: {signal.trade_id} - {signal.symbol} {signal.direction}")
    
    async def check_trade_outcomes(self, fetcher: RateLimitedDataFetcher):
        """Check if any pending trades have hit SL or TP."""
        async with self._lock:
            trades_to_check = list(self.pending_trades.items())
        
        for trade_id, trade in trades_to_check:
            try:
                # Fetch latest price data for this symbol
                df = await fetcher.fetch_time_series(trade.symbol, "15min", 50)
                if df is None or len(df) < 2:
                    continue
                
                current_price = df["close"].iloc[-1]
                current_high = df["high"].iloc[-1]
                current_low = df["low"].iloc[-1]
                
                # Determine if SL or TP was hit
                sl_hit = False
                tp_hit = False
                
                if trade.direction == "BUY":
                    # For BUY: SL is below, TP is above
                    if current_low <= trade.stop_loss:
                        sl_hit = True
                    if current_high >= trade.take_profit:
                        tp_hit = True
                else:  # SELL
                    # For SELL: SL is above, TP is below
                    if current_high >= trade.stop_loss:
                        sl_hit = True
                    if current_low <= trade.take_profit:
                        tp_hit = True
                
                # Determine outcome (TP takes precedence if both hit, or whichever came first)
                # For simplicity, we check current state - in production you'd check historical sequence
                if tp_hit and not sl_hit:
                    await self._mark_trade_result(trade_id, TradeStatus.WIN, current_price, trade.risk_reward)
                elif sl_hit and not tp_hit:
                    await self._mark_trade_result(trade_id, TradeStatus.LOSS, current_price, -1.0)
                elif tp_hit and sl_hit:
                    # Both hit - ambiguous, mark as win if TP is closer to entry than SL was
                    entry_to_tp = abs(trade.take_profit - trade.entry_price)
                    entry_to_sl = abs(trade.stop_loss - trade.entry_price)
                    if entry_to_tp < entry_to_sl:
                        await self._mark_trade_result(trade_id, TradeStatus.WIN, current_price, trade.risk_reward)
                    else:
                        await self._mark_trade_result(trade_id, TradeStatus.LOSS, current_price, -1.0)
                
                # Check for expiration (24 hours old)
                elif (datetime.utcnow() - trade.timestamp) > timedelta(hours=24):
                    await self._mark_trade_result(trade_id, TradeStatus.EXPIRED, current_price, 0.0)
                    
            except Exception as e:
                logging.error(f"Error checking trade {trade_id}: {e}")
    
    async def _mark_trade_result(self, trade_id: str, status: TradeStatus, exit_price: float, pnl_rr: float):
        """Mark a trade as completed with result."""
        async with self._lock:
            if trade_id not in self.pending_trades:
                return
            
            trade = self.pending_trades[trade_id]
            trade.status = status
            trade.exit_price = exit_price
            trade.exit_time = datetime.utcnow()
            trade.pnl_rr = pnl_rr
            
            del self.pending_trades[trade_id]
            
            if status == TradeStatus.WIN:
                self.daily_stats["wins"] += 1
                self.daily_stats["total_profit_rr"] += pnl_rr
            elif status == TradeStatus.LOSS:
                self.daily_stats["losses"] += 1
                self.daily_stats["total_loss_rr"] += abs(pnl_rr)
            elif status == TradeStatus.EXPIRED:
                self.daily_stats["expired"] += 1
            
            logging.info(f"Trade {trade_id} marked as {status.value} | PnL: {pnl_rr:+.2f}R")
    
    async def calculate_statistics(self) -> Dict:
        """Calculate current statistics."""
        async with self._lock:
            total_closed = self.daily_stats["wins"] + self.daily_stats["losses"]
            win_rate = (self.daily_stats["wins"] / total_closed * 100) if total_closed > 0 else 0
            
            total = self.daily_stats["total_signals"]
            avg_r = (self.daily_stats["total_profit_rr"] - self.daily_stats["total_loss_rr"]) / total if total > 0 else 0
            
            profit_factor = (
                self.daily_stats["total_profit_rr"] / self.daily_stats["total_loss_rr"] 
                if self.daily_stats["total_loss_rr"] > 0 else float('inf')
            )
            
            return {
                "total_signals": total,
                "wins": self.daily_stats["wins"],
                "losses": self.daily_stats["losses"],
                "pending": len(self.pending_trades),
                "expired": self.daily_stats["expired"],
                "win_rate": win_rate,
                "total_profit_rr": self.daily_stats["total_profit_rr"],
                "total_loss_rr": self.daily_stats["total_loss_rr"],
                "net_performance_rr": self.daily_stats["total_profit_rr"] - self.daily_stats["total_loss_rr"],
                "avg_r_per_trade": avg_r,
                "profit_factor": profit_factor if profit_factor != float('inf') else 0
            }
    
    async def export_to_csv(self, date_str: str) -> Optional[str]:
        """Export daily trades to CSV file."""
        if not CSV_EXPORT_DIR:
            return None
        
        try:
            os.makedirs(CSV_EXPORT_DIR, exist_ok=True)
            filename = f"{CSV_EXPORT_DIR}/trades_{date_str}.csv"
            
            async with self._lock:
                if not self.trades:
                    return None
                
                # Convert trades to dicts for DataFrame
                trade_dicts = []
                for trade in self.trades:
                    trade_dicts.append({
                        "trade_id": trade.trade_id,
                        "symbol": trade.symbol,
                        "direction": trade.direction,
                        "entry_price": trade.entry_price,
                        "stop_loss": trade.stop_loss,
                        "take_profit": trade.take_profit,
                        "timestamp": trade.timestamp.isoformat(),
                        "status": trade.status.value,
                        "exit_price": trade.exit_price,
                        "exit_time": trade.exit_time.isoformat() if trade.exit_time else None,
                        "pnl_rr": trade.pnl_rr
                    })
            
            df = pd.DataFrame(trade_dicts)
            df.to_csv(filename, index=False)
            logging.info(f"Exported {len(trade_dicts)} trades to {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Failed to export CSV: {e}")
            return None
    
    async def reset_daily(self):
        """Reset counters for new day."""
        async with self._lock:
            self.trades = []
            self.pending_trades = {}
            self.daily_stats = {
                "date": datetime.utcnow().date(),
                "total_signals": 0,
                "wins": 0,
                "losses": 0,
                "pending": 0,
                "expired": 0,
                "total_profit_rr": 0.0,
                "total_loss_rr": 0.0
            }
            logging.info("Daily counters reset")

# ================= MAIN BOT =================

class NamiBot:
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.scorer = SignalScorer()
        self.risk_manager = RiskManager()
        self.trade_tracker = TradeTracker()
        self.last_signals: dict = {}
        self.stats = {"scanned": 0, "signals": 0, "errors": 0}
        self._running = True
    
    async def scan_symbol(
        self, 
        fetcher: RateLimitedDataFetcher, 
        symbol: str
    ) -> Optional[Signal]:
        self.stats["scanned"] += 1
        
        df_15m = await fetcher.fetch_time_series(symbol, "15min", 300)
        if df_15m is None:
            return None
        
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
    
    async def send_daily_report(self):
        """Generate and send daily performance report."""
        try:
            stats = await self.trade_tracker.calculate_statistics()
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            
            # Export to CSV
            filename = await self.trade_tracker.export_to_csv(date_str)
            
            # Send report
            message = self.telegram.format_daily_report(stats, date_str, filename)
            success = await self.telegram.send_message(message)
            
            if success:
                logging.info(f"Daily report sent for {date_str}")
            else:
                logging.error("Failed to send daily report")
            
            # Reset for next day
            await self.trade_tracker.reset_daily()
            
        except Exception as e:
            logging.error(f"Error in daily report: {e}")
    
    async def report_scheduler(self):
        """Schedule daily reports at 9:00 PM."""
        while self._running:
            try:
                now = datetime.utcnow()
                target_time = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
                
                # If we've passed 9 PM today, schedule for tomorrow
                if now >= target_time:
                    target_time += timedelta(days=1)
                
                sleep_seconds = (target_time - now).total_seconds()
                logging.info(f"Next report scheduled in {sleep_seconds/3600:.1f} hours (at {target_time})")
                
                # Sleep until report time
                await asyncio.sleep(sleep_seconds)
                
                # Send report
                await self.send_daily_report()
                
                # Small delay to avoid double-triggering
                await asyncio.sleep(60)
                
            except Exception as e:
                logging.error(f"Report scheduler error: {e}")
                await asyncio.sleep(300)  # Retry in 5 minutes if error
    
    async def outcome_checker(self):
        """Periodically check pending trade outcomes."""
        while self._running:
            try:
                async with RateLimitedDataFetcher(TWELVEDATA_KEY) as fetcher:
                    await self.trade_tracker.check_trade_outcomes(fetcher)
                await asyncio.sleep(900)  # Check every 15 minutes
            except Exception as e:
                logging.error(f"Outcome checker error: {e}")
                await asyncio.sleep(300)
    
    async def run(self):
        logging.info("🧭 Nami Institutional Engine starting...")
        
        await self.telegram.send_message(
            "🧭 <b>Nami Institutional Engine</b> is now LIVE.\n\n"
            f"Monitoring {len(WATCHLIST)} instruments every {SCAN_INTERVAL//60} minutes.\n"
            f"Rate limit: {MAX_CALLS_PER_MINUTE} calls/minute.\n"
            f"Daily reports at {REPORT_HOUR:02d}:{REPORT_MINUTE:02d} UTC."
        )
        
        # Start background tasks
        asyncio.create_task(self.report_scheduler())
        asyncio.create_task(self.outcome_checker())
        
        while self._running:
            try:
                async with RateLimitedDataFetcher(TWELVEDATA_KEY) as fetcher:
                    for symbol in WATCHLIST:
                        try:
                            signal = await self.scan_symbol(fetcher, symbol)
                            
                            if signal:
                                # Log the trade
                                await self.trade_tracker.add_signal(signal)
                                
                                # Send signal notification
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
                        f"Errors: {self.stats['errors']}, "
                        f"Pending: {len(self.trade_tracker.pending_trades)}"
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
