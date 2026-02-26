import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
import csv
import json
from dataclasses import dataclass, field, asdict
from typing import Optional, Tuple, List, Dict, Literal
from datetime import datetime, timedelta, time
from enum import Enum
from pathlib import Path
import pytz

# ================= CONFIGURATION =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

# Personality selection: "nami" or "sogeking"
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

DATA_SOURCE = os.getenv("DATA_SOURCE", "twelvedata").lower()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

WATCHLIST = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
    "BTC/USDT", "ETH/USDT", "SOL/USDT"
]

TIMEFRAMES = {
    "signal": "15min",
    "trend": "1h",
    "higher": "4h"
}

SCAN_INTERVAL = 900
MIN_SCORE = 3 if PERSONALITY == "nami" else 5
MAX_RETRIES = 3
RETRY_DELAY = 5

ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 1.0
ATR_MULTIPLIER_TP = 2.5 if PERSONALITY == "nami" else 3.0
MAX_CANDLES_TIMEOUT = 20
MIN_RR_RATIO = 1.0 if PERSONALITY == "nami" else 2.0

ADX_PERIOD = 14
ADX_THRESHOLD = 20.0 if PERSONALITY == "nami" else 25.0

COOLDOWN_MINUTES = 120 if PERSONALITY == "nami" else 240
TRADING_START_HOUR = 6
TRADING_END_HOUR = 20

REPORT_HOUR = 21
REPORT_MINUTE = 0
REPORT_TIMEZONE = "UTC"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("NamiEngine")

# ================= ENUMS & DATA CLASSES =================

class TradeStatus(Enum):
    PENDING = "pending"
    WIN = "win"
    LOSS = "loss"
    EXPIRED = "expired"

class TradeOutcome(Enum):
    NONE = "none"
    TP_HIT = "tp_hit"
    SL_HIT = "sl_hit"
    EXPIRED = "expired"

@dataclass
class Signal:
    id: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    score: int
    confidence: int
    entry_price: float
    stop_loss: float
    take_profit: float
    atr_value: float
    risk_reward: float
    adx_value: float
    strategy_type: str
    timestamp: datetime
    timeframe: str = "15min"
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class Trade:
    signal: Signal
    status: TradeStatus = field(default=TradeStatus.PENDING)
    outcome: TradeOutcome = field(default=TradeOutcome.NONE)
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    candles_evaluated: int = 0
    max_profit_reached: float = 0.0
    max_loss_reached: float = 0.0
    actual_rr: float = 0.0
    
    @property
    def is_closed(self) -> bool:
        return self.status != TradeStatus.PENDING
    
    @property
    def profit_in_r(self) -> float:
        if self.status == TradeStatus.WIN:
            return self.signal.risk_reward
        elif self.status == TradeStatus.LOSS:
            return -1.0
        elif self.status == TradeStatus.EXPIRED:
            if self.exit_price:
                risk = abs(self.signal.entry_price - self.signal.stop_loss)
                actual = self.exit_price - self.signal.entry_price
                if trade.signal.direction == "SELL":
                    actual = -actual
                return actual / risk if risk != 0 else 0
        return 0.0

@dataclass
class DailyStats:
    date: str
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    expired: int = 0
    total_r: float = 0.0
    gross_profit_r: float = 0.0
    gross_loss_r: float = 0.0
    max_consecutive_losses: int = 0
    current_consecutive_losses: int = 0
    max_drawdown_r: float = 0.0
    
    @property
    def win_rate(self) -> float:
        closed = self.wins + self.losses
        return (self.wins / closed * 100) if closed > 0 else 0.0
    
    @property
    def profit_factor(self) -> float:
        return abs(self.gross_profit_r / self.gross_loss_r) if self.gross_loss_r != 0 else float('inf')
    
    @property
    def avg_r_per_trade(self) -> float:
        total = self.wins + self.losses + self.expired
        return self.total_r / total if total > 0 else 0.0

# ================= PERSONALITIES =================

NAMI_LINES = [
    "💰 Smart money is active — follow the liquidity.",
    "🌊 Liquidity shift detected — institutional footprint confirmed.",
    "🔥 Structure confirms directional bias — high conviction.",
    "⚡ Institutional momentum building — volatility incoming.",
    "💎 High probability setup — edge is present.",
    "🎯 Order block respected — smart money accumulation zone.",
    "📊 Volume profile supports the move — conviction validated.",
    "🏛️ Institutional absorption zone identified."
]

SOGEKING_LINES = [
    "🎯 SNIPER SHOT — Perfect alignment detected.",
    "👑 The King waits... and strikes with precision.",
    "⚡ 80-90% probability — This is the moment.",
    "🎯 One shot, one kill — Institutional levels taken.",
    "👁️‍🗨️ I've been watching... Now I act.",
    "⚔️ The Sniper fires — Maximum confluence achieved.",
    "🎯 Patience pays — Perfect setup acquired.",
    "👑 Only the best for the King — High conviction entry."
]

def get_personality_line() -> str:
    if PERSONALITY == "sogeking":
        return random.choice(SOGEKING_LINES)
    return random.choice(NAMI_LINES)

def get_personality_emoji(direction: str) -> str:
    if PERSONALITY == "sogeking":
        return "👑 SNIPER LONG" if direction == "BUY" else "🎯 SNIPER SHORT"
    return "🟢 LONG" if direction == "BUY" else "🔴 SHORT"

# ================= TELEGRAM CLIENT =================

class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session
    
    async def send_message(self, message: str) -> bool:
        if not self.token or not self.chat_id:
            logger.error("Telegram credentials not configured")
            return False
        
        try:
            session = await self._get_session()
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with session.post(url, data=payload) as response:
                result = await response.json()
                if not result.get("ok"):
                    logger.error(f"Telegram API error: {result}")
                    return False
                return True
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    def format_signal(self, signal: Signal) -> str:
        emoji = get_personality_emoji(signal.direction)
        personality_name = "SOGEKING SNIPER" if PERSONALITY == "sogeking" else "NAMI INSTITUTIONAL"
        
        confidence_text = f"{signal.confidence}%"
        if PERSONALITY == "sogeking":
            if signal.confidence >= 85:
                confidence_text = f"🔥 {signal.confidence}% (ELITE)"
            elif signal.confidence >= 80:
                confidence_text = f"⚡ {signal.confidence}% (HIGH)"
        
        return f"""
{emoji} <b>{personality_name} SIGNAL</b>

<b>Strategy:</b> {signal.strategy_type}
<b>Symbol:</b> <code>{signal.symbol}</code>
<b>Direction:</b> {signal.direction}
<b>Score:</b> {signal.score}/{6 if PERSONALITY == 'nami' else 8} | <b>Confidence:</b> {confidence_text}
<b>ADX:</b> {signal.adx_value:.1f} | <b>ATR:</b> {signal.atr_value:.5f} | <b>R:R:</b> 1:{signal.risk_reward:.1f}

<b>Entry:</b> <code>{signal.entry_price:.5f}</code>
<b>Stop Loss:</b> <code>{signal.stop_loss:.5f}</code> (1 ATR)
<b>Take Profit:</b> <code>{signal.take_profit:.5f}</code> ({ATR_MULTIPLIER_TP:.1f} ATR)

<i>{get_personality_line()}</i>
<code>ID: {signal.id}</code>
"""

    def format_daily_report(self, stats: DailyStats, trades: List[Trade]) -> str:
        date_str = datetime.strptime(stats.date, "%Y-%m-%d").strftime("%B %d, %Y")
        personality_name = "SOGEKING" if PERSONALITY == "sogeking" else "NAMI"
        
        if stats.total_r > 0:
            performance_emoji = "🟢" if PERSONALITY == "nami" else "👑"
        elif stats.total_r < 0:
            performance_emoji = "🔴" if PERSONALITY == "nami" else "💀"
        else:
            performance_emoji = "⚪"
        
        recent_trades = ""
        closed_trades = [t for t in trades if t.is_closed][-5:]
        for t in closed_trades:
            status_emoji = "✅" if t.status == TradeStatus.WIN else "❌" if t.status == TradeStatus.LOSS else "⏱️"
            pnl = f"+{t.signal.risk_reward:.1f}R" if t.status == TradeStatus.WIN else f"{t.profit_in_r:.1f}R"
            recent_trades += f"{status_emoji} {t.signal.symbol} {t.signal.direction} {pnl}\n"
        
        tagline = "The King rests. Tomorrow we hunt again. 👑" if PERSONALITY == "sogeking" else "Markets are closed. Rest well, trader. 🌙"
        
        return f"""
{performance_emoji} <b>{personality_name} DAILY PERFORMANCE REPORT</b>
<i>{date_str}</i>

<b>📊 SIGNAL STATISTICS</b>
Total Signals: <code>{stats.total_signals}</code>
Wins: <code>{stats.wins}</code> | Losses: <code>{stats.losses}</code> | Expired: <code>{stats.expired}</code>
Win Rate: <code>{stats.win_rate:.1f}%</code>

<b>💰 PERFORMANCE METRICS</b>
Net R: <code>{stats.total_r:+.2f}R</code>
Gross Profit: <code>+{stats.gross_profit_r:.2f}R</code>
Gross Loss: <code>{stats.gross_loss_r:.2f}R</code>
Profit Factor: <code>{stats.profit_factor:.2f}</code>
Avg R/Trade: <code>{stats.avg_r_per_trade:+.2f}R</code>

<b>⚠️ RISK METRICS</b>
Max Consecutive Losses: <code>{stats.max_consecutive_losses}</code>
Max Drawdown: <code>{stats.max_drawdown_r:.2f}R</code>

<b>📝 RECENT TRADES</b>
{recent_trades if recent_trades else "No closed trades today."}

<i>{tagline}</i>
"""

# ================= DATA FETCHER =================

class DataFetcher:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"Accept": "application/json"}
            )
        return self._session
    
    async def fetch_time_series(
        self, 
        symbol: str, 
        interval: str, 
        outputsize: int = 500
    ) -> Optional[pd.DataFrame]:
        session = await self._get_session()
        
        url = f"{self.base_url}/time_series"
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": self.api_key
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for {symbol}")
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    
                    data = await response.json()
                    
                    if "values" not in data or not data["values"]:
                        logger.warning(f"No data for {symbol}")
                        return None
                    
                    return self._process_data(data["values"])
                    
            except Exception as e:
                logger.error(f"Fetch error {symbol}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        
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
        df.dropna(subset=numeric_cols, inplace=True)
        
        return df
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# ================= TECHNICAL ANALYZER =================

class TechnicalAnalyzer:
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        
        atr = true_range.ewm(span=period, adjust=False).mean()
        return atr
    
    @staticmethod
    def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        plus_dm = df['high'].diff()
        minus_dm = df['low'].diff().abs() * -1
        
        plus_dm = plus_dm.where((plus_dm > minus_dm.abs()) & (plus_dm > 0), 0)
        minus_dm = minus_dm.abs().where((minus_dm.abs() > plus_dm) & (minus_dm.abs() > 0), 0)
        
        tr = TechnicalAnalyzer.calculate_atr(df, period)
        atr = tr.ewm(span=period, adjust=False).mean()
        
        plus_di = 100 * plus_dm.ewm(span=period, adjust=False).mean() / atr
        minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / atr
        
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx = dx.ewm(span=period, adjust=False).mean()
        
        return adx
    
    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> pd.Series:
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
        return vwap
    
    @staticmethod
    def ema_trend(df: pd.DataFrame, fast: int = 20, slow: int = 50) -> str:
        if len(df) < slow:
            return "NEUTRAL"
        
        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        
        if ema_fast.iloc[-1] > ema_slow.iloc[-1]:
            return "BUY"
        else:
            return "SELL"
    
    @staticmethod
    def sme_pattern(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        if len(df) < lookback + 5:
            return None
        
        consolidation = df['close'].iloc[-lookback-5:-5]
        consolidation_range = (consolidation.max() - consolidation.min()) / consolidation.mean()
        
        expansion = df.iloc[-5:]
        expansion_range = (expansion['high'].max() - expansion['low'].min()) / expansion['close'].mean()
        
        avg_volume = df['volume'].iloc[-lookback-5:-5].mean()
        expansion_volume = expansion['volume'].mean()
        
        if consolidation_range < 0.005 and expansion_range > 0.01 and expansion_volume > avg_volume * 1.3:
            if expansion['close'].iloc[-1] > consolidation.max():
                return "BUY"
            elif expansion['close'].iloc[-1] < consolidation.min():
                return "SELL"
        
        return None
    
    @staticmethod
    def breakout_retest(df: pd.DataFrame, lookback: int = 20) -> Optional[str]:
        if len(df) < lookback + 10:
            return None
        
        recent_high = df['high'].iloc[-lookback-10:-10].max()
        recent_low = df['low'].iloc[-lookback-10:-10].min()
        
        recent = df.iloc[-10:]
        
        if recent['high'].max() > recent_high:
            retest_threshold = recent_high * 0.001
            if abs(recent['low'].min() - recent_high) < retest_threshold or recent['low'].min() < recent_high:
                if recent['close'].iloc[-1] > recent_high and recent['close'].iloc[-1] > recent['open'].iloc[-1]:
                    return "BUY"
        
        if recent['low'].min() < recent_low:
            retest_threshold = recent_low * 0.001
            if abs(recent['high'].max() - recent_low) < retest_threshold or recent['high'].max() > recent_low:
                if recent['close'].iloc[-1] < recent_low and recent['close'].iloc[-1] < recent['open'].iloc[-1]:
                    return "SELL"
        
        return None
    
    @staticmethod
    def vwap_mean_reversion(df: pd.DataFrame, lookback: int = 20) -> Optional[str]:
        if len(df) < lookback or df['volume'].sum() == 0:
            return None
        
        vwap = TechnicalAnalyzer.calculate_vwap(df)
        current_price = df['close'].iloc[-1]
        current_vwap = vwap.iloc[-1]
        
        deviation = (current_price - current_vwap) / current_vwap
        
        threshold = 0.002
        
        if deviation > threshold:
            if df['close'].iloc[-1] < df['open'].iloc[-1]:
                return "SELL"
        
        if deviation < -threshold:
            if df['close'].iloc[-1] > df['open'].iloc[-1]:
                return "BUY"
        
        return None
    
    @staticmethod
    def liquidity_sweep(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        if len(df) < lookback + 3:
            return None
        
        recent_highs = df['high'].iloc[-lookback-1:-1]
        recent_lows = df['low'].iloc[-lookback-1:-1]
        
        recent_high = recent_highs.max()
        recent_low = recent_lows.min()
        
        current = df.iloc[-1]
        previous = df.iloc[-2]
        
        if previous['low'] < recent_low and current['close'] > previous['open']:
            body = abs(current['close'] - current['open'])
            range_val = current['high'] - current['low']
            if body > range_val * 0.6 and current['close'] > current['open']:
                return "BUY"
        
        if previous['high'] > recent_high and current['close'] < previous['open']:
            body = abs(current['close'] - current['open'])
            range_val = current['high'] - current['low']
            if body > range_val * 0.6 and current['close'] < current['open']:
                return "SELL"
        
        return None
    
    @staticmethod
    def opening_range_breakout(df: pd.DataFrame, range_periods: int = 4) -> Optional[str]:
        if len(df) < range_periods + 5:
            return None
        
        opening_range = df.iloc[-range_periods-5:-5]
        or_high = opening_range['high'].max()
        or_low = opening_range['low'].min()
        
        recent = df.iloc[-5:]
        
        if recent['close'].iloc[-1] > or_high:
            or_volume = opening_range['volume'].mean()
            recent_volume = recent['volume'].mean()
            if recent_volume > or_volume * 1.2:
                return "BUY"
        
        if recent['close'].iloc[-1] < or_low:
            or_volume = opening_range['volume'].mean()
            recent_volume = recent['volume'].mean()
            if recent_volume > or_volume * 1.2:
                return "SELL"
        
        return None

# ================= SIGNAL SCORER =================

class SignalScorer:
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
    
    def calculate_score(self, df_15m: pd.DataFrame, df_1h: pd.DataFrame, df_4h: Optional[pd.DataFrame] = None) -> Tuple[int, str, Optional[str], float]:
        direction = None
        score = 0
        triggered_strategy = None
        
        # 5 Base Strategies
        sme = self.analyzer.sme_pattern(df_15m)
        if sme:
            score += 1
            direction = sme
            triggered_strategy = "SME Pattern"
        
        breakout = self.analyzer.breakout_retest(df_15m)
        if breakout:
            score += 1
            if direction is None:
                direction = breakout
            if triggered_strategy is None:
                triggered_strategy = "Breakout Retest"
        
        vwap = self.analyzer.vwap_mean_reversion(df_15m)
        if vwap:
            score += 1
            if direction is None:
                direction = vwap
            if triggered_strategy is None:
                triggered_strategy = "VWAP Reversion"
        
        sweep = self.analyzer.liquidity_sweep(df_15m)
        if sweep:
            score += 1
            if direction is None:
                direction = sweep
            if triggered_strategy is None:
                triggered_strategy = "Liquidity Sweep"
        
        orb = self.analyzer.opening_range_breakout(df_15m)
        if orb:
            score += 1
            if direction is None:
                direction = orb
            if triggered_strategy is None:
                triggered_strategy = "ORB"
        
        # Trend alignment bonuses
        if direction:
            trend_15m = self.analyzer.ema_trend(df_15m)
            trend_1h = self.analyzer.ema_trend(df_1h)
            
            if trend_15m == direction:
                score += 1
            
            if trend_1h == direction:
                score += 1
            
            # SOGEKING: Higher timeframe confirmation
            if PERSONALITY == "sogeking" and df_4h is not None:
                trend_4h = self.analyzer.ema_trend(df_4h)
                if trend_4h == direction:
                    score += 1
                else:
                    # SOGEKING requires 4h alignment
                    return 0, "NEUTRAL", None, 0.0
        
        adx = self.analyzer.calculate_adx(df_15m, ADX_PERIOD).iloc[-1]
        
        return score, direction or "NEUTRAL", triggered_strategy, adx

# ================= RISK MANAGER =================

class RiskManager:
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
    
    def calculate_dynamic_levels(
        self, 
        df: pd.DataFrame, 
        direction: str
    ) -> Tuple[float, float, float, float, float]:
        entry = df["close"].iloc[-1]
        atr = self.analyzer.calculate_atr(df, ATR_PERIOD).iloc[-1]
        
        if direction == "BUY":
            sl = entry - (atr * ATR_MULTIPLIER_SL)
            tp = entry + (atr * ATR_MULTIPLIER_TP)
        else:
            sl = entry + (atr * ATR_MULTIPLIER_SL)
            tp = entry - (atr * ATR_MULTIPLIER_TP)
        
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = reward / risk if risk != 0 else 0
        
        return entry, sl, tp, atr, rr

# ================= TRADE TRACKER =================

class TradeTracker:
    def __init__(self):
        self.trades: Dict[str, Trade] = {}
        self.signals_today: List[Signal] = []
        self.active_trades: List[Trade] = []
        self.closed_trades: List[Trade] = []
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.traded_symbols_today: set = set()
        self.cooldowns: Dict[str, datetime] = {}
    
    def add_signal(self, signal: Signal) -> Trade:
        self.signals_today.append(signal)
        trade = Trade(signal=signal)
        self.trades[signal.id] = trade
        self.active_trades.append(trade)
        self.traded_symbols_today.add(signal.symbol)
        logger.info(f"New trade tracked: {signal.id} {signal.symbol} {signal.direction}")
        return trade
    
    def can_trade_symbol(self, symbol: str) -> bool:
        if symbol in self.traded_symbols_today:
            return False
        
        if symbol in self.cooldowns:
            if datetime.now() < self.cooldowns[symbol]:
                return False
            else:
                del self.cooldowns[symbol]
        
        return True
    
    def has_active_signal(self, symbol: str, direction: str) -> bool:
        for trade in self.active_trades:
            if (trade.signal.symbol == symbol and 
                trade.signal.direction == direction and 
                not trade.is_closed):
                return True
        return False
    
    def _determine_first_touch(self, candle: pd.Series, direction: str, sl: float, tp: float) -> Optional[str]:
        open_price = candle['open']
        
        if direction == "BUY":
            distance_to_sl = open_price - sl
            distance_to_tp = tp - open_price
            
            if distance_to_sl <= 0:
                return "SL"
            if distance_to_tp <= 0:
                return "TP"
            
            if distance_to_sl < distance_to_tp:
                return "SL"
            else:
                return "TP"
        else:
            distance_to_sl = sl - open_price
            distance_to_tp = open_price - tp
            
            if distance_to_sl <= 0:
                return "SL"
            if distance_to_tp <= 0:
                return "TP"
            
            if distance_to_sl < distance_to_tp:
                return "SL"
            else:
                return "TP"
    
    async def evaluate_trade(self, trade: Trade, df: pd.DataFrame) -> bool:
        if trade.is_closed:
            return True
        
        trade.candles_evaluated += 1
        
        signal_time = trade.signal.timestamp
        future_candles = df[df.index > signal_time]
        
        if len(future_candles) == 0:
            return False
        
        for idx, candle in future_candles.iterrows():
            high = candle['high']
            low = candle['low']
            
            sl_hit = False
            tp_hit = False
            
            if trade.signal.direction == "BUY":
                if low <= trade.signal.stop_loss:
                    sl_hit = True
                if high >= trade.signal.take_profit:
                    tp_hit = True
            else:
                if high >= trade.signal.stop_loss:
                    sl_hit = True
                if low <= trade.signal.take_profit:
                    tp_hit = True
            
            if sl_hit and tp_hit:
                first_touch = self._determine_first_touch(
                    candle, trade.signal.direction, 
                    trade.signal.stop_loss, trade.signal.take_profit
                )
                
                if first_touch == "SL":
                    trade.status = TradeStatus.LOSS
                    trade.outcome = TradeOutcome.SL_HIT
                    trade.exit_price = trade.signal.stop_loss
                    trade.exit_time = idx
                    trade.actual_rr = -1.0
                else:
                    trade.status = TradeStatus.WIN
                    trade.outcome = TradeOutcome.TP_HIT
                    trade.exit_price = trade.signal.take_profit
                    trade.exit_time = idx
                    trade.actual_rr = trade.signal.risk_reward
                
                self._close_trade(trade)
                return True
            
            if sl_hit:
                trade.status = TradeStatus.LOSS
                trade.outcome = TradeOutcome.SL_HIT
                trade.exit_price = trade.signal.stop_loss
                trade.exit_time = idx
                trade.actual_rr = -1.0
                self._close_trade(trade)
                return True
            
            if tp_hit:
                trade.status = TradeStatus.WIN
                trade.outcome = TradeOutcome.TP_HIT
                trade.exit_price = trade.signal.take_profit
                trade.exit_time = idx
                trade.actual_rr = trade.signal.risk_reward
                self._close_trade(trade)
                return True
        
        if trade.candles_evaluated >= MAX_CANDLES_TIMEOUT:
            current_close = df['close'].iloc[-1]
            trade.status = TradeStatus.EXPIRED
            trade.outcome = TradeOutcome.EXPIRED
            trade.exit_price = current_close
            trade.exit_time = df.index[-1]
            
            risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
            actual_pnl = current_close - trade.signal.entry_price
            if trade.signal.direction == "SELL":
                actual_pnl = -actual_pnl
            trade.actual_rr = actual_pnl / risk if risk != 0 else 0
            
            self._close_trade(trade)
            return True
        
        return False
    
    def _close_trade(self, trade: Trade):
        if trade in self.active_trades:
            self.active_trades.remove(trade)
        self.closed_trades.append(trade)
        self.cooldowns[trade.signal.symbol] = datetime.now() + timedelta(minutes=COOLDOWN_MINUTES)
        logger.info(f"Trade closed: {trade.signal.id} | {trade.status.value} | {trade.actual_rr:+.2f}R")
    
    def get_daily_stats(self) -> DailyStats:
        stats = DailyStats(date=self.today)
        stats.total_signals = len(self.signals_today)
        
        peak_r = 0
        current_r = 0
        max_dd = 0
        
        for trade in self.closed_trades:
            r = trade.profit_in_r
            
            if trade.status == TradeStatus.WIN:
                stats.wins += 1
                stats.gross_profit_r += r
                stats.current_consecutive_losses = 0
            elif trade.status == TradeStatus.LOSS:
                stats.losses += 1
                stats.gross_loss_r += r
                stats.current_consecutive_losses += 1
                stats.max_consecutive_losses = max(
                    stats.max_consecutive_losses, 
                    stats.current_consecutive_losses
                )
            elif trade.status == TradeStatus.EXPIRED:
                stats.expired += 1
                stats.current_consecutive_losses = 0
            
            stats.total_r += r
            current_r += r
            peak_r = max(peak_r, current_r)
            max_dd = max(max_dd, peak_r - current_r)
        
        stats.max_drawdown_r = max_dd
        return stats
    
    def reset_daily(self):
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.signals_today = []
        self.active_trades = []
        self.closed_trades = []
        self.trades = {}
        self.traded_symbols_today.clear()
        self.cooldowns.clear()
        logger.info("Daily counters reset")
    
    def export_to_csv(self, filename: Optional[str] = None) -> str:
        if filename is None:
            filename = DATA_DIR / f"trades_{self.today}.csv"
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'signal_id', 'symbol', 'direction', 'strategy', 'entry', 'sl', 'tp', 
                'atr', 'rr', 'adx', 'timestamp', 'status', 'exit_price', 
                'exit_time', 'actual_rr', 'candles_evaluated'
            ])
            
            for trade in list(self.trades.values()):
                writer.writerow([
                    trade.signal.id,
                    trade.signal.symbol,
                    trade.signal.direction,
                    trade.signal.strategy_type,
                    trade.signal.entry_price,
                    trade.signal.stop_loss,
                    trade.signal.take_profit,
                    trade.signal.atr_value,
                    trade.signal.risk_reward,
                    trade.signal.adx_value,
                    trade.signal.timestamp.isoformat(),
                    trade.status.value,
                    trade.exit_price,
                    trade.exit_time.isoformat() if trade.exit_time else '',
                    trade.actual_rr,
                    trade.candles_evaluated
                ])
        
        return str(filename)

# ================= PERFORMANCE TRACKER =================

class PerformanceTracker:
    def __init__(self, trade_tracker: TradeTracker):
        self.trade_tracker = trade_tracker
    
    async def schedule_daily_report(self, telegram: TelegramClient):
        while True:
            now = datetime.now(pytz.timezone(REPORT_TIMEZONE))
            target = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
            
            if now >= target:
                target += timedelta(days=1)
            
            wait_seconds = (target - now).total_seconds()
            logger.info(f"Next report scheduled in {wait_seconds/3600:.1f} hours")
            
            await asyncio.sleep(wait_seconds)
            await self.generate_report(telegram)
    
    async def generate_report(self, telegram: TelegramClient):
        try:
            stats = self.trade_tracker.get_daily_stats()
            all_trades = list(self.trade_tracker.trades.values())
            
            message = telegram.format_daily_report(stats, all_trades)
            await telegram.send_message(message)
            
            csv_path = self.trade_tracker.export_to_csv()
            logger.info(f"Trades exported to {csv_path}")
            
            self.trade_tracker.reset_daily()
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")

# ================= MAIN BOT =================

class NamiEngine:
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.fetcher = DataFetcher(TWELVEDATA_KEY)
        self.analyzer = TechnicalAnalyzer()
        self.scorer = SignalScorer()
        self.risk_manager = RiskManager()
        self.trade_tracker = TradeTracker()
        self.performance = PerformanceTracker(self.trade_tracker)
        self.last_scan_time: Optional[datetime] = None
    
    def generate_signal_id(self, symbol: str, timestamp: datetime) -> str:
        return f"{symbol.replace('/', '').replace(':', '_')}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
    
    def is_trading_hours(self) -> bool:
        now = datetime.now(pytz.UTC)
        return TRADING_START_HOUR <= now.hour < TRADING_END_HOUR
    
    async def scan_symbol(self, symbol: str) -> Optional[Signal]:
        if not self.trade_tracker.can_trade_symbol(symbol):
            return None
        
        df_15m = await self.fetcher.fetch_time_series(symbol, TIMEFRAMES["signal"], 500)
        df_1h = await self.fetcher.fetch_time_series(symbol, TIMEFRAMES["trend"], 300)
        
        # SOGEKING fetches 4h for triple confirmation
        df_4h = None
        if PERSONALITY == "sogeking":
            df_4h = await self.fetcher.fetch_time_series(symbol, TIMEFRAMES["higher"], 200)
        
        if df_15m is None or df_1h is None:
            return None
        
        if len(df_15m) < 50 or len(df_1h) < 50:
            return None
        
        if PERSONALITY == "sogeking" and (df_4h is None or len(df_4h) < 50):
            logger.info(f"{symbol}: Insufficient 4h data for SOGEKING")
            return None
        
        score, direction, strategy_type, adx = self.scorer.calculate_score(df_15m, df_1h, df_4h)
        
        if direction == "NEUTRAL":
            return None
        
        if adx < ADX_THRESHOLD:
            logger.info(f"{symbol}: ADX {adx:.1f} below threshold {ADX_THRESHOLD}")
            return None
        
        if score < MIN_SCORE:
            logger.info(f"{symbol}: Score {score} below minimum {MIN_SCORE}")
            return None
        
        if self.trade_tracker.has_active_signal(symbol, direction):
            logger.info(f"{symbol}: Duplicate {direction} signal prevented")
            return None
        
        entry, sl, tp, atr, rr = self.risk_manager.calculate_dynamic_levels(df_15m, direction)
        
        if rr < MIN_RR_RATIO:
            logger.info(f"{symbol}: R:R {rr:.2f} below minimum {MIN_RR_RATIO}")
            return None
        
        # SOGEKING: Additional quality filters
        if PERSONALITY == "sogeking":
            # Require minimum ATR for volatility
            if atr < entry * 0.0005:  # 0.05% of price
                logger.info(f"{symbol}: ATR too low for SOGEKING")
                return None
            
            # Calculate confidence - must be 80+
            confidence = min(95, 30 + score * 8)
            if confidence < 80:
                logger.info(f"{symbol}: Confidence {confidence}% below 80% for SOGEKING")
                return None
        else:
            confidence = min(95, 40 + score * 9)
        
        signal = Signal(
            id=self.generate_signal_id(symbol, datetime.now()),
            symbol=symbol,
            direction=direction,
            score=score,
            confidence=confidence,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            atr_value=atr,
            risk_reward=rr,
            adx_value=adx,
            strategy_type=strategy_type or "Unknown",
            timestamp=datetime.now(),
            timeframe=TIMEFRAMES["signal"]
        )
        
        self.trade_tracker.add_signal(signal)
        
        return signal
    
    async def evaluate_active_trades(self):
        for trade in list(self.trade_tracker.active_trades):
            try:
                df = await self.fetcher.fetch_time_series(
                    trade.signal.symbol, 
                    trade.signal.timeframe, 
                    100
                )
                if df is not None:
                    await self.trade_tracker.evaluate_trade(trade, df)
            except Exception as e:
                logger.error(f"Error evaluating trade {trade.signal.id}: {e}")
    
    async def run_scanner(self):
        personality_name = "SOGEKING" if PERSONALITY == "sogeking" else "NAMI"
        logger.info(f"🧭 {personality_name} Engine starting...")
        
        strategy_text = (
            "• SME Pattern (Session Manipulation/Expansion)\n"
            "• Breakout Retest (Continuation)\n"
            "• VWAP Mean Reversion\n"
            "• Liquidity Sweep (Stop Hunt)\n"
            "• Opening Range Breakout"
        ) if PERSONALITY == "nami" else (
            "🔥 SNIPER MODE ACTIVATED 🔥\n"
            "• Triple Timeframe Confirmation (15m/1h/4h)\n"
            "• Only 80-90% Probability Setups\n"
            "• Minimum 1:2 R:R (Target 1:3)\n"
            "• Extended 4h Cooldown Between Trades\n"
            "• Elite Confluence Required"
        )
        
        await self.telegram.send_message(
            f"🧭 <b>{personality_name} {'SNIPER' if PERSONALITY == 'sogeking' else 'Performance'} Engine</b> is LIVE\n\n"
            f"<b>Trading Hours:</b> {TRADING_START_HOUR:02d}:00 - {TRADING_END_HOUR:02d}:00 UTC\n"
            f"{strategy_text}\n\n"
            f"<b>Min Score:</b> {MIN_SCORE}/{6 if PERSONALITY == 'nami' else 8} | "
            f"<b>ADX Threshold:</b> {ADX_THRESHOLD}\n"
            f"<b>Cooldown:</b> {COOLDOWN_MINUTES} min | "
            f"<b>TP:</b> {ATR_MULTIPLIER_TP:.1f} ATR | "
            f"<b>Min R:R:</b> {MIN_RR_RATIO}:1\n"
            f"<b>Daily Report:</b> {REPORT_HOUR}:00 {REPORT_TIMEZONE}"
        )
        
        while True:
            try:
                if not self.is_trading_hours():
                    now = datetime.now(pytz.UTC)
                    next_start = now.replace(hour=TRADING_START_HOUR, minute=0, second=0, microsecond=0)
                    if now.hour >= TRADING_END_HOUR:
                        next_start += timedelta(days=1)
                    wait_seconds = (next_start - now).total_seconds()
                    logger.info(f"Outside trading hours. Waiting {wait_seconds/3600:.1f} hours...")
                    await asyncio.sleep(min(wait_seconds, 300))
                    continue
                
                start_time = datetime.now()
                logger.info(f"=== {personality_name} scan at {start_time} ===")
                
                await self.evaluate_active_trades()
                
                for symbol in WATCHLIST:
                    try:
                        signal = await self.scan_symbol(symbol)
                        
                        if signal:
                            message = self.telegram.format_signal(signal)
                            await self.telegram.send_message(message)
                            logger.info(f"✅ {'SNIPER SHOT' if PERSONALITY == 'sogeking' else 'SIGNAL'}: "
                                      f"{signal.symbol} {signal.direction} "
                                      f"Strategy:{signal.strategy_type} Score:{signal.score} "
                                      f"Conf:{signal.confidence}% ADX:{signal.adx_value:.1f} RR:{signal.risk_reward:.1f}")
                        
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error scanning {symbol}: {e}")
                        continue
                
                stats = self.trade_tracker.get_daily_stats()
                logger.info(
                    f"=== Scan complete | Active: {len(self.trade_tracker.active_trades)} | "
                    f"Closed: {len(self.trade_tracker.closed_trades)} | "
                    f"Today's R: {stats.total_r:+.2f} ==="
                )
                
                self.last_scan_time = datetime.now()
                await asyncio.sleep(SCAN_INTERVAL)
                
            except Exception as e:
                logger.error(f"Scanner error: {e}")
                await asyncio.sleep(60)
    
    async def run(self):
        await asyncio.gather(
            self.run_scanner(),
            self.performance.schedule_daily_report(self.telegram)
        )

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
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        exit(1)
    
    logger.info(f"Starting with personality: {PERSONALITY.upper()}")
    
    engine = NamiEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Engine stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
Is the code
