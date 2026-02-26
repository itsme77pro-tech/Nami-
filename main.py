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
from typing import Optional, Tuple, List, Dict, Literal, Set
from datetime import datetime, timedelta, time
from enum import Enum, auto
from pathlib import Path
import pytz
from collections import defaultdict, deque
import hashlib
from abc import ABC, abstractmethod

# ================= CONFIGURATION =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

# Personality selection: "nami" or "sogeking"
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

DATA_SOURCE = os.getenv("DATA_SOURCE", "twelvedata").lower()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# Session timing configuration
SESSION_LONDON_START = int(os.getenv("SESSION_LONDON_START", "8"))  # UTC
SESSION_LONDON_END = int(os.getenv("SESSION_LONDON_END", "17"))     # UTC
SESSION_NY_START = int(os.getenv("SESSION_NY_START", "13"))         # UTC
SESSION_NY_END = int(os.getenv("SESSION_NY_END", "22"))             # UTC
SESSION_OVERLAP_ONLY = os.getenv("SESSION_OVERLAP_ONLY", "false").lower() == "true"

# Risk management configuration
SPREAD_THRESHOLD_PCT = float(os.getenv("SPREAD_THRESHOLD_PCT", "0.3"))  # 0.3% of ATR
SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.1")) / 100  # 0.1% default
PARTIAL_TP_PCT = float(os.getenv("PARTIAL_TP_PCT", "0.5"))  # Close 50% at TP1
TRAILING_ACTIVATION_R = float(os.getenv("TRAILING_ACTIVATION_R", "1.0"))  # Activate at 1R
VOLATILITY_PERCENTILE_THRESHOLD = int(os.getenv("VOLATILITY_PERCENTILE", "20"))  # Bottom 20% = compression

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
MIN_SCORE = 4 if PERSONALITY == "nami" else 6
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
    BREAKEVEN = "breakeven"
    PARTIAL = "partial"

class TradeOutcome(Enum):
    NONE = "none"
    TP_HIT = "tp_hit"
    SL_HIT = "sl_hit"
    EXPIRED = "expired"
    BREAKEVEN = "breakeven"
    TRAILING_STOP = "trailing_stop"
    PARTIAL_TP = "partial_tp"

class MarketRegime(Enum):
    TRENDING = auto()
    RANGING = auto()
    VOLATILE = auto()
    COMPRESSION = auto()

@dataclass
class Signal:
    id: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    score: float  # Changed to float for weighted scoring
    confidence: int
    entry_price: float
    stop_loss: float
    take_profit: float
    take_profit_1: Optional[float] = None  # For partial profits
    atr_value: float
    risk_reward: float
    adx_value: float
    strategy_type: str
    triggered_strategies: List[str]
    timestamp: datetime
    timeframe: str = "15min"
    market_regime: MarketRegime = MarketRegime.TRENDING
    weighted_score: float = 0.0
    spread_at_signal: float = 0.0
    slippage_estimate: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat(),
            'triggered_strategies': ','.join(self.triggered_strategies),
            'market_regime': self.market_regime.name
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
    partial_exit_price: Optional[float] = None
    partial_exit_time: Optional[datetime] = None
    trailing_stop_active: bool = False
    trailing_stop_level: Optional[float] = None
    realized_pnl: float = 0.0  # For partial profits
    
    @property
    def is_closed(self) -> bool:
        return self.status in [TradeStatus.WIN, TradeStatus.LOSS, TradeStatus.EXPIRED, TradeStatus.BREAKEVEN]
    
    @property
    def profit_in_r(self) -> float:
        if self.status == TradeStatus.WIN:
            return self.signal.risk_reward
        elif self.status == TradeStatus.LOSS:
            return -1.0
        elif self.status == TradeStatus.BREAKEVEN:
            return 0.0
        elif self.status == TradeStatus.PARTIAL:
            # Weighted average of partial profit and remaining position
            partial_r = self.signal.risk_reward * PARTIAL_TP_PCT
            remaining_r = self.actual_rr * (1 - PARTIAL_TP_PCT)
            return partial_r + remaining_r
        elif self.status == TradeStatus.EXPIRED:
            if self.exit_price:
                risk = abs(self.signal.entry_price - self.signal.stop_loss)
                actual = self.exit_price - self.signal.entry_price
                if self.signal.direction == "SELL":
                    actual = -actual
                return actual / risk if risk != 0 else 0
        return 0.0

@dataclass
class LiquidityLevel:
    price: float
    level_type: Literal["equal_high", "equal_low", "session_high", "session_low", "weekly_high", "weekly_low"]
    strength: int  # Number of touches
    timestamp: datetime
    is_swept: bool = False

@dataclass
class DailyStats:
    date: str
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    expired: int = 0
    breakeven: int = 0
    partials: int = 0
    total_r: float = 0.0
    gross_profit_r: float = 0.0
    gross_loss_r: float = 0.0
    max_consecutive_losses: int = 0
    current_consecutive_losses: int = 0
    max_drawdown_r: float = 0.0
    avg_slippage_cost: float = 0.0
    avg_spread_cost: float = 0.0
    
    @property
    def win_rate(self) -> float:
        closed = self.wins + self.losses + self.breakeven
        return (self.wins / closed * 100) if closed > 0 else 0.0
    
    @property
    def profit_factor(self) -> float:
        return abs(self.gross_profit_r / self.gross_loss_r) if self.gross_loss_r != 0 else float('inf')
    
    @property
    def avg_r_per_trade(self) -> float:
        total = self.wins + self.losses + self.expired + self.breakeven + self.partials
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
        
        # Format triggered strategies with weights
        strategies_text = "\n".join([f"  • {s}" for s in signal.triggered_strategies[:5]])
        if len(signal.triggered_strategies) > 5:
            strategies_text += f"\n  ... and {len(signal.triggered_strategies) - 5} more"
        
        # Add regime info
        regime_emoji = {
            MarketRegime.TRENDING: "📈",
            MarketRegime.RANGING: "↔️",
            MarketRegime.VOLATILE: "⚠️",
            MarketRegime.COMPRESSION: "🌀"
        }.get(signal.market_regime, "📊")
        
        return f"""
{emoji} <b>{personality_name} SIGNAL</b>

<b>Primary Strategy:</b> {signal.strategy_type}
<b>Symbol:</b> <code>{signal.symbol}</code>
<b>Direction:</b> {signal.direction}
<b>Score:</b> {signal.score:.1f}/{12 if PERSONALITY == 'nami' else 13} | <b>Confidence:</b> {confidence_text}
<b>ADX:</b> {signal.adx_value:.1f} | <b>ATR:</b> {signal.atr_value:.5f} | <b>R:R:</b> 1:{signal.risk_reward:.1f}
<b>Regime:</b> {regime_emoji} {signal.market_regime.name}

<b>Triggered Strategies:</b>
{strategies_text}

<b>Entry:</b> <code>{signal.entry_price:.5f}</code>
<b>Stop Loss:</b> <code>{signal.stop_loss:.5f}</code> (Structure-based)
<b>Take Profit:</b> <code>{signal.take_profit:.5f}</code> ({ATR_MULTIPLIER_TP:.1f} ATR)
<b>Partial TP:</b> <code>{signal.take_profit_1:.5f if signal.take_profit_1 else 'N/A'}</code> (50%)

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
            status_emoji = {
                TradeStatus.WIN: "✅",
                TradeStatus.LOSS: "❌",
                TradeStatus.EXPIRED: "⏱️",
                TradeStatus.BREAKEVEN: "➖",
                TradeStatus.PARTIAL: "💰"
            }.get(t.status, "❓")
            
            pnl = f"{t.profit_in_r:+.1f}R"
            strategy = t.signal.strategy_type[:15] + "..." if len(t.signal.strategy_type) > 15 else t.signal.strategy_type
            recent_trades += f"{status_emoji} {t.signal.symbol} {strategy} {pnl}\n"
        
        tagline = "The King rests. Tomorrow we hunt again. 👑" if PERSONALITY == "sogeking" else "Markets are closed. Rest well, trader. 🌙"
        
        return f"""
{performance_emoji} <b>{personality_name} DAILY PERFORMANCE REPORT</b>
<i>{date_str}</i>

<b>📊 SIGNAL STATISTICS</b>
Total Signals: <code>{stats.total_signals}</code>
Wins: <code>{stats.wins}</code> | Losses: <code>{stats.losses}</code> | BE: <code>{stats.breakeven}</code> | Partial: <code>{stats.partials}</code>
Win Rate: <code>{stats.win_rate:.1f}%</code>

<b>💰 PERFORMANCE METRICS</b>
Net R: <code>{stats.total_r:+.2f}R</code>
Gross Profit: <code>+{stats.gross_profit_r:.2f}R</code>
Gross Loss: <code>{stats.gross_loss_r:.2f}R</code>
Profit Factor: <code>{stats.profit_factor:.2f}</code>
Avg R/Trade: <code>{stats.avg_r_per_trade:+.2f}R</code>

<b>🎯 EXECUTION COSTS</b>
Avg Slippage: <code>{stats.avg_slippage_cost:.3f}%</code>
Avg Spread: <code>{stats.avg_spread_cost:.3f}%</code>

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
        self._rate_limiter = asyncio.Semaphore(5)  # Max 5 concurrent requests
    
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
        async with self._rate_limiter:
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
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
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

# ================= REGIME FILTER =================

class RegimeFilter:
    """
    Detects market regime (trending, ranging, volatile, compression) using ADX and ATR.
    Disables inappropriate strategies based on regime.
    """
    
    def __init__(self, adx_threshold: float = 25.0, atr_percentile_lookback: int = 50):
        self.adx_threshold = adx_threshold
        self.atr_percentile_lookback = atr_percentile_lookback
    
    def detect_regime(self, df: pd.DataFrame, analyzer: 'TechnicalAnalyzer') -> MarketRegime:
        """
        Detect market regime based on ADX and ATR percentile.
        
        Args:
            df: Price DataFrame with OHLC data
            analyzer: TechnicalAnalyzer instance for calculations
            
        Returns:
            MarketRegime enum value
        """
        if len(df) < self.atr_percentile_lookback + 10:
            return MarketRegime.TRENDING  # Default to trending if insufficient data
        
        # Calculate ADX
        adx = analyzer.calculate_adx(df, ADX_PERIOD).iloc[-1]
        
        # Calculate ATR and its percentile
        atr = analyzer.calculate_atr(df, ATR_PERIOD)
        current_atr = atr.iloc[-1]
        atr_history = atr.tail(self.atr_percentile_lookback)
        atr_percentile = (atr_history < current_atr).mean() * 100
        
        # Calculate ATR expansion (current vs recent average)
        atr_avg = atr_history.mean()
        atr_expansion = current_atr / atr_avg if atr_avg > 0 else 1.0
        
        # Regime detection logic
        if adx > self.adx_threshold and atr_expansion > 1.2:
            return MarketRegime.TRENDING
        elif adx < 20 and atr_percentile < VOLATILITY_PERCENTILE_THRESHOLD:
            return MarketRegime.COMPRESSION
        elif adx < self.adx_threshold and atr_percentile < 50:
            return MarketRegime.RANGING
        elif atr_expansion > 1.5 or atr_percentile > 80:
            return MarketRegime.VOLATILE
        else:
            return MarketRegime.TRENDING  # Default
    
    def is_strategy_allowed(self, strategy_name: str, regime: MarketRegime) -> bool:
        """
        Check if a strategy should be active in current regime.
        
        Args:
            strategy_name: Name of the strategy
            regime: Current market regime
            
        Returns:
            True if strategy is allowed, False otherwise
        """
        # Mean reversion strategies disabled during trend
        mean_reversion_strategies = ["VWAP Reversion", "EMA Reversion", "Mean Reversion"]
        if any(mr in strategy_name for mr in mean_reversion_strategies):
            return regime in [MarketRegime.RANGING, MarketRegime.COMPRESSION]
        
        # Breakout strategies disabled during range (but enabled in compression for expansion plays)
        breakout_strategies = ["Breakout", "ORB", "SME Pattern"]
        if any(b in strategy_name for b in breakout_strategies):
            return regime in [MarketRegime.TRENDING, MarketRegime.VOLATILE, MarketRegime.COMPRESSION]
        
        # Trend following strategies disabled during range
        trend_strategies = ["EMA Trend", "Trend Following"]
        if any(t in strategy_name for t in trend_strategies):
            return regime in [MarketRegime.TRENDING, MarketRegime.VOLATILE]
        
        # Liquidity sweep works in all regimes but best in trend/volatile
        if "Liquidity Sweep" in strategy_name or "SLSE" in strategy_name:
            return True
        
        return True  # Default allow

# ================= LIQUIDITY MAP ENGINE =================

class LiquidityMapEngine:
    """
    Tracks and maps liquidity levels including session highs/lows, 
    weekly levels, and equal highs/lows for smart money concepts.
    """
    
    def __init__(self, session_lookback: int = 96, weekly_lookback: int = 672):  # 15min candles
        self.session_lookback = session_lookback  # 24 hours = 96 candles of 15min
        self.weekly_lookback = weekly_lookback    # 7 days = 672 candles
        self.liquidity_levels: Dict[str, List[LiquidityLevel]] = defaultdict(list)
        self.session_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=session_lookback))
        self.equal_levels_tolerance = 0.001  # 0.1% tolerance for equal highs/lows
    
    def update(self, symbol: str, df: pd.DataFrame):
        """
        Update liquidity map with new price data.
        
        Args:
            symbol: Trading symbol
            df: Recent price DataFrame
        """
        if len(df) < 10:
            return
        
        # Update session history
        for idx, row in df.iterrows():
            self.session_history[symbol].append({
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'timestamp': idx
            })
        
        # Calculate session levels (last 24 hours)
        if len(self.session_history[symbol]) >= self.session_lookback:
            recent_data = list(self.session_history[symbol])[-self.session_lookback:]
            session_high = max(d['high'] for d in recent_data)
            session_low = min(d['low'] for d in recent_data)
            
            self._add_or_update_level(symbol, session_high, "session_high", recent_data[-1]['timestamp'])
            self._add_or_update_level(symbol, session_low, "session_low", recent_data[-1]['timestamp'])
        
        # Detect equal highs/lows
        self._detect_equal_levels(symbol, df)
    
    def _add_or_update_level(self, symbol: str, price: float, level_type: str, timestamp: datetime):
        """Add new liquidity level or update existing one."""
        # Check if level already exists within tolerance
        for level in self.liquidity_levels[symbol]:
            if abs(level.price - price) / price < self.equal_levels_tolerance:
                level.strength += 1
                level.timestamp = timestamp
                return
        
        # Add new level
        new_level = LiquidityLevel(
            price=price,
            level_type=level_type,
            strength=1,
            timestamp=timestamp
        )
        self.liquidity_levels[symbol].append(new_level)
        
        # Cleanup old levels
        self._cleanup_levels(symbol)
    
    def _detect_equal_levels(self, symbol: str, df: pd.DataFrame):
        """Detect equal highs and lows (double tops/bottoms)."""
        if len(df) < 20:
            return
        
        recent = df.tail(20)
        highs = recent['high'].values
        lows = recent['low'].values
        
        # Find equal highs (within tolerance)
        for i in range(len(highs)):
            for j in range(i+1, len(highs)):
                if abs(highs[i] - highs[j]) / highs[i] < self.equal_levels_tolerance:
                    self._add_or_update_level(symbol, highs[i], "equal_high", df.index[j])
        
        # Find equal lows
        for i in range(len(lows)):
            for j in range(i+1, len(lows)):
                if abs(lows[i] - lows[j]) / lows[i] < self.equal_levels_tolerance:
                    self._add_or_update_level(symbol, lows[i], "equal_low", df.index[j])
    
    def _cleanup_levels(self, symbol: str, max_levels: int = 20):
        """Keep only the most relevant liquidity levels."""
        levels = self.liquidity_levels[symbol]
        if len(levels) > max_levels:
            # Sort by strength (descending) and recency
            levels.sort(key=lambda x: (x.strength, x.timestamp), reverse=True)
            self.liquidity_levels[symbol] = levels[:max_levels]
    
    def get_nearest_liquidity(self, symbol: str, price: float, direction: str) -> Optional[LiquidityLevel]:
        """
        Get nearest unswept liquidity level in specified direction.
        
        Args:
            symbol: Trading symbol
            price: Current price
            direction: "BUY" (look for lows) or "SELL" (look for highs)
            
        Returns:
            Nearest LiquidityLevel or None
        """
        if symbol not in self.liquidity_levels:
            return None
        
        relevant_levels = []
        for level in self.liquidity_levels[symbol]:
            if level.is_swept:
                continue
            
            if direction == "BUY" and level.price < price:
                relevant_levels.append((price - level.price, level))
            elif direction == "SELL" and level.price > price:
                relevant_levels.append((level.price - price, level))
        
        if not relevant_levels:
            return None
        
        # Return nearest level
        relevant_levels.sort(key=lambda x: x[0])
        return relevant_levels[0][1]
    
    def mark_level_swept(self, symbol: str, price: float, tolerance: float = 0.002):
        """Mark liquidity level as swept if price reached it."""
        if symbol not in self.liquidity_levels:
            return
        
        for level in self.liquidity_levels[symbol]:
            if abs(level.price - price) / price < tolerance:
                level.is_swept = True

# ================= TECHNICAL ANALYZER (15 STRATEGIES + FIXES) =================

class TechnicalAnalyzer:
    
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate Average True Range using Wilder's smoothing method.
        """
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        
        # Wilder's smoothing (RMA)
        atr = true_range.ewm(alpha=1/period, adjust=False).mean()
        return atr
    
    @staticmethod
    def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate ADX using correct Wilder methodology.
        
        Proper implementation with:
        - Correct +DM/-DM logic
        - Smoothed True Range
        - Smoothed DI+ and DI-
        - DX calculation
        - Smoothed ADX
        """
        high = df['high']
        low = df['low']
        close = df['close']
        
        # Calculate True Range
        tr1 = high - low
        tr2 = np.abs(high - close.shift())
        tr3 = np.abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Calculate +DM and -DM
        plus_dm = high.diff()
        minus_dm = -low.diff()
        
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
        
        # Wilder's smoothing for TR, +DM, -DM
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
        
        # Calculate DX
        dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        
        # Smooth DX to get ADX
        adx = dx.ewm(alpha=1/period, adjust=False).mean()
        
        return adx
    
    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> pd.Series:
        """Calculate Volume Weighted Average Price."""
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
        return vwap
    
    @staticmethod
    def calculate_ema_deviation(df: pd.DataFrame, period: int = 20) -> Tuple[pd.Series, pd.Series]:
        """
        Calculate EMA and price deviation from it for mean reversion.
        
        Returns:
            Tuple of (EMA series, deviation percentage series)
        """
        ema = df['close'].ewm(span=period, adjust=False).mean()
        deviation = (df['close'] - ema) / ema
        return ema, deviation
    
    @staticmethod
    def ema_trend(df: pd.DataFrame, fast: int = 20, slow: int = 50) -> str:
        """Determine trend direction using EMA crossover."""
        if len(df) < slow:
            return "NEUTRAL"
        
        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        
        if ema_fast.iloc[-1] > ema_slow.iloc[-1]:
            return "BUY"
        else:
            return "SELL"
    
    # ========== ORIGINAL 8 STRATEGIES (FIXED) ==========
    
    @staticmethod
    def breakout(df: pd.DataFrame, lookback: int = 20) -> Optional[str]:
        """
        Fixed breakout logic - excludes current candle from range calculation.
        
        Args:
            df: Price DataFrame
            lookback: Number of periods to look back (excluding current)
            
        Returns:
            "BUY" for upside breakout, "SELL" for downside, None for no breakout
        """
        if len(df) < lookback + 1:
            return None
        
        # Use only historical data, excluding current candle
        historical = df.iloc[-lookback-1:-1]
        current = df.iloc[-1]
        
        range_high = historical["high"].max()
        range_low = historical["low"].min()
        
        # Current close must break the range
        if current['close'] > range_high:
            return "BUY"
        if current['close'] < range_low:
            return "SELL"
        return None
    
    @staticmethod
    def bull_flag(df: pd.DataFrame) -> Optional[str]:
        """Bull/Bear flag pattern detection."""
        if len(df) < 20:
            return None
        
        impulse = df["close"].iloc[-15:-10]
        flag = df["close"].iloc[-5:]
        
        if impulse.iloc[-1] > impulse.iloc[0]:
            if flag.max() - flag.min() < impulse.mean() * 0.01:
                return "BUY"
        
        if impulse.iloc[-1] < impulse.iloc[0]:
            if flag.max() - flag.min() < abs(impulse.mean()) * 0.01:
                return "SELL"
        
        return None
    
    @staticmethod
    def double_top_bottom(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        """Double top/bottom pattern detection."""
        if len(df) < lookback + 2:
            return None
        
        highs = df["high"].tail(lookback)
        lows = df["low"].tail(lookback)
        
        if abs(highs.iloc[-1] - highs.iloc[-3]) < highs.mean() * 0.001:
            return "SELL"
        
        if abs(lows.iloc[-1] - lows.iloc[-3]) < lows.mean() * 0.001:
            return "BUY"
        
        return None
    
    @staticmethod
    def head_shoulders(df: pd.DataFrame) -> Optional[str]:
        """Head and shoulders pattern detection."""
        if len(df) < 20:
            return None
        
        highs = df["high"].tail(20)
        head_idx = highs.idxmax()
        head_val = highs.max()
        
        left = highs.loc[:head_idx]
        right = highs.loc[head_idx:]
        
        if len(left) > 0 and len(right) > 0:
            if left.max() < head_val and right.max() < head_val:
                return "SELL"
        
        return None
    
    @staticmethod
    def triangle(df: pd.DataFrame) -> Optional[str]:
        """Triangle pattern detection."""
        if len(df) < 20:
            return None
        
        highs = df["high"].tail(20)
        lows = df["low"].tail(20)
        
        if highs.std() < highs.mean() * 0.001:
            return "BUY"
        
        if lows.std() < lows.mean() * 0.001:
            return "SELL"
        
        return None
    
    @staticmethod
    def fair_value_gap(df: pd.DataFrame) -> Optional[str]:
        """Fair Value Gap (FVG) detection."""
        if len(df) < 3:
            return None
        
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        body = abs(c2["close"] - c2["open"])
        rng = c2["high"] - c2["low"]
        
        if rng == 0 or body < 0.6 * rng:
            return None
        
        if c3["low"] > c1["high"]:
            return "BUY"
        
        if c3["high"] < c1["low"]:
            return "SELL"
        
        return None
    
    @staticmethod
    def volume_spike(df: pd.DataFrame, lookback: int = 20, threshold: float = 1.5) -> bool:
        """Detect volume spike above threshold."""
        if "volume" not in df.columns or df["volume"].sum() == 0:
            return False
        return df["volume"].iloc[-1] > df["volume"].tail(lookback).mean() * threshold
    
    # ========== 5 NEW STRATEGIES ==========
    
    @staticmethod
    def sme_pattern(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        """Smart Money Expansion pattern."""
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
        """Breakout and retest strategy."""
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
    def ema_mean_reversion(df: pd.DataFrame, lookback: int = 20, threshold: float = 0.002) -> Optional[str]:
        """
        EMA deviation mean reversion (replaces VWAP for forex).
        
        Args:
            df: Price DataFrame
            lookback: EMA period
            threshold: Deviation threshold (0.002 = 0.2%)
            
        Returns:
            "BUY" if price below EMA threshold, "SELL" if above, None otherwise
        """
        if len(df) < lookback:
            return None
        
        ema, deviation = TechnicalAnalyzer.calculate_ema_deviation(df, lookback)
        current_deviation = deviation.iloc[-1]
        
        # Check for mean reversion setup
        if current_deviation < -threshold:
            # Price too far below EMA, look for bullish reversal candle
            if df['close'].iloc[-1] > df['open'].iloc[-1]:  # Bullish candle
                return "BUY"
        
        if current_deviation > threshold:
            # Price too far above EMA, look for bearish reversal candle
            if df['close'].iloc[-1] < df['open'].iloc[-1]:  # Bearish candle
                return "SELL"
        
        return None
    
    @staticmethod
    def vwap_mean_reversion_crypto(df: pd.DataFrame, threshold: float = 0.002) -> Optional[str]:
        """
        VWAP mean reversion - ONLY for crypto assets with valid volume.
        
        Args:
            df: Price DataFrame with volume
            threshold: Deviation threshold
            
        Returns:
            Signal direction or None
        """
        if len(df) < 20 or df['volume'].sum() == 0:
            return None
        
        vwap = TechnicalAnalyzer.calculate_vwap(df)
        current_price = df['close'].iloc[-1]
        current_vwap = vwap.iloc[-1]
        
        deviation = (current_price - current_vwap) / current_vwap
        
        if deviation > threshold:
            if df['close'].iloc[-1] < df['open'].iloc[-1]:
                return "SELL"
        
        if deviation < -threshold:
            if df['close'].iloc[-1] > df['open'].iloc[-1]:
                return "BUY"
        
        return None
    
    @staticmethod
    def liquidity_sweep(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        """Liquidity sweep detection."""
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
        """Opening Range Breakout (ORB) strategy."""
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
    
    # ========== 2 NEW ADVANCED STRATEGIES (IMPROVED) ==========
    
    @staticmethod
    def session_liquidity_sweep_engine(df: pd.DataFrame, session_lookback: int = 24) -> Optional[str]:
        """
        Improved Session Liquidity Sweep Engine (SLSE) with displacement detection.
        
        Args:
            df: Price DataFrame
            session_lookback: Periods defining session (24 = 6 hours on 15min)
            
        Returns:
            Signal direction or None
        """
        if len(df) < session_lookback + 5:
            return None
        
        session_data = df.iloc[-session_lookback:]
        session_high = session_data['high'].max()
        session_low = session_data['low'].min()
        
        recent_candles = df.iloc[-5:]
        
        # Check for displacement (large momentum candle) after sweep
        def is_displacement(candle, direction):
            body = abs(candle['close'] - candle['open'])
            range_val = candle['high'] - candle['low']
            if range_val == 0:
                return False
            body_ratio = body / range_val
            
            if direction == "BUY":
                return body_ratio > 0.7 and candle['close'] > candle['open'] and body > session_data['close'].std()
            else:
                return body_ratio > 0.7 and candle['close'] < candle['open'] and body > session_data['close'].std()
        
        # Bullish SLSE
        for i in range(len(recent_candles) - 1, -1, -1):
            candle = recent_candles.iloc[i]
            
            if candle['low'] < session_low * 1.001:  # Sweep below session low
                if i < len(recent_candles) - 1:
                    next_candles = recent_candles.iloc[i+1:]
                    for _, rev_candle in next_candles.iterrows():
                        if is_displacement(rev_candle, "BUY"):
                            return "BUY"
        
        # Bearish SLSE
        for i in range(len(recent_candles) - 1, -1, -1):
            candle = recent_candles.iloc[i]
            
            if candle['high'] > session_high * 0.999:  # Sweep above session high
                if i < len(recent_candles) - 1:
                    next_candles = recent_candles.iloc[i+1:]
                    for _, rev_candle in next_candles.iterrows():
                        if is_displacement(rev_candle, "SELL"):
                            return "SELL"
        
        return None
    
    @staticmethod
    def daily_liquidity_cycle_model(df: pd.DataFrame, df_1h: Optional[pd.DataFrame] = None) -> Optional[str]:
        """
        Improved Daily Liquidity Cycle Model (DLCM) with imbalance detection.
        
        Args:
            df: 15min DataFrame
            df_1h: Optional 1h DataFrame for higher timeframe context
            
        Returns:
            Signal direction or None
        """
        if len(df) < 48:
            return None
        
        day_high = df['high'].tail(48).max()
        day_low = df['low'].tail(48).min()
        current_price = df['close'].iloc[-1]
        
        if day_high == day_low:
            return None
        
        position_in_range = (current_price - day_low) / (day_high - day_low)
        
        recent_volume = df['volume'].tail(12).mean()
        avg_volume = df['volume'].tail(48).mean()
        
        # Imbalance detection (large wicks)
        last_candle = df.iloc[-1]
        upper_wick = last_candle['high'] - max(last_candle['open'], last_candle['close'])
        lower_wick = min(last_candle['open'], last_candle['close']) - last_candle['low']
        body = abs(last_candle['close'] - last_candle['open'])
        
        # Distribution at highs
        if position_in_range > 0.85 and recent_volume > avg_volume * 1.2:
            recent_candles = df.tail(3)
            bearish_count = sum(1 for _, c in recent_candles.iterrows() if c['close'] < c['open'])
            if bearish_count >= 2:
                return "SELL"
        
        # Accumulation at lows
        if position_in_range < 0.15 and recent_volume > avg_volume * 1.2:
            recent_candles = df.tail(3)
            bullish_count = sum(1 for _, c in recent_candles.iterrows() if c['close'] > c['open'])
            if bullish_count >= 2:
                return "BUY"
        
        # Mid-range manipulation with imbalance
        if 0.4 < position_in_range < 0.6:
            if upper_wick > body * 2 and last_candle['close'] < last_candle['open']:
                return "SELL"
            
            if lower_wick > body * 2 and last_candle['close'] > last_candle['open']:
                return "BUY"
        
        return None

# ================= SIGNAL SCORER (WEIGHTED) =================

class SignalScorer:
    """
    Weighted signal scoring engine with regime awareness.
    """
    
    # Strategy weights configuration
    STRATEGY_WEIGHTS = {
        # Trend alignment (highest weight)
        "EMA Trend 15m": 2.0,
        "EMA Trend 1h": 2.0,
        "EMA Trend 4h": 2.0,
        
        # Liquidity and structure (high weight)
        "Liquidity Sweep": 2.5,
        "SLSE": 2.5,
        "Breakout Retest": 2.0,
        
        # Fair value (medium-high weight)
        "Fair Value Gap": 1.5,
        "FVG": 1.5,
        
        # Patterns (medium weight)
        "Breakout": 1.0,
        "Bull/Bear Flag": 1.0,
        "Double Top/Bottom": 1.0,
        "Head & Shoulders": 1.0,
        "Triangle": 1.0,
        "SME Pattern": 1.0,
        "ORB": 1.0,
        
        # Mean reversion (medium weight, regime dependent)
        "EMA Reversion": 1.0,
        "VWAP Reversion": 1.0,
        
        # Volume (lower weight, confirmation only)
        "Volume Spike": 0.5,
        
        # Advanced models
        "DLCM": 1.5,
    }
    
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
        self.regime_filter = RegimeFilter()
    
    def calculate_score(
        self, 
        df_15m: pd.DataFrame, 
        df_1h: pd.DataFrame, 
        df_4h: Optional[pd.DataFrame] = None,
        is_crypto: bool = False
    ) -> Tuple[float, str, Optional[str], float, List[str], MarketRegime, float]:
        """
        Calculate weighted score using all 15 strategies with regime filtering.
        
        Args:
            df_15m: 15-minute timeframe data
            df_1h: 1-hour timeframe data
            df_4h: 4-hour timeframe data (optional)
            is_crypto: Whether symbol is cryptocurrency (for VWAP usage)
            
        Returns:
            Tuple of (weighted_score, direction, primary_strategy, adx, 
                     triggered_strategies, regime, raw_score)
        """
        # Detect market regime first
        regime = self.regime_filter.detect_regime(df_15m, self.analyzer)
        
        direction = None
        raw_score = 0
        weighted_score = 0.0
        triggered_strategies = []
        primary_strategy = None
        
        # 1. EMA Trend (15m) - Base direction
        ema_15m = self.analyzer.ema_trend(df_15m)
        if ema_15m != "NEUTRAL":
            direction = ema_15m
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("EMA Trend 15m", 1.0)
            if self.regime_filter.is_strategy_allowed("EMA Trend 15m", regime):
                weighted_score += weight
                triggered_strategies.append(f"EMA Trend 15m ({weight}x)")
        
        # 2. EMA Trend (1h) - Higher timeframe alignment
        ema_1h = self.analyzer.ema_trend(df_1h)
        if ema_1h == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("EMA Trend 1h", 1.0)
            if self.regime_filter.is_strategy_allowed("EMA Trend 1h", regime):
                weighted_score += weight
                triggered_strategies.append(f"EMA Trend 1h ({weight}x)")
        
        # 3. Breakout (FIXED - excludes current candle)
        breakout = self.analyzer.breakout(df_15m)
        if breakout == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Breakout", 1.0)
            if self.regime_filter.is_strategy_allowed("Breakout", regime):
                weighted_score += weight
                triggered_strategies.append(f"Breakout ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Breakout"
        
        # 4. Bull/Bear Flag
        flag = self.analyzer.bull_flag(df_15m)
        if flag == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Bull/Bear Flag", 1.0)
            if self.regime_filter.is_strategy_allowed("Bull/Bear Flag", regime):
                weighted_score += weight
                triggered_strategies.append(f"Bull/Bear Flag ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Flag"
        
        # 5. Double Top/Bottom
        dt = self.analyzer.double_top_bottom(df_15m)
        if dt == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Double Top/Bottom", 1.0)
            if self.regime_filter.is_strategy_allowed("Double Top/Bottom", regime):
                weighted_score += weight
                triggered_strategies.append(f"Double Top/Bottom ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Double Top/Bottom"
        
        # 6. Head & Shoulders
        hs = self.analyzer.head_shoulders(df_15m)
        if hs == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Head & Shoulders", 1.0)
            if self.regime_filter.is_strategy_allowed("Head & Shoulders", regime):
                weighted_score += weight
                triggered_strategies.append(f"Head & Shoulders ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Head & Shoulders"
        
        # 7. Triangle
        triangle = self.analyzer.triangle(df_15m)
        if triangle == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Triangle", 1.0)
            if self.regime_filter.is_strategy_allowed("Triangle", regime):
                weighted_score += weight
                triggered_strategies.append(f"Triangle ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Triangle"
        
        # 8. Fair Value Gap
        fvg = self.analyzer.fair_value_gap(df_15m)
        if fvg == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Fair Value Gap", 1.5)
            if self.regime_filter.is_strategy_allowed("Fair Value Gap", regime):
                weighted_score += weight
                triggered_strategies.append(f"FVG ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "FVG"
        
        # 9. Volume Spike
        if self.analyzer.volume_spike(df_15m):
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Volume Spike", 0.5)
            if self.regime_filter.is_strategy_allowed("Volume Spike", regime):
                weighted_score += weight
                triggered_strategies.append(f"Volume Spike ({weight}x)")
        
        # 10. SME Pattern
        sme = self.analyzer.sme_pattern(df_15m)
        if sme == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("SME Pattern", 1.0)
            if self.regime_filter.is_strategy_allowed("SME Pattern", regime):
                weighted_score += weight
                triggered_strategies.append(f"SME Pattern ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "SME Pattern"
        
        # 11. Breakout Retest (HIGH WEIGHT)
        retest = self.analyzer.breakout_retest(df_15m)
        if retest == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Breakout Retest", 2.0)
            if self.regime_filter.is_strategy_allowed("Breakout Retest", regime):
                weighted_score += weight
                triggered_strategies.append(f"Breakout Retest ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Breakout Retest"
        
        # 12. Mean Reversion (Regime-dependent: EMA for forex, VWAP for crypto)
        if is_crypto:
            vwap = self.analyzer.vwap_mean_reversion_crypto(df_15m)
            if vwap == direction:
                raw_score += 1
                weight = self.STRATEGY_WEIGHTS.get("VWAP Reversion", 1.0)
                if self.regime_filter.is_strategy_allowed("VWAP Reversion", regime):
                    weighted_score += weight
                    triggered_strategies.append(f"VWAP Reversion ({weight}x)")
                    if primary_strategy is None:
                        primary_strategy = "VWAP Reversion"
        else:
            ema_rev = self.analyzer.ema_mean_reversion(df_15m)
            if ema_rev == direction:
                raw_score += 1
                weight = self.STRATEGY_WEIGHTS.get("EMA Reversion", 1.0)
                if self.regime_filter.is_strategy_allowed("EMA Reversion", regime):
                    weighted_score += weight
                    triggered_strategies.append(f"EMA Reversion ({weight}x)")
                    if primary_strategy is None:
                        primary_strategy = "EMA Reversion"
        
        # 13. Liquidity Sweep (HIGH WEIGHT)
        sweep = self.analyzer.liquidity_sweep(df_15m)
        if sweep == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("Liquidity Sweep", 2.5)
            if self.regime_filter.is_strategy_allowed("Liquidity Sweep", regime):
                weighted_score += weight
                triggered_strategies.append(f"Liquidity Sweep ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Liquidity Sweep"
        
        # 14. Opening Range Breakout
        orb = self.analyzer.opening_range_breakout(df_15m)
        if orb == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("ORB", 1.0)
            if self.regime_filter.is_strategy_allowed("ORB", regime):
                weighted_score += weight
                triggered_strategies.append(f"ORB ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "ORB"
        
        # 15. Session Liquidity Sweep Engine (SLSE) - IMPROVED
        slse = self.analyzer.session_liquidity_sweep_engine(df_15m)
        if slse == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("SLSE", 2.5)
            if self.regime_filter.is_strategy_allowed("SLSE", regime):
                weighted_score += weight
                triggered_strategies.append(f"SLSE ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "SLSE"
        
        # 16. Daily Liquidity Cycle Model (DLCM) - IMPROVED
        dlcm = self.analyzer.daily_liquidity_cycle_model(df_15m, df_1h)
        if dlcm == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS.get("DLCM", 1.5)
            if self.regime_filter.is_strategy_allowed("DLCM", regime):
                weighted_score += weight
                triggered_strategies.append(f"DLCM ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "DLCM"
        
        # SOGEKING: 4h confirmation required
        if PERSONALITY == "sogeking" and df_4h is not None:
            ema_4h = self.analyzer.ema_trend(df_4h)
            if ema_4h == direction:
                raw_score += 1
                weight = self.STRATEGY_WEIGHTS.get("EMA Trend 4h", 2.0)
                weighted_score += weight
                triggered_strategies.append(f"EMA Trend 4h ({weight}x)")
            else:
                # SOGEKING requires 4h alignment - hard filter
                return 0.0, "NEUTRAL", None, 0.0, [], regime, 0.0
        
        adx = self.analyzer.calculate_adx(df_15m, ADX_PERIOD).iloc[-1]
        
        return weighted_score, direction or "NEUTRAL", primary_strategy, adx, triggered_strategies, regime, float(raw_score)

# ================= SESSION FILTER =================

class SessionFilter:
    """
    Filters trading signals based on forex market sessions.
    London: 08:00-17:00 UTC
    New York: 13:00-22:00 UTC
    Overlap: 13:00-17:00 UTC (highest volatility)
    """
    
    def __init__(self):
        self.london_start = time(SESSION_LONDON_START, 0)
        self.london_end = time(SESSION_LONDON_END, 0)
        self.ny_start = time(SESSION_NY_START, 0)
        self.ny_end = time(SESSION_NY_END, 0)
        self.timezone = pytz.UTC
    
    def is_trading_allowed(self, timestamp: Optional[datetime] = None) -> bool:
        """
        Check if current time is within allowed trading sessions.
        
        Args:
            timestamp: Optional timestamp to check (defaults to now)
            
        Returns:
            True if trading is allowed, False otherwise
        """
        if timestamp is None:
            timestamp = datetime.now(self.timezone)
        elif timestamp.tzinfo is None:
            timestamp = self.timezone.localize(timestamp)
        
        current_time = timestamp.time()
        
        # Check London session
        in_london = self.london_start <= current_time <= self.london_end
        
        # Check New York session
        in_ny = self.ny_start <= current_time <= self.ny_end
        
        # Check overlap (both sessions active)
        in_overlap = in_london and in_ny
        
        if SESSION_OVERLAP_ONLY:
            return in_overlap
        
        return in_london or in_ny
    
    def get_current_session(self, timestamp: Optional[datetime] = None) -> str:
        """Get current session name for logging."""
        if timestamp is None:
            timestamp = datetime.now(self.timezone)
        elif timestamp.tzinfo is None:
            timestamp = self.timezone.localize(timestamp)
        
        current_time = timestamp.time()
        
        in_london = self.london_start <= current_time <= self.london_end
        in_ny = self.ny_start <= current_time <= self.ny_end
        
        if in_london and in_ny:
            return "LONDON-NY OVERLAP"
        elif in_london:
            return "LONDON"
        elif in_ny:
            return "NEW YORK"
        else:
            return "OFF-HOURS"

# ================= RISK MANAGER (STRUCTURE-BASED) =================

class RiskManager:
    """
    Advanced risk management with structure-based stop losses,
    spread filtering, and signal clustering prevention.
    """
    
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
        self.recent_signals: Dict[str, List[Tuple[datetime, float, str]]] = defaultdict(list)  # symbol -> [(time, price, id)]
        self.clustering_threshold_atr = 1.0  # Minimum 1 ATR distance between signals
    
    def calculate_structure_based_sl(
        self, 
        df: pd.DataFrame, 
        direction: str,
        liquidity_engine: Optional[LiquidityMapEngine] = None,
        symbol: str = ""
    ) -> Tuple[float, float, str]:
        """
        Calculate stop loss based on market structure, liquidity levels, or ATR fallback.
        
        Args:
            df: Price DataFrame
            direction: "BUY" or "SELL"
            liquidity_engine: Optional LiquidityMapEngine for liquidity-based SL
            symbol: Trading symbol
            
        Returns:
            Tuple of (stop_loss_price, sl_type, description)
        """
        current_price = df["close"].iloc[-1]
        atr = self.analyzer.calculate_atr(df, ATR_PERIOD).iloc[-1]
        
        # 1. Try liquidity level first
        if liquidity_engine and symbol:
            nearest_liq = liquidity_engine.get_nearest_liquidity(symbol, current_price, direction)
            if nearest_liq:
                # Add buffer beyond liquidity level
                buffer = atr * 0.3
                if direction == "BUY":
                    sl = nearest_liq.price - buffer
                else:
                    sl = nearest_liq.price + buffer
                
                # Validate SL is not too far (max 2 ATR)
                if abs(sl - current_price) <= 2 * atr:
                    return sl, "liquidity", f"Liquidity {nearest_liq.level_type}"
        
        # 2. Try swing high/low structure
        swing_sl = self._calculate_swing_sl(df, direction, atr)
        if swing_sl:
            return swing_sl[0], "swing", swing_sl[1]
        
        # 3. Fallback to ATR-based
        if direction == "BUY":
            sl = current_price - (atr * ATR_MULTIPLIER_SL)
        else:
            sl = current_price + (atr * ATR_MULTIPLIER_SL)
        
        return sl, "atr", f"ATR({ATR_PERIOD})x{ATR_MULTIPLIER_SL}"
    
    def _calculate_swing_sl(self, df: pd.DataFrame, direction: str, atr: float) -> Optional[Tuple[float, str]]:
        """
        Calculate SL based on recent swing highs/lows.
        
        Returns:
            Tuple of (price, description) or None
        """
        lookback = 10
        
        if len(df) < lookback + 1:
            return None
        
        recent = df.tail(lookback)
        
        if direction == "BUY":
            # Find recent swing low (lowest low with higher lows on each side)
            for i in range(1, len(recent) - 1):
                if recent['low'].iloc[i] < recent['low'].iloc[i-1] and \
                   recent['low'].iloc[i] < recent['low'].iloc[i+1]:
                    swing_low = recent['low'].iloc[i]
                    # Check it's not too far
                    current_price = df['close'].iloc[-1]
                    if current_price - swing_low <= 2 * atr:
                        return swing_low - (atr * 0.2), f"Swing Low {i}"
        else:
            # Find recent swing high
            for i in range(1, len(recent) - 1):
                if recent['high'].iloc[i] > recent['high'].iloc[i-1] and \
                   recent['high'].iloc[i] > recent['high'].iloc[i+1]:
                    swing_high = recent['high'].iloc[i]
                    current_price = df['close'].iloc[-1]
                    if swing_high - current_price <= 2 * atr:
                        return swing_high + (atr * 0.2), f"Swing High {i}"
        
        return None
    
    def calculate_dynamic_levels(
        self, 
        df: pd.DataFrame, 
        direction: str,
        liquidity_engine: Optional[LiquidityMapEngine] = None,
        symbol: str = ""
    ) -> Tuple[float, float, float, float, float, Optional[float], str]:
        """
        Calculate entry, SL, TP with structure-based SL and partial TP.
        
        Returns:
            Tuple of (entry, sl, tp, tp1, atr, rr, sl_type)
        """
        entry = df["close"].iloc[-1]
        atr = self.analyzer.calculate_atr(df, ATR_PERIOD).iloc[-1]
        
        # Structure-based SL
        sl, sl_type, sl_desc = self.calculate_structure_based_sl(df, direction, liquidity_engine, symbol)
        
        # Calculate TP based on ATR multiple
        if direction == "BUY":
            tp = entry + (atr * ATR_MULTIPLIER_TP)
        else:
            tp = entry - (atr * ATR_MULTIPLIER_TP)
        
        # Partial TP at 50% of the way (or 1:1 R:R whichever is closer)
        risk = abs(entry - sl)
        if risk > 0:
            if direction == "BUY":
                tp1 = entry + risk  # 1:1 R:R
            else:
                tp1 = entry - risk
        else:
            tp1 = None
        
        reward = abs(tp - entry)
        rr = reward / risk if risk != 0 else 0
        
        return entry, sl, tp, tp1, atr, rr, sl_desc
    
    def check_signal_clustering(self, symbol: str, price: float, atr: float) -> bool:
        """
        Check if signal is too close to recent signals (clustering filter).
        
        Returns:
            True if allowed (no clustering), False if too close to existing signal
        """
        current_time = datetime.now()
        min_distance = atr * self.clustering_threshold_atr
        
        # Clean old signals (> 4 hours)
        self.recent_signals[symbol] = [
            (t, p, sid) for t, p, sid in self.recent_signals[symbol]
            if (current_time - t).total_seconds() < 14400
        ]
        
        # Check distance from recent signals
        for sig_time, sig_price, sig_id in self.recent_signals[symbol]:
            if abs(sig_price - price) < min_distance:
                logger.info(f"Signal clustering prevented for {symbol}: {abs(sig_price - price):.5f} < {min_distance:.5f}")
                return False
        
        return True
    
    def register_signal(self, symbol: str, price: float, signal_id: str):
        """Register new signal for clustering tracking."""
        self.recent_signals[symbol].append((datetime.now(), price, signal_id))
    
    def estimate_spread(self, df: pd.DataFrame, symbol: str) -> float:
        """
        Estimate spread as percentage of price using high-low proximity.
        For forex, uses typical spread estimation.
        """
        if "USD" in symbol and "/USD" in symbol:  # Forex pairs
            # Estimate based on ATR and typical forex spreads
            atr = self.analyzer.calculate_atr(df, 5).iloc[-1]  # 5-period ATR
            current_price = df['close'].iloc[-1]
            # Typical forex spread is 0.01% to 0.03% of price
            estimated_spread = current_price * 0.0002  # 0.02% default
            return (estimated_spread / current_price) * 100  # Return as percentage
        else:  # Crypto
            # Crypto typically has higher spreads/volatility
            atr = self.analyzer.calculate_atr(df, 5).iloc[-1]
            current_price = df['close'].iloc[-1]
            estimated_spread = atr * 0.1  # 10% of 5-period ATR as spread proxy
            return (estimated_spread / current_price) * 100
    
    def check_spread_filter(self, df: pd.DataFrame, symbol: str, atr: float) -> bool:
        """
        Check if spread is acceptable relative to ATR.
        
        Returns:
            True if spread acceptable, False if too wide
        """
        spread_pct = self.estimate_spread(df, symbol)
        atr_pct = (atr / df['close'].iloc[-1]) * 100
        threshold = atr_pct * (SPREAD_THRESHOLD_PCT / 100)  # SPREAD_THRESHOLD_PCT% of ATR
        
        if spread_pct > threshold:
            logger.info(f"Spread filter blocked {symbol}: {spread_pct:.3f}% > {threshold:.3f}%")
            return False
        return True
    
    def check_volatility_compression(self, df: pd.DataFrame) -> bool:
        """
        Check if market is in volatility compression (low volatility period).
        
        Returns:
            True if volatility is acceptable, False if compressed (avoid trading)
        """
        if len(df) < 50:
            return True  # Not enough data, allow trading
        
        atr = self.analyzer.calculate_atr(df, ATR_PERIOD)
        current_atr = atr.iloc[-1]
        atr_history = atr.tail(50)
        
        percentile = (atr_history < current_atr).mean() * 100
        
        if percentile < VOLATILITY_PERCENTILE_THRESHOLD:
            logger.info(f"Volatility compression detected: {percentile:.1f}th percentile")
            return False
        
        return True

# ================= TRADE TRACKER (ENHANCED) =================

class TradeTracker:
    """
    Enhanced trade tracking with break-even, trailing stops, and partial TP logic.
    """
    
    def __init__(self):
        self.trades: Dict[str, Trade] = {}
        self.signals_today: List[Signal] = []
        self.active_trades: List[Trade] = []
        self.closed_trades: List[Trade] = []
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.traded_symbols_today: set = set()
        self.cooldowns: Dict[str, datetime] = {}
        self.slippage_stats: List[float] = []
        self.spread_stats: List[float] = []
    
    def add_signal(self, signal: Signal) -> Trade:
        """Add new signal and create trade tracking."""
        self.signals_today.append(signal)
        
        # Apply slippage to entry
        slippage = signal.entry_price * SLIPPAGE_PCT
        if signal.direction == "BUY":
            executed_entry = signal.entry_price + slippage  # Worse fill
        else:
            executed_entry = signal.entry_price - slippage
        
        # Create modified signal with executed price
        executed_signal = Signal(
            id=signal.id,
            symbol=signal.symbol,
            direction=signal.direction,
            score=signal.score,
            confidence=signal.confidence,
            entry_price=executed_entry,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            take_profit_1=signal.take_profit_1,
            atr_value=signal.atr_value,
            risk_reward=signal.risk_reward,
            adx_value=signal.adx_value,
            strategy_type=signal.strategy_type,
            triggered_strategies=signal.triggered_strategies,
            timestamp=signal.timestamp,
            timeframe=signal.timeframe,
            market_regime=signal.market_regime,
            weighted_score=signal.weighted_score,
            spread_at_signal=signal.spread_at_signal,
            slippage_estimate=slippage
        )
        
        trade = Trade(signal=executed_signal)
        self.trades[signal.id] = trade
        self.active_trades.append(trade)
        self.traded_symbols_today.add(signal.symbol)
        
        # Record stats
        self.slippage_stats.append(slippage / signal.entry_price * 100)
        self.spread_stats.append(signal.spread_at_signal)
        
        logger.info(f"New trade tracked: {signal.id} {signal.symbol} {signal.direction} (Entry with slippage: {executed_entry:.5f})")
        return trade
    
    def can_trade_symbol(self, symbol: str) -> bool:
        """Check if symbol is available for trading (cooldown check)."""
        if symbol in self.traded_symbols_today:
            return False
        
        if symbol in self.cooldowns:
            if datetime.now() < self.cooldowns[symbol]:
                return False
            else:
                del self.cooldowns[symbol]
        
        return True
    
    def has_active_signal(self, symbol: str, direction: str) -> bool:
        """Check for duplicate active signals."""
        for trade in self.active_trades:
            if (trade.signal.symbol == symbol and 
                trade.signal.direction == direction and 
                not trade.is_closed):
                return True
        return False
    
    def _determine_first_touch(self, candle: pd.Series, direction: str, sl: float, tp: float) -> Optional[str]:
        """Determine which level was hit first in a candle."""
        open_price = candle['open']
        
        if direction == "BUY":
            distance_to_sl = open_price - sl
            distance_to_tp = tp - open_price
            
            if distance_to_sl <= 0:
                return "SL"
            if distance_to_tp <= 0:
                return "TP"
            
            return "SL" if distance_to_sl < distance_to_tp else "TP"
        else:
            distance_to_sl = sl - open_price
            distance_to_tp = open_price - tp
            
            if distance_to_sl <= 0:
                return "SL"
            if distance_to_tp <= 0:
                return "TP"
            
            return "SL" if distance_to_sl < distance_to_tp else "TP"
    
    def _update_trailing_stop(self, trade: Trade, current_price: float):
        """Update trailing stop if activated."""
        if not trade.trailing_stop_active:
            # Check if we should activate trailing stop (at 1R profit)
            risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
            current_profit = abs(current_price - trade.signal.entry_price)
            
            if current_profit >= risk * TRAILING_ACTIVATION_R:
                trade.trailing_stop_active = True
                # Set initial trailing stop at breakeven + small profit
                if trade.signal.direction == "BUY":
                    trade.trailing_stop_level = trade.signal.entry_price + (risk * 0.2)
                else:
                    trade.trailing_stop_level = trade.signal.entry_price - (risk * 0.2)
                logger.info(f"Trailing stop activated for {trade.signal.id} at {trade.trailing_stop_level:.5f}")
        else:
            # Update trailing stop (lock in profits)
            risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
            if trade.signal.direction == "BUY":
                new_level = current_price - (risk * 0.5)  # Trail at 50% of risk
                if new_level > trade.trailing_stop_level:
                    trade.trailing_stop_level = new_level
            else:
                new_level = current_price + (risk * 0.5)
                if new_level < trade.trailing_stop_level:
                    trade.trailing_stop_level = new_level
    
    async def evaluate_trade(self, trade: Trade, df: pd.DataFrame) -> bool:
        """
        Evaluate trade with support for partial TP, break-even, and trailing stops.
        
        Returns:
            True if trade is closed, False if still active
        """
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
            close = candle['close']
            
            # Update max profit/loss tracking
            if trade.signal.direction == "BUY":
                current_profit = (close - trade.signal.entry_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
            else:
                current_profit = (trade.signal.entry_price - close) / abs(trade.signal.entry_price - trade.signal.stop_loss)
            
            trade.max_profit_reached = max(trade.max_profit_reached, current_profit)
            trade.max_loss_reached = min(trade.max_loss_reached, current_profit)
            
            # Update trailing stop
            self._update_trailing_stop(trade, close)
            
            # Check trailing stop first if active
            if trade.trailing_stop_active and trade.trailing_stop_level:
                if trade.signal.direction == "BUY" and low <= trade.trailing_stop_level:
                    trade.status = TradeStatus.WIN
                    trade.outcome = TradeOutcome.TRAILING_STOP
                    trade.exit_price = trade.trailing_stop_level
                    trade.exit_time = idx
                    trade.actual_rr = (trade.exit_price - trade.signal.entry_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                    self._close_trade(trade)
                    return True
                elif trade.signal.direction == "SELL" and high >= trade.trailing_stop_level:
                    trade.status = TradeStatus.WIN
                    trade.outcome = TradeOutcome.TRAILING_STOP
                    trade.exit_price = trade.trailing_stop_level
                    trade.exit_time = idx
                    trade.actual_rr = (trade.signal.entry_price - trade.exit_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                    self._close_trade(trade)
                    return True
            
            # Check partial TP (if not already hit)
            if trade.status == TradeStatus.PENDING and trade.signal.take_profit_1:
                if trade.signal.direction == "BUY" and high >= trade.signal.take_profit_1:
                    # Move SL to breakeven
                    trade.status = TradeStatus.PARTIAL
                    trade.partial_exit_price = trade.signal.take_profit_1
                    trade.partial_exit_time = idx
                    trade.realized_pnl = abs(trade.signal.take_profit_1 - trade.signal.entry_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                    # Update SL to breakeven (minus small buffer for spread)
                    buffer = trade.signal.atr_value * 0.1
                    trade.signal.stop_loss = trade.signal.entry_price - buffer if trade.signal.direction == "BUY" else trade.signal.entry_price + buffer
                    logger.info(f"Partial TP hit for {trade.signal.id}, moved SL to breakeven")
                    continue  # Continue monitoring for full TP or SL
                
                elif trade.signal.direction == "SELL" and low <= trade.signal.take_profit_1:
                    trade.status = TradeStatus.PARTIAL
                    trade.partial_exit_price = trade.signal.take_profit_1
                    trade.partial_exit_time = idx
                    trade.realized_pnl = abs(trade.signal.entry_price - trade.signal.take_profit_1) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                    buffer = trade.signal.atr_value * 0.1
                    trade.signal.stop_loss = trade.signal.entry_price + buffer if trade.signal.direction == "SELL" else trade.signal.entry_price - buffer
                    logger.info(f"Partial TP hit for {trade.signal.id}, moved SL to breakeven")
                    continue
            
            # Check main TP and SL
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
            
            # Handle simultaneous hits
            if sl_hit and tp_hit:
                first_touch = self._determine_first_touch(
                    candle, trade.signal.direction, 
                    trade.signal.stop_loss, trade.signal.take_profit
                )
                
                if first_touch == "SL":
                    trade.status = TradeStatus.LOSS
                    trade.outcome = TradeOutcome.SL_HIT
                    trade.exit_price = trade.signal.stop_loss
                    trade.actual_rr = -1.0
                else:
                    trade.status = TradeStatus.WIN
                    trade.outcome = TradeOutcome.TP_HIT
                    trade.exit_price = trade.signal.take_profit
                    trade.actual_rr = trade.signal.risk_reward
                
                trade.exit_time = idx
                self._close_trade(trade)
                return True
            
            if sl_hit:
                # Check if this is breakeven stop
                if trade.status == TradeStatus.PARTIAL:
                    trade.status = TradeStatus.BREAKEVEN
                    trade.outcome = TradeOutcome.BREAKEVEN
                else:
                    trade.status = TradeStatus.LOSS
                    trade.outcome = TradeOutcome.SL_HIT
                    trade.actual_rr = -1.0
                
                trade.exit_price = trade.signal.stop_loss
                trade.exit_time = idx
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
        
        # Timeout check
        if trade.candles_evaluated >= MAX_CANDLES_TIMEOUT:
            current_close = df['close'].iloc[-1]
            
            if trade.status == TradeStatus.PARTIAL:
                # Close remaining position at market
                trade.status = TradeStatus.PARTIAL
                trade.outcome = TradeOutcome.PARTIAL_TP
                trade.exit_price = current_close
                trade.exit_time = df.index[-1]
                
                # Calculate weighted R
                risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
                remaining_r = (trade.exit_price - trade.signal.entry_price) / risk if trade.signal.direction == "BUY" else (trade.signal.entry_price - trade.exit_price) / risk
                trade.actual_rr = trade.realized_pnl + (remaining_r * (1 - PARTIAL_TP_PCT))
            else:
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
        """Close trade and update tracking."""
        if trade in self.active_trades:
            self.active_trades.remove(trade)
        self.closed_trades.append(trade)
        self.cooldowns[trade.signal.symbol] = datetime.now() + timedelta(minutes=COOLDOWN_MINUTES)
        logger.info(f"Trade closed: {trade.signal.id} | {trade.status.value} | {trade.actual_rr:+.2f}R")
    
    def get_daily_stats(self) -> DailyStats:
        """Calculate daily performance statistics."""
        stats = DailyStats(date=self.today)
        stats.total_signals = len(self.signals_today)
        stats.avg_slippage_cost = np.mean(self.slippage_stats) if self.slippage_stats else 0.0
        stats.avg_spread_cost = np.mean(self.spread_stats) if self.spread_stats else 0.0
        
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
                stats.max_consecutive_losses = max(stats.max_consecutive_losses, stats.current_consecutive_losses)
            elif trade.status == TradeStatus.BREAKEVEN:
                stats.breakeven += 1
                stats.current_consecutive_losses = 0
            elif trade.status == TradeStatus.PARTIAL:
                stats.partials += 1
                stats.gross_profit_r += max(0, r)
                stats.gross_loss_r += min(0, r)
                stats.current_consecutive_losses = 0
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
        """Reset daily counters."""
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.signals_today = []
        self.active_trades = []
        self.closed_trades = []
        self.trades = {}
        self.traded_symbols_today.clear()
        self.cooldowns.clear()
        self.slippage_stats = []
        self.spread_stats = []
        logger.info("Daily counters reset")
    
    def export_to_csv(self, filename: Optional[str] = None) -> str:
        """Export trades to CSV."""
        if filename is None:
            filename = DATA_DIR / f"trades_{self.today}.csv"
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'signal_id', 'symbol', 'direction', 'primary_strategy', 
                'all_strategies', 'entry', 'sl', 'tp', 'tp1',
                'atr', 'rr', 'adx', 'timestamp', 'status', 'exit_price', 
                'exit_time', 'actual_rr', 'candles_evaluated', 'market_regime',
                'slippage_cost', 'spread_cost'
            ])
            
            for trade in list(self.trades.values()):
                writer.writerow([
                    trade.signal.id,
                    trade.signal.symbol,
                    trade.signal.direction,
                    trade.signal.strategy_type,
                    '|'.join(trade.signal.triggered_strategies),
                    trade.signal.entry_price,
                    trade.signal.stop_loss,
                    trade.signal.take_profit,
                    trade.signal.take_profit_1,
                    trade.signal.atr_value,
                    trade.signal.risk_reward,
                    trade.signal.adx_value,
                    trade.signal.timestamp.isoformat(),
                    trade.status.value,
                    trade.exit_price,
                    trade.exit_time.isoformat() if trade.exit_time else '',
                    trade.actual_rr,
                    trade.candles_evaluated,
                    trade.signal.market_regime.name,
                    trade.signal.slippage_estimate,
                    trade.signal.spread_at_signal
                ])
        
        return str(filename)

# ================= PERFORMANCE TRACKER =================

class PerformanceTracker:
    def __init__(self, trade_tracker: TradeTracker):
        self.trade_tracker = trade_tracker
    
    async def schedule_daily_report(self, telegram: TelegramClient):
        """Schedule daily report generation."""
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
        """Generate and send daily performance report."""
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
    """
    Main trading engine with all upgrades: weighted scoring, regime filtering,
    session timing, structure-based SL, and enhanced risk management.
    """
    
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.fetcher = DataFetcher(TWELVEDATA_KEY)
        self.analyzer = TechnicalAnalyzer()
        self.scorer = SignalScorer()
        self.risk_manager = RiskManager()
        self.trade_tracker = TradeTracker()
        self.performance = PerformanceTracker(self.trade_tracker)
        self.liquidity_engine = LiquidityMapEngine()
        self.session_filter = SessionFilter()
        self.last_scan_time: Optional[datetime] = None
        
        # Track crypto symbols
        self.crypto_symbols = {"BTC/USDT", "ETH/USDT", "SOL/USDT"}
    
    def generate_signal_id(self, symbol: str, timestamp: datetime) -> str:
        """Generate unique signal ID."""
        return f"{symbol.replace('/', '').replace(':', '_')}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
    
    def is_trading_hours(self) -> bool:
        """Check if within trading hours."""
        now = datetime.now(pytz.UTC)
        return TRADING_START_HOUR <= now.hour < TRADING_END_HOUR
    
    def is_crypto(self, symbol: str) -> bool:
        """Check if symbol is cryptocurrency."""
        return symbol in self.crypto_symbols
    
    async def scan_symbol(self, symbol: str) -> Optional[Signal]:
        """
        Scan symbol for trading signals with all filters applied.
        
        Args:
            symbol: Trading symbol to scan
            
        Returns:
            Signal object if valid signal found, None otherwise
        """
        # Session filter check
        if not self.session_filter.is_trading_allowed():
            session = self.session_filter.get_current_session()
            logger.debug(f"{symbol}: Outside trading session ({session})")
            return None
        
        # Symbol availability check
        if not self.trade_tracker.can_trade_symbol(symbol):
            return None
        
        # Fetch data
        df_15m = await self.fetcher.fetch_time_series(symbol, TIMEFRAMES["signal"], 500)
        df_1h = await self.fetcher.fetch_time_series(symbol, TIMEFRAMES["trend"], 300)
        
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
        
        # Update liquidity map
        self.liquidity_engine.update(symbol, df_15m)
        
        # Volatility compression filter
        if not self.risk_manager.check_volatility_compression(df_15m):
            return None
        
        # Calculate weighted score with regime detection
        is_crypto = self.is_crypto(symbol)
        weighted_score, direction, primary_strategy, adx, triggered_strategies, regime, raw_score = \
            self.scorer.calculate_score(df_15m, df_1h, df_4h, is_crypto)
        
        if direction == "NEUTRAL":
            return None
        
        # ADX filter
        if adx < ADX_THRESHOLD:
            logger.info(f"{symbol}: ADX {adx:.1f} below threshold {ADX_THRESHOLD}")
            return None
        
        # Minimum weighted score check
        min_weighted_score = MIN_SCORE * 1.5 if PERSONALITY == "nami" else MIN_SCORE * 2.0
        if weighted_score < min_weighted_score:
            logger.info(f"{symbol}: Weighted score {weighted_score:.1f} below minimum {min_weighted_score}")
            return None
        
        # Duplicate signal check
        if self.trade_tracker.has_active_signal(symbol, direction):
            logger.info(f"{symbol}: Duplicate {direction} signal prevented")
            return None
        
        # Calculate risk levels with structure-based SL
        entry, sl, tp, tp1, atr, rr, sl_type = self.risk_manager.calculate_dynamic_levels(
            df_15m, direction, self.liquidity_engine, symbol
        )
        
        # R:R filter
        if rr < MIN_RR_RATIO:
            logger.info(f"{symbol}: R:R {rr:.2f} below minimum {MIN_RR_RATIO}")
            return None
        
        # Spread filter
        if not self.risk_manager.check_spread_filter(df_15m, symbol, atr):
            return None
        
        # Clustering filter
        if not self.risk_manager.check_signal_clustering(symbol, entry, atr):
            return None
        
        # SOGEKING specific filters
        if PERSONALITY == "sogeking":
            if atr < entry * 0.0005:
                logger.info(f"{symbol}: ATR too low for SOGEKING")
                return None
            
            confidence = min(95, int(20 + weighted_score * 8))
            if confidence < 80:
                logger.info(f"{symbol}: Confidence {confidence}% below 80% for SOGEKING")
                return None
        else:
            confidence = min(95, int(30 + weighted_score * 5))
        
        # Create signal
        signal = Signal(
            id=self.generate_signal_id(symbol, datetime.now()),
            symbol=symbol,
            direction=direction,
            score=weighted_score,  # Now using weighted score
            confidence=confidence,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            take_profit_1=tp1,
            atr_value=atr,
            risk_reward=rr,
            adx_value=adx,
            strategy_type=primary_strategy or "Unknown",
            triggered_strategies=triggered_strategies,
            timestamp=datetime.now(),
            timeframe=TIMEFRAMES["signal"],
            market_regime=regime,
            weighted_score=weighted_score,
            spread_at_signal=self.risk_manager.estimate_spread(df_15m, symbol),
            slippage_estimate=entry * SLIPPAGE_PCT
        )
        
        # Register for clustering tracking
        self.risk_manager.register_signal(symbol, entry, signal.id)
        
        # Add to trade tracker
        self.trade_tracker.add_signal(signal)
        
        return signal
    
    async def evaluate_active_trades(self):
        """Evaluate all active trades for exit conditions."""
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
        """Main scanner loop."""
        personality_name = "SOGEKING" if PERSONALITY == "sogeking" else "NAMI"
        logger.info(f"🧭 {personality_name} Engine starting...")
        
        # Startup message
        strategy_text = (
            "📊 <b>15 Active Strategies with Weighted Scoring:</b>\n"
            "• EMA Trend (2.0x weight)\n"
            "• Breakout (1.0x) - FIXED exclusion logic\n"
            "• Bull/Bear Flag (1.0x)\n"
            "• Double Top/Bottom (1.0x)\n"
            "• Head & Shoulders (1.0x)\n"
            "• Triangle (1.0x)\n"
            "• Fair Value Gap (1.5x)\n"
            "• Volume Spike (0.5x)\n"
            "• SME Pattern (1.0x)\n"
            "• Breakout Retest (2.0x)\n"
            "• EMA Reversion (1.0x) - Forex only\n"
            "• VWAP Reversion (1.0x) - Crypto only\n"
            "• Liquidity Sweep (2.5x)\n"
            "• ORB (1.0x)\n"
            "• <b>SLSE</b> (2.5x) - Improved with displacement\n"
            "• <b>DLCM</b> (1.5x) - With imbalance detection\n\n"
            "<b>New Filters:</b> Regime detection, Session timing, Spread check, Clustering prevention"
        ) if PERSONALITY == "nami" else (
            "🔥 <b>SNIPER MODE — Weighted Scoring + 4H Filter</b> 🔥\n"
            "• All 16 strategies with weighted scoring\n"
            "• Structure-based SL (Liquidity → Swing → ATR)\n"
            "• Session filter: London/NY hours only\n"
            "• Partial TP (50%) + Trailing stops\n"
            "• Minimum 2.0 weighted score required"
        )
        
        await self.telegram.send_message(
            f"🧭 <b>{personality_name} {'SNIPER' if PERSONALITY == 'sogeking' else 'Performance'} Engine v2.0</b> is LIVE\n\n"
            f"{strategy_text}\n\n"
            f"<b>Trading Hours:</b> {TRADING_START_HOUR:02d}:00 - {TRADING_END_HOUR:02d}:00 UTC\n"
            f"<b>Session Filter:</b> {self.session_filter.get_current_session()}\n"
            f"<b>Min Weighted Score:</b> {MIN_SCORE * 1.5 if PERSONALITY == 'nami' else MIN_SCORE * 2.0:.1f} | "
            f"<b>ADX Threshold:</b> {ADX_THRESHOLD}\n"
            f"<b>SL Logic:</b> Liquidity → Swing → ATR fallback\n"
            f"<b>Cooldown:</b> {COOLDOWN_MINUTES} min | "
            f"<b>TP:</b> {ATR_MULTIPLIER_TP:.1f} ATR | "
            f"<b>Min R:R:</b> {MIN_RR_RATIO}:1\n"
            f"<b>Daily Report:</b> {REPORT_HOUR}:00 {REPORT_TIMEZONE}"
        )
        
        while True:
            try:
                # Session check
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
                session = self.session_filter.get_current_session()
                logger.info(f"=== {personality_name} scan at {start_time} | Session: {session} ===")
                
                # Evaluate existing trades
                await self.evaluate_active_trades()
                
                # Scan watchlist with concurrency control
                semaphore = asyncio.Semaphore(3)
                
                async def scan_with_limit(symbol):
                    async with semaphore:
                        return await self.scan_symbol(symbol)
                
                tasks = [scan_with_limit(symbol) for symbol in WATCHLIST]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                signals_sent = 0
                for symbol, result in zip(WATCHLIST, results):
                    if isinstance(result, Exception):
                        logger.error(f"Exception scanning {symbol}: {result}")
                        continue
                    if result:
                        message = self.telegram.format_signal(result)
                        await self.telegram.send_message(message)
                        logger.info(
                            f"✅ {'SNIPER SHOT' if PERSONALITY == 'sogeking' else 'SIGNAL'}: "
                            f"{result.symbol} {result.direction} "
                            f"Primary:{result.strategy_type} "
                            f"Weight:{result.weighted_score:.1f} "
                            f"Regime:{result.market_regime.name} "
                            f"ADX:{result.adx_value:.1f} RR:{result.risk_reward:.1f}"
                        )
                        signals_sent += 1
                
                # Stats logging
                stats = self.trade_tracker.get_daily_stats()
                logger.info(
                    f"=== Scan complete | Signals: {signals_sent} | Active: {len(self.trade_tracker.active_trades)} | "
                    f"Closed: {len(self.trade_tracker.closed_trades)} | "
                    f"Today's R: {stats.total_r:+.2f} ==="
                )
                
                # Maintain exact scan interval
                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0, SCAN_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Scanner error: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def run(self):
        """Run main engine with daily reporting."""
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
    logger.info(f"Session filter: London {SESSION_LONDON_START:02d}:00-{SESSION_LONDON_END:02d}:00, "
                f"NY {SESSION_NY_START:02d}:00-{SESSION_NY_END:02d}:00 UTC")
    
    engine = NamiEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Engine stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
