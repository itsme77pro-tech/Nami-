"""
HedgeFundEngine v4.0 - Institutional Trading Intelligence with UX Optimization
=============================================================================
Integration of:
- Original HedgeFundEngine (v3.0) - Core trading engine
- NamiEngine v3.1 UX Dashboard - Engagement optimization
- 16 Original Strategy Methods - Complete pattern detection suite

Features:
- Multi-layer regime detection with probability scoring
- 16 institutional-grade trading strategies
- High-conversion UX alerts with visual enhancements
- Whale detection and liquidity heatmaps
- Real-time risk dashboards
- Session-based ceremonies
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
import csv
import json
import hashlib
import time
import pytz
import warnings
from dataclasses import dataclass, field, asdict
from typing import Optional, Tuple, List, Dict, Literal, Set, Callable, Any, Protocol
from datetime import datetime, timedelta, time
from enum import Enum, auto
from pathlib import Path
from collections import defaultdict, deque
from abc import ABC, abstractmethod
import hmac
import urllib.parse

warnings.filterwarnings('ignore')

# Try to import exchange libraries
try:
    import ccxt.async_support as ccxt
    CCXT_AVAILABLE = True
except ImportError:
    CCXT_AVAILABLE = False
    logging.warning("CCXT not installed. Binance integration limited.")

# ================= CONFIGURATION =================

# Core Environment
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

# Exchange configuration
EXCHANGE_MODE = os.getenv("EXCHANGE_MODE", "twelvedata").lower()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "true").lower() == "true"

# Personality & UX
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

# UX Configuration
ALERT_COOLDOWN_MINUTES = 30
MAX_DAILY_ALERTS = 20
ENGAGEMENT_HOURS = [8, 13, 21]
EMOJI_DENSITY = "high"

# Whale Detection
WHALE_VOLUME_MULTIPLIER = 3.0
WHALE_PRICE_IMPACT_PCT = 0.5

# Notification Priority
NOTIFICATION_PRIORITY = {
    "CRITICAL": 0,
    "HIGH": 1,
    "MEDIUM": 2,
    "LOW": 3,
    "BACKGROUND": 4
}

# Visual Configuration
PROGRESS_BAR_LENGTH = 10
HEATMAP_COLORS = ["🟢", "🟡", "🟠", "🔴", "⚫"]
CONFIDENCE_EMOJIS = ["🎯", "💎", "🔥", "⚡", "⭐"]

# Session timing
SESSION_LONDON_START = int(os.getenv("SESSION_LONDON_START", "8"))
SESSION_LONDON_END = int(os.getenv("SESSION_LONDON_END", "17"))
SESSION_NY_START = int(os.getenv("SESSION_NY_START", "13"))
SESSION_NY_END = int(os.getenv("SESSION_NY_END", "22"))
SESSION_OVERLAP_ONLY = os.getenv("SESSION_OVERLAP_ONLY", "false").lower() == "true"

# Risk configuration
MAX_PORTFOLIO_EXPOSURE = float(os.getenv("MAX_PORTFOLIO_EXPOSURE", "3.0"))
MAX_CORRELATION_EXPOSURE = float(os.getenv("MAX_CORRELATION_EXPOSURE", "2.0"))
DRAWDOWN_THROTTLE_PCT = float(os.getenv("DRAWDOWN_THROTTLE_PCT", "5.0"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "60"))

# Execution
SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.05")) / 100
PARTIAL_TP_PCT = float(os.getenv("PARTIAL_TP_PCT", "0.5"))
TRAILING_ACTIVATION_R = float(os.getenv("TRAILING_ACTIVATION_R", "1.0"))

# Strategy Parameters
ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 1.0
ATR_MULTIPLIER_TP = 2.0 if PERSONALITY == "nami" else 3.0
MIN_PROBABILITY = 0.65 if PERSONALITY == "nami" else 0.75
MIN_EDGE = 0.02

# Watchlists
FOREX_WATCHLIST = ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD"]
CRYPTO_WATCHLIST = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT"]

WATCHLIST = FOREX_WATCHLIST + CRYPTO_WATCHLIST if EXCHANGE_MODE == "hybrid" else \
            CRYPTO_WATCHLIST if EXCHANGE_MODE == "binance" else FOREX_WATCHLIST

TIMEFRAMES = {
    "micro": "5m",
    "signal": "15m",
    "trend": "1h",
    "macro": "4h",
    "structural": "1d"
}

SCAN_INTERVAL = 300  # 5 minutes for crypto
COOLDOWN_MINUTES = 60 if PERSONALITY == "nami" else 120
TRADING_START_HOUR = 0
TRADING_END_HOUR = 24

REPORT_HOUR = 23
REPORT_MINUTE = 59
REPORT_TIMEZONE = "UTC"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("HedgeFundEngine")

# ================= ENUMS =================

class MarketRegime(Enum):
    TRENDING_STRONG = auto()
    TRENDING_WEAK = auto()
    RANGING_COMPRESSION = auto()
    RANGING_EXPANSION = auto()
    VOLATILE_BREAKOUT = auto()
    VOLATILE_REVERSAL = auto()
    ACCUMULATION = auto()
    DISTRIBUTION = auto()

class VolatilityRegime(Enum):
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    EXTREME = auto()

class LiquidityType(Enum):
    SESSION_HIGH = "session_high"
    SESSION_LOW = "session_low"
    WEEKLY_HIGH = "weekly_high"
    WEEKLY_LOW = "weekly_low"
    EQUAL_HIGH = "equal_high"
    EQUAL_LOW = "equal_low"
    POOL_ABOVE = "pool_above"
    POOL_BELOW = "pool_below"
    STOP_CLUSTER = "stop_cluster"

class TradeStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    WIN = "win"
    LOSS = "loss"
    BREAKEVEN = "breakeven"
    EXPIRED = "expired"

class TradeOutcome(Enum):
    NONE = "none"
    TP_HIT = "tp_hit"
    SL_HIT = "sl_hit"
    PARTIAL_TP = "partial_tp"
    TRAILING_STOP = "trailing_stop"
    BREAKEVEN = "breakeven"
    EXPIRED = "expired"

class AlertType(Enum):
    PRE_SIGNAL = "pre_signal"
    SIGNAL = "signal"
    LIQUIDITY_ZONE = "liquidity_zone"
    SESSION_OPEN = "session_open"
    CONFIDENCE_HEATMAP = "confidence_heatmap"
    RISK_DASHBOARD = "risk_dashboard"
    INVALIDATION = "invalidation"
    WHALE_ACTIVITY = "whale_activity"
    MULTI_RANKING = "multi_ranking"
    WEEKLY_ANALYTICS = "weekly_analytics"
    DRAWDOWN_WARNING = "drawdown_warning"
    PROBABILITY_VIZ = "probability_viz"
    RR_SUGGESTION = "rr_suggestion"
    REGIME_CHANGE = "regime_change"

class UrgencyLevel(Enum):
    CRITICAL = "🔴 CRITICAL"
    HIGH = "🟠 HIGH"
    MEDIUM = "🟡 MEDIUM"
    LOW = "🟢 LOW"
    INFO = "⚪ INFO"

# ================= DATA CLASSES =================

@dataclass
class LiquidityLevel:
    price: float
    liquidity_type: LiquidityType
    strength: float
    volume_proxy: float
    timestamp: datetime
    is_swept: bool = False
    sweep_timestamp: Optional[datetime] = None

@dataclass
class OrderflowImprint:
    displacement: bool = False
    imbalance: bool = False
    absorption: bool = False
    momentum_burst: bool = False
    volume_anomaly: bool = False
    delta_direction: Optional[str] = None

@dataclass
class RegimeState:
    primary: MarketRegime
    volatility: VolatilityRegime
    trend_strength: float
    range_position: float
    adx: float
    atr_percentile: float

@dataclass
class ProbabilityModel:
    base_probability: float
    confluence_boost: float
    regime_adjustment: float
    liquidity_score: float
    orderflow_score: float
    final_probability: float
    edge: float
    kelly_fraction: float
    confidence_interval: Tuple[float, float]

@dataclass
class Signal:
    id: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    entry_price: float
    stop_loss: float
    take_profit: float
    take_profit_1: Optional[float]
    atr_value: float
    timestamp: datetime
    probability: ProbabilityModel
    regime: RegimeState
    primary_strategy: str
    triggered_strategies: List[str]
    liquidity_levels: List[LiquidityLevel]
    orderflow: OrderflowImprint
    position_size_r: float
    risk_reward: float
    expected_value: float
    timeframe: str = "15m"
    spread_at_signal: float = 0.0
    slippage_estimate: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat(),
            'regime': self.regime.primary.name,
            'probability_final': self.probability.final_probability,
            'liquidity_levels': len(self.liquidity_levels)
        }

@dataclass
class Trade:
    signal: Signal
    status: TradeStatus = field(default=TradeStatus.PENDING)
    outcome: TradeOutcome = field(default=TradeOutcome.NONE)
    entry_executed: float = 0.0
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    partial_size_closed: float = 0.0
    partial_exit_price: Optional[float] = None
    trailing_active: bool = False
    trailing_level: Optional[float] = None
    highest_profit_r: float = 0.0
    realized_pnl_r: float = 0.0
    unrealized_pnl_r: float = 0.0
    candles_evaluated: int = 0
    max_adverse_excursion: float = 0.0
    max_favorable_excursion: float = 0.0
    
    @property
    def is_closed(self) -> bool:
        return self.status in [TradeStatus.WIN, TradeStatus.LOSS, TradeStatus.BREAKEVEN, TradeStatus.EXPIRED]
    
    @property
    def current_pnl_r(self) -> float:
        return self.realized_pnl_r + self.unrealized_pnl_r

@dataclass
class PortfolioState:
    total_exposure_r: float = 0.0
    open_trades: int = 0
    daily_pnl_r: float = 0.0
    peak_equity_r: float = 0.0
    current_drawdown_pct: float = 0.0
    correlation_matrix: Dict[str, Dict[str, float]] = field(default_factory=dict)
    last_signals: Dict[str, datetime] = field(default_factory=dict)

# UX Enhanced Data Classes
@dataclass
class UXSignal:
    """Enhanced signal with UX metadata."""
    id: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    probability: float
    confidence: int
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward: float
    timestamp: datetime
    countdown_seconds: int = 0
    setup_quality_score: int = 0
    confluence_factors: List[str] = field(default_factory=list)
    risk_visualization: Dict[str, Any] = field(default_factory=dict)
    probability_breakdown: Dict[str, float] = field(default_factory=dict)
    
    def to_alert_format(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'symbol': self.symbol,
            'direction': self.direction,
            'probability': self.probability,
            'confidence': self.confidence,
            'setup_quality': self.setup_quality_score,
            'countdown': self.countdown_seconds,
            'confluence': len(self.confluence_factors)
        }

@dataclass
class LiquidityZone:
    """Visual liquidity zone representation."""
    symbol: str
    zone_type: Literal["support", "resistance", "equal_high", "equal_low", "stop_cluster"]
    price_level: float
    strength_score: int
    distance_pct: float
    test_count: int
    volume_at_level: float
    
    @property
    def heatmap_intensity(self) -> str:
        if self.strength_score >= 80:
            return "🔥🔥🔥"
        elif self.strength_score >= 60:
            return "🔥🔥"
        elif self.strength_score >= 40:
            return "🔥"
        else:
            return "⚡"

@dataclass
class WhaleActivity:
    """Whale movement detection."""
    symbol: str
    timestamp: datetime
    activity_type: Literal["accumulation", "distribution", "stop_hunt", "momentum_ignition"]
    volume_anomaly: float
    price_impact: float
    direction: Literal["BUY", "SELL", "NEUTRAL"]
    confidence: int
    
    @property
    def magnitude_emoji(self) -> str:
        if self.volume_anomaly >= 5:
            return "🐋🐋🐋"
        elif self.volume_anomaly >= 3:
            return "🐋🐋"
        else:
            return "🐋"

@dataclass
class RiskMetrics:
    """Real-time risk dashboard data."""
    timestamp: datetime
    portfolio_heat_score: int
    active_exposure: float
    available_capacity: float
    drawdown_status: Literal["normal", "elevated", "critical"]
    correlation_risk: int
    session_risk: int
    top_risks: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def risk_color(self) -> str:
        if self.portfolio_heat_score >= 80:
            return "🔴"
        elif self.portfolio_heat_score >= 60:
            return "🟠"
        elif self.portfolio_heat_score >= 40:
            return "🟡"
        else:
            return "🟢"

@dataclass
class StrategyDetection:
    """Strategy detection result."""
    name: str
    detected: bool
    confidence: float
    direction: Optional[Literal["BUY", "SELL"]]
    entry_price: Optional[float]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    metadata: Dict[str, Any] = field(default_factory=dict)
    confluence_factors: List[str] = field(default_factory=list)

# ================= EXCHANGE INTERFACES =================

class ExchangeInterface(ABC):
    @abstractmethod
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
        pass
    
    @abstractmethod
    async def get_spread(self, symbol: str) -> float:
        pass
    
    @abstractmethod
    async def close(self):
        pass

class TwelveDataInterface(ExchangeInterface):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(5)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session
    
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
        async with self._semaphore:
            session = await self._get_session()
            interval = timeframe.replace('m', 'min').replace('h', 'hour').replace('d', 'day')
            
            url = f"{self.base_url}/time_series"
            params = {
                "symbol": symbol,
                "interval": interval,
                "outputsize": limit,
                "apikey": self.api_key
            }
            
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    
                    if "values" not in data:
                        return None
                    
                    df = pd.DataFrame(data["values"])
                    df["datetime"] = pd.to_datetime(df["datetime"])
                    df.set_index("datetime", inplace=True)
                    df = df.astype(float)
                    df.sort_index(inplace=True)
                    return df
                    
            except Exception as e:
                logger.error(f"TwelveData error: {e}")
                return None
    
    async def get_spread(self, symbol: str) -> float:
        return 0.0002
    
    async def close(self):
        if self._session:
            await self._session.close()

class BinanceInterface(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        
        if CCXT_AVAILABLE:
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'future',
                    'adjustForTimeDifference': True
                }
            })
            if testnet:
                self.exchange.set_sandbox_mode(True)
        else:
            self.exchange = None
            self.base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
            self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    def _generate_signature(self, query_string: str) -> str:
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
        if self.exchange:
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                return df
            except Exception as e:
                logger.error(f"CCXT error: {e}")
                return None
        
        session = await self._get_session()
        interval = timeframe.replace('m', '').replace('h', 'h').replace('d', 'd')
        
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            "symbol": symbol.replace('/', ''),
            "interval": interval,
            "limit": limit
        }
        
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                df = pd.DataFrame(data, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                    'taker_buy_quote', 'ignore'
                ])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                return df
        except Exception as e:
            logger.error(f"Binance API error: {e}")
            return None
    
    async def get_spread(self, symbol: str) -> float:
        if self.exchange:
            try:
                ticker = await self.exchange.fetch_ticker(symbol)
                return (ticker['ask'] - ticker['bid']) / ticker['last']
            except:
                pass
        return 0.0005
    
    async def close(self):
        if self.exchange:
            await self.exchange.close()
        if self._session:
            await self._session.close()

class HybridExchangeManager:
    def __init__(self, twelvedata_key: str, binance_key: str, binance_secret: str):
        self.forex = TwelveDataInterface(twelvedata_key) if twelvedata_key else None
        self.crypto = BinanceInterface(binance_key, binance_secret, BINANCE_TESTNET) if binance_key and binance_secret else None
        
        self.asset_class_map = {
            **{s: "forex" for s in FOREX_WATCHLIST},
            **{s: "crypto" for s in CRYPTO_WATCHLIST}
        }
    
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
        asset_class = self.asset_class_map.get(symbol, "forex")
        
        if asset_class == "forex" and self.forex:
            return await self.forex.fetch_ohlcv(symbol, timeframe, limit)
        elif asset_class == "crypto" and self.crypto:
            return await self.crypto.fetch_ohlcv(symbol, timeframe, limit)
        
        if self.forex:
            return await self.forex.fetch_ohlcv(symbol, timeframe, limit)
        if self.crypto:
            return await self.crypto.fetch_ohlcv(symbol, timeframe, limit)
        
        return None
    
    async def get_spread(self, symbol: str) -> float:
        asset_class = self.asset_class_map.get(symbol, "forex")
        
        if asset_class == "forex" and self.forex:
            return await self.forex.get_spread(symbol)
        elif asset_class == "crypto" and self.crypto:
            return await self.crypto.get_spread(symbol)
        
        return 0.0003
    
    async def close(self):
        if self.forex:
            await self.forex.close()
        if self.crypto:
            await self.crypto.close()

# ================= TECHNICAL ANALYSIS CORE =================

class TechnicalCore:
    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.ewm(alpha=1/period, adjust=False).mean()
    
    @staticmethod
    def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high, low, close = df['high'], df['low'], df['close']
        
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
        
        tr = TechnicalCore.atr(df, period) * period
        plus_di = 100 * plus_dm.ewm(alpha=1/period, adjust=False).mean() / tr
        minus_di = 100 * minus_dm.ewm(alpha=1/period, adjust=False).mean() / tr
        
        dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        return dx.ewm(alpha=1/period, adjust=False).mean()
    
    @staticmethod
    def ema(df: pd.DataFrame, period: int) -> pd.Series:
        return df['close'].ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def vwap(df: pd.DataFrame) -> pd.Series:
        tp = (df['high'] + df['low'] + df['close']) / 3
        return (tp * df['volume']).cumsum() / df['volume'].cumsum()
    
    @staticmethod
    def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
        return series.rolling(lookback).apply(lambda x: (x < x.iloc[-1]).mean() * 100, raw=False)

# ================= STRATEGY DETECTORS (16 Original Methods) =================

class StrategyDetector(ABC):
    @abstractmethod
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        pass
    
    @property
    @abstractmethod
    def weight(self) -> float:
        pass

class BreakoutStrategy(StrategyDetector):
    """Replaces: breakout(), breakout_retest()"""
    
    def __init__(self, lookback: int = 20, confirmation_candles: int = 2):
        self.lookback = lookback
        self.confirmation_candles = confirmation_candles
    
    @property
    def weight(self) -> float:
        return 2.0
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback + 5:
            return StrategyDetection("breakout", False, 0, None, None, None, None)
        
        recent_high = df['high'].tail(self.lookback).max()
        recent_low = df['low'].tail(self.lookback).min()
        current_close = df['close'].iloc[-1]
        
        resistance_break = current_close > recent_high * 0.998
        support_break = current_close < recent_low * 1.002
        
        volume_confirm = False
        if 'volume' in df.columns:
            avg_volume = df['volume'].tail(20).mean()
            current_volume = df['volume'].iloc[-1]
            volume_confirm = current_volume > avg_volume * 1.5
        
        retest_confirm = False
        if len(df) >= 3:
            if resistance_break:
                retest_confirm = df['low'].iloc[-2] <= recent_high <= df['high'].iloc[-2]
            elif support_break:
                retest_confirm = df['low'].iloc[-2] <= recent_low <= df['high'].iloc[-2]
        
        if resistance_break and (volume_confirm or retest_confirm):
            confidence = 60 + (20 if volume_confirm else 0) + (20 if retest_confirm else 0)
            entry = current_close
            sl = recent_high - (recent_high - recent_low) * 0.1
            tp = entry + (entry - sl) * 2.5
            
            return StrategyDetection(
                name="breakout",
                detected=True,
                confidence=min(confidence, 95),
                direction="BUY",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'breakout_level': recent_high,
                    'retest_confirmed': retest_confirm,
                    'volume_confirmed': volume_confirm
                },
                confluence_factors=[
                    f"Resistance breakout @ {recent_high:.5f}",
                    "Volume surge" if volume_confirm else "Price momentum",
                    "Retest confirmed" if retest_confirm else "Direct breakout"
                ]
            )
        
        elif support_break and (volume_confirm or retest_confirm):
            confidence = 60 + (20 if volume_confirm else 0) + (20 if retest_confirm else 0)
            entry = current_close
            sl = recent_low + (recent_high - recent_low) * 0.1
            tp = entry - (sl - entry) * 2.5
            
            return StrategyDetection(
                name="breakout",
                detected=True,
                confidence=min(confidence, 95),
                direction="SELL",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'breakout_level': recent_low,
                    'retest_confirmed': retest_confirm,
                    'volume_confirmed': volume_confirm
                },
                confluence_factors=[
                    f"Support breakout @ {recent_low:.5f}",
                    "Volume surge" if volume_confirm else "Price momentum",
                    "Retest confirmed" if retest_confirm else "Direct breakout"
                ]
            )
        
        return StrategyDetection("breakout", False, 0, None, None, None, None)

class FlagStrategy(StrategyDetector):
    """Replaces: bull_flag()"""
    
    def __init__(self, pole_lookback: int = 10, flag_lookback: int = 5):
        self.pole_lookback = pole_lookback
        self.flag_lookback = flag_lookback
    
    @property
    def weight(self) -> float:
        return 1.8
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.pole_lookback + self.flag_lookback:
            return StrategyDetection("flag", False, 0, None, None, None, None)
        
        pole_data = df.tail(self.pole_lookback + self.flag_lookback).head(self.pole_lookback)
        flag_data = df.tail(self.flag_lookback)
        
        pole_move = (pole_data['close'].iloc[-1] - pole_data['close'].iloc[0]) / pole_data['close'].iloc[0] * 100
        strong_pole = abs(pole_move) > 2.0
        
        if not strong_pole:
            return StrategyDetection("flag", False, 0, None, None, None, None)
        
        flag_trend = (flag_data['close'].iloc[-1] - flag_data['close'].iloc[0]) / flag_data['close'].iloc[0] * 100
        
        bull_flag_candidate = pole_move > 0 and flag_trend < pole_move * 0.3
        bear_flag_candidate = pole_move < 0 and flag_trend > pole_move * 0.3
        
        pole_atr = self._calculate_atr(pole_data)
        flag_atr = self._calculate_atr(flag_data)
        volatility_contraction = flag_atr < pole_atr * 0.6
        
        flag_high = flag_data['high'].max()
        flag_low = flag_data['low'].min()
        entry = df['close'].iloc[-1]
        
        if bull_flag_candidate and volatility_contraction and entry > flag_high * 0.999:
            sl = flag_low
            tp = entry + (entry - sl) * 2.0
            
            return StrategyDetection(
                name="bull_flag",
                detected=True,
                confidence=75 if volatility_contraction else 65,
                direction="BUY",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'pole_move': pole_move,
                    'pattern': 'bull_flag' if flag_trend < 0 else 'pennant'
                },
                confluence_factors=[
                    f"Strong pole: +{pole_move:.1f}%",
                    "Volatility contraction" if volatility_contraction else "Consolidation pattern",
                    "Breakout from flag"
                ]
            )
        
        elif bear_flag_candidate and volatility_contraction and entry < flag_low * 1.001:
            sl = flag_high
            tp = entry - (sl - entry) * 2.0
            
            return StrategyDetection(
                name="bear_flag",
                detected=True,
                confidence=75 if volatility_contraction else 65,
                direction="SELL",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'pole_move': pole_move,
                    'pattern': 'bear_flag' if flag_trend > 0 else 'pennant'
                },
                confluence_factors=[
                    f"Strong pole: {pole_move:.1f}%",
                    "Volatility contraction" if volatility_contraction else "Consolidation pattern",
                    "Breakdown from flag"
                ]
            )
        
        return StrategyDetection("flag", False, 0, None, None, None, None)
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 5) -> float:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.tail(period).mean()

class DoubleTopBottomStrategy(StrategyDetector):
    """Replaces: double_top_bottom()"""
    
    def __init__(self, tolerance: float = 0.002, lookback: int = 50):
        self.tolerance = tolerance
        self.lookback = lookback
    
    @property
    def weight(self) -> float:
        return 2.2
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback:
            return StrategyDetection("double_top_bottom", False, 0, None, None, None, None)
        
        highs = df['high'].tail(self.lookback).values
        lows = df['low'].tail(self.lookback).values
        recent_high = df['high'].iloc[-1]
        recent_low = df['low'].iloc[-1]
        
        double_top = False
        top_level = None
        for i in range(-10, -self.lookback, -1):
            if abs(highs[i] - recent_high) / recent_high < self.tolerance:
                between_low = df['low'].iloc[i:-1].min()
                valley_depth = (highs[i] - between_low) / highs[i]
                if valley_depth > 0.01:
                    double_top = True
                    top_level = highs[i]
                    break
        
        double_bottom = False
        bottom_level = None
        for i in range(-10, -self.lookback, -1):
            if abs(lows[i] - recent_low) / recent_low < self.tolerance:
                between_high = df['high'].iloc[i:-1].max()
                peak_height = (between_high - lows[i]) / lows[i]
                if peak_height > 0.01:
                    double_bottom = True
                    bottom_level = lows[i]
                    break
        
        if double_top and df['close'].iloc[-1] < recent_high * 0.998:
            valley_idx = df['low'].iloc[-10:-1].idxmin()
            neckline = df.loc[valley_idx, 'low']
            entry = df['close'].iloc[-1]
            sl = recent_high + (recent_high - neckline) * 0.1
            tp = neckline - (recent_high - neckline) * 1.0
            
            return StrategyDetection(
                name="double_top",
                detected=True,
                confidence=80,
                direction="SELL",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'top_level': top_level,
                    'neckline': neckline
                },
                confluence_factors=[
                    f"Equal highs at {top_level:.5f}",
                    f"Neckline break @ {neckline:.5f}",
                    "Measured move target",
                    "Liquidity sweep setup"
                ]
            )
        
        elif double_bottom and df['close'].iloc[-1] > recent_low * 1.002:
            peak_idx = df['high'].iloc[-10:-1].idxmax()
            neckline = df.loc[peak_idx, 'high']
            entry = df['close'].iloc[-1]
            sl = recent_low - (neckline - recent_low) * 0.1
            tp = neckline + (neckline - recent_low) * 1.0
            
            return StrategyDetection(
                name="double_bottom",
                detected=True,
                confidence=80,
                direction="BUY",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'bottom_level': bottom_level,
                    'neckline': neckline
                },
                confluence_factors=[
                    f"Equal lows at {bottom_level:.5f}",
                    f"Neckline break @ {neckline:.5f}",
                    "Measured move target",
                    "Liquidity accumulation"
                ]
            )
        
        return StrategyDetection("double_top_bottom", False, 0, None, None, None, None)

class HeadAndShouldersStrategy(StrategyDetector):
    """Replaces: head_shoulders()"""
    
    def __init__(self, lookback: int = 30, shoulder_tolerance: float = 0.03):
        self.lookback = lookback
        self.shoulder_tolerance = shoulder_tolerance
    
    @property
    def weight(self) -> float:
        return 1.5
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback:
            return StrategyDetection("head_shoulders", False, 0, None, None, None, None)
        
        try:
            from scipy.signal import find_peaks
            highs = df['high'].tail(self.lookback).values
            lows = df['low'].tail(self.lookback).values
            
            peaks, _ = find_peaks(highs, distance=5, prominence=np.std(highs)*0.5)
            
            if len(peaks) >= 3:
                left_shoulder = highs[peaks[-3]]
                head = highs[peaks[-2]]
                right_shoulder = highs[peaks[-1]]
                
                shoulders_equal = abs(left_shoulder - right_shoulder) / left_shoulder < self.shoulder_tolerance
                head_higher = head > max(left_shoulder, right_shoulder) * 1.01
                
                trough1_idx = np.argmin(lows[peaks[-3]:peaks[-2]]) + peaks[-3]
                trough2_idx = np.argmin(lows[peaks[-2]:peaks[-1]]) + peaks[-2]
                neckline = max(lows[trough1_idx], lows[trough2_idx])
                
                if shoulders_equal and head_higher and df['close'].iloc[-1] < neckline:
                    entry = df['close'].iloc[-1]
                    sl = right_shoulder
                    tp = neckline - (head - neckline)
                    
                    return StrategyDetection(
                        name="head_and_shoulders",
                        detected=True,
                        confidence=70,
                        direction="SELL",
                        entry_price=entry,
                        stop_loss=sl,
                        take_profit=tp,
                        metadata={
                            'left_shoulder': left_shoulder,
                            'head': head,
                            'right_shoulder': right_shoulder,
                            'neckline': neckline
                        },
                        confluence_factors=[
                            "Three-peak structure",
                            "Shoulder symmetry",
                            "Neckline violation",
                            "Classic reversal pattern"
                        ]
                    )
        except ImportError:
            pass
        
        return StrategyDetection("head_shoulders", False, 0, None, None, None, None)

class TriangleStrategy(StrategyDetector):
    """Replaces: triangle()"""
    
    def __init__(self, lookback: int = 20, min_touches: int = 2):
        self.lookback = lookback
        self.min_touches = min_touches
    
    @property
    def weight(self) -> float:
        return 1.6
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback:
            return StrategyDetection("triangle", False, 0, None, None, None, None)
        
        highs = df['high'].tail(self.lookback).values
        lows = df['low'].tail(self.lookback).values
        x = np.arange(len(highs))
        
        high_slope, high_intercept = np.polyfit(x, highs, 1)
        low_slope, low_intercept = np.polyfit(x, lows, 1)
        
        flat_high = abs(high_slope) < 0.0001
        flat_low = abs(low_slope) < 0.0001
        converging = high_slope < 0 and low_slope > 0
        
        high_touches = sum(1 for h in highs if abs(h - (high_intercept + high_slope * x[len(highs)//2])) / h < 0.001)
        low_touches = sum(1 for l in lows if abs(l - (low_intercept + low_slope * x[len(lows)//2])) / l < 0.001)
        
        current_close = df['close'].iloc[-1]
        upper_bound = high_intercept + high_slope * (len(highs) - 1)
        lower_bound = low_intercept + low_slope * (len(lows) - 1)
        
        ascending_breakout = flat_high and low_slope > 0 and current_close > upper_bound * 0.999
        descending_breakdown = flat_low and high_slope < 0 and current_close < lower_bound * 1.001
        symmetrical_breakout = converging and (current_close > upper_bound * 0.999 or current_close < lower_bound * 1.001)
        
        if ascending_breakout and high_touches >= self.min_touches:
            entry = current_close
            sl = lower_bound
            tp = entry + (entry - sl) * 2.0
            
            return StrategyDetection(
                name="ascending_triangle",
                detected=True,
                confidence=75,
                direction="BUY",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={'resistance_level': high_intercept, 'touches': high_touches},
                confluence_factors=[
                    "Horizontal resistance",
                    "Rising support trendline",
                    f"{high_touches} resistance touches",
                    "Bullish breakout"
                ]
            )
        
        elif descending_breakdown and low_touches >= self.min_touches:
            entry = current_close
            sl = upper_bound
            tp = entry - (sl - entry) * 2.0
            
            return StrategyDetection(
                name="descending_triangle",
                detected=True,
                confidence=75,
                direction="SELL",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={'support_level': low_intercept, 'touches': low_touches},
                confluence_factors=[
                    "Horizontal support",
                    "Falling resistance trendline",
                    f"{low_touches} support touches",
                    "Bearish breakdown"
                ]
            )
        
        elif symmetrical_breakout and (high_touches + low_touches) >= self.min_touches * 2:
            direction = "BUY" if current_close > upper_bound else "SELL"
            entry = current_close
            sl = (upper_bound + lower_bound) / 2
            
            if direction == "BUY":
                tp = entry + (entry - sl) * 2.0
            else:
                tp = entry - (sl - entry) * 2.0
            
            return StrategyDetection(
                name="symmetrical_triangle",
                detected=True,
                confidence=70,
                direction=direction,
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={'converging_lines': True, 'total_touches': high_touches + low_touches},
                confluence_factors=[
                    "Converging trendlines",
                    f"{high_touches + low_touches} total touches",
                    "Volatility compression",
                    "Directional breakout"
                ]
            )
        
        return StrategyDetection("triangle", False, 0, None, None, None, None)

class FairValueGapStrategy(StrategyDetector):
    """Replaces: fair_value_gap()"""
    
    def __init__(self, min_gap_size: float = 0.001):
        self.min_gap_size = min_gap_size
    
    @property
    def weight(self) -> float:
        return 1.5
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < 3:
            return StrategyDetection("fvg", False, 0, None, None, None, None)
        
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        bullish_fvg = c1['high'] < c3['low']
        bullish_gap_size = (c3['low'] - c1['high']) / c1['close'] if bullish_fvg else 0
        
        bearish_fvg = c1['low'] > c3['high']
        bearish_gap_size = (c1['low'] - c3['high']) / c1['close'] if bearish_fvg else 0
        
        current = df['close'].iloc[-1]
        
        if bullish_fvg and bullish_gap_size > self.min_gap_size:
            gap_low = c1['high']
            gap_high = c3['low']
            
            if gap_low * 0.999 <= current <= gap_high * 1.001:
                entry = current
                sl = min(c1['low'], c2['low']) - (gap_high - gap_low) * 0.5
                tp = entry + (entry - sl) * 2.0
                
                return StrategyDetection(
                    name="bullish_fvg",
                    detected=True,
                    confidence=75,
                    direction="BUY",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'gap_low': gap_low, 'gap_high': gap_high, 'gap_size': bullish_gap_size},
                    confluence_factors=[
                        "3-candle bullish imbalance",
                        f"Gap: {bullish_gap_size*100:.2f}%",
                        "Price retracing into gap",
                        "Institutional reference level"
                    ]
                )
        
        elif bearish_fvg and bearish_gap_size > self.min_gap_size:
            gap_low = c3['high']
            gap_high = c1['low']
            
            if gap_low * 0.999 <= current <= gap_high * 1.001:
                entry = current
                sl = max(c1['high'], c2['high']) + (gap_high - gap_low) * 0.5
                tp = entry - (sl - entry) * 2.0
                
                return StrategyDetection(
                    name="bearish_fvg",
                    detected=True,
                    confidence=75,
                    direction="SELL",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'gap_low': gap_low, 'gap_high': gap_high, 'gap_size': bearish_gap_size},
                    confluence_factors=[
                        "3-candle bearish imbalance",
                        f"Gap: {bearish_gap_size*100:.2f}%",
                        "Price retracing into gap",
                        "Institutional reference level"
                    ]
                )
        
        return StrategyDetection("fvg", False, 0, None, None, None, None)

class VolumeSpikeStrategy(StrategyDetector):
    """Replaces: volume_spike()"""
    
    def __init__(self, multiplier: float = 2.0, lookback: int = 20):
        self.multiplier = multiplier
        self.lookback = lookback
    
    @property
    def weight(self) -> float:
        return 0.8
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if 'volume' not in df.columns or len(df) < self.lookback:
            return StrategyDetection("volume_spike", False, 0, None, None, None, None)
        
        avg_volume = df['volume'].tail(self.lookback).mean()
        current_volume = df['volume'].iloc[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
        
        if volume_ratio >= self.multiplier:
            body = df['close'].iloc[-1] - df['open'].iloc[-1]
            direction = "BUY" if body > 0 else "SELL" if body < 0 else None
            price_change = abs(body) / df['open'].iloc[-1] * 100
            
            return StrategyDetection(
                name="volume_spike",
                detected=True,
                confidence=min(60 + volume_ratio * 5, 90),
                direction=direction,
                entry_price=None,
                stop_loss=None,
                take_profit=None,
                metadata={'volume_ratio': volume_ratio, 'price_impact': price_change},
                confluence_factors=[
                    f"{volume_ratio:.1f}x volume surge",
                    f"{price_change:.2f}% price move",
                    "Institutional participation" if volume_ratio > 3 else "Above average interest"
                ]
            )
        
        return StrategyDetection("volume_spike", False, 0, None, None, None, None)

class SMEStrategy(StrategyDetector):
    """Replaces: sme_pattern()"""
    
    def __init__(self, vwap_deviation: float = 0.002):
        self.vwap_deviation = vwap_deviation
    
    @property
    def weight(self) -> float:
        return 1.4
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < 20 or 'volume' not in df.columns:
            return StrategyDetection("sme", False, 0, None, None, None, None)
        
        tp = (df['high'] + df['low'] + df['close']) / 3
        vwap = (tp * df['volume']).cumsum() / df['volume'].cumsum()
        
        current_price = df['close'].iloc[-1]
        current_vwap = vwap.iloc[-1]
        deviation = (current_price - current_vwap) / current_vwap
        
        if abs(deviation) > self.vwap_deviation:
            recent_trend = (df['close'].iloc[-5:].iloc[-1] - df['close'].iloc[-5:].iloc[0]) / df['close'].iloc[-5:].iloc[0]
            
            mean_reversion_long = deviation < -self.vwap_deviation and recent_trend > -0.001
            mean_reversion_short = deviation > self.vwap_deviation and recent_trend < 0.001
            
            if mean_reversion_long:
                entry = current_price
                sl = df['low'].tail(5).min()
                tp = current_vwap
                
                return StrategyDetection(
                    name="sme_long",
                    detected=True,
                    confidence=70,
                    direction="BUY",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'vwap': current_vwap, 'deviation': deviation},
                    confluence_factors=[
                        f"Price {abs(deviation)*100:.2f}% below VWAP",
                        "VWAP as magnet target",
                        "Potential smart money entry",
                        "Mean reversion edge"
                    ]
                )
            
            elif mean_reversion_short:
                entry = current_price
                sl = df['high'].tail(5).max()
                tp = current_vwap
                
                return StrategyDetection(
                    name="sme_short",
                    detected=True,
                    confidence=70,
                    direction="SELL",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'vwap': current_vwap, 'deviation': deviation},
                    confluence_factors=[
                        f"Price {deviation*100:.2f}% above VWAP",
                        "VWAP as magnet target",
                        "Potential smart money entry",
                        "Mean reversion edge"
                    ]
                )
        
        return StrategyDetection("sme", False, 0, None, None, None, None)

class EMAMeanReversionStrategy(StrategyDetector):
    """Replaces: ema_mean_reversion()"""
    
    def __init__(self, fast_ema: int = 20, deviation_threshold: float = 0.015):
        self.fast_ema = fast_ema
        self.deviation_threshold = deviation_threshold
    
    @property
    def weight(self) -> float:
        return 1.3
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.fast_ema * 2:
            return StrategyDetection("ema_mean_reversion", False, 0, None, None, None, None)
        
        ema = df['close'].ewm(span=self.fast_ema, adjust=False).mean()
        current_ema = ema.iloc[-1]
        current_price = df['close'].iloc[-1]
        
        deviation = (current_price - current_ema) / current_ema
        
        if abs(deviation) > self.deviation_threshold:
            current_open = df['open'].iloc[-1]
            current_close = df['close'].iloc[-1]
            prev_close = df['close'].iloc[-2]
            
            bullish_reversal = deviation < -self.deviation_threshold and current_close > current_open and current_close > prev_close
            bearish_reversal = deviation > self.deviation_threshold and current_close < current_open and current_close < prev_close
            
            if bullish_reversal:
                entry = current_close
                sl = df['low'].tail(3).min()
                tp = current_ema
                
                return StrategyDetection(
                    name="ema_mean_reversion_long",
                    detected=True,
                    confidence=65,
                    direction="BUY",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'ema_period': self.fast_ema, 'deviation': deviation},
                    confluence_factors=[
                        f"Price {abs(deviation)*100:.2f}% below EMA{self.fast_ema}",
                        "Bullish reversal candle",
                        "Mean reversion to EMA",
                        "Momentum exhaustion signal"
                    ]
                )
            
            elif bearish_reversal:
                entry = current_close
                sl = df['high'].tail(3).max()
                tp = current_ema
                
                return StrategyDetection(
                    name="ema_mean_reversion_short",
                    detected=True,
                    confidence=65,
                    direction="SELL",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'ema_period': self.fast_ema, 'deviation': deviation},
                    confluence_factors=[
                        f"Price {deviation*100:.2f}% above EMA{self.fast_ema}",
                        "Bearish reversal candle",
                        "Mean reversion to EMA",
                        "Momentum exhaustion signal"
                    ]
                )
        
        return StrategyDetection("ema_mean_reversion", False, 0, None, None, None, None)

class VWAPCryptoStrategy(StrategyDetector):
    """Replaces: vwap_mean_reversion_crypto()"""
    
    def __init__(self, session_aware: bool = True):
        self.session_aware = session_aware
    
    @property
    def weight(self) -> float:
        return 1.4
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < 24 or 'volume' not in df.columns:
            return StrategyDetection("vwap_crypto", False, 0, None, None, None, None)
        
        anchor_start = -24 if self.session_aware else 0
        session_data = df.iloc[anchor_start:]
        tp = (session_data['high'] + session_data['low'] + session_data['close']) / 3
        vwap = (tp * session_data['volume']).cumsum() / session_data['volume'].cumsum()
        
        current_vwap = vwap.iloc[-1]
        current_price = df['close'].iloc[-1]
        deviation = (current_price - current_vwap) / current_vwap
        
        threshold = 0.025
        
        if abs(deviation) > threshold:
            avg_vol = df['volume'].tail(24).mean()
            current_vol = df['volume'].iloc[-1]
            vol_confirm = current_vol > avg_vol
            
            if deviation < -threshold:
                entry = current_price
                sl = df['low'].tail(5).min()
                tp = current_vwap
                
                return StrategyDetection(
                    name="vwap_crypto_long",
                    detected=True,
                    confidence=70 if vol_confirm else 65,
                    direction="BUY",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'vwap': current_vwap, 'deviation': deviation, 'volume_confirmed': vol_confirm},
                    confluence_factors=[
                        f"Price {abs(deviation)*100:.2f}% below 24h VWAP",
                        "Crypto mean reversion edge",
                        "Volume confirmation" if vol_confirm else "Price structure",
                        "Institutional reference level"
                    ]
                )
            
            elif deviation > threshold:
                entry = current_price
                sl = df['high'].tail(5).max()
                tp = current_vwap
                
                return StrategyDetection(
                    name="vwap_crypto_short",
                    detected=True,
                    confidence=70 if vol_confirm else 65,
                    direction="SELL",
                    entry_price=entry,
                    stop_loss=sl,
                    take_profit=tp,
                    metadata={'vwap': current_vwap, 'deviation': deviation, 'volume_confirmed': vol_confirm},
                    confluence_factors=[
                        f"Price {deviation*100:.2f}% above 24h VWAP",
                        "Crypto mean reversion edge",
                        "Volume confirmation" if vol_confirm else "Price structure",
                        "Institutional reference level"
                    ]
                )
        
        return StrategyDetection("vwap_crypto", False, 0, None, None, None, None)

class LiquiditySweepStrategy(StrategyDetector):
    """Replaces: liquidity_sweep(), session_liquidity_sweep_engine()"""
    
    def __init__(self, sweep_tolerance: float = 0.001, lookback: int = 20):
        self.sweep_tolerance = sweep_tolerance
        self.lookback = lookback
    
    @property
    def weight(self) -> float:
        return 2.5
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback:
            return StrategyDetection("liquidity_sweep", False, 0, None, None, None, None)
        
        recent_highs = df['high'].tail(self.lookback)
        recent_lows = df['low'].tail(self.lookback)
        
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        current_close = df['close'].iloc[-1]
        prev_close = df['close'].iloc[-2]
        
        recent_low_level = recent_lows.min()
        bullish_sweep = current_low <= recent_low_level * (1 + self.sweep_tolerance) and current_close > recent_low_level and prev_close <= recent_low_level
        
        recent_high_level = recent_highs.max()
        bearish_sweep = current_high >= recent_high_level * (1 - self.sweep_tolerance) and current_close < recent_high_level and prev_close >= recent_high_level
        
        displacement = abs(current_close - prev_close) / prev_close > 0.003
        
        if bullish_sweep and displacement:
            entry = current_close
            sl = current_low - (recent_high_level - recent_low_level) * 0.1
            tp = recent_high_level
            
            return StrategyDetection(
                name="bullish_liquidity_sweep",
                detected=True,
                confidence=85,
                direction="BUY",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'swept_level': recent_low_level,
                    'recovery_close': current_close,
                    'displacement': displacement
                },
                confluence_factors=[
                    f"Sweep of lows @ {recent_low_level:.5f}",
                    "Strong recovery close",
                    "Displacement confirmed",
                    "Institutional stop hunt reversal"
                ]
            )
        
        elif bearish_sweep and displacement:
            entry = current_close
            sl = current_high + (recent_high_level - recent_low_level) * 0.1
            tp = recent_low_level
            
            return StrategyDetection(
                name="bearish_liquidity_sweep",
                detected=True,
                confidence=85,
                direction="SELL",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'swept_level': recent_high_level,
                    'recovery_close': current_close,
                    'displacement': displacement
                },
                confluence_factors=[
                    f"Sweep of highs @ {recent_high_level:.5f}",
                    "Strong rejection close",
                    "Displacement confirmed",
                    "Institutional stop hunt reversal"
                ]
            )
        
        return StrategyDetection("liquidity_sweep", False, 0, None, None, None, None)

class ORBStrategy(StrategyDetector):
    """Replaces: opening_range_breakout()"""
    
    def __init__(self, range_minutes: int = 30):
        self.range_minutes = range_minutes
    
    @property
    def weight(self) -> float:
        return 1.7
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.range_minutes // 5:
            return StrategyDetection("orb", False, 0, None, None, None, None)
        
        opening_candles = self.range_minutes // 5
        range_data = df.head(opening_candles)
        
        or_high = range_data['high'].max()
        or_low = range_data['low'].min()
        
        current = df['close'].iloc[-1]
        current_idx = len(df) - 1
        
        if current_idx < opening_candles:
            return StrategyDetection("orb", False, 0, None, None, None, None)
        
        breakout_up = current > or_high * 1.001 and df['close'].iloc[-2] <= or_high
        breakout_down = current < or_low * 0.999 and df['close'].iloc[-2] >= or_low
        
        or_avg_vol = range_data['volume'].mean() if 'volume' in df.columns else 0
        current_vol = df['volume'].iloc[-1] if 'volume' in df.columns else 0
        vol_confirm = current_vol > or_avg_vol * 1.5 if or_avg_vol > 0 else True
        
        if breakout_up:
            entry = current
            sl = or_low
            tp = entry + (or_high - or_low) * 2.0
            
            return StrategyDetection(
                name="orb_long",
                detected=True,
                confidence=75 if vol_confirm else 65,
                direction="BUY",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'opening_range_high': or_high,
                    'opening_range_low': or_low,
                    'range_size': or_high - or_low
                },
                confluence_factors=[
                    f"Opening range: {or_high-or_low:.5f}",
                    "Upside breakout",
                    "Volume confirmation" if vol_confirm else "Price momentum",
                    "Session momentum"
                ]
            )
        
        elif breakout_down:
            entry = current
            sl = or_high
            tp = entry - (or_high - or_low) * 2.0
            
            return StrategyDetection(
                name="orb_short",
                detected=True,
                confidence=75 if vol_confirm else 65,
                direction="SELL",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                metadata={
                    'opening_range_high': or_high,
                    'opening_range_low': or_low,
                    'range_size': or_high - or_low
                },
                confluence_factors=[
                    f"Opening range: {or_high-or_low:.5f}",
                    "Downside breakout",
                    "Volume confirmation" if vol_confirm else "Price momentum",
                    "Session momentum"
                ]
            )
        
        return StrategyDetection("orb", False, 0, None, None, None, None)

class DailyLiquidityCycleStrategy(StrategyDetector):
    """Replaces: daily_liquidity_cycle_model()"""
    
    def __init__(self):
        self.session_times = {
            'asia': (0, 8),
            'london': (8, 13),
            'ny': (13, 22),
            'overlap': (13, 17)
        }
    
    @property
    def weight(self) -> float:
        return 1.2
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        current_hour = datetime.now(pytz.UTC).hour
        
        session = None
        for name, (start, end) in self.session_times.items():
            if start <= current_hour < end:
                session = name
                break
        
        if not session:
            return StrategyDetection("liquidity_cycle", False, 0, None, None, None, None)
        
        if session == 'london' and len(df) >= 20:
            asia_high = df.head(20)['high'].max()
            asia_low = df.head(20)['low'].min()
            current = df['close'].iloc[-1]
            
            if current < asia_low * 1.001 and df['close'].iloc[-2] > asia_low:
                return StrategyDetection(
                    name="london_sweep",
                    detected=True,
                    confidence=70,
                    direction="BUY",
                    entry_price=current,
                    stop_loss=df['low'].iloc[-1],
                    take_profit=asia_high,
                    metadata={
                        'session': 'london',
                        'asia_range_high': asia_high,
                        'asia_range_low': asia_low
                    },
                    confluence_factors=[
                        "London session open",
                        "Sweep of Asia session lows",
                        "Classic liquidity cycle",
                        "Mean reversion setup"
                    ]
                )
        
        elif session == 'overlap' and len(df) >= 10:
            recent_trend = (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10]
            
            if abs(recent_trend) > 0.005:
                direction = "BUY" if recent_trend > 0 else "SELL"
                
                return StrategyDetection(
                    name="overlap_momentum",
                    detected=True,
                    confidence=65,
                    direction=direction,
                    entry_price=df['close'].iloc[-1],
                    stop_loss=None,
                    take_profit=None,
                    metadata={
                        'session': 'london_ny_overlap',
                        'trend_strength': recent_trend
                    },
                    confluence_factors=[
                        "London/NY overlap",
                        "High liquidity window",
                        "Trend continuation",
                        "Volatility expansion"
                    ]
                )
        
        return StrategyDetection("liquidity_cycle", False, 0, None, None, None, None)

# ================= STRATEGY ORCHESTRATOR =================

class StrategyOrchestrator:
    """Orchestrates all 16 strategy detectors."""
    
    def __init__(self):
        self.strategies: List[StrategyDetector] = [
            BreakoutStrategy(),
            FlagStrategy(),
            DoubleTopBottomStrategy(),
            HeadAndShouldersStrategy(),
            TriangleStrategy(),
            FairValueGapStrategy(),
            VolumeSpikeStrategy(),
            SMEStrategy(),
            EMAMeanReversionStrategy(),
            VWAPCryptoStrategy(),
            LiquiditySweepStrategy(),
            ORBStrategy(),
            DailyLiquidityCycleStrategy()
        ]
        self.detections_cache: Dict[str, List[StrategyDetection]] = {}
    
    def analyze(self, symbol: str, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> Tuple[List[StrategyDetection], float, List[str]]:
        """Run all strategies and return detections, aggregated confidence, and confluence factors."""
        detections = []
        total_weight = 0.0
        weighted_confidence = 0.0
        all_factors = []
        
        for strategy in self.strategies:
            try:
                detection = strategy.detect(df, higher_tf)
                
                if detection.detected:
                    detections.append(detection)
                    weight = strategy.weight
                    total_weight += weight
                    weighted_confidence += detection.confidence * weight
                    all_factors.extend(detection.confluence_factors)
            
            except Exception as e:
                logger.error(f"Strategy {strategy.__class__.__name__} error: {e}")
        
        aggregate_confidence = weighted_confidence / total_weight if total_weight > 0 else 0
        unique_factors = list(dict.fromkeys(all_factors))
        
        self.detections_cache[symbol] = detections
        
        return detections, aggregate_confidence, unique_factors
    
    def get_primary_setup(self, symbol: str) -> Optional[StrategyDetection]:
        """Get highest confidence setup for symbol."""
        detections = self.detections_cache.get(symbol, [])
        if not detections:
            return None
        
        return max(detections, key=lambda d: d.confidence)

# ================= MULTI-LAYER REGIME ENGINE =================

class RegimeEngine:
    def __init__(self):
        self.ta = TechnicalCore()
        self.lookback = 50
    
    def analyze(self, df: pd.DataFrame, higher_tf_df: Optional[pd.DataFrame] = None) -> RegimeState:
        if len(df) < self.lookback:
            return RegimeState(MarketRegime.TRENDING_WEAK, VolatilityRegime.NORMAL, 0.5, 0.5, 20.0, 50.0)
        
        adx = self.ta.adx(df, 14).iloc[-1]
        atr = self.ta.atr(df, 14)
        current_atr = atr.iloc[-1]
        atr_percentile = (atr.tail(self.lookback) < current_atr).mean() * 100
        
        ema_fast = self.ta.ema(df, 20)
        ema_slow = self.ta.ema(df, 50)
        trend_slope = (ema_fast.iloc[-1] - ema_fast.iloc[-10]) / ema_fast.iloc[-10] * 100
        
        range_high = df['high'].tail(self.lookback).max()
        range_low = df['low'].tail(self.lookback).min()
        range_position = (df['close'].iloc[-1] - range_low) / (range_high - range_low) if range_high != range_low else 0.5
        
        if atr_percentile < 20:
            vol_regime = VolatilityRegime.LOW
        elif atr_percentile < 50:
            vol_regime = VolatilityRegime.NORMAL
        elif atr_percentile < 80:
            vol_regime = VolatilityRegime.HIGH
        else:
            vol_regime = VolatilityRegime.EXTREME
        
        trend_strength = min(adx / 50.0, 1.0) if adx > 25 else adx / 100.0
        
        if adx > 40 and trend_slope > 1:
            primary = MarketRegime.TRENDING_STRONG
        elif adx > 25:
            primary = MarketRegime.TRENDING_WEAK
        elif atr_percentile < 30:
            primary = MarketRegime.RANGING_COMPRESSION
        elif vol_regime == VolatilityRegime.HIGH:
            if range_position > 0.7:
                primary = MarketRegime.DISTRIBUTION
            elif range_position < 0.3
"""
HedgeFundEngine v4.0 - Complete Integrated System
==================================================
Full integration of:
- Original HedgeFundEngine core
- NamiEngine v3.1 UX Dashboard
- All 16 strategy methods
"""

# ... [Previous code up to RegimeEngine] ...

        else:
            primary = MarketRegime.VOLATILE_BREAKOUT
        else:
            primary = MarketRegime.RANGING_EXPANSION
        
        return RegimeState(
            primary=primary,
            volatility=vol_regime,
            trend_strength=trend_strength,
            range_position=range_position,
            adx=adx,
            atr_percentile=atr_percentile
        )

# ================= INSTITUTIONAL LIQUIDITY ENGINE =================

class LiquidityEngine:
    def __init__(self):
        self.levels: Dict[str, List[LiquidityLevel]] = defaultdict(list)
        self.session_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=96))
        self.equal_tolerance = 0.002
    
    def update(self, symbol: str, df: pd.DataFrame):
        if len(df) < 20:
            return
        
        for idx, row in df.iterrows():
            self.session_history[symbol].append({
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row.get('volume', 0),
                'timestamp': idx
            })
        
        history = list(self.session_history[symbol])
        if len(history) < 96:
            return
        
        session_data = history[-96:]
        session_high = max(d['high'] for d in session_data)
        session_low = min(d['low'] for d in session_data)
        
        self._add_level(symbol, session_high, LiquidityType.SESSION_HIGH, session_data)
        self._add_level(symbol, session_low, LiquidityType.SESSION_LOW, session_data)
        
        if len(history) >= 672:
            weekly_data = history[-672:]
            weekly_high = max(d['high'] for d in weekly_data)
            weekly_low = min(d['low'] for d in weekly_data)
            self._add_level(symbol, weekly_high, LiquidityType.WEEKLY_HIGH, weekly_data)
            self._add_level(symbol, weekly_low, LiquidityType.WEEKLY_LOW, weekly_data)
        
        self._detect_equal_levels(symbol, df.tail(50))
        self._detect_liquidity_pools(symbol, df.tail(50))
        self._cleanup(symbol)
    
    def _add_level(self, symbol: str, price: float, ltype: LiquidityType, data: list):
        for level in self.levels[symbol]:
            if abs(level.price - price) / price < self.equal_tolerance:
                level.strength = min(1.0, level.strength + 0.1)
                level.timestamp = datetime.now()
                return
        
        vol_proxy = sum(d.get('volume', 0) for d in data[-10:]) / len(data[-10:]) if data else 0
        
        self.levels[symbol].append(LiquidityLevel(
            price=price,
            liquidity_type=ltype,
            strength=0.5,
            volume_proxy=vol_proxy,
            timestamp=datetime.now()
        ))
    
    def _detect_equal_levels(self, symbol: str, df: pd.DataFrame):
        highs = df['high'].values
        lows = df['low'].values
        
        for i in range(len(highs)):
            for j in range(i+1, len(highs)):
                if abs(highs[i] - highs[j]) / highs[i] < self.equal_tolerance:
                    self._add_level(symbol, highs[i], LiquidityType.EQUAL_HIGH, df.to_dict('records'))
        
        for i in range(len(lows)):
            for j in range(i+1, len(lows)):
                if abs(lows[i] - lows[j]) / lows[i] < self.equal_tolerance:
                    self._add_level(symbol, lows[i], LiquidityType.EQUAL_LOW, df.to_dict('records'))
    
    def _detect_liquidity_pools(self, symbol: str, df: pd.DataFrame):
        if 'volume' not in df.columns or df['volume'].sum() == 0:
            return
        
        vol_percentile = df['volume'].quantile(0.8)
        high_vol_candles = df[df['volume'] > vol_percentile]
        
        for _, candle in high_vol_candles.iterrows():
            mid = (candle['high'] + candle['low']) / 2
            self._add_level(symbol, mid, LiquidityType.POOL_ABOVE, df.to_dict('records'))
    
    def _cleanup(self, symbol: str, max_levels: int = 30):
        levels = self.levels[symbol]
        if len(levels) > max_levels:
            levels.sort(key=lambda x: (x.strength, x.timestamp), reverse=True)
            self.levels[symbol] = levels[:max_levels]
    
    def get_nearest(self, symbol: str, price: float, direction: str, min_strength: float = 0.3) -> Optional[LiquidityLevel]:
        candidates = []
        
        for level in self.levels[symbol]:
            if level.is_swept or level.strength < min_strength:
                continue
            
            if direction == "BUY" and level.price < price:
                candidates.append((price - level.price, level))
            elif direction == "SELL" and level.price > price:
                candidates.append((level.price - price, level))
        
        if not candidates:
            return None
        
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    
    def mark_swept(self, symbol: str, price: float, tolerance: float = 0.003):
        for level in self.levels[symbol]:
            if abs(level.price - price) / price < tolerance:
                level.is_swept = True
                level.sweep_timestamp = datetime.now()
    
    def get_profile(self, symbol: str) -> Dict[str, Any]:
        """Get liquidity profile for symbol."""
        levels = self.levels.get(symbol, [])
        
        return {
            'symbol': symbol,
            'session_high': max((l.price for l in levels if l.liquidity_type == LiquidityType.SESSION_HIGH), default=None),
            'session_low': min((l.price for l in levels if l.liquidity_type == LiquidityType.SESSION_LOW), default=None),
            'equal_highs': [l for l in levels if l.liquidity_type == LiquidityType.EQUAL_HIGH],
            'equal_lows': [l for l in levels if l.liquidity_type == LiquidityType.EQUAL_LOW],
            'total_levels': len(levels)
        }

# ================= ORDERFLOW PROXY DETECTOR =================

class OrderflowDetector:
    def __init__(self):
        self.ta = TechnicalCore()
    
    def analyze(self, df: pd.DataFrame, lookback: int = 20) -> OrderflowImprint:
        if len(df) < 5:
            return OrderflowImprint()
        
        recent = df.tail(5)
        current = df.iloc[-1]
        
        imprint = OrderflowImprint()
        
        body = abs(current['close'] - current['open'])
        range_val = current['high'] - current['low']
        
        if range_val > 0:
            body_ratio = body / range_val
            avg_range = recent['high'].sub(recent['low']).mean()
            imprint.displacement = body_ratio > 0.7 and range_val > avg_range * 1.5
        
        upper_wick = current['high'] - max(current['open'], current['close'])
        lower_wick = min(current['open'], current['close']) - current['low']
        
        if body > 0:
            if upper_wick > body * 2:
                imprint.imbalance = True
                imprint.delta_direction = "SELL"
            elif lower_wick > body * 2:
                imprint.imbalance = True
                imprint.delta_direction = "BUY"
        
        if 'volume' in df.columns and df['volume'].sum() > 0:
            vol_percentile = (df['volume'].tail(lookback) < current['volume']).mean()
            imprint.absorption = vol_percentile > 0.8 and body_ratio < 0.3
        
        closes = df['close'].tail(3).values
        opens = df['open'].tail(3).values
        
        if all(c > o for c, o in zip(closes, opens)):
            imprint.momentum_burst = True
            imprint.delta_direction = "BUY"
        elif all(c < o for c, o in zip(closes, opens)):
            imprint.momentum_burst = True
            imprint.delta_direction = "SELL"
        
        if 'volume' in df.columns:
            vol_ma = df['volume'].tail(20).mean()
            imprint.volume_anomaly = current['volume'] > vol_ma * 2
        
        return imprint

# ================= WHALE DETECTION ENGINE =================

class WhaleDetectionEngine:
    def __init__(self):
        self.volume_baselines: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.recent_activities: List[WhaleActivity] = []
    
    def update(self, symbol: str, df: pd.DataFrame):
        if len(df) < 2:
            return
        
        self.volume_baselines[symbol].append(df['volume'].iloc[-1])
        self.price_history[symbol].append({
            'price': df['close'].iloc[-1],
            'timestamp': df.index[-1]
        })
        
        activity = self._detect_anomaly(symbol, df)
        if activity:
            self.recent_activities.append(activity)
    
    def _detect_anomaly(self, symbol: str, df: pd.DataFrame) -> Optional[WhaleActivity]:
        if len(self.volume_baselines[symbol]) < 20:
            return None
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = np.mean(list(self.volume_baselines[symbol])[:-5])
        
        if avg_volume == 0:
            return None
        
        volume_ratio = current_volume / avg_volume
        
        if len(df) >= 2:
            price_change = abs(df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100
        else:
            price_change = 0
        
        if volume_ratio >= WHALE_VOLUME_MULTIPLIER and price_change >= WHALE_PRICE_IMPACT_PCT:
            body = df['close'].iloc[-1] - df['open'].iloc[-1]
            direction = "BUY" if body > 0 else "SELL" if body < 0 else "NEUTRAL"
            
            if volume_ratio >= 5 and price_change >= 1.0:
                activity_type = "momentum_ignition"
            elif self._is_stop_hunt_pattern(df):
                activity_type = "stop_hunt"
            elif direction == "BUY" and df['close'].iloc[-1] > df['high'].rolling(20).mean().iloc[-1]:
                activity_type = "accumulation"
            elif direction == "SELL" and df['close'].iloc[-1] < df['low'].rolling(20).mean().iloc[-1]:
                activity_type = "distribution"
            else:
                activity_type = "momentum_ignition"
            
            confidence = min(95, int(50 + volume_ratio * 5 + price_change * 10))
            
            return WhaleActivity(
                symbol=symbol,
                timestamp=df.index[-1],
                activity_type=activity_type,
                volume_anomaly=volume_ratio,
                price_impact=price_change,
                direction=direction,
                confidence=confidence
            )
        
        return None
    
    def _is_stop_hunt_pattern(self, df: pd.DataFrame) -> bool:
        if len(df) < 3:
            return False
        
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        if c2['low'] < c1['low'] and c3['close'] > c2['open']:
            return True
        if c2['high'] > c1['high'] and c3['close'] < c2['open']:
            return True
        
        return False
    
    def get_recent_activities(self, lookback_minutes: int = 15) -> List[WhaleActivity]:
        cutoff = datetime.now(pytz.UTC) - timedelta(minutes=lookback_minutes)
        return [a for a in self.recent_activities if a.timestamp >= cutoff]

# ================= UX MESSAGE TEMPLATES =================

class MessageTemplateLibrary:
    """Production-grade message templates optimized for engagement."""
    
    @staticmethod
    def pre_signal_alert(signal: UXSignal, time_to_signal: int) -> str:
        countdown_bar = MessageTemplateLibrary._generate_progress_bar(time_to_signal, 300, reverse=True)
        quality_stars = "⭐" * (signal.setup_quality_score // 20)
        
        return f"""
⏰ <b>SETUP FORMING — {signal.symbol}</b>

{countdown_bar}
<i>Signal maturity in {time_to_signal}s</i>

{quality_stars} <b>Quality Score:</b> {signal.setup_quality_score}/100

<b>📊 Emerging Confluence:</b>
{MessageTemplateLibrary._format_confluence_preview(signal.confluence_factors)}

<b>🎯 Directional Bias:</b> {"🟢 BULLISH" if signal.direction == "BUY" else "🔴 BEARISH"}
<b>🎲 Estimated Probability:</b> {signal.probability:.0%}

<i>🔔 Keep watching... Full signal incoming</i>
<code>ID: {signal.id[:8]}</code>
"""
    
    @staticmethod
    def _format_confluence_preview(factors: List[str]) -> str:
        preview = []
        for i, factor in enumerate(factors[:3], 1):
            preview.append(f"  {i}. {factor} ⚡")
        if len(factors) > 3:
            preview.append(f"  ... and {len(factors)-3} more factors")
        return "\n".join(preview) if preview else "  Analyzing market structure..."
    
    @staticmethod
    def signal_alert(signal: UXSignal, market_context: Dict[str, Any]) -> str:
        prob_bar = MessageTemplateLibrary._generate_probability_bar(signal.probability)
        risk_matrix = MessageTemplateLibrary._generate_risk_matrix(signal)
        badges = MessageTemplateLibrary._generate_context_badges(market_context)
        
        return f"""
🎯 <b>SIGNAL CONFIRMED — {signal.symbol}</b>

{badges}

{prob_bar}
<b>Confidence:</b> {signal.confidence}% | <b>Probability:</b> {signal.probability:.1%}

<b>💰 Trade Parameters</b>
├ Entry: <code>{signal.entry_price:.5f}</code>
├ Stop: <code>{signal.stop_loss:.5f}</code> ({abs(signal.entry_price - signal.stop_loss)/signal.entry_price*10000:.0f} pips)
├ Target: <code>{signal.take_profit:.5f}</code>
└ R:R Ratio: <b>1:{signal.risk_reward:.1f}</b> ⭐

{risk_matrix}

<b>🧠 Strategy Confluence</b>
{MessageTemplateLibrary._format_confluence_detailed(signal.confluence_factors)}

<i>⚡ Execute now or set alert</i>
<code>ID: {signal.id}</code>
"""
    
    @staticmethod
    def _generate_probability_bar(probability: float) -> str:
        filled = int(probability * PROGRESS_BAR_LENGTH)
        bar = "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)
        emoji = "🟢" if probability >= 0.80 else "🟡" if probability >= 0.65 else "🟠"
        return f"{emoji} <b>[{bar}]</b> {probability:.0%}"
    
    @staticmethod
    def _generate_risk_matrix(signal: UXSignal) -> str:
        risk_amount = abs(signal.entry_price - signal.stop_loss)
        reward_amount = abs(signal.take_profit - signal.entry_price)
        risk_bar = "🔴" * min(5, int(risk_amount / signal.entry_price * 10000))
        reward_bar = "🟢" * min(5, int(reward_amount / signal.entry_price * 10000))
        
        return f"""<b>📊 Risk Matrix</b>
├ Risk:    {risk_bar} ${risk_amount:.2f}
└ Reward:  {reward_bar} ${reward_amount:.2f}"""
    
    @staticmethod
    def _generate_context_badges(context: Dict[str, Any]) -> str:
        badges = []
        regime = context.get('regime', 'unknown')
        if 'trend' in regime.lower():
            badges.append("📈 TRENDING")
        elif 'range' in regime.lower():
            badges.append("↔️ RANGING")
        elif 'volatile' in regime.lower():
            badges.append("⚠️ VOLATILE")
        
        session = context.get('session', 'unknown')
        if 'overlap' in session.lower():
            badges.append("🔥 HIGH LIQUIDITY")
        
        volatility = context.get('volatility', 'normal')
        if volatility == 'extreme':
            badges.append("🌊 EXTREME VOL")
        
        return " | ".join(badges) if badges else "📊 STANDARD CONDITIONS"
    
    @staticmethod
    def _format_confluence_detailed(factors: List[str]) -> str:
        lines = []
        weights = [2.0, 1.5, 1.0, 0.8, 0.5]
        
        for i, factor in enumerate(factors[:5]):
            weight = weights[min(i, len(weights)-1)]
            bar = "█" * int(weight * 3)
            lines.append(f"  {bar} {factor}")
        
        return "\n".join(lines)
    
    @staticmethod
    def liquidity_zone_heatmap(zones: List[LiquidityZone], current_price: float) -> str:
        zones_sorted = sorted(zones, key=lambda z: z.strength_score, reverse=True)
        heatmap_lines = []
        
        for zone in zones_sorted[:5]:
            distance_emoji = "🎯" if zone.distance_pct < 0.5 else "📍" if zone.distance_pct < 1.0 else "👁️"
            direction = "↑" if zone.price_level > current_price else "↓"
            
            heatmap_lines.append(
                f"{zone.heatmap_intensity} {distance_emoji} "
                f"<code>{zone.price_level:.5f}</code> {direction} "
                f"({zone.distance_pct:.1f}%) — {zone.zone_type.replace('_', ' ').title()}"
            )
        
        price_position = MessageTemplateLibrary._generate_price_ladder(zones_sorted, current_price)
        
        return f"""
💧 <b>LIQUIDITY ZONE HEATMAP</b>

<i>Current Price: <code>{current_price:.5f}</code></i>

{price_position}

<b>🏛️ Institutional Levels:</b>
{chr(10).join(heatmap_lines)}

<i>🔔 Alert when price approaches 🎯 zones</i>
"""
    
    @staticmethod
    def _generate_price_ladder(zones: List[LiquidityZone], current_price: float) -> str:
        if not zones:
            return ""
        
        prices = [z.price_level for z in zones] + [current_price]
        min_p, max_p = min(prices), max(prices)
        range_p = max_p - min_p if max_p != min_p else 1
        
        ladder = []
        for zone in sorted(zones, key=lambda z: z.price_level, reverse=True):
            position = int((zone.price_level - min_p) / range_p * 20)
            bar = "─" * position + "●" + "─" * (20 - position)
            marker = "🎯" if abs(zone.price_level - current_price) / current_price * 100 < 0.5 else " "
            ladder.append(f"{marker} <code>{zone.price_level:.5f}</code> {bar}")
        
        pos = int((current_price - min_p) / range_p * 20)
        current_bar = "─" * pos + "🔴 YOU ARE HERE" + "─" * (20 - pos)
        ladder.append(f"  <code>{current_price:.5f}</code> {current_bar}")
        
        return "\n".join(ladder)
    
    @staticmethod
    def session_open_ceremony(session_type: str, high_impact_news: List[str]) -> str:
        session_emoji = {
            "LONDON": "🏰", "NEW_YORK": "🗽", "OVERLAP": "⚡",
            "TOKYO": "🗾", "SYDNEY": "🏖️"
        }.get(session_type, "🌍")
        
        news_alert = ""
        if high_impact_news:
            news_alert = f"""
⚠️ <b>HIGH IMPACT NEWS (Next 2H):</b>
{chr(10).join(f"  • {news}" for news in high_impact_news)}
"""
        
        return f"""
{session_emoji} <b>{session_type} SESSION OPEN</b> {session_emoji}

⏰ <i>Market ceremony beginning...</i>

<b>📋 Pre-Session Checklist:</b>
  ✅ Review overnight levels
  ✅ Check economic calendar
  ✅ Set liquidity alerts
  ✅ Prepare watchlist
  ⏳ Wait for first 15min candle

{news_alert}

<b>🎯 Today's Focus:</b>
  • Major pairs with {session_type} exposure
  • Volatility expansion expected
  • Institutional flow activation

<i>🎪 The stage is set. Perform.</i>
"""
    
    @staticmethod
    def confidence_heatmap(signals: List[UXSignal]) -> str:
        if not signals:
            return "📊 No active signals"
        
        ranked = sorted(signals, key=lambda s: s.confidence, reverse=True)
        leaderboard = []
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        
        for i, signal in enumerate(ranked[:5]):
            medal = medals[i] if i < 3 else f"{i+1}."
            bar_filled = int(signal.probability * 8)
            bar = "█" * bar_filled + "░" * (8 - bar_filled)
            
            leaderboard.append(
                f"{medal} <b>{signal.symbol}</b> {signal.direction[:1]} "
                f"[{bar}] {signal.confidence}% "
                f"(R:R 1:{signal.risk_reward:.1f})"
            )
        
        return f"""
🏆 <b>SIGNAL CONFIDENCE LEADERBOARD</b>

<i>Top opportunities ranked by probability × edge</i>

{chr(10).join(leaderboard)}

<b>💡 Recommendation:</b>
Focus on 🥇-🥉 for optimal risk-adjusted returns
"""
    
    @staticmethod
    def risk_dashboard(metrics: RiskMetrics, active_trades: List[Dict[str, Any]]) -> str:
        heat_gauge = MessageTemplateLibrary._generate_heat_gauge(metrics.portfolio_heat_score)
        exposure_bar = MessageTemplateLibrary._generate_exposure_bar(
            metrics.active_exposure, 
            metrics.active_exposure + metrics.available_capacity
        )
        trades_table = MessageTemplateLibrary._format_active_trades(active_trades)
        
        warnings = []
        if metrics.drawdown_status == "critical":
            warnings.append("🔴 CRITICAL: Drawdown exceeds 10% — HALF SIZE")
        elif metrics.drawdown_status == "elevated":
            warnings.append("🟠 WARNING: Drawdown at 7% — Reduce exposure")
        
        if metrics.correlation_risk > 70:
            warnings.append("🟠 HIGH CORRELATION: Multiple similar positions")
        
        warning_section = "\n".join(warnings) if warnings else "✅ All risk parameters normal"
        
        return f"""
⚠️ <b>RISK COMMAND CENTER</b>

{heat_gauge}

<b>💰 Exposure Status</b>
{exposure_bar}
├ Active: <code>{metrics.active_exposure:.2f}R</code>
└ Available: <code>{metrics.available_capacity:.2f}R</code>

<b>📊 Active Positions</b>
{trades_table}

<b>🚨 Risk Alerts</b>
{warning_section}

<i>Last updated: {datetime.now(pytz.UTC).strftime('%H:%M:%S')} UTC</i>
"""
    
    @staticmethod
    def _generate_heat_gauge(score: int) -> str:
        if score >= 80:
            color, emoji = "🔴", "CRITICAL"
        elif score >= 60:
            color, emoji = "🟠", "ELEVATED"
        elif score >= 40:
            color, emoji = "🟡", "MODERATE"
        else:
            color, emoji = "🟢", "OPTIMAL"
        
        filled = int(score / 10)
        bar = "█" * filled + "░" * (10 - filled)
        
        return f"{color} <b>PORTFOLIO HEAT: {emoji}</b> [{bar}] {score}%"
    
    @staticmethod
    def _generate_exposure_bar(used: float, total: float) -> str:
        if total == 0:
            return "  [░░░░░░░░░░] 0%"
        
        pct = used / total
        filled = int(pct * 10)
        
        if pct > 0.9:
            color = "🔴"
        elif pct > 0.7:
            color = "🟠"
        else:
            color = "🟢"
        
        bar = "█" * filled + "░" * (10 - filled)
        return f"  {color}[{bar}] {pct:.0%}"
    
    @staticmethod
    def _format_active_trades(trades: List[Dict[str, Any]]) -> str:
        if not trades:
            return "  No active positions"
        
        lines = []
        for trade in trades:
            pnl_emoji = "🟢" if trade.get('unrealized_pnl', 0) > 0 else "🔴" if trade.get('unrealized_pnl', 0) < 0 else "⚪"
            lines.append(
                f"  {pnl_emoji} {trade['symbol']} {trade['direction'][:1]} "
                f"@ {trade['entry']:.5f} "
                f"(P&L: {trade.get('unrealized_pnl', 0):+.1f}R)"
            )
        
        return "\n".join(lines)
    
    @staticmethod
    def whale_activity_alert(activities: List[WhaleActivity]) -> str:
        if not activities:
            return ""
        
        by_symbol = defaultdict(list)
        for act in activities:
            by_symbol[act.symbol].append(act)
        
        sections = []
        for symbol, acts in by_symbol.items():
            total_volume = sum(a.volume_anomaly for a in acts)
            avg_impact = np.mean([a.price_impact for a in acts])
            
            direction_consensus = "BUY" if all(a.direction == "BUY" for a in acts) else \
                                 "SELL" if all(a.direction == "SELL" for a in acts) else "MIXED"
            
            magnitude = "🐋🐋🐋" if total_volume >= 5 else "🐋🐋" if total_volume >= 3 else "🐋"
            
            sections.append(f"""
<b>🐋 {symbol}</b>
{magnitude} Volume: {total_volume:.1f}x average
📈 Impact: {avg_impact:+.2f}%
🎯 Consensus: {direction_consensus}
""")
        
        return f"""
🌊 <b>WHALE ACTIVITY DETECTED</b>

<i>Institutional footprint in last 15 minutes</i>

{chr(10).join(sections)}

<b>💡 Trading Implications:</b>
  • Follow whale direction with confirmation
  • Avoid counter-trend entries
  • Expect volatility expansion

<i>🏛️ Smart money is moving. Track it.</i>
"""
    
    @staticmethod
    def _generate_progress_bar(current: int, total: int, reverse: bool = False) -> str:
        if reverse:
            filled = int((1 - current/total) * PROGRESS_BAR_LENGTH) if total > 0 else 0
        else:
            filled = int((current/total) * PROGRESS_BAR_LENGTH) if total > 0 else 0
        
        filled = max(0, min(PROGRESS_BAR_LENGTH, filled))
        return "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)

# ================= EVENT TRIGGER ENGINE =================

class EventTriggerEngine:
    def __init__(self, telegram_manager):
        self.telegram = telegram_manager
        self.templates = MessageTemplateLibrary()
        
        self.last_alert_time: Dict[str, datetime] = {}
        self.alert_cooldowns: Dict[AlertType, int] = {
            AlertType.PRE_SIGNAL: 60,
            AlertType.SIGNAL: 0,
            AlertType.LIQUIDITY_ZONE: 300,
            AlertType.SESSION_OPEN: 3600,
            AlertType.RISK_DASHBOARD: 600,
            AlertType.WHALE_ACTIVITY: 180,
            AlertType.REGIME_CHANGE: 0,
            AlertType.DRAWDOWN_WARNING: 0
        }
        
        self.daily_alert_count: Dict[str, int] = defaultdict(int)
        self.daily_reset_time = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0)
    
    async def trigger_event(self, event_type: AlertType, data: Dict[str, Any], 
                          priority: int = 2) -> bool:
        self._check_daily_reset()
        
        if not self._check_quota(event_type):
            return False
        
        if not self._check_cooldown(event_type):
            return False
        
        handler = self._get_handler(event_type)
        if handler:
            message = handler(data)
            success = await self.telegram.send_message(message)
            
            if success:
                self._record_alert(event_type)
                return True
        
        return False
    
    def _check_daily_reset(self):
        now = datetime.now(pytz.UTC)
        if now.date() > self.daily_reset_time.date():
            self.daily_alert_count.clear()
            self.daily_reset_time = now
    
    def _check_quota(self, event_type: AlertType) -> bool:
        if event_type in [AlertType.DRAWDOWN_WARNING, AlertType.REGIME_CHANGE]:
            return True
        
        daily_total = sum(self.daily_alert_count.values())
        return daily_total < MAX_DAILY_ALERTS
    
    def _check_cooldown(self, event_type: AlertType) -> bool:
        cooldown = self.alert_cooldowns.get(event_type, 300)
        last_time = self.last_alert_time.get(event_type.value)
        
        if last_time is None:
            return True
        
        elapsed = (datetime.now(pytz.UTC) - last_time).total_seconds()
        return elapsed >= cooldown
    
    def _get_handler(self, event_type: AlertType) -> Callable:
        handlers = {
            AlertType.PRE_SIGNAL: self._handle_pre_signal,
            AlertType.SIGNAL: self._handle_signal,
            AlertType.LIQUIDITY_ZONE: self._handle_liquidity_zone,
            AlertType.SESSION_OPEN: self._handle_session_open,
            AlertType.CONFIDENCE_HEATMAP: self._handle_confidence_heatmap,
            AlertType.RISK_DASHBOARD: self._handle_risk_dashboard,
            AlertType.INVALIDATION: self._handle_invalidation,
            AlertType.WHALE_ACTIVITY: self._handle_whale_activity,
            AlertType.MULTI_RANKING: self._handle_multi_ranking,
            AlertType.WEEKLY_ANALYTICS: self._handle_weekly_analytics,
            AlertType.DRAWDOWN_WARNING: self._handle_drawdown_warning,
            AlertType.PROBABILITY_VIZ: self._handle_probability_viz,
            AlertType.RR_SUGGESTION: self._handle_rr_suggestion,
            AlertType.REGIME_CHANGE: self._handle_regime_change
        }
        return handlers.get(event_type)
    
    def _record_alert(self, event_type: AlertType):
        self.last_alert_time[event_type.value] = datetime.now(pytz.UTC)
        self.daily_alert_count[event_type.value] += 1
    
    def _handle_pre_signal(self, data: Dict[str, Any]) -> str:
        signal = UXSignal(**data['signal'])
        time_to = data.get('time_to_signal', 60)
        return self.templates.pre_signal_alert(signal, time_to)
    
    def _handle_signal(self, data: Dict[str, Any]) -> str:
        signal = UXSignal(**data['signal'])
        context = data.get('market_context', {})
        return self.templates.signal_alert(signal, context)
    
    def _handle_liquidity_zone(self, data: Dict[str, Any]) -> str:
        zones = [LiquidityZone(**z) for z in data['zones']]
        current_price = data['current_price']
        return self.templates.liquidity_zone_heatmap(zones, current_price)
    
    def _handle_session_open(self, data: Dict[str, Any]) -> str:
        return self.templates.session_open_ceremony(
            data['session_type'],
            data.get('high_impact_news', [])
        )
    
    def _handle_confidence_heatmap(self, data: Dict[str, Any]) -> str:
        signals = [UXSignal(**s) for s in data['signals']]
        return self.templates.confidence_heatmap(signals)
    
    def _handle_risk_dashboard(self, data: Dict[str, Any]) -> str:
        metrics = RiskMetrics(**data['metrics'])
        trades = data.get('active_trades', [])
        return self.templates.risk_dashboard(metrics, trades)
    
    def _handle_invalidation(self, data: Dict[str, Any]) -> str:
        return self.templates.invalidation_alert(
            data['trade_id'],
            data['symbol'],
            data['reason'],
            data['entry'],
            data['current'],
            data['loss']
        )
    
    def _handle_whale_activity(self, data: Dict[str, Any]) -> str:
        activities = [WhaleActivity(**a) for a in data['activities']]
        return self.templates.whale_activity_alert(activities)
    
    def _handle_multi_ranking(self, data: Dict[str, Any]) -> str:
        signals = [UXSignal(**s) for s in data['signals']]
        return self.templates.confidence_heatmap(signals)
    
    def _handle_weekly_analytics(self, data: Dict[str, Any]) -> str:
        return self.templates.weekly_analytics_report(data['stats'])
    
    def _handle_drawdown_warning(self, data: Dict[str, Any]) -> str:
        return self.templates.drawdown_intervention(
            data['current_dd'],
            data['max_dd'],
            data['recommended_action']
        )
    
    def _handle_probability_viz(self, data: Dict[str, Any]) -> str:
        return self.templates.probability_visualization(data['breakdown'])
    
    def _handle_rr_suggestion(self, data: Dict[str, Any]) -> str:
        return self.templates.dynamic_rr_suggestion(
            data['current_rr'],
            data['optimal_rr'],
            data['reason']
        )
    
    def _handle_regime_change(self, data: Dict[str, Any]) -> str:
        return self.templates.regime_change_ceremony(
            data['old_regime'],
            data['new_regime'],
            data['impact'],
            data['adjustments']
        )

# ================= PORTFOLIO RISK MANAGER =================

class PortfolioRiskManager:
    def __init__(self):
        self.state = PortfolioState()
        self.correlation_window = 20
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.cooldowns: Dict[str, datetime] = {}
    
    def update_price(self, symbol: str, price: float):
        self.price_history[symbol].append(price)
    
    def calculate_correlation(self, sym1: str, sym2: str) -> float:
        if len(self.price_history[sym1]) < self.correlation_window or len(self.price_history[sym2]) < self.correlation_window:
            return 0.0
        
        s1 = pd.Series(list(self.price_history[sym1])[-self.correlation_window:])
        s2 = pd.Series(list(self.price_history[sym2])[-self.correlation_window:])
        
        returns1 = s1.pct_change().dropna()
        returns2 = s2.pct_change().dropna()
        
        if len(returns1) < 5:
            return 0.0
        
        return returns1.corr(returns2)
    
    def can_add_trade(self, new_signal: Signal, open_trades: List[Trade]) -> Tuple[bool, str]:
        total_risk = sum(t.signal.position_size_r for t in open_trades)
        if total_risk + new_signal.position_size_r > MAX_PORTFOLIO_EXPOSURE:
            return False, f"Max exposure {MAX_PORTFOLIO_EXPOSURE}R would be exceeded"
        
        for trade in open_trades:
            if trade.signal.direction != new_signal.direction:
                continue
            
            corr = self.calculate_correlation(new_signal.symbol, trade.signal.symbol)
            if abs(corr) > 0.7:
                correlated_exposure = sum(
                    t.signal.position_size_r for t in open_trades 
                    if abs(self.calculate_correlation(new_signal.symbol, t.signal.symbol)) > 0.7
                )
                if correlated_exposure + new_signal.position_size_r > MAX_CORRELATION_EXPOSURE:
                    return False, f"Max correlated exposure {MAX_CORRELATION_EXPOSURE}R would be exceeded"
        
        if new_signal.symbol in self.cooldowns:
            if datetime.now() < self.cooldowns[new_signal.symbol]:
                return False, "Symbol in cooldown"
        
        if new_signal.symbol in self.state.last_signals:
            time_since_last = (datetime.now() - self.state.last_signals[new_signal.symbol]).total_seconds() / 60
            if time_since_last < SIGNAL_COOLDOWN_MINUTES:
                return False, f"Signal clustering: {time_since_last:.0f}min since last signal"
        
        return True, "OK"
    
    def register_trade(self, signal: Signal):
        self.state.open_trades += 1
        self.state.total_exposure_r += signal.position_size_r
        self.state.last_signals[signal.symbol] = datetime.now()
    
    def close_trade(self, trade: Trade):
        self.state.open_trades -= 1
        self.state.total_exposure_r -= trade.signal.position_size_r
        self.state.daily_pnl_r += trade.current_pnl_r
        
        if self.state.daily_pnl_r > self.state.peak_equity_r:
            self.state.peak_equity_r = self.state.daily_pnl_r
        
        if self.state.peak_equity_r > 0:
            self.state.current_drawdown_pct = (self.state.peak_equity_r - self.state.daily_pnl_r) / self.state.peak_equity_r * 100
        
        self.cooldowns[trade.signal.symbol] = datetime.now() + timedelta(minutes=SIGNAL_COOLDOWN_MINUTES)
    
    def check_drawdown_throttle(self) -> bool:
        return self.state.current_drawdown_pct > DRAWDOWN_THROTTLE_PCT
    
    def get_risk_metrics(self) -> RiskMetrics:
        """Generate UX risk metrics."""
        heat_score = min(100, int((self.state.total_exposure_r / MAX_PORTFOLIO_EXPOSURE) * 100))
        
        if self.state.current_drawdown_pct > 10:
            dd_status = "critical"
        elif self.state.current_drawdown_pct > 7:
            dd_status = "elevated"
        else:
            dd_status = "normal"
        
        return RiskMetrics(
            timestamp=datetime.now(pytz.UTC),
            portfolio_heat_score=heat_score,
            active_exposure=self.state.total_exposure_r,
            available_capacity=MAX_PORTFOLIO_EXPOSURE - self.state.total_exposure_r,
            drawdown_status=dd_status,
            correlation_risk=50,  # Simplified
            session_risk=30  # Simplified
        )

# ================= EXECUTION ENGINE =================

class ExecutionEngine:
    def __init__(self):
        self.slippage_model = {
            'forex': 0.0001,
            'crypto': 0.0005
        }
    
    def simulate_entry(self, signal: Signal, asset_class: str) -> float:
        base_slippage = self.slippage_model.get(asset_class, 0.0003)
        volatility_adj = 1 + (signal.regime.atr_percentile / 100)
        slippage = signal.entry_price * base_slippage * volatility_adj
        
        if signal.direction == "BUY":
            return signal.entry_price + slippage
        else:
            return signal.entry_price - slippage
    
    def simulate_exit(self, trade: Trade, exit_price: float, asset_class: str) -> float:
        base_slippage = self.slippage_model.get(asset_class, 0.0003)
        slippage = exit_price * base_slippage
        
        if trade.signal.direction == "BUY":
            return exit_price - slippage
        else:
            return exit_price + slippage
    
    def should_trigger_partial(self, trade: Trade, current_price: float) -> bool:
        if trade.signal.take_profit_1 is None:
            return False
        
        if trade.signal.direction == "BUY":
            return current_price >= trade.signal.take_profit_1
        else:
            return current_price <= trade.signal.take_profit_1
    
    def update_trailing_stop(self, trade: Trade, current_price: float) -> Optional[float]:
        if not trade.trailing_active:
            risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
            profit = abs(current_price - trade.signal.entry_price)
            
            if profit >= risk * TRAILING_ACTIVATION_R:
                trade.trailing_active = True
                if trade.signal.direction == "BUY":
                    return trade.signal.entry_price + (risk * 0.1)
                else:
                    return trade.signal.entry_price - (risk * 0.1)
        else:
            risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
            if trade.signal.direction == "BUY":
                new_level = current_price - (risk * 0.5)
                return max(new_level, trade.trailing_level or 0)
            else:
                new_level = current_price + (risk * 0.5)
                return min(new_level, trade.trailing_level or float('inf'))
        
        return None

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
            
            async with session.post(url, data=payload) as resp:
                result = await resp.json()
                return result.get("ok", False)
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False
    
    async def close(self):
        if self._session:
            await self._session.close()

# ================= INTEGRATED NOTIFICATION SYSTEM =================

class IntegratedNotificationSystem:
    def __init__(self, telegram_manager: TelegramClient):
        self.telegram = telegram_manager
        self.triggers = EventTriggerEngine(telegram_manager)
        self.whale_detector = WhaleDetectionEngine()
        
        self.current_regime: Dict[str, str] = {}
        self.active_signals: Dict[str, UXSignal] = {}
        self.risk_state: Optional[RiskMetrics] = None
    
    async def process_market_data(self, symbol: str, df: pd.DataFrame, context: Dict[str, Any]):
        self.whale_detector.update(symbol, df)
        
        whale_activities = self.whale_detector.get_recent_activities()
        if whale_activities:
            await self.triggers.trigger_event(
                AlertType.WHALE_ACTIVITY,
                {'activities': [self._whale_to_dict(a) for a in whale_activities]},
                priority=3
            )
        
        new_regime = context.get('regime', 'unknown')
        if symbol in self.current_regime and self.current_regime[symbol] != new_regime:
            await self.triggers.trigger_event(
                AlertType.REGIME_CHANGE,
                {
                    'old_regime': self.current_regime[symbol],
                    'new_regime': new_regime,
                    'impact': context.get('regime_impact', 'Market structure shift'),
                    'adjustments': context.get('strategy_adjustments', ['Review active signals'])
                },
                priority=1
            )
        
        self.current_regime[symbol] = new_regime
    
    async def send_pre_signal(self, signal: UXSignal, time_to_signal: int):
        await self.triggers.trigger_event(
            AlertType.PRE_SIGNAL,
            {
                'signal': self._signal_to_dict(signal),
                'time_to_signal': time_to_signal
            },
            priority=2
        )
    
    async def send_signal(self, signal: UXSignal, context: Dict[str, Any]):
        self.active_signals[signal.id] = signal
        
        await self.triggers.trigger_event(
            AlertType.SIGNAL,
            {
                'signal': self._signal_to_dict(signal),
                'market_context': context
            },
            priority=0
        )
    
    async def send_risk_dashboard(self, metrics: RiskMetrics, active_trades: List[Dict[str, Any]]):
        self.risk_state = metrics
        
        if metrics.drawdown_status in ["elevated", "critical"]:
            await self.triggers.trigger_event(
                AlertType.DRAWDOWN_WARNING,
                {
                    'current_dd': 0.08 if metrics.drawdown_status == "elevated" else 0.12,
                    'max_dd': 0.15,
                    'recommended_action': 'HALF_SIZE' if metrics.drawdown_status == "critical" else 'REDUCE_EXPOSURE'
                },
                priority=0
            )
        
        await self.triggers.trigger_event(
            AlertType.RISK_DASHBOARD,
            {
                'metrics': self._risk_to_dict(metrics),
                'active_trades': active_trades
            },
            priority=2
        )
    
    async def send_liquidity_alert(self, symbol: str, zones: List[LiquidityZone], current_price: float):
        await self.triggers.trigger_event(
            AlertType.LIQUIDITY_ZONE,
            {
                'zones': [self._zone_to_dict(z) for z in zones],
                'current_price': current_price
            },
            priority=3
        )
    
    async def send_session_open(self, session_type: str, high_impact_news: List[str] = None):
        await self.triggers.trigger_event(
            AlertType.SESSION_OPEN,
            {
                'session_type': session_type,
                'high_impact_news': high_impact_news or []
            },
            priority=3
        )
    
    def _signal_to_dict(self, signal: UXSignal) -> Dict[str, Any]:
        return {
            'id': signal.id,
            'symbol': signal.symbol,
            'direction': signal.direction,
            'probability': signal.probability,
            'confidence': signal.confidence,
            'entry_price': signal.entry_price,
            'stop_loss': signal.stop_loss,
            'take_profit': signal.take_profit,
            'risk_reward': signal.risk_reward,
            'timestamp': signal.timestamp.isoformat(),
            'countdown_seconds': signal.countdown_seconds,
            'setup_quality_score': signal.setup_quality_score,
            'confluence_factors': signal.confluence_factors,
            'risk_visualization': signal.risk_visualization,
            'probability_breakdown': signal.probability_breakdown
        }
    
    def _whale_to_dict(self, activity: WhaleActivity) -> Dict[str, Any]:
        return {
            'symbol': activity.symbol,
            'timestamp': activity.timestamp.isoformat(),
            'activity_type': activity.activity_type,
            'volume_anomaly': activity.volume_anomaly,
            'price_impact': activity.price_impact,
            'direction': activity.direction,
            'confidence': activity.confidence
        }
    
    def _zone_to_dict(self, zone: LiquidityZone) -> Dict[str, Any]:
        return {
            'symbol': zone.symbol,
            'zone_type': zone.zone_type,
            'price_level': zone.price_level,
            'strength_score': zone.strength_score,
            'distance_pct': zone.distance_pct,
            'test_count': zone.test_count,
            'volume_at_level': zone.volume_at_level
        }
    
    def _risk_to_dict(self, metrics: RiskMetrics) -> Dict[str, Any]:
        return {
            'timestamp': metrics.timestamp.isoformat(),
            'portfolio_heat_score': metrics.portfolio_heat_score,
            'active_exposure': metrics.active_exposure,
            'available_capacity': metrics.available_capacity,
            'drawdown_status': metrics.drawdown_status,
            'correlation_risk': metrics.correlation_risk,
            'session_risk': metrics.session_risk,
            'top_risks': metrics.top_risks
        }

# ================= MAIN HEDGE FUND ENGINE =================

class HedgeFundEngine:
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.exchange = HybridExchangeManager(TWELVEDATA_KEY, BINANCE_API_KEY, BINANCE_API_SECRET)
        self.regime_engine = RegimeEngine()
        self.liquidity_engine = LiquidityEngine()
        self.orderflow_detector = OrderflowDetector()
        self.strategy_orchestrator = StrategyOrchestrator()
        self.portfolio_risk = PortfolioRiskManager()
        self.execution = ExecutionEngine()
        self.notification_system = IntegratedNotificationSystem(self.telegram)
        
        self.trades: Dict[str, Trade] = {}
        self.active_trades: List[Trade] = []
        self.closed_trades: List[Trade] = []
        
        self.pending_pre_signals: Dict[str, UXSignal] = {}
        self.running = False
    
    def get_asset_class(self, symbol: str) -> str:
        if any(crypto in symbol for crypto in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA']):
            return 'crypto'
        return 'forex'
    
    async def scan_symbol(self, symbol: str) -> Optional[Signal]:
        df_15m = await self.exchange.fetch_ohlcv(symbol, "15m", 200)
        df_1h = await self.exchange.fetch_ohlcv(symbol, "1h", 100)
        
        if df_15m is None or len(df_15m) < 50:
            return None
        
        self.portfolio_risk.update_price(symbol, df_15m['close'].iloc[-1])
        
        if self.portfolio_risk.check_drawdown_throttle():
            logger.warning("Drawdown throttle active - skipping signals")
            return None
        
        # Multi-layer analysis
        regime = self.regime_engine.analyze(df_15m, df_1h)
        orderflow = self.orderflow_detector.analyze(df_15m)
        
        self.liquidity_engine.update(symbol, df_15m)
        nearest_liq = self.liquidity_engine.get_nearest(symbol, df_15m['close'].iloc[-1], "BUY")
        
        # Strategy orchestration
        detections, aggregate_confidence, confluence_factors = self.strategy_orchestrator.analyze(
            symbol, df_15m, df_1h
        )
        
        primary_strategy = self.strategy_orchestrator.get_primary_setup(symbol)
        
        if not primary_strategy or primary_strategy.confidence < 65:
            return None
        
        # Create enhanced signal
        entry = df_15m['close'].iloc[-1]
        atr = TechnicalCore().atr(df_15m, ATR_PERIOD).iloc[-1]
        
        if primary_strategy.entry_price:
            entry = primary_strategy.entry_price
        
        if primary_strategy.stop_loss:
            sl = primary_strategy.stop_loss
        else:
            sl = entry - atr * ATR_MULTIPLIER_SL if primary_strategy.direction == "BUY" else entry + atr * ATR_MULTIPLIER_SL
        
        if primary_strategy.take_profit:
            tp = primary_strategy.take_profit
        else:
            tp = entry + atr * ATR_MULTIPLIER_TP if primary_strategy.direction == "BUY" else entry - atr * ATR_MULTIPLIER_TP
        
        risk = abs(entry - sl)
        tp1 = entry + risk if primary_strategy.direction == "BUY" else entry - risk
        
        rr = abs(tp - entry) / risk if risk > 0 else 0
        
        # Probability model
        breakeven = 1 / (1 + ATR_MULTIPLIER_TP)
        edge = (aggregate_confidence / 100) - breakeven
        
        if edge < MIN_EDGE:
            return None
        
        if aggregate_confidence / 100 < MIN_PROBABILITY:
            return None
        
        kelly = edge / (1 - breakeven) if edge > 0 else 0
        kelly_fraction = min(kelly * 0.25, 0.02)
        
        prob_model = ProbabilityModel(
            base_probability=aggregate_confidence / 100,
            confluence_boost=aggregate_confidence / 100,
            regime_adjustment=0.05 if regime.volatility == VolatilityRegime.NORMAL else -0.05 if regime.volatility == VolatilityRegime.LOW else 0,
            liquidity_score=nearest_liq.strength if nearest_liq else 0,
            orderflow_score=1.0 if orderflow.displacement else 0.5,
            final_probability=min(0.95, aggregate_confidence / 100),
            edge=edge,
            kelly_fraction=kelly_fraction,
            confidence_interval=(max(0, aggregate_confidence/100 - 0.15), min(1, aggregate_confidence/100 + 0.15))
        )
        
        signal = Signal(
            id=f"{symbol.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            symbol=symbol,
            direction=primary_strategy.direction,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            take_profit_1=tp1,
            atr_value=atr,
            timestamp=datetime.now(),
            probability=prob_model,
            regime=regime,
            primary_strategy=primary_strategy.name,
            triggered_strategies=[d.name for d in detections],
            liquidity_levels=[nearest_liq] if nearest_liq else [],
            orderflow=orderflow,
            position_size_r=kelly_fraction * 100,
            risk_reward=rr,
            expected_value=(prob_model.final_probability * rr) - ((1 - prob_model.final_probability) * 1)
        )
        
        # Risk check
        can_trade, reason = self.portfolio_risk.can_add_trade(signal, self.active_trades)
        if not can_trade:
            logger.info(f"{symbol}: Risk check failed - {reason}")
            return None
        
        # Simulate execution
        asset_class = self.get_asset_class(symbol)
        executed_entry = self.execution.simulate_entry(signal, asset_class)
        signal.entry_price = executed_entry
        signal.slippage_estimate = abs(executed_entry - signal.entry_price)
        
        # Create trade
        trade = Trade(signal=signal)
        self.trades[signal.id] = trade
        self.active_trades.append(trade)
        self.portfolio_risk.register_trade(signal)
        
        # Send UX-enhanced notification
        ux_signal = UXSignal(
            id=signal.id,
            symbol=signal.symbol,
            direction=signal.direction,
            probability=prob_model.final_probability,
            confidence=int(aggregate_confidence),
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            risk_reward=signal.risk_reward,
            timestamp=signal.timestamp,
            setup_quality_score=int(aggregate_confidence),
            confluence_factors=confluence_factors[:6],
            probability_breakdown={d.name: d.confidence / 100 for d in detections}
        )
        
        await self.notification_system.send_signal(
            ux_signal,
            {
                'regime': regime.primary.name,
                'session': self._get_current_session(),
                'volatility': regime.volatility.name,
                'primary_strategy': primary_strategy.name
            }
        )
        
        return signal
    
    async def evaluate_trades(self):
        for trade in list(self.active_trades):
            if trade.is_closed:
                continue
            
            df = await self.exchange.fetch_ohlcv(trade.signal.symbol, "15m", 50)
            if df is None:
                continue
            
            current_price = df['close'].iloc[-1]
            asset_class = self.get_asset_class(trade.signal.symbol)
            
            # Update P&L
            if trade.signal.direction == "BUY":
                trade.unrealized_pnl_r = (current_price - trade.signal.entry_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
            else:
                trade.unrealized_pnl_r = (trade.signal.entry_price - current_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
            
            # Partial TP
            if trade.status == TradeStatus.PENDING and self.execution.should_trigger_partial(trade, current_price):
                trade.status = TradeStatus.PARTIAL
                trade.partial_exit_price = trade.signal.take_profit_1
                trade.partial_size_closed = trade.signal.position_size_r * PARTIAL_TP_PCT
                trade.realized_pnl_r = 1.0 * PARTIAL_TP_PCT
                trade.signal.stop_loss = trade.signal.entry_price
            
            # Trailing stop
            new_trailing = self.execution.update_trailing_stop(trade, current_price)
            if new_trailing:
                trade.trailing_level = new_trailing
            
            # Check exits
            exit_triggered = False
            
            if trade.trailing_active and trade.trailing_level:
                if trade.signal.direction == "BUY" and df['low'].iloc[-1] <= trade.trailing_level:
                    exit_price = self.execution.simulate_exit(trade, trade.trailing_level, asset_class)
                    trade.exit_price = exit_price
                    trade.outcome = TradeOutcome.TRAILING_STOP
                    exit_triggered = True
                elif trade.signal.direction == "SELL" and df['high'].iloc[-1] >= trade.trailing_level:
                    exit_price = self.execution.simulate_exit(trade, trade.trailing_level, asset_class)
                    trade.exit_price = exit_price
                    trade.outcome = TradeOutcome.TRAILING_STOP
                    exit_triggered = True
            
            if not exit_triggered:
                if trade.signal.direction == "BUY" and df['low'].iloc[-1] <= trade.signal.stop_loss:
                    exit_price = self.execution.simulate_exit(trade, trade.signal.stop_loss, asset_class)
                    trade.exit_price = exit_price
                    trade.outcome = TradeOutcome.SL_HIT
                    exit_triggered = True
                elif trade.signal.direction == "SELL" and df['high'].iloc[-1] >= trade.signal.stop_loss:
                    exit_price = self.execution.simulate_exit(trade, trade.signal.stop_loss, asset_class)
                    trade.exit_price = exit_price
                    trade.outcome = TradeOutcome.SL_HIT
                    exit_triggered = True
            
            if not exit_triggered:
                if trade.signal.direction == "BUY" and df['high'].iloc[-1] >= trade.signal.take_profit:
                    exit_price = self.execution.simulate_exit(trade, trade.signal.take_profit, asset_class)
                    trade.exit_price = exit_price
                    trade.outcome = TradeOutcome.TP_HIT
                    exit_triggered = True
                elif trade.signal.direction == "SELL" and df['low'].iloc[-1] <= trade.signal.take_profit:
                    exit_price = self.execution.simulate_exit(trade, trade.signal.take_profit, asset_class)
                    trade.exit_price = exit_price
                    trade.outcome = TradeOutcome.TP_HIT
                    exit_triggered = True
            
            if exit_triggered:
                trade.exit_time = datetime.now()
                
                if trade.status == TradeStatus.PARTIAL:
                    remaining_pnl = trade.unrealized_pnl_r * (1 - PARTIAL_TP_PCT)
                    trade.realized_pnl_r += remaining_pnl
                
                if trade.outcome == TradeOutcome.TP_HIT:
                    trade.status = TradeStatus.WIN
                elif trade.outcome == TradeOutcome.SL_HIT:
                    trade.status = TradeStatus.LOSS if trade.status != TradeStatus.PARTIAL else TradeStatus.BREAKEVEN
                elif trade.outcome == TradeOutcome.TRAILING_STOP:
                    trade.status = TradeStatus.WIN
                
                self.portfolio_risk.close_trade(trade)
                self.active_trades.remove(trade)
                self.closed_trades.append(trade)
                
                # Send invalidation alert if loss
                if trade.status == TradeStatus.LOSS:
                    await self.notification_system.triggers.trigger_event(
                        AlertType.INVALIDATION,
                        {
                            'trade_id': trade.signal.id,
                            'symbol': trade.signal.symbol,
                            'reason': f"Stop loss hit at {trade.exit_price:.5f}",
                            'entry': trade.signal.entry_price,
                            'current': trade.exit_price,
                            'loss': trade.realized_pnl_r
                        },
                        priority=1
                    )
                
                logger.info(f"Trade closed: {trade.signal.id} | {trade.outcome.value} | P&L: {trade.realized_pnl_r:+.2f}R")
    
    async def _pre_signal_loop(self):
        """Monitor for pre-signal opportunities."""
        while self.running:
            try:
                for symbol in WATCHLIST:
                    df = await self.exchange.fetch_ohlcv(symbol, "15m", 50)
                    
                    if df is not None:
                        detections, conf, _ = self.strategy_orchestrator.analyze(symbol, df)
                        
                        forming = [d for d in detections if 55 <= d.confidence < 70]
                        
                        if forming:
                            primary = max(forming, key=lambda x: x.confidence)
                            pre_id = f"PRE_{symbol}_{datetime.now().strftime('%H%M%S')}"
                            
                            if pre_id not in self.pending_pre_signals:
                                pre_signal = UXSignal(
                                    id=pre_id,
                                    symbol=symbol,
                                    direction=primary.direction,
                                    probability=primary.confidence / 100,
                                    confidence=primary.confidence,
                                    entry_price=primary.entry_price or df['close'].iloc[-1],
                                    stop_loss=primary.stop_loss or df['close'].iloc[-1] * 0.99,
                                    take_profit=primary.take_profit or df['close'].iloc[-1] * 1.02,
                                    risk_reward=2.0,
                                    timestamp=datetime.now(),
                                    setup_quality_score=primary.confidence,
                                    confluence_factors=primary.confluence_factors[:3],
                                    countdown_seconds=120
                                )
                                
                                self.pending_pre_signals[pre_id] = pre_signal
                                await self.notification_system.send_pre_signal(pre_signal, 120)
                
                # Cleanup
                now = datetime.now()
                expired = [
                    sid for sid, sig in self.pending_pre_signals.items()
                    if (now - sig.timestamp).total_seconds() > 300
                ]
                for sid in expired:
                    del self.pending_pre_signals[sid]
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Pre-signal loop error: {e}")
                await asyncio.sleep(60)
    
    async def _liquidity_monitoring_loop(self):
        """Monitor liquidity zones."""
        while self.running:
            try:
                for symbol in WATCHLIST:
                    df = await self.exchange.fetch_ohlcv(symbol, "15m", 20)
                    if df is None or len(df) == 0:
                        continue
                    
                    current_price = df['close'].iloc[-1]
                    self.liquidity_engine.update(symbol, df)
                    profile = self.liquidity_engine.get_profile(symbol)
                    
                    zones = self._profile_to_zones(profile, current_price, symbol)
                    
                    proximity_zones = [
                        z for z in zones 
                        if z.distance_pct < 0.5 and z.strength_score > 60
                    ]
                    
                    if proximity_zones:
                        await self.notification_system.send_liquidity_alert(
                            symbol, proximity_zones, current_price
                        )
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Liquidity monitoring error: {e}")
                await asyncio.sleep(300)
    
    async def _session_ceremony_loop(self):
        """Send session open ceremonies."""
        last_session_sent = None
        
        while self.running:
            try:
                now = datetime.now(pytz.UTC)
                current_time = now.time()
                
                session_opens = {
                    (8, 0): "LONDON",
                    (13, 0): "NEW_YORK",
                    (0, 0): "TOKYO",
                    (21, 0): "SYDNEY"
                }
                
                for (hour, minute), session_name in session_opens.items():
                    if current_time.hour == hour and current_time.minute == minute:
                        if last_session_sent != session_name:
                            await self.notification_system.send_session_open(session_name)
                            last_session_sent = session_name
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Session ceremony error: {e}")
                await asyncio.sleep(60)
    
    async def _risk_dashboard_loop(self):
        """Periodic risk dashboard updates."""
        while self.running:
            try:
                metrics = self.portfolio_risk.get_risk_metrics()
                
                active_trades_data = []
                for trade in self.active_trades:
                    active_trades_data.append({
                        'symbol': trade.signal.symbol,
                        'direction': trade.signal.direction,
                        'entry': trade.signal.entry_price,
                        'unrealized_pnl': trade.unrealized_pnl_r
                    })
                
                await self.notification_system.send_risk_dashboard(metrics, active_trades_data)
                
                await asyncio.sleep(600)  # Every 10 minutes
                
            except Exception as e:
                logger.error(f"Risk dashboard error: {e}")
                await asyncio.sleep(600)
    
    async def _whale_monitoring_loop(self):
        """Continuous whale monitoring."""
        while self.running:
            try:
                for symbol in WATCHLIST:
                    df = await self.exchange.fetch_ohlcv(symbol, "5m", 20)
                    if df is not None:
                        regime = self.regime_engine.analyze(df)
                        await self.notification_system.process_market_data(
                            symbol, df, {'regime': regime.primary.name}
                        )
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Whale monitoring error: {e}")
                await asyncio.sleep(60)
    
    def _profile_to_zones(self, profile: Dict[str, Any], current_price: float, symbol: str) -> List[LiquidityZone]:
        """Convert liquidity profile to zones."""
        zones = []
        
        if profile.get('session_high'):
            zones.append(LiquidityZone(
                symbol=symbol,
                zone_type="resistance",
                price_level=profile['session_high'],
                strength_score=70,
                distance_pct=abs(profile['session_high'] - current_price) / current_price * 100,
                test_count=1,
                volume_at_level=0
            ))
        
        if profile.get('session_low'):
            zones.append(LiquidityZone(
                symbol=symbol,
                zone_type="support",
                price_level=profile['session_low'],
                strength_score=70,
                distance_pct=abs(profile['session_low'] - current_price) / current_price * 100,
                test_count=1,
                volume_at_level=0
            ))
        
        for level in profile.get('equal_highs', []):
            zones.append(LiquidityZone(
                symbol=symbol,
                zone_type="equal_high",
                price_level=level.price,
                strength_score=int(level.strength * 100),
                distance_pct=abs(level.price - current_price) / current_price * 100,
                test_count=2 if level.liquidity_type == LiquidityType.EQUAL_HIGH else 1,
                volume_at_level=level.volume_proxy
            ))
        
        for level in profile.get('equal_lows', []):
            zones.append(LiquidityZone(
                symbol=symbol,
                zone_type="equal_low",
                price_level=level.price,
                strength_score=int(level.strength * 100),
                distance_pct=abs(level.price - current_price) / current_price * 100,
                test_count=2 if level.liquidity_type == LiquidityType.EQUAL_LOW else 1,
                volume_at_level=level.volume_proxy
            ))
        
        return zones
    
    def _get_current_session(self) -> str:
        """Get current trading session."""
        hour = datetime.now(pytz.UTC).hour
        if 13 <= hour < 17:
            return "overlap"
        elif 8 <= hour < 13:
            return "london"
        elif 13 <= hour < 22:
            return "new_york"
        elif 0 <= hour < 8:
            return "asia"
        else:
            return "quiet"
    
    async def run(self):
        """Main engine loop with all UX enhancements."""
        self.running = True
        
        await self.telegram.send_message(
            "🎯 <b>Hedge Fund Grade Trading Engine v4.0</b> Initialized\n\n"
            f"<b>Mode:</b> {EXCHANGE_MODE.upper()}\n"
            f"<b>Assets:</b> {len(WATCHLIST)} pairs\n"
            f"<b>Strategies:</b> 16 institutional patterns\n"
            f"<b>Min Probability:</b> {MIN_PROBABILITY:.0%}\n"
            f"<b>Max Exposure:</b> {MAX_PORTFOLIO_EXPOSURE}R\n"
            "<b>Features:</b> Multi-layer regime | 16 strategies | UX optimization | Whale detection"
        )
        
        # Start all loops
        main_task = asyncio.create_task(self._main_loop())
        pre_signal_task = asyncio.create_task(self._pre_signal_loop())
        liquidity_task = asyncio.create_task(self._liquidity_monitoring_loop())
        session_task = asyncio.create_task(self._session_ceremony_loop())
        risk_task = asyncio.create_task(self._risk_dashboard_loop())
        whale_task = asyncio.create_task(self._whale_monitoring_loop())
        
        await asyncio.gather(
            main_task, 
            pre_signal_task, 
            liquidity_task, 
            session_task, 
            risk_task, 
            whale_task
        )
    
    async def _main_loop(self):
        """Core trading loop."""
        while self.running:
            try:
                start_time = time.time()
                
                await self.evaluate_trades()
                
                semaphore = asyncio.Semaphore(4)
                
                async def scan_limited(symbol):
                    async with semaphore:
                        return await self.scan_symbol(symbol)
                
                tasks = [scan_limited(s) for s in WATCHLIST]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                signals_sent = 0
                for symbol, result in zip(WATCHLIST, results):
                    if isinstance(result, Exception):
                        logger.error(f"Error scanning {symbol}: {result}")
                        continue
                    if result:
                        signals_sent += 1
                
                logger.info(
                    f"Cycle complete | Signals: {signals_sent} | "
                    f"Active: {len(self.active_trades)} | "
                    f"Exposure: {self.portfolio_risk.state.total_exposure_r:.2f}R | "
                    f"DD: {self.portfolio_risk.state.current_drawdown_pct:.1f}%"
                )
                
                elapsed = time.time() - start_time
                sleep_time = max(0, SCAN_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(60)
        
        await self.exchange.close()
        await self.telegram.close()

# ================= ENTRY POINT =================

if __name__ == "__main__":
    engine = HedgeFundEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Engine stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
