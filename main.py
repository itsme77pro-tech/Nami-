"""
HedgeFundEngine v4.0 - Complete Integrated System (FIXED)
===========================================================
Full integration with all syntax errors corrected.
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
from typing import Optional, Tuple, List, Dict, Literal, Set, Callable, Any
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

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

ALERT_COOLDOWN_MINUTES = 30
MAX_DAILY_ALERTS = 20
ENGAGEMENT_HOURS = [8, 13, 21]
EMOJI_DENSITY = "high"

WHALE_VOLUME_MULTIPLIER = 3.0
WHALE_PRICE_IMPACT_PCT = 0.5

NOTIFICATION_PRIORITY = {
    "CRITICAL": 0,
    "HIGH": 1,
    "MEDIUM": 2,
    "LOW": 3,
    "BACKGROUND": 4
}

PROGRESS_BAR_LENGTH = 10
HEATMAP_COLORS = ["🟢", "🟡", "🟠", "🔴", "⚫"]
CONFIDENCE_EMOJIS = ["🎯", "💎", "🔥", "⚡", "⭐"]

SESSION_LONDON_START = int(os.getenv("SESSION_LONDON_START", "8"))
SESSION_LONDON_END = int(os.getenv("SESSION_LONDON_END", "17"))
SESSION_NY_START = int(os.getenv("SESSION_NY_START", "13"))
SESSION_NY_END = int(os.getenv("SESSION_NY_END", "22"))
SESSION_OVERLAP_ONLY = os.getenv("SESSION_OVERLAP_ONLY", "false").lower() == "true"

MAX_PORTFOLIO_EXPOSURE = float(os.getenv("MAX_PORTFOLIO_EXPOSURE", "3.0"))
MAX_CORRELATION_EXPOSURE = float(os.getenv("MAX_CORRELATION_EXPOSURE", "2.0"))
DRAWDOWN_THROTTLE_PCT = float(os.getenv("DRAWDOWN_THROTTLE_PCT", "5.0"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "60"))

SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.05")) / 100
PARTIAL_TP_PCT = float(os.getenv("PARTIAL_TP_PCT", "0.5"))
TRAILING_ACTIVATION_R = float(os.getenv("TRAILING_ACTIVATION_R", "1.0"))

ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 1.0
ATR_MULTIPLIER_TP = 2.0 if PERSONALITY == "nami" else 3.0
MIN_PROBABILITY = 0.65 if PERSONALITY == "nami" else 0.75
MIN_EDGE = 0.02

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

SCAN_INTERVAL = 300
COOLDOWN_MINUTES = 60 if PERSONALITY == "nami" else 120
TRADING_START_HOUR = 0
TRADING_END_HOUR = 24

REPORT_HOUR = 23
REPORT_MINUTE = 59
REPORT_TIMEZONE = "UTC"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

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

@dataclass
class UXSignal:
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
                    detected=True
