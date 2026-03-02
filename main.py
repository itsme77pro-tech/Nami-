"""
NamiEngine v3.2 - Complete Unified Trading Intelligence Platform
All 16 Original Strategies + Institutional Upgrades + UX Dashboard

Features:
- All original strategies preserved with enhancements
- Institutional risk management
- UX dashboard with 14 alert types
- Probabilistic scoring with Bayesian calibration
- Full strategy regime suitability mapping
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
from dataclasses import dataclass, field, asdict
from typing import Optional, Tuple, List, Dict, Literal, Set, Callable, Any, Union
from datetime import datetime, timedelta, time
from enum import Enum, auto, IntEnum
from pathlib import Path
import pytz
from collections import defaultdict, deque, Counter
from abc import ABC, abstractmethod
import warnings
from contextlib import asynccontextmanager

warnings.filterwarnings('ignore')

# ================= CONFIGURATION =================

def get_env_float(key: str, default: float, min_val: float = 0.0, max_val: float = float('inf')) -> float:
    try:
        val = float(os.getenv(key, str(default)))
        return max(min_val, min(max_val, val))
    except ValueError:
        return default

def get_env_int(key: str, default: int, min_val: int = 0, max_val: int = 999999) -> int:
    try:
        val = int(os.getenv(key, str(default)))
        return max(min_val, min(max_val, val))
    except ValueError:
        return default

def get_env_bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default).lower()).lower() in ('true', '1', 'yes', 'on')

# Core Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()
DATA_SOURCE = os.getenv("DATA_SOURCE", "twelvedata").lower()

# Original Configuration Preserved
SESSION_LONDON_START = get_env_int("SESSION_LONDON_START", 8, 0, 23)
SESSION_LONDON_END = get_env_int("SESSION_LONDON_END", 17, 0, 23)
SESSION_NY_START = get_env_int("SESSION_NY_START", 13, 0, 23)
SESSION_NY_END = get_env_int("SESSION_NY_END", 22, 0, 23)
SESSION_OVERLAP_ONLY = get_env_bool("SESSION_OVERLAP_ONLY", False)

# Risk Configuration
SPREAD_THRESHOLD_PCT = get_env_float("SPREAD_THRESHOLD_PCT", "0.3")
SLIPPAGE_PCT = get_env_float("SLIPPAGE_PCT", "0.1") / 100
PARTIAL_TP_PCT = get_env_float("PARTIAL_TP_PCT", "0.5")
TRAILING_ACTIVATION_R = get_env_float("TRAILING_ACTIVATION_R", "1.0")
VOLATILITY_PERCENTILE_THRESHOLD = get_env_int("VOLATILITY_PERCENTILE", "20")

# Institutional Upgrades
MAX_PORTFOLIO_RISK_PCT = get_env_float("MAX_PORTFOLIO_RISK_PCT", 2.0, 0.1, 10.0)
MAX_CORRELATED_EXPOSURE = get_env_int("MAX_CORRELATED_EXPOSURE", 2, 1, 5)
KELLY_FRACTION = get_env_float("KELLY_FRACTION", 0.25, 0.05, 0.5)

# UX Configuration
ALERT_COOLDOWN_MINUTES = 30
MAX_DAILY_ALERTS = 20
ENGAGEMENT_HOURS = [8, 13, 21]
PROGRESS_BAR_LENGTH = 10

# Original Watchlist Preserved + Metadata
WATCHLIST = {
    "EUR/USD": {"group": 1, "volatility": "medium", "session": "london-ny"},
    "GBP/USD": {"group": 1, "volatility": "high", "session": "london-ny"},
    "USD/JPY": {"group": 1, "volatility": "medium", "session": "ny-tokyo"},
    "USD/CHF": {"group": 1, "volatility": "medium", "session": "london-ny"},
    "BTC/USDT": {"group": 2, "volatility": "very_high", "session": "24h"},
    "ETH/USDT": {"group": 2, "volatility": "very_high", "session": "24h"},
    "SOL/USDT": {"group": 2, "volatility": "extreme", "session": "24h"},
}

# Original Timeframes Preserved + Micro/Macro
TIMEFRAMES = {
    "micro": "5min",
    "signal": "15min",
    "trend": "1h",
    "higher": "4h",
    "macro": "1d"
}

# Original Intervals
SCAN_INTERVAL = 900  # Original 15 minutes
MIN_SCORE = 4 if PERSONALITY == "nami" else 6
MAX_RETRIES = 3
RETRY_DELAY = 5

# Original Technical Parameters
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

# Original Personality Lines Preserved
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

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(DATA_DIR / "nami_engine.log"),
        logging.StreamHandler()
    ]
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
    TRAILING = "trailing"

class TradeOutcome(Enum):
    NONE = "none"
    TP_HIT = "tp_hit"
    SL_HIT = "sl_hit"
    EXPIRED = "expired"
    BREAKEVEN = "breakeven"
    TRAILING_STOP = "trailing_stop"
    PARTIAL_TP = "partial_tp"

class MarketRegime(Enum):
    TRENDING_STRONG = auto()
    TRENDING_WEAK = auto()
    RANGING_COMPRESSION = auto()
    RANGING_EXPANSION = auto()
    VOLATILE_BREAKOUT = auto()
    VOLATILE_REVERSAL = auto()
    COMPRESSION_SQUEEZE = auto()
    DISTRIBUTION = auto()
    ACCUMULATION = auto()

class VolatilityRegime(Enum):
    VERY_LOW = auto()
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    EXTREME = auto()

class SessionType(Enum):
    LONDON = auto()
    NEW_YORK = auto()
    TOKYO = auto()
    SYDNEY = auto()
    OVERLAP_LONDON_NY = auto()
    OVERLAP_TOKYO_LONDON = auto()
    OFF_HOURS = auto()

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

@dataclass
class MarketContext:
    symbol: str
    timestamp: datetime
    regime: MarketRegime
    volatility_regime: VolatilityRegime
    session: SessionType
    adx: float
    atr: float
    atr_percentile: float
    volume_regime: VolatilityRegime
    spread_estimate: float
    liquidity_score: float
    trend_strength: float
    mean_reversion_potential: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat(),
            'regime': self.regime.name,
            'volatility_regime': self.volatility_regime.name,
            'session': self.session.name,
            'adx': self.adx,
            'atr': self.atr,
            'atr_percentile': self.atr_percentile,
            'volume_regime': self.volume_regime.name,
            'spread_estimate': self.spread_estimate,
            'liquidity_score': self.liquidity_score,
            'trend_strength': self.trend_strength,
            'mean_reversion_potential': self.mean_reversion_potential
        }

@dataclass
class ProbabilityModel:
    base_probability: float
    confluence_score: float
    regime_adjustment: float
    liquidity_adjustment: float
    timing_adjustment: float
    historical_calibration: float
    
    @property
    def final_probability(self) -> float:
        prob = self.base_probability
        adjustments = [
            self.regime_adjustment,
            self.liquidity_adjustment,
            self.timing_adjustment,
            self.historical_calibration
        ]
        for adj in adjustments:
            prob = prob * (1 + adj) if adj > 0 else prob / (1 + abs(adj))
        return min(0.95, max(0.05, prob))
    
    @property
    def edge_score(self) -> float:
        return self.final_probability - 0.5

@dataclass
class LiquidityLevel:
    price: float
    level_type: Literal["equal_high", "equal_low", "session_high", "session_low", "weekly_high", "weekly_low"]
    strength: int
    timestamp: datetime
    is_swept: bool = False

@dataclass
class LiquidityProfile:
    session_high: Optional[float] = None
    session_low: Optional[float] = None
    weekly_high: Optional[float] = None
    weekly_low: Optional[float] = None
    equal_highs: List[float] = field(default_factory=list)
    equal_lows: List[float] = field(default_factory=list)
    stop_clusters_above: List[Tuple[float, float]] = field(default_factory=list)
    stop_clusters_below: List[Tuple[float, float]] = field(default_factory=list)
    liquidity_pools: List[Dict[str, Any]] = field(default_factory=list)
    nearest_liquidity_distance: float = float('inf')
    
    def get_nearest_liquidity(self, price: float, direction: str) -> Optional[Tuple[float, str, float]]:
        candidates = []
        for level in self.equal_highs + ([self.session_high] if self.session_high else []):
            if direction == "SELL" and level > price:
                candidates.append((level, "equal_high" if level in self.equal_highs else "session_high", abs(level - price)))
        for level in self.equal_lows + ([self.session_low] if self.session_low else []):
            if direction == "BUY" and level < price:
                candidates.append((level, "equal_low" if level in self.equal_lows else "session_low", abs(level - price)))
        for cluster, density in self.stop_clusters_above:
            if direction == "SELL" and cluster > price:
                candidates.append((cluster, "stop_cluster", abs(cluster - price)))
        for cluster, density in self.stop_clusters_below:
            if direction == "BUY" and cluster < price:
                candidates.append((cluster, "stop_cluster", abs(cluster - price)))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[2])
        return candidates[0]

@dataclass
class OrderflowSignature:
    displacement_detected: bool = False
    displacement_magnitude: float = 0.0
    imbalance_zone: Optional[Tuple[float, float]] = None
    momentum_burst: bool = False
    momentum_strength: float = 0.0
    absorption_detected: bool = False
    absorption_volume_ratio: float = 0.0
    delta_pressure: float = 0.0
    
    @property
    def is_institutional_footprint(self) -> bool:
        return (self.displacement_detected and self.displacement_magnitude > 2.0) or \
               (self.momentum_burst and self.momentum_strength > 1.5) or \
               (self.absorption_detected and self.absorption_volume_ratio > 2.0)

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
    
    @property
    def is_drawdown_throttle_active(self) -> bool:
        return self.current_drawdown > 0.05 if hasattr(self, 'current_drawdown') else False

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
class Signal:
    id: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    score: float
    confidence: int
    entry_price: float
    stop_loss: float
    take_profit: float
    atr_value: float
    risk_reward: float
    adx_value: float
    strategy_type: str
    triggered_strategies: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(pytz.UTC))
    timeframe: str = "15min"
    market_regime: MarketRegime = field(default=MarketRegime.TRENDING_STRONG)
    weighted_score: float = 0.0
    spread_at_signal: float = 0.0
    slippage_estimate: float = 0.0
    take_profit_1: Optional[float] = None
    probability_model: Optional[ProbabilityModel] = None
    market_context: Optional[MarketContext] = None
    liquidity_profile: Optional[LiquidityProfile] = None
    orderflow_signature: Optional[OrderflowSignature] = None
    position_size_risk: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat(),
            'triggered_strategies': ','.join(self.triggered_strategies),
            'market_regime': self.market_regime.name,
            'probability': self.probability_model.final_probability if self.probability_model else 0.5
        }

@dataclass
class Trade:
    signal: Signal
    status: TradeStatus = field(default=TradeStatus.PENDING)
    outcome: TradeOutcome = field(default=TradeOutcome.NONE)
    entry_fill_price: Optional[float] = None
    entry_slippage: float = 0.0
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_slippage: float = 0.0
    candles_evaluated: int = 0
    max_profit_reached: float = 0.0
    max_loss_reached: float = 0.0
    max_adverse_excursion: float = 0.0
    max_favorable_excursion: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    actual_rr: float = 0.0
    partial_exit_price: Optional[float] = None
    partial_exit_time: Optional[datetime] = None
    partial_volume_closed: float = 0.0
    trailing_stop_active: bool = False
    trailing_stop_level: Optional[float] = None
    trailing_activation_price: Optional[float] = None
    breakeven_triggered: bool = False
    
    @property
    def is_closed(self) -> bool:
        return self.status in [TradeStatus.WIN, TradeStatus.LOSS, TradeStatus.EXPIRED, TradeStatus.BREAKEVEN]
    
    @property
    def is_partial(self) -> bool:
        return self.status == TradeStatus.PARTIAL or self.partial_volume_closed > 0
    
    @property
    def profit_in_r(self) -> float:
        if self.status == TradeStatus.WIN:
            return self.signal.risk_reward
        elif self.status == TradeStatus.LOSS:
            return -1.0
        elif self.status == TradeStatus.BREAKEVEN:
            return 0.0
        elif self.status == TradeStatus.PARTIAL:
            partial_r = self.signal.risk_reward * self.partial_volume_closed
            remaining_r = self.actual_rr * (1 - self.partial_volume_closed)
            return partial_r + remaining_r
        elif self.status == TradeStatus.EXPIRED and self.exit_price:
            risk = abs(self.signal.entry_price - self.signal.stop_loss)
            if risk == 0:
                return 0.0
            actual = self.exit_price - self.signal.entry_price
            if self.signal.direction == "SELL":
                actual = -actual
            return actual / risk
        return 0.0
    
    @property
    def fill_efficiency(self) -> float:
        if self.entry_fill_price is None:
            return 0.0
        expected = self.signal.entry_price
        actual = self.entry_fill_price
        slippage = abs(actual - expected) / expected
        return max(0, 1 - slippage * 100)

@dataclass
class DailyStats:
    date: str
    total_signals: int = 0
    executed_signals: int = 0
    wins: int = 0
    losses: int = 0
    expired: int = 0
    breakeven: int = 0
    partials: int = 0
    trailings: int = 0
    total_r: float = 0.0
    gross_profit_r: float = 0.0
    gross_loss_r: float = 0.0
    max_profit_r: float = 0.0
    max_loss_r: float = 0.0
    avg_slippage_cost: float = 0.0
    avg_spread_cost: float = 0.0
    avg_fill_efficiency: float = 0.0
    max_consecutive_losses: int = 0
    current_consecutive_losses: int = 0
    max_drawdown_r: float = 0.0
    prob_calibration_error: float = 0.0
    high_prob_win_rate: float = 0.0
    low_prob_win_rate: float = 0.0
    
    @property
    def win_rate(self) -> float:
        closed = self.wins + self.losses + self.breakeven
        return (self.wins / closed * 100) if closed > 0 else 0.0
    
    @property
    def profit_factor(self) -> float:
        return abs(self.gross_profit_r / self.gross_loss_r) if self.gross_loss_r != 0 else float('inf')
    
    @property
    def expectancy(self) -> float:
        total = self.wins + self.losses + self.expired + self.breakeven + self.partials
        if total == 0:
            return 0.0
        win_rate = self.wins / total
        avg_win = self.gross_profit_r / self.wins if self.wins > 0 else 0
        avg_loss = abs(self.gross_loss_r / self.losses) if self.losses > 0 else 0
        return (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

@dataclass
class PortfolioState:
    timestamp: datetime
    total_exposure: float = 0.0
    active_trades: int = 0
    exposure_by_group: Dict[int, float] = field(default_factory=dict)
    exposure_by_symbol: Dict[str, float] = field(default_factory=dict)
    correlation_matrix: Optional[np.ndarray] = None
    daily_drawdown: float = 0.0
    current_drawdown: float = 0.0
    peak_equity: float = 0.0
    current_equity: float = 0.0
    
    @property
    def is_drawdown_throttle_active(self) -> bool:
        return self.current_drawdown > 0.05
    
    @property
    def available_risk_capacity(self) -> float:
        used = sum(self.exposure_by_symbol.values())
        return MAX_PORTFOLIO_RISK_PCT - used

# ================= ALL 16 ORIGINAL STRATEGIES =================

class TechnicalAnalyzer:
    """
    Complete technical analysis with all 16 original strategies preserved.
    Enhanced with institutional-grade calculations.
    """
    
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        atr = true_range.ewm(alpha=1/period, adjust=False).mean()
        return atr
    
    @staticmethod
    def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high = df['high']
        low = df['low']
        close = df['close']
        tr1 = high - low
        tr2 = np.abs(high - close.shift())
        tr3 = np.abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
        dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx = dx.ewm(alpha=1/period, adjust=False).mean()
        return adx
    
    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> pd.Series:
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        return (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
    
    @staticmethod
    def calculate_ema_deviation(df: pd.DataFrame, period: int = 20) -> Tuple[pd.Series, pd.Series]:
        ema = df['close'].ewm(span=period, adjust=False).mean()
        deviation = (df['close'] - ema) / ema
        return ema, deviation
    
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
    
    # ========== ORIGINAL STRATEGY 1: Breakout ==========
    @staticmethod
    def breakout(df: pd.DataFrame, lookback: int = 20) -> Optional[str]:
        if len(df) < lookback + 1:
            return None
        historical = df.iloc[-lookback-1:-1]
        current = df.iloc[-1]
        range_high = historical["high"].max()
        range_low = historical["low"].min()
        if current['close'] > range_high:
            return "BUY"
        if current['close'] < range_low:
            return "SELL"
        return None
    
    # ========== ORIGINAL STRATEGY 2: Bull/Bear Flag ==========
    @staticmethod
    def bull_flag(df: pd.DataFrame) -> Optional[str]:
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
    
    # ========== ORIGINAL STRATEGY 3: Double Top/Bottom ==========
    @staticmethod
    def double_top_bottom(df: pd.DataFrame, lookback: int = 10) -> Optional[str]:
        if len(df) < lookback + 2:
            return None
        highs = df["high"].tail(lookback)
        lows = df["low"].tail(lookback)
        if abs(highs.iloc[-1] - highs.iloc[-3]) < highs.mean() * 0.001:
            return "SELL"
        if abs(lows.iloc[-1] - lows.iloc[-3]) < lows.mean() * 0.001:
            return "BUY"
        return None
    
    # ========== ORIGINAL STRATEGY 4: Head & Shoulders ==========
    @staticmethod
    def head_shoulders(df: pd.DataFrame) -> Optional[str]:
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
    
    # ========== ORIGINAL STRATEGY 5: Triangle ==========
    @staticmethod
    def triangle(df: pd.DataFrame) -> Optional[str]:
        if len(df) < 20:
            return None
        highs = df["high"].tail(20)
        lows = df["low"].tail(20)
        if highs.std() < highs.mean() * 0.001:
            return "BUY"
        if lows.std() < lows.mean() * 0.001:
            return "SELL"
        return None
    
    # ========== ORIGINAL STRATEGY 6: Fair Value Gap ==========
    @staticmethod
    def fair_value_gap(df: pd.DataFrame) -> Optional[str]:
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
    
    # ========== ORIGINAL STRATEGY 7: Volume Spike ==========
    @staticmethod
    def volume_spike(df: pd.DataFrame, lookback: int = 20, threshold: float = 1.5) -> bool:
        if "volume" not in df.columns or df["volume"].sum() == 0:
            return False
        return df["volume"].iloc[-1] > df["volume"].tail(lookback).mean() * threshold
    
    # ========== ORIGINAL STRATEGY 8: SME Pattern ==========
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
    
    # ========== ORIGINAL STRATEGY 9: Breakout Retest ==========
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
    
    # ========== ORIGINAL STRATEGY 10: EMA Mean Reversion ==========
    @staticmethod
    def ema_mean_reversion(df: pd.DataFrame, lookback: int = 20, threshold: float = 0.002) -> Optional[str]:
        if len(df) < lookback:
            return None
        ema, deviation = TechnicalAnalyzer.calculate_ema_deviation(df, lookback)
        current_deviation = deviation.iloc[-1]
        if current_deviation < -threshold:
            if df['close'].iloc[-1] > df['open'].iloc[-1]:
                return "BUY"
        if current_deviation > threshold:
            if df['close'].iloc[-1] < df['open'].iloc[-1]:
                return "SELL"
        return None
    
    # ========== ORIGINAL STRATEGY 11: VWAP Mean Reversion ==========
    @staticmethod
    def vwap_mean_reversion_crypto(df: pd.DataFrame, threshold: float = 0.002) -> Optional[str]:
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
    
    # ========== ORIGINAL STRATEGY 12: Liquidity Sweep ==========
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
    
    # ========== ORIGINAL STRATEGY 13: Opening Range Breakout ==========
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
    
    # ========== ORIGINAL STRATEGY 14: Session Liquidity Sweep Engine ==========
    @staticmethod
    def session_liquidity_sweep_engine(df: pd.DataFrame, session_lookback: int = 24) -> Optional[str]:
        if len(df) < session_lookback + 5:
            return None
        session_data = df.iloc[-session_lookback:]
        session_high = session_data['high'].max()
        session_low = session_data['low'].min()
        recent_candles = df.iloc[-5:]
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
        for i in range(len(recent_candles) - 1, -1, -1):
            candle = recent_candles.iloc[i]
            if candle['low'] < session_low * 1.001:
                if i < len(recent_candles) - 1:
                    next_candles = recent_candles.iloc[i+1:]
                    for _, rev_candle in next_candles.iterrows():
                        if is_displacement(rev_candle, "BUY"):
                            return "BUY"
        for i in range(len(recent_candles) - 1, -1, -1):
            candle = recent_candles.iloc[i]
            if candle['high'] > session_high * 0.999:
                if i < len(recent_candles) - 1:
                    next_candles = recent_candles.iloc[i+1:]
                    for _, rev_candle in next_candles.iterrows():
                        if is_displacement(rev_candle, "SELL"):
                            return "SELL"
        return None
    
    # ========== ORIGINAL STRATEGY 15: Daily Liquidity Cycle Model ==========
    @staticmethod
    def daily_liquidity_cycle_model(df: pd.DataFrame, df_1h: Optional[pd.DataFrame] = None) -> Optional[str]:
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
        last_candle = df.iloc[-1]
        upper_wick = last_candle['high'] - max(last_candle['open'], last_candle['close'])
        lower_wick = min(last_candle['open'], last_candle['close']) - last_candle['low']
        body = abs(last_candle['close'] - last_candle['open'])
        if position_in_range > 0.85 and recent_volume > avg_volume * 1.2:
            recent_candles = df.tail(3)
            bearish_count = sum(1 for _, c in recent_candles.iterrows() if c['close'] < c['open'])
            if bearish_count >= 2:
                return "SELL"
        if position_in_range < 0.15 and recent_volume > avg_volume * 1.2:
            recent_candles = df.tail(3)
            bullish_count = sum(1 for _, c in recent_candles.iterrows() if c['close'] > c['open'])
            if bullish_count >= 2:
                return "BUY"
        if 0.4 < position_in_range < 0.6:
            if upper_wick > body * 2 and last_candle['close'] < last_candle['open']:
                return "SELL"
            if lower_wick > body * 2 and last_candle['close'] > last_candle['open']:
                return "BUY"
        return None
    
    # ========== ORIGINAL STRATEGY 16: EMA Trend (Multi-timeframe) ==========
    @staticmethod
    def ema_trend_15m(df: pd.DataFrame) -> Optional[str]:
        return TechnicalAnalyzer.ema_trend(df, 20, 50)
    
    @staticmethod
    def ema_trend_1h(df: pd.DataFrame) -> Optional[str]:
        return TechnicalAnalyzer.ema_trend(df, 20, 50)
    
    @staticmethod
    def ema_trend_4h(df: pd.DataFrame) -> Optional[str]:
        return TechnicalAnalyzer.ema_trend(df, 20, 50)

# ================= STRATEGY AGGREGATION ENGINE =================

class StrategyAggregationEngine:
    """
    Aggregates all 16 original strategies with weighted scoring.
    Preserves original logic while adding institutional enhancements.
    """
    
    # Original strategy weights preserved
    STRATEGY_WEIGHTS = {
        "EMA_Trend_15m": 2.0,
        "EMA_Trend_1h": 2.0,
        "EMA_Trend_4h": 2.0,
        "Breakout": 1.0,
        "Bull_Bear_Flag": 1.0,
        "Double_Top_Bottom": 1.0,
        "Head_Shoulders": 1.0,
        "Triangle": 1.0,
        "Fair_Value_Gap": 1.5,
        "Volume_Spike": 0.5,
        "SME_Pattern": 1.0,
        "Breakout_Retest": 2.0,
        "EMA_Reversion": 1.0,
        "VWAP_Reversion": 1.0,
        "Liquidity_Sweep": 2.5,
        "ORB": 1.0,
        "SLSE": 2.5,
        "DLCM": 1.5,
    }
    
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
        self.regime_engine = RegimeDetectionEngine()
        self.prob_engine = ProbabilisticScoringEngine()
    
    def calculate_score(
        self,
        df_15m: pd.DataFrame,
        df_1h: pd.DataFrame,
        df_4h: Optional[pd.DataFrame] = None,
        is_crypto: bool = False
    ) -> Tuple[float, str, Optional[str], float, List[str], MarketRegime, float]:
        """
        Calculate weighted score using ALL 16 original strategies.
        """
        regime = self.regime_engine.detect_regime(df_15m)
        direction = None
        raw_score = 0
        weighted_score = 0.0
        triggered_strategies = []
        primary_strategy = None
        
        # 1. EMA Trend (15m)
        ema_15m = self.analyzer.ema_trend_15m(df_15m)
        if ema_15m != "NEUTRAL":
            direction = ema_15m
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["EMA_Trend_15m"]
            if self._is_strategy_allowed("EMA_Trend_15m", regime):
                weighted_score += weight
                triggered_strategies.append(f"EMA Trend 15m ({weight}x)")
        
        # 2. EMA Trend (1h)
        ema_1h = self.analyzer.ema_trend_1h(df_1h)
        if ema_1h == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["EMA_Trend_1h"]
            if self._is_strategy_allowed("EMA_Trend_1h", regime):
                weighted_score += weight
                triggered_strategies.append(f"EMA Trend 1h ({weight}x)")
        
        # 3. Breakout (FIXED)
        breakout = self.analyzer.breakout(df_15m)
        if breakout == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Breakout"]
            if self._is_strategy_allowed("Breakout", regime):
                weighted_score += weight
                triggered_strategies.append(f"Breakout ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Breakout"
        
        # 4. Bull/Bear Flag
        flag = self.analyzer.bull_flag(df_15m)
        if flag == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Bull_Bear_Flag"]
            if self._is_strategy_allowed("Bull_Bear_Flag", regime):
                weighted_score += weight
                triggered_strategies.append(f"Bull/Bear Flag ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Flag"
        
        # 5. Double Top/Bottom
        dt = self.analyzer.double_top_bottom(df_15m)
        if dt == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Double_Top_Bottom"]
            if self._is_strategy_allowed("Double_Top_Bottom", regime):
                weighted_score += weight
                triggered_strategies.append(f"Double Top/Bottom ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Double Top/Bottom"
        
        # 6. Head & Shoulders
        hs = self.analyzer.head_shoulders(df_15m)
        if hs == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Head_Shoulders"]
            if self._is_strategy_allowed("Head_Shoulders", regime):
                weighted_score += weight
                triggered_strategies.append(f"Head & Shoulders ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Head & Shoulders"
        
        # 7. Triangle
        triangle = self.analyzer.triangle(df_15m)
        if triangle == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Triangle"]
            if self._is_strategy_allowed("Triangle", regime):
                weighted_score += weight
                triggered_strategies.append(f"Triangle ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Triangle"
        
        # 8. Fair Value Gap
        fvg = self.analyzer.fair_value_gap(df_15m)
        if fvg == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Fair_Value_Gap"]
            if self._is_strategy_allowed("Fair_Value_Gap", regime):
                weighted_score += weight
                triggered_strategies.append(f"FVG ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "FVG"
        
        # 9. Volume Spike
        if self.analyzer.volume_spike(df_15m):
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Volume_Spike"]
            if self._is_strategy_allowed("Volume_Spike", regime):
                weighted_score += weight
                triggered_strategies.append(f"Volume Spike ({weight}x)")
        
        # 10. SME Pattern
        sme = self.analyzer.sme_pattern(df_15m)
        if sme == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["SME_Pattern"]
            if self._is_strategy_allowed("SME_Pattern", regime):
                weighted_score += weight
                triggered_strategies.append(f"SME Pattern ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "SME Pattern"
        
        # 11. Breakout Retest
        retest = self.analyzer.breakout_retest(df_15m)
        if retest == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Breakout_Retest"]
            if self._is_strategy_allowed("Breakout_Retest", regime):
                weighted_score += weight
                triggered_strategies.append(f"Breakout Retest ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Breakout Retest"
        
        # 12. Mean Reversion (Regime-dependent)
        if is_crypto:
            vwap = self.analyzer.vwap_mean_reversion_crypto(df_15m)
            if vwap == direction:
                raw_score += 1
                weight = self.STRATEGY_WEIGHTS["VWAP_Reversion"]
                if self._is_strategy_allowed("VWAP_Reversion", regime):
                    weighted_score += weight
                    triggered_strategies.append(f"VWAP Reversion ({weight}x)")
                    if primary_strategy is None:
                        primary_strategy = "VWAP Reversion"
        else:
            ema_rev = self.analyzer.ema_mean_reversion(df_15m)
            if ema_rev == direction:
                raw_score += 1
                weight = self.STRATEGY_WEIGHTS["EMA_Reversion"]
                if self._is_strategy_allowed("EMA_Reversion", regime):
                    weighted_score += weight
                    triggered_strategies.append(f"EMA Reversion ({weight}x)")
                    if primary_strategy is None:
                        primary_strategy = "EMA Reversion"
        
        # 13. Liquidity Sweep
        sweep = self.analyzer.liquidity_sweep(df_15m)
        if sweep == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["Liquidity_Sweep"]
            if self._is_strategy_allowed("Liquidity_Sweep", regime):
                weighted_score += weight
                triggered_strategies.append(f"Liquidity Sweep ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "Liquidity Sweep"
        
        # 14. Opening Range Breakout
        orb = self.analyzer.opening_range_breakout(df_15m)
        if orb == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["ORB"]
            if self._is_strategy_allowed("ORB", regime):
                weighted_score += weight
                triggered_strategies.append(f"ORB ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "ORB"
        
        # 15. Session Liquidity Sweep Engine
        slse = self.analyzer.session_liquidity_sweep_engine(df_15m)
        if slse == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["SLSE"]
            if self._is_strategy_allowed("SLSE", regime):
                weighted_score += weight
                triggered_strategies.append(f"SLSE ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "SLSE"
        
        # 16. Daily Liquidity Cycle Model
        dlcm = self.analyzer.daily_liquidity_cycle_model(df_15m, df_1h)
        if dlcm == direction:
            raw_score += 1
            weight = self.STRATEGY_WEIGHTS["DLCM"]
            if self._is_strategy_allowed("DLCM", regime):
                weighted_score += weight
                triggered_strategies.append(f"DLCM ({weight}x)")
                if primary_strategy is None:
                    primary_strategy = "DLCM"
        
        # SOGEKING: 4h confirmation required
        if PERSONALITY == "sogeking" and df_4h is not None:
            ema_4h = self.analyzer.ema_trend_4h(df_4h)
            if ema_4h == direction:
                raw_score += 1
                weight = self.STRATEGY_WEIGHTS["EMA_Trend_4h"]
                weighted_score += weight
                triggered_strategies.append(f"EMA Trend 4h ({weight}x)")
            else:
                return 0.0, "NEUTRAL", None, 0.0, [], regime, 0.0
        
        adx = self.analyzer.calculate_adx(df_15m, ADX_PERIOD).iloc[-1]
        
        return weighted_score, direction or "NEUTRAL", primary_strategy, adx, triggered_strategies, regime, float(raw_score)
    
    def _is_strategy_allowed(self, strategy_name: str, regime: MarketRegime) -> bool:
        """Check if strategy is allowed in current regime."""
        mean_reversion = ["VWAP_Reversion", "EMA_Reversion"]
        if any(mr in strategy_name for mr in mean_reversion):
            return regime in [MarketRegime.RANGING_COMPRESSION, MarketRegime.COMPRESSION_SQUEEZE, MarketRegime.ACCUMULATION]
        
        breakout = ["Breakout", "ORB", "SME_Pattern", "SLSE"]
        if any(b in strategy_name for b in breakout):
            return regime in [MarketRegime.TRENDING_STRONG, MarketRegime.VOLATILE_BREAKOUT, MarketRegime.COMPRESSION_SQUEEZE]
        
        trend = ["EMA_Trend", "EMA_Trend_15m", "EMA_Trend_1h", "EMA_Trend_4h"]
        if any(t in strategy_name for t in trend):
            return regime in [MarketRegime.TRENDING_STRONG, MarketRegime.TRENDING_WEAK, MarketRegime.VOLATILE_BREAKOUT]
        
        if "Liquidity_Sweep" in strategy_name:
            return True
        
        return True

# ================= SUPPORTING ENGINES (Preserved from v3.0/v3.1) =================

class RegimeDetectionEngine:
    def __init__(self):
        self.lookback_periods = {"micro": 20, "short": 50, "medium": 100, "long": 200}
        self.regime_history = defaultdict(lambda: deque(maxlen=100))
    
    def detect_regime(self, df: pd.DataFrame, df_higher: Optional[pd.DataFrame] = None) -> MarketRegime:
        if len(df) < 50:
            return MarketRegime.RANGING_COMPRESSION
        
        analyzer = TechnicalAnalyzer()
        adx = analyzer.calculate_adx(df, 14).iloc[-1]
        atr = analyzer.calculate_atr(df, 14)
        atr_percentile = (atr.tail(50) <= atr.iloc[-1]).mean() * 100
        
        ema_fast = df['close'].ewm(span=20, adjust=False).mean().iloc[-1]
        ema_slow = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
        trend_direction = 1 if ema_fast > ema_slow else -1
        
        volatility_regime = self._classify_volatility(atr_percentile)
        trend_strength = "strong" if adx > 30 else "moderate" if adx > 20 else "weak"
        
        if trend_strength == "strong" and volatility_regime in ["normal", "high"]:
            return MarketRegime.TRENDING_STRONG if trend_direction > 0 else MarketRegime.TRENDING_STRONG
        elif trend_strength == "weak" and volatility_regime == "low":
            return MarketRegime.TRENDING_WEAK
        elif volatility_regime == "extreme":
            return MarketRegime.VOLATILE_BREAKOUT
        elif volatility_regime == "very_low":
            return MarketRegime.COMPRESSION_SQUEEZE
        
        return MarketRegime.RANGING_COMPRESSION
    
    def _classify_volatility(self, percentile: float) -> str:
        if percentile < 10: return "very_low"
        elif percentile < 30: return "low"
        elif percentile < 70: return "normal"
        elif percentile < 90: return "high"
        else: return "extreme"

class ProbabilisticScoringEngine:
    STRATEGY_BASE_PROBABILITIES = {
        "EMA_Trend_15m": 0.58, "EMA_Trend_1h": 0.62, "EMA_Trend_4h": 0.65,
        "Breakout": 0.55, "Breakout_Retest": 0.60, "Liquidity_Sweep": 0.63,
        "SLSE": 0.65, "Fair_Value_Gap": 0.57, "VWAP_Reversion": 0.54,
        "EMA_Reversion": 0.52, "Bollinger_Squeeze": 0.56, "Momentum_Burst": 0.61,
        "Absorption_Zone": 0.64, "Session_High_Low": 0.58, "Weekly_High_Low": 0.60
    }
    
    def __init__(self):
        self.calibration_history = []
        self.confluence_exponent = CONFLUENCE_WEIGHT_EXPONENT
    
    def calculate_probability(self, strategies: List[str], market_context: MarketContext,
                            liquidity_profile: LiquidityProfile, orderflow: OrderflowSignature,
                            direction: str) -> ProbabilityModel:
        if not strategies:
            return ProbabilityModel(0.5, 0.0, 0.0, 0.0, 0.0, 0.0)
        
        base_probs = [self.STRATEGY_BASE_PROBABILITIES.get(s, 0.52) for s in strategies]
        weighted_probs = sorted(base_probs, reverse=True)
        
        confluence_score = sum(weighted_probs) / len(weighted_probs) if weighted_probs else 0.5
        confluence_score = min(0.95, confluence_score * (1 + 0.1 * len(strategies)))
        
        return ProbabilityModel(
            base_probability=confluence_score,
            confluence_score=confluence_score,
            regime_adjustment=0.0,
            liquidity_adjustment=0.0,
            timing_adjustment=0.0,
            historical_calibration=0.0
        )

class InstitutionalLiquidityEngine:
    def __init__(self):
        self.liquidity_data = defaultdict(lambda: {
            'session': {'high': None, 'low': None, 'timestamp': None},
            'weekly': {'high': None, 'low': None, 'timestamp': None},
            'equal_highs': [], 'equal_lows': [],
            'stop_clusters': {'above': [], 'below': []}, 'pools': []
        })
        self.price_history = defaultdict(lambda: deque(maxlen=500))
        self.tolerance = 0.001
    
    def update(self, symbol: str, df: pd.DataFrame):
        if len(df) < 10:
            return
        
        for idx, row in df.iterrows():
            self.price_history[symbol].append({
                'timestamp': idx, 'open': row['open'], 'high': row['high'],
                'low': row['low'], 'close': row['close'], 'volume': row.get('volume', 0)
            })
        
        self._update_session_levels(symbol, df.index[-1])
        self._update_weekly_levels(symbol, df.index[-1])
        self._detect_equal_levels(symbol)
        self._detect_stop_clusters(symbol)
        self._identify_liquidity_pools(symbol)
    
    def _update_session_levels(self, symbol: str, current_time: datetime):
        history = list(self.price_history[symbol])
        if len(history) < 96:
            return
        session_data = history[-96:]
        self.liquidity_data[symbol]['session'] = {
            'high': max(d['high'] for d in session_data),
            'low': min(d['low'] for d in session_data),
            'timestamp': current_time
        }
    
    def _update_weekly_levels(self, symbol: str, current_time: datetime):
        history = list(self.price_history[symbol])
        if len(history) < 672:
            return
        weekly_data = history[-672:]
        self.liquidity_data[symbol]['weekly'] = {
            'high': max(d['high'] for d in weekly_data),
            'low': min(d['low'] for d in weekly_data),
            'timestamp': current_time
        }
    
    def _detect_equal_levels(self, symbol: str, lookback: int = 50):
        history = list(self.price_history[symbol])[-lookback:]
        if len(history) < 20:
            return
        highs = [d['high'] for d in history]
        lows = [d['low'] for d in history]
        
        equal_highs = []
        for i, h1 in enumerate(highs):
            for h2 in highs[i+1:]:
                if abs(h1 - h2) / h1 < self.tolerance:
                    equal_highs.append(h1)
        
        equal_lows = []
        for i, l1 in enumerate(lows):
            for l2 in lows[i+1:]:
                if abs(l1 - l2) / l1 < self.tolerance:
                    equal_lows.append(l1)
        
        self.liquidity_data[symbol]['equal_highs'] = list(dict.fromkeys(equal_highs))[:5]
        self.liquidity_data[symbol]['equal_lows'] = list(dict.fromkeys(equal_lows))[:5]
    
    def _detect_stop_clusters(self, symbol: str, lookback: int = 100):
        history = list(self.price_history[symbol])[-lookback:]
        if len(history) < 50:
            return
        
        highs = [d['high'] for d in history]
        stop_clusters_above = []
        for level in highs:
            pierced = sum(1 for d in history if d['high'] > level and d['close'] < level)
            if pierced >= 3:
                density = pierced / len(history)
                stop_clusters_above.append((level * 1.001, density))
        
        lows = [d['low'] for d in history]
        stop_clusters_below = []
        for level in lows:
            pierced = sum(1 for d in history if d['low'] < level and d['close'] > level)
            if pierced >= 3:
                density = pierced / len(history)
                stop_clusters_below.append((level * 0.999, density))
        
        self.liquidity_data[symbol]['stop_clusters']['above'] = sorted(stop_clusters_above, key=lambda x: x[1], reverse=True)[:3]
        self.liquidity_data[symbol]['stop_clusters']['below'] = sorted(stop_clusters_below, key=lambda x: x[1], reverse=True)[:3]
    
    def _identify_liquidity_pools(self, symbol: str):
        history = list(self.price_history[symbol])
        if len(history) < 50 or all(d['volume'] == 0 for d in history):
            return
        
        price_range = max(d['high'] for d in history) - min(d['low'] for d in history)
        if price_range == 0:
            return
        
        bins = 20
        bin_size = price_range / bins
        volume_by_price = defaultdict(float)
        
        for d in history:
            mid_price = (d['high'] + d['low']) / 2
            bin_idx = int((mid_price - min(d['low'] for d in history)) / bin_size)
            volume_by_price[bin_idx] += d['volume']
        
        avg_volume = sum(volume_by_price.values()) / len(volume_by_price)
        pools = []
        
        for bin_idx, volume in volume_by_price.items():
            if volume > avg_volume * 1.5:
                price_level = min(d['low'] for d in history) + (bin_idx * bin_size)
                pools.append({'price': price_level, 'volume': volume, 'strength': volume / avg_volume})
        
        self.liquidity_data[symbol]['pools'] = sorted(pools, key=lambda x: x['volume'], reverse=True)[:5]
    
    def get_profile(self, symbol: str) -> LiquidityProfile:
        data = self.liquidity_data[symbol]
        return LiquidityProfile(
            session_high=data['session']['high'],
            session_low=data['session']['low'],
            weekly_high=data['weekly']['high'],
            weekly_low=data['weekly']['low'],
            equal_highs=data['equal_highs'],
            equal_lows=data['equal_lows'],
            stop_clusters_above=data['stop_clusters']['above'],
            stop_clusters_below=data['stop_clusters']['below'],
            liquidity_pools=data['pools']
        )

class OrderflowDetectionEngine:
    def __init__(self):
        self.displacement_threshold = 2.0
        self.momentum_threshold = 1.5
        self.absorption_volume_factor = 2.0
    
    def analyze(self, df: pd.DataFrame, df_micro: Optional[pd.DataFrame] = None) -> OrderflowSignature:
        if len(df) < 10:
            return OrderflowSignature()
        
        analyzer = TechnicalAnalyzer()
        atr = analyzer.calculate_atr(df, 14).iloc[-1]
        
        displacement, disp_mag = self._detect_displacement(df, atr)
        imbalance = self._detect_imbalance(df, atr)
        momentum, mom_strength = self._detect_momentum_burst(df, atr)
        absorption, abs_ratio = self._detect_absorption(df)
        delta = self._estimate_delta_pressure(df)
        
        return OrderflowSignature(
            displacement_detected=displacement,
            displacement_magnitude=disp_mag,
            imbalance_zone=imbalance,
            momentum_burst=momentum,
            momentum_strength=mom_strength,
            absorption_detected=absorption,
            absorption_volume_ratio=abs_ratio,
            delta_pressure=delta
        )
    
    def _detect_displacement(self, df: pd.DataFrame, atr: float) -> Tuple[bool, float]:
        if len(df) < 3 or atr == 0:
            return False, 0.0
        recent = df.tail(3)
        for i in range(len(recent)):
            candle = recent.iloc[i]
            body = abs(candle['close'] - candle['open'])
            range_val = candle['high'] - candle['low']
            if range_val == 0:
                continue
            body_ratio = body / range_val
            if body_ratio > 0.7 and body > atr * self.displacement_threshold:
                return True, body / atr
        return False, 0.0
    
    def _detect_imbalance(self, df: pd.DataFrame, atr: float) -> Optional[Tuple[float, float]]:
        if len(df) < 3:
            return None
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        if c3['low'] > c1['high']:
            return (c1['high'], c3['low'])
        if c3['high'] < c1['low']:
            return (c3['high'], c1['low'])
        return None
    
    def _detect_momentum_burst(self, df: pd.DataFrame, atr: float) -> Tuple[bool, float]:
        if len(df) < 5:
            return False, 0.0
        recent = df.tail(5)
        closes = recent['close'].values
        velocities = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        if len(velocities) < 2:
            return False, 0.0
        recent_velocity = abs(velocities[-1])
        avg_velocity = np.mean([abs(v) for v in velocities[:-1]])
        if avg_velocity == 0:
            return False, 0.0
        acceleration = recent_velocity / avg_velocity
        if acceleration > self.momentum_threshold and recent_velocity > atr:
            return True, acceleration
        return False, 0.0
    
    def _detect_absorption(self, df: pd.DataFrame) -> Tuple[bool, float]:
        if len(df) < 5 or 'volume' not in df.columns:
            return False, 0.0
        recent = df.tail(5)
        volumes = recent['volume'].values
        if np.mean(volumes[:-1]) == 0:
            return False, 0.0
        volume_ratio = volumes[-1] / np.mean(volumes[:-1])
        last_candle = recent.iloc[-1]
        body = abs(last_candle['close'] - last_candle['open'])
        range_val = last_candle['high'] - last_candle['low']
        if range_val == 0:
            return False, 0.0
        body_ratio = body / range_val
        if volume_ratio > self.absorption_volume_factor and body_ratio < 0.3:
            return True, volume_ratio
        return False, 0.0
    
    def _estimate_delta_pressure(self, df: pd.DataFrame) -> float:
        if len(df) < 5:
            return 0.0
        recent = df.tail(5)
        pressure = 0.0
        for _, candle in recent.iterrows():
            body = candle['close'] - candle['open']
            range_val = candle['high'] - candle['low']
            if range_val == 0:
                continue
            body_ratio = body / range_val
            if body > 0:
                position = (candle['close'] - candle['low']) / range_val
            else:
                position = (candle['high'] - candle['close']) / range_val
            pressure += body_ratio * (position - 0.5) * 2
        return pressure / len(recent)

class DynamicRiskEngine:
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
        self.atr_multiplier_sl_base = ATR_MULTIPLIER_SL
        self.atr_multiplier_tp_min = ATR_MULTIPLIER_TP_BASE if 'ATR_MULTIPLIER_TP_BASE' in globals() else 2.0
        self.atr_multiplier_tp_max = ATR_MULTIPLIER_TP_MAX if 'ATR_MULTIPLIER_TP_MAX' in globals() else 4.0
    
    def calculate_risk_levels(self, df: pd.DataFrame, direction: str, market_context: MarketContext,
                            liquidity_profile: LiquidityProfile, probability: float) -> Tuple[float, float, float, float, float, str]:
        entry = df['close'].iloc[-1]
        atr = market_context.atr
        
        sl, sl_type = self._calculate_optimal_sl(df, direction, entry, atr, liquidity_profile)
        tp, tp1 = self._calculate_optimal_tp(entry, sl, atr, direction, market_context, probability)
        
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = reward / risk if risk > 0 else 0
        
        return entry, sl, tp, tp1, atr, rr, sl_type
    
    def _calculate_optimal_sl(self, df: pd.DataFrame, direction: str, entry: float, atr: float,
                            liquidity_profile: LiquidityProfile) -> Tuple[float, str]:
        nearest_liq = liquidity_profile.get_nearest_liquidity(entry, direction)
        if nearest_liq:
            level_price, level_type, distance = nearest_liq
            max_sl_distance = atr * 2.5
            if distance <= max_sl_distance:
                buffer = atr * 0.2
                sl = level_price - buffer if direction == "BUY" else level_price + buffer
                min_distance = atr * 0.5
                if abs(entry - sl) < min_distance:
                    sl = entry - min_distance if direction == "BUY" else entry + min_distance
                return sl, f"liquidity_{level_type}"
        
        swing_sl = self._find_swing_sl(df, direction, atr)
        if swing_sl:
            return swing_sl
        
        multiplier = self.atr_multiplier_sl_base
        sl = entry - (atr * multiplier) if direction == "BUY" else entry + (atr * multiplier)
        return sl, f"atr_{multiplier}x"
    
    def _find_swing_sl(self, df: pd.DataFrame, direction: str, atr: float) -> Optional[Tuple[float, str]]:
        lookback = min(20, len(df) - 1)
        if lookback < 5:
            return None
        recent = df.tail(lookback)
        
        if direction == "BUY":
            for i in range(2, len(recent) - 2):
                if (recent['low'].iloc[i] < recent['low'].iloc[i-1] and
                    recent['low'].iloc[i] < recent['low'].iloc[i-2] and
                    recent['low'].iloc[i] < recent['low'].iloc[i+1] and
                    recent['low'].iloc[i] < recent['low'].iloc[i+2]):
                    swing_low = recent['low'].iloc[i]
                    current_price = df['close'].iloc[-1]
                    if 0.5 * atr <= current_price - swing_low <= 2.5 * atr:
                        return (swing_low - (atr * 0.15), f"swing_low_{i}")
        else:
            for i in range(2, len(recent) - 2):
                if (recent['high'].iloc[i] > recent['high'].iloc[i-1] and
                    recent['high'].iloc[i] > recent['high'].iloc[i-2] and
                    recent['high'].iloc[i] > recent['high'].iloc[i+1] and
                    recent['high'].iloc[i] > recent['high'].iloc[i+2]):
                    swing_high = recent['high'].iloc[i]
                    current_price = df['close'].iloc[-1]
                    if 0.5 * atr <= swing_high - current_price <= 2.5 * atr:
                        return (swing_high + (atr * 0.15), f"swing_high_{i}")
        return None
    
    def _calculate_optimal_tp(self, entry: float, sl: float, atr: float, direction: str,
                            market_context: MarketContext, probability: float) -> Tuple[float, Optional[float]]:
        risk = abs(entry - sl)
        prob_factor = (probability - 0.5) * 2
        tp_multiplier = self.atr_multiplier_tp_min + (prob_factor * (self.atr_multiplier_tp_max - self.atr_multiplier_tp_min))
        
        if market_context.volatility_regime == VolatilityRegime.EXTREME:
            tp_multiplier *= 0.7
        
        tp = entry + (atr * tp_multiplier) if direction == "BUY" else entry - (atr * tp_multiplier)
        
        if PARTIAL_TP_ENABLED:
            tp1 = entry + risk if direction == "BUY" else entry - risk
            if (direction == "BUY" and tp1 >= tp) or (direction == "SELL" and tp1 <= tp):
                tp1 = (entry + tp) / 2 if direction == "BUY" else (entry + tp) / 2
        else:
            tp1 = None
        
        return tp, tp1
    
    def calculate_position_size(self, account_size: float, risk_per_trade_pct: float, entry: float,
                              sl: float, probability: float, portfolio_state: PortfolioState) -> Tuple[float, float]:
        edge = probability - 0.5
        kelly_pct = edge * 2
        kelly_conservative = kelly_pct * KELLY_FRACTION
        
        risk_amount = account_size * (risk_per_trade_pct / 100) * kelly_conservative
        
        if portfolio_state.is_drawdown_throttle_active:
            risk_amount *= 0.5
        
        available_risk = portfolio_state.available_risk_capacity
        if available_risk <= 0:
            return 0.0, 0.0
        
        max_risk_amount = account_size * (available_risk / 100)
        risk_amount = min(risk_amount, max_risk_amount)
        
        price_risk = abs(entry - sl)
        if price_risk == 0:
            return 0.0, 0.0
        
        position_size = risk_amount / price_risk
        r_risk = risk_amount / account_size * 100
        
        return position_size, r_risk

class PortfolioRiskManager:
    def __init__(self):
        self.state = PortfolioState(timestamp=datetime.now(pytz.UTC))
        self.correlation_matrix = defaultdict(dict)
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.drawdown_threshold = 0.05
        self.max_correlation = 0.7
    
    def update_state(self, trades: List[Trade], account_size: float = 10000.0):
        self.state.timestamp = datetime.now(pytz.UTC)
        self.state.active_trades = len([t for t in trades if not t.is_closed])
        self.state.exposure_by_group = defaultdict(float)
        self.state.exposure_by_symbol = defaultdict(float)
        
        total_risk = 0.0
        for trade in trades:
            if not trade.is_closed:
                risk = trade.signal.position_size_risk
                symbol = trade.signal.symbol
                group = WATCHLIST.get(symbol, {}).get('group', 0)
                self.state.exposure_by_symbol[symbol] += risk
                self.state.exposure_by_group[group] += risk
                total_risk += risk
        
        self.state.total_exposure = total_risk
        self.state.current_equity = account_size
        
        if self.state.current_equity > self.state.peak_equity:
            self.state.peak_equity = self.state.current_equity
        
        if self.state.peak_equity > 0:
            self.state.current_drawdown = (self.state.peak_equity - self.state.current_equity) / self.state.peak_equity
    
    def can_add_position(self, symbol: str, direction: str, risk: float) -> Tuple[bool, str]:
        if self.state.total_exposure + risk > MAX_PORTFOLIO_RISK_PCT:
            return False, f"Max exposure reached"
        
        group = WATCHLIST.get(symbol, {}).get('group', 0)
        correlated_exposure = self.state.exposure_by_group.get(group, 0)
        
        if correlated_exposure > 0 and risk > 0:
            active_in_group = sum(1 for s, r in self.state.exposure_by_symbol.items()
                                if WATCHLIST.get(s, {}).get('group') == group and r > 0)
            if active_in_group >= MAX_CORRELATED_EXPOSURE:
                return False, f"Max correlated positions in group {group}"
        
        if self.state.exposure_by_symbol.get(symbol, 0) > 0:
            return False, f"Existing position in {symbol}"
        
        if self.state.is_drawdown_throttle_active:
            if risk > MAX_PORTFOLIO_RISK_PCT / 6:
                return False, f"Drawdown throttle active"
        
        return True, "OK"
    
    def update_correlation(self, symbol: str, df: pd.DataFrame):
        if len(df) < 20:
            return
        returns = df['close'].pct_change().dropna()
        self.price_history[symbol].extend(returns.tail(50))
        
        for other_symbol, other_history in self.price_history.items():
            if other_symbol == symbol:
                continue
            if len(other_history) < 20 or len(self.price_history[symbol]) < 20:
                continue
            min_len = min(len(self.price_history[symbol]), len(other_history))
            corr = np.corrcoef(list(self.price_history[symbol])[-min_len:], list(other_history)[-min_len:])[0, 1]
            self.correlation_matrix[symbol][other_symbol] = corr
    
    def get_correlation(self, symbol1: str, symbol2: str) -> float:
        return self.correlation_matrix.get(symbol1, {}).get(symbol2, 0.0)

class ExecutionSimulator:
    def __init__(self):
        self.slippage_model = "volatility"
        self.spread_model = "adaptive"
    
    def simulate_entry(self, signal: Signal, market_context: MarketContext) -> Tuple[float, float, float]:
        intended_price = signal.entry_price
        direction = signal.direction
        spread = self._estimate_spread(market_context)
        slippage = self._estimate_slippage(signal, market_context)
        
        fill_price = intended_price + spread + slippage if direction == "BUY" else intended_price - spread - slippage
        slippage_cost = abs(fill_price - intended_price) / intended_price
        fill_prob = self._estimate_fill_probability(signal, market_context)
        
        return fill_price, slippage_cost, fill_prob
    
    def simulate_exit(self, trade: Trade, exit_type: str, market_price: float,
                     market_context: MarketContext) -> Tuple[float, float]:
        direction = trade.signal.direction
        is_profit = exit_type in ["tp_hit", "trailing_stop", "partial_tp"]
        base_slippage = 0.0001 if is_profit else 0.0002
        
        vol_mult = {VolatilityRegime.VERY_LOW: 0.5, VolatilityRegime.LOW: 0.8,
                   VolatilityRegime.NORMAL: 1.0, VolatilityRegime.HIGH: 1.5,
                   VolatilityRegime.EXTREME: 2.5}.get(market_context.volatility_regime, 1.0)
        
        slippage = base_slippage * vol_mult * market_price
        fill_price = market_price - slippage if direction == "BUY" else market_price + slippage
        slippage_cost = abs(fill_price - market_price) / market_price
        
        return fill_price, slippage_cost
    
    def _estimate_spread(self, context: MarketContext) -> float:
        base = context.spread_estimate
        vol_mult = {VolatilityRegime.VERY_LOW: 0.8, VolatilityRegime.LOW: 0.9,
                   VolatilityRegime.NORMAL: 1.0, VolatilityRegime.HIGH: 1.3,
                   VolatilityRegime.EXTREME: 2.0}.get(context.volatility_regime, 1.0)
        return base * vol_mult
    
    def _estimate_slippage(self, signal: Signal, context: MarketContext) -> float:
        atr_pct = context.atr / signal.entry_price
        return signal.entry_price * atr_pct * 0.1
    
    def _estimate_fill_probability(self, signal: Signal, context: MarketContext) -> float:
        base_prob = 0.95
        if context.volatility_regime == VolatilityRegime.EXTREME:
            base_prob -= 0.1
        return max(0.5, base_prob)

class SessionManager:
    def __init__(self):
        self.timezone = pytz.UTC
        self.session_hours = {
            SessionType.SYDNEY: (time(21, 0), time(6, 0)),
            SessionType.TOKYO: (time(0, 0), time(9, 0)),
            SessionType.LONDON: (time(8, 0), time(17, 0)),
            SessionType.NEW_YORK: (time(13, 0), time(22, 0))
        }
    
    def get_current_session(self, timestamp: Optional[datetime] = None) -> SessionType:
        if timestamp is None:
            timestamp = datetime.now(self.timezone)
        elif timestamp.tzinfo is None:
            timestamp = pytz.UTC.localize(timestamp)
        
        current_time = timestamp.time()
        london_start, london_end = self.session_hours[SessionType.LONDON]
        ny_start, ny_end = self.session_hours[SessionType.NEW_YORK]
        tokyo_start, tokyo_end = self.session_hours[SessionType.TOKYO]
        
        if london_start <= current_time <= london_end and ny_start <= current_time <= ny_end:
            return SessionType.OVERLAP_LONDON_NY
        
        if tokyo_start <= current_time <= tokyo_end and london_start <= current_time <= london_end:
            return SessionType.OVERLAP_TOKYO_LONDON
        
        for session_type, (start, end) in self.session_hours.items():
            if start <= current_time <= end:
                return session_type
        
        return SessionType.OFF_HOURS
    
    def is_trading_allowed(self, timestamp: Optional[datetime] = None) -> bool:
        session = self.get_current_session(timestamp)
        if SESSION_OVERLAP_ONLY:
            return session in [SessionType.OVERLAP_LONDON_NY, SessionType.OVERLAP_TOKYO_LONDON]
        return session not in [SessionType.OFF_HOURS, SessionType.SYDNEY]

class WhaleDetectionEngine:
    def __init__(self):
        self.volume_baselines = defaultdict(lambda: deque(maxlen=100))
        self.price_history = defaultdict(lambda: deque(maxlen=50))
        self.recent_activities = []
    
    def update(self, symbol: str, df: pd.DataFrame):
        if len(df) < 2:
            return
        
        self.volume_baselines[symbol].append(df['volume'].iloc[-1])
        self.price_history[symbol].append({'price': df['close'].iloc[-1], 'timestamp': df.index[-1]})
        
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
        
        WHALE_VOLUME_MULTIPLIER = 3.0
        WHALE_PRICE_IMPACT_PCT = 0.5
        
        if volume_ratio >= WHALE_VOLUME_MULTIPLIER and price_change >= WHALE_PRICE_IMPACT_PCT:
            direction = "BUY" if df['close'].iloc[-1] > df['open'].iloc[-1] else "SELL"
            
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

# ================= UX MESSAGE TEMPLATES (Complete from v3.1) =================

class MessageTemplateLibrary:
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
        session_emoji = {"LONDON": "🏰", "NEW_YORK": "🗽", "OVERLAP": "⚡",
                        "TOKYO": "🗾", "SYDNEY": "🏖️"}.get(session_type, "🌍")
        
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
            metrics.active_exposure, metrics.active_exposure + metrics.available_capacity
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
        color = "🔴" if pct > 0.9 else "🟠" if pct > 0.7 else "🟢"
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
    def invalidation_alert(trade_id: str, symbol: str, reason: str,
                         entry: float, current: float, loss: float) -> str:
        loss_bar = "🔴" * min(5, int(abs(loss)))
        
        return f"""
❌ <b>TRADE INVALIDATED — {symbol}</b>

<b>📝 Invalidation Reason:</b>
<code>{reason}</code>

<b>📊 Trade Post-Mortem:</b>
├ Entry: <code>{entry:.5f}</code>
├ Current: <code>{current:.5f}</code>
├ Loss: {loss_bar} <code>{loss:.2f}R</code>
└ Trade ID: <code>{trade_id[:8]}</code>

<b>🎓 Learning Context:</b>
  • Review confluence factors
  • Check regime alignment
  • Validate liquidity assumptions

<i>💡 Every invalidation is data. Adapt.</i>
"""
    
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
            
            sections.append(f"""
<b>🐋 {symbol}</b>
{acts[0].magnitude_emoji} Volume: {total_volume:.1f}x average
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
    def weekly_analytics_report(stats: Dict[str, Any]) -> str:
        if stats['total_r'] > 3:
            narrative = "🏆 <b>Exceptional Week</b> — Mastery in motion"
            emoji_mood = "🎉"
        elif stats['total_r'] > 0:
            narrative = "📈 <b>Profitable Week</b> — Edge confirmed"
            emoji_mood = "✅"
        elif stats['total_r'] > -2:
            narrative = "⚖️ <b>Consolidation Week</b> — Learning mode"
            emoji_mood = "📚"
        else:
            narrative = "🎯 <b>Drawdown Week</b> — System stress test"
            emoji_mood = "💪"
        
        win_rate_bar = MessageTemplateLibrary._generate_mini_bar(stats['win_rate'], 100)
        profit_factor_bar = MessageTemplateLibrary._generate_mini_bar(min(stats['profit_factor'], 3) * 33, 100)
        
        return f"""
{emoji_mood} <b>WEEKLY INTELLIGENCE REPORT</b>

{narrative}

<b>📊 Performance Snapshot</b>
├ Net R: <code>{stats['total_r']:+.2f}R</code> {MessageTemplateLibrary._trend_emoji(stats['total_r'])}
├ Win Rate: {win_rate_bar} <code>{stats['win_rate']:.1f}%</code>
├ Profit Factor: {profit_factor_bar} <code>{stats['profit_factor']:.2f}</code>
└ Expectancy: <code>{stats['expectancy']:+.3f}R</code> per trade

<b>🎯 Trade Distribution</b>
{MessageTemplateLibrary._generate_trade_distribution(stats)}

<b>🧠 Model Performance</b>
├ Calibration: <code>{stats.get('calibration_error', 0):.3f}</code>
├ High Prob Win%: <code>{stats.get('high_prob_win_rate', 0):.1f}%</code>
└ Edge Capture: <code>{stats.get('edge_capture', 0):.1f}%</code>

<b>📈 Week-over-Week</b>
{stats.get('wow_comparison', 'No prior data')}

<i>🎪 Next week: {stats.get('next_week_focus', 'Maintain discipline')}</i>
"""
    
    @staticmethod
    def _generate_mini_bar(value: float, max_val: float, length: int = 8) -> str:
        filled = int((value / max_val) * length) if max_val > 0 else 0
        return "█" * filled + "░" * (length - filled)
    
    @staticmethod
    def _trend_emoji(value: float) -> str:
        if value > 2:
            return "🚀"
        elif value > 0:
            return "📈"
        elif value > -2:
            return "➡️"
        else:
            return "📉"
    
    @staticmethod
    def _generate_trade_distribution(stats: Dict[str, Any]) -> str:
        outcomes = [
            ("✅ Wins", stats.get('wins', 0), "🟢"),
            ("❌ Losses", stats.get('losses', 0), "🔴"),
            ("💰 Partials", stats.get('partials', 0), "🟡"),
            ("➖ BE", stats.get('breakeven', 0), "⚪"),
            ("⏱️ Expired", stats.get('expired', 0), "🔵")
        ]
        
        total = sum(o[1] for o in outcomes)
        if total == 0:
            return "  No trades this week"
        
        lines = []
        for label, count, color in outcomes:
            if count > 0:
                pct = count / total
                bar = color * int(pct * 10)
                lines.append(f"  {bar} {label}: {count} ({pct:.0%})")
        
        return "\n".join(lines)
    
    @staticmethod
    def drawdown_intervention(current_dd: float, max_dd: float, recommended_action: str) -> str:
        if current_dd >= 0.15:
            severity = "🔴 CRITICAL"
            stage = "STOP TRADING"
            color = "🔴"
        elif current_dd >= 0.10:
            severity = "🟠 HIGH"
            stage = "DEFENSIVE MODE"
            color = "🟠"
        elif current_dd >= 0.07:
            severity = "🟡 ELEVATED"
            stage = "CAUTION MODE"
            color = "🟡"
        else:
            severity = "🟢 NORMAL"
            stage = "STANDARD OPS"
            color = "🟢"
        
        weeks_to_recovery = int(np.ceil(current_dd * 100 / 2))
        
        return f"""
{color} <b>DRAWDOWN INTERVENTION</b> {color}

<b>⚠️ Status:</b> {severity} — {stage}
<b>📉 Current Drawdown:</b> <code>{current_dd:.1%}</code>
<b>📊 Max Drawdown:</b> <code>{max_dd:.1%}</code>

<b>🎯 Recommended Action:</b>
<code>{recommended_action}</code>

<b>📋 Recovery Protocol:</b>
  1. ✅ Review all active signals
  2. ✅ Reduce position size by 50%
  3. ✅ Tighten stops to 0.75x ATR
  4. ✅ Only A+ setups (probability >80%)
  5. ⏳ Daily risk review mandatory

<b>📈 Recovery Projection:</b>
Estimated {weeks_to_recovery} weeks to new highs
(at 2% weekly return with discipline)

<i>💪 This is a test of process, not prediction.</i>
"""
    
    @staticmethod
    def probability_visualization(prob_breakdown: Dict[str, float]) -> str:
        total = sum(prob_breakdown.values())
        lines = []
        for factor, contribution in sorted(prob_breakdown.items(), key=lambda x: x[1], reverse=True):
            pct = (contribution / total) * 100 if total > 0 else 0
            bar = MessageTemplateLibrary._generate_mini_bar(pct, 100, 6)
            lines.append(f"  {bar} <b>{factor}:</b> {contribution:.1%}")
        
        return f"""
🎲 <b>PROBABILITY ARCHITECTURE</b>

<i>Bayesian factor decomposition</i>

{chr(10).join(lines)}

<b>🎯 Combined Probability:</b> <code>{sum(prob_breakdown.values()):.1%}</b>
<b>📊 Confidence Interval:</b> 68% range [{(sum(prob_breakdown.values())*0.9):.1%}, {(sum(prob_breakdown.values())*1.1):.1%}]

<i>💡 Higher confluence = tighter confidence interval</i>
"""
    
    @staticmethod
    def dynamic_rr_suggestion(current_rr: float, optimal_rr: float, adjustment_reason: str) -> str:
        if optimal_rr > current_rr:
            improvement = f"📈 +{((optimal_rr/current_rr - 1) * 100):.0f}% better expectancy"
            action = "EXTEND TARGET"
        else:
            improvement = "🎯 Tighten for better win rate"
            action = "REDUCE TARGET"
        
        return f"""
⚖️ <b>DYNAMIC R:R OPTIMIZATION</b>

<b>Current Setup:</b> 1:{current_rr:.1f}
<b>Optimal R:R:</b> 1:{optimal_rr:.1f}
<b>Improvement:</b> {improvement}

<b>🧠 Adjustment Rationale:</b>
<code>{adjustment_reason}</code>

<b>📊 Impact Simulation:</b>
├ Win Rate @ {current_rr:.1f}R: <code>{MessageTemplateLibrary._estimate_win_rate(current_rr):.0%}</code>
├ Win Rate @ {optimal_rr:.1f}R: <code>{MessageTemplateLibrary._estimate_win_rate(optimal_rr):.0%}</code>
└ Expectancy Delta: <code>{MessageTemplateLibrary._calculate_expectancy_delta(current_rr, optimal_rr):+.2f}R</code>

<b>🎯 Recommendation:</b> <code>{action}</code>

<i>⚡ Adaptive optimization based on volatility regime</i>
"""
    
    @staticmethod
    def _estimate_win_rate(rr: float) -> float:
        base = 0.65
        return max(0.35, base - (rr - 2) * 0.05)
    
    @staticmethod
    def _calculate_expectancy_delta(current: float, optimal: float) -> float:
        wr_current = MessageTemplateLibrary._estimate_win_rate(current)
        wr_optimal = MessageTemplateLibrary._estimate_win_rate(optimal)
        exp_current = wr_current * current - (1 - wr_current)
        exp_optimal = wr_optimal * optimal - (1 - wr_optimal)
        return exp_optimal - exp_current
    
    @staticmethod
    def regime_change_ceremony(old_regime: str, new_regime: str, impact_assessment: str,
                              strategy_adjustments: List[str]) -> str:
        transition_emoji = {
            ("TRENDING", "RANGING"): "📉➡️↔️",
            ("RANGING", "TRENDING"): "↔️➡️📈",
            ("LOW_VOL", "HIGH_VOL"): "🌊",
            ("COMPRESSION", "EXPANSION"): "🌀➡️💥"
        }.get((old_regime.split('_')[0], new_regime.split('_')[0]), "🔄")
        
        return f"""
{transition_emoji} <b>REGIME CHANGE DETECTED</b> {transition_emoji}

<b>📊 Transition:</b>
<code>{old_regime}</code> ➡️ <code>{new_regime}</code>

<b>⚡ Market Impact:</b>
<code>{impact_assessment}</code>

<b>🎯 Strategy Adaptation:</b>
{chr(10).join(f"  • {adj}" for adj in strategy_adjustments)}

<b>📋 Action Checklist:</b>
  ⏸️ Pause new entries (2 candles)
  🔍 Reassess active signals
  📊 Adjust position sizing
  🎯 Update probability models
  ✅ Resume with confirmation

<i>🎪 Adapt or perish. The market has spoken.</i>
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
        self.last_alert_time = {}
        self.alert_cooldowns = {
            AlertType.PRE_SIGNAL: 60,
            AlertType.SIGNAL: 0,
            AlertType.LIQUIDITY_ZONE: 300,
            AlertType.SESSION_OPEN: 3600,
            AlertType.RISK_DASHBOARD: 600,
            AlertType.WHALE_ACTIVITY: 180,
            AlertType.REGIME_CHANGE: 0,
            AlertType.DRAWDOWN_WARNING: 0
        }
        self.daily_alert_count = defaultdict(int)
        self.daily_reset_time = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0)
    
    async def trigger_event(self, event_type: AlertType, data: Dict[str, Any], priority: int = 2) -> bool:
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
        if daily_total >= MAX_DAILY_ALERTS:
            return False
        
        return True
    
    def _check_cooldown(self, event_type: AlertType) -> bool:
        cooldown = self.alert_cooldowns.get(event_type, 300)
        last_time = self.last_alert_time.get(event_type.value)
        
        if last_time is None:
            return True
        
        elapsed = (datetime.now(pytz.UTC) - last_time).total_seconds()
        return elapsed >= cooldown
    
    def _get_handler(self, event_type: AlertType):
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
        return self.templates.pre_signal_alert(signal, data.get('time_to_signal', 60))
    
    def _handle_signal(self, data: Dict[str, Any]) -> str:
        signal = UXSignal(**data['signal'])
        return self.templates.signal_alert(signal, data.get('market_context', {}))
    
    def _handle_liquidity_zone(self, data: Dict[str, Any]) -> str:
        return self.templates.liquidity_zone_heatmap(
            [LiquidityZone(**z) for z in data['zones']],
            data['current_price']
        )
    
    def _handle_session_open(self, data: Dict[str, Any]) -> str:
        return self.templates.session_open_ceremony(
            data['session_type'],
            data.get('high_impact_news', [])
        )
    
    def _handle_confidence_heatmap(self, data: Dict[str, Any]) -> str:
        return self.templates.confidence_heatmap([UXSignal(**s) for s in data['signals']])
    
    def _handle_risk_dashboard(self, data: Dict[str, Any]) -> str:
        return self.templates.risk_dashboard(
            RiskMetrics(**data['metrics']),
            data.get('active_trades', [])
        )
    
    def _handle_invalidation(self, data: Dict[str, Any]) -> str:
        return self.templates.invalidation_alert(
            data['trade_id'], data['symbol'], data['reason'],
            data['entry'], data['current'], data['loss']
        )
    
    def _handle_whale_activity(self, data: Dict[str, Any]) -> str:
        return self.templates.whale_activity_alert([WhaleActivity(**a) for a in data['activities']])
    
    def _handle_multi_ranking(self, data: Dict[str, Any]) -> str:
        return self.templates.confidence_heatmap([UXSignal(**s) for s in data['signals']])
    
    def _handle_weekly_analytics(self, data: Dict[str, Any]) -> str:
        return self.templates.weekly_analytics_report(data['stats'])
    
    def _handle_drawdown_warning(self, data: Dict[str, Any]) -> str:
        return self.templates.drawdown_intervention(
            data['current_dd'], data['max_dd'], data['recommended_action']
        )
    
    def _handle_probability_viz(self, data: Dict[str, Any]) -> str:
        return self.templates.probability_visualization(data['breakdown'])
    
    def _handle_rr_suggestion(self, data: Dict[str, Any]) -> str:
        return self.templates.dynamic_rr_suggestion(
            data['current_rr'], data['optimal_rr'], data['reason']
        )
    
    def _handle_regime_change(self, data: Dict[str, Any]) -> str:
        return self.templates.regime_change_ceremony(
            data['old_regime'], data['new_regime'],
            data['impact'], data['adjustments']
        )

# ================= TELEGRAM MANAGER =================

class TelegramManager:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
    
    async def send_message(self, message: str) -> bool:
        if not self.token or not self.chat_id:
            return False
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"{self.base_url}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
                async with session.post(url, data=payload) as response:
                    result = await response.json()
                    return result.get("ok", False)
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False

# ================= DATA FETCHER =================

class DataFetcher:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._semaphore = asyncio.Semaphore(5)
        self._cache = {}
        self._cache_ttl = timedelta(seconds=30)
    
    async def fetch_time_series(self, symbol: str, interval: str, outputsize: int = 500) -> Optional[pd.DataFrame]:
        cache_key = f"{symbol}_{interval}"
        
        if cache_key in self._cache:
            df, timestamp = self._cache[cache_key]
            if datetime.now(pytz.UTC) - timestamp < self._cache_ttl:
                return df
        
        async with self._semaphore:
            url = f"{self.base_url}/time_series"
            params = {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": self.api_key
            }
            
            for attempt in range(MAX_RETRIES):
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url, params=params) as response:
                            if response.status != 200:
                                await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                                continue
                            
                            data = await response.json()
                            if "values" not in data or not data["values"]:
                                return None
                            
                            df = self._process_data(data["values"])
                            self._cache[cache_key] = (df, datetime.now(pytz.UTC))
                            return df
                            
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
            
            return None
    
    def _process_data(self, values: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame(values)
        
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df.dropna(subset=["open", "high", "low", "close"], inplace=True)
        
        return df

# ================= TRADE MANAGER =================

class TradeManager:
    def __init__(self):
        self.trades = {}
        self.active_trades = []
        self.closed_trades = []
        self.daily_stats = DailyStats(date=datetime.now(pytz.UTC).strftime("%Y-%m-%d"))
        self.execution_sim = ExecutionSimulator()
    
    def open_trade(self, signal: Signal, account_size: float = 10000.0) -> Optional[Trade]:
        fill_price, slippage_cost, fill_prob = self.execution_sim.simulate_entry(
            signal, signal.market_context or MarketContext(
                symbol=signal.symbol, timestamp=datetime.now(pytz.UTC),
                regime=MarketRegime.TRENDING_STRONG, volatility_regime=VolatilityRegime.NORMAL,
                session=SessionType.LONDON, adx=25, atr=signal.atr_value,
                atr_percentile=50, volume_regime=VolatilityRegime.NORMAL,
                spread_estimate=0.0002, liquidity_score=0.8,
                trend_strength=0.5, mean_reversion_potential=0.2
            )
        )
        
        if random.random() > fill_prob:
            return None
        
        signal.entry_price = fill_price
        signal.slippage_estimate = slippage_cost
        
        trade = Trade(signal=signal, entry_fill_price=fill_price, entry_slippage=slippage_cost)
        self.trades[signal.id] = trade
        self.active_trades.append(trade)
        self.daily_stats.total_signals += 1
        self.daily_stats.executed_signals += 1
        
        return trade
    
    async def manage_trades(self, data_fetcher: DataFetcher):
        for trade in list(self.active_trades):
            if trade.is_closed:
                continue
            
            try:
                df = await data_fetcher.fetch_time_series(trade.signal.symbol, trade.signal.timeframe, 100)
                if df is not None:
                    await self._evaluate_trade(trade, df)
            except Exception as e:
                logger.error(f"Error managing trade {trade.signal.id}: {e}")
    
    async def _evaluate_trade(self, trade: Trade, df: pd.DataFrame):
        trade.candles_evaluated += 1
        signal_time = trade.signal.timestamp
        future_candles = df[df.index > signal_time]
        
        if len(future_candles) == 0:
            return
        
        for idx, candle in future_candles.iterrows():
            high, low, close = candle['high'], candle['low'], candle['close']
            
            if trade.signal.direction == "BUY":
                mfe = (high - trade.entry_fill_price) / abs(trade.entry_fill_price - trade.signal.stop_loss)
                mae = (trade.entry_fill_price - low) / abs(trade.entry_fill_price - trade.signal.stop_loss)
            else:
                mfe = (trade.entry_fill_price - low) / abs(trade.entry_fill_price - trade.signal.stop_loss)
                mae = (high - trade.entry_fill_price) / abs(trade.entry_fill_price - trade.signal.stop_loss)
            
            trade.max_favorable_excursion = max(trade.max_favorable_excursion, mfe)
            trade.max_adverse_excursion = min(trade.max_adverse_excursion, mae)
            
            if TRAILING_ACTIVATION_R > 0 and not trade.trailing_stop_active:
                risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
                profit = abs(close - trade.entry_fill_price)
                
                if profit >= risk * TRAILING_ACTIVATION_R:
                    trade.trailing_stop_active = True
                    trade.trailing_activation_price = close
                    trade.trailing_stop_level = trade.entry_fill_price + (risk * 0.3) if trade.signal.direction == "BUY" else trade.entry_fill_price - (risk * 0.3)
            
            elif trade.trailing_stop_active:
                risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
                if trade.signal.direction == "BUY":
                    new_level = close - (risk * 0.5)
                    if new_level > (trade.trailing_stop_level or 0):
                        trade.trailing_stop_level = new_level
                else:
                    new_level = close + (risk * 0.5)
                    if new_level < (trade.trailing_stop_level or float('inf')):
                        trade.trailing_stop_level = new_level
            
            exit_triggered = self._check_exits(trade, high, low, close, idx)
            if exit_triggered:
                return
        
        if trade.candles_evaluated >= MAX_CANDLES_TIMEOUT:
            await self._timeout_exit(trade, df)
    
    def _check_exits(self, trade: Trade, high: float, low: float, close: float, timestamp: datetime) -> bool:
        direction = trade.signal.direction
        
        if trade.trailing_stop_active and trade.trailing_stop_level:
            if (direction == "BUY" and low <= trade.trailing_stop_level) or \
               (direction == "SELL" and high >= trade.trailing_stop_level):
                self._close_trade(trade, trade.trailing_stop_level, timestamp, TradeOutcome.TRAILING_STOP)
                return True
        
        if PARTIAL_TP_PCT > 0 and trade.signal.take_profit_1 and trade.status == TradeStatus.PENDING:
            if (direction == "BUY" and high >= trade.signal.take_profit_1) or \
               (direction == "SELL" and low <= trade.signal.take_profit_1):
                trade.status = TradeStatus.PARTIAL
                trade.partial_exit_price = trade.signal.take_profit_1
                trade.partial_exit_time = timestamp
                trade.partial_volume_closed = PARTIAL_TP_PCT
                
                buffer = trade.signal.atr_value * 0.1
                trade.signal.stop_loss = trade.entry_fill_price - buffer if direction == "BUY" else trade.entry_fill_price + buffer
                trade.breakeven_triggered = True
        
        if (direction == "BUY" and high >= trade.signal.take_profit) or \
           (direction == "SELL" and low <= trade.signal.take_profit):
            self._close_trade(trade, trade.signal.take_profit, timestamp, TradeOutcome.TP_HIT)
            return True
        
        if (direction == "BUY" and low <= trade.signal.stop_loss) or \
           (direction == "SELL" and high >= trade.signal.stop_loss):
            if trade.status == TradeStatus.PARTIAL or trade.breakeven_triggered:
                trade.status = TradeStatus.BREAKEVEN
                trade.outcome = TradeOutcome.BREAKEVEN
            else:
                trade.status = TradeStatus.LOSS
                trade.outcome = TradeOutcome.SL_HIT
                trade.actual_rr = -1.0
            
            trade.exit_price = trade.signal.stop_loss
            trade.exit_time = timestamp
            self._finalize_trade(trade)
            return True
        
        return False
    
    async def _timeout_exit(self, trade: Trade, df: pd.DataFrame):
        current_close = df['close'].iloc[-1]
        trade.status = TradeStatus.EXPIRED
        trade.outcome = TradeOutcome.EXPIRED
        trade.exit_price = current_close
        trade.exit_time = df.index[-1]
        
        risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
        if risk > 0:
            actual_pnl = current_close - trade.entry_fill_price
            if trade.signal.direction == "SELL":
                actual_pnl = -actual_pnl
            trade.actual_rr = actual_pnl / risk
        
        self._finalize_trade(trade)
    
    def _close_trade(self, trade: Trade, exit_price: float, timestamp: datetime, outcome: TradeOutcome):
        trade.exit_price = exit_price
        trade.exit_time = timestamp
        trade.outcome = outcome
        
        if outcome == TradeOutcome.TP_HIT:
            trade.status = TradeStatus.WIN
            trade.actual_rr = trade.signal.risk_reward
        elif outcome == TradeOutcome.TRAILING_STOP:
            trade.status = TradeStatus.WIN
            risk = abs(trade.signal.entry_price - trade.signal.stop_loss)
            profit = abs(exit_price - trade.entry_fill_price)
            trade.actual_rr = profit / risk if risk > 0 else 0
        
        self._finalize_trade(trade)
    
    def _finalize_trade(self, trade: Trade):
        if trade in self.active_trades:
            self.active_trades.remove(trade)
        self.closed_trades.append(trade)
        
        r_pnl = trade.profit_in_r
        
        if trade.status == TradeStatus.WIN:
            self.daily_stats.wins += 1
            self.daily_stats.gross_profit_r += r_pnl
            self.daily_stats.max_profit_r = max(self.daily_stats.max_profit_r, r_pnl)
        elif trade.status == TradeStatus.LOSS:
            self.daily_stats.losses += 1
            self.daily_stats.gross_loss_r += r_pnl
            self.daily_stats.max_loss_r = min(self.daily_stats.max_loss_r, r_pnl)
        elif trade.status == TradeStatus.BREAKEVEN:
            self.daily_stats.breakeven += 1
        elif trade.status == TradeStatus.PARTIAL:
            self.daily_stats.partials += 1
        
        self.daily_stats.total_r += r_pnl
        
        if trade.status == TradeStatus.LOSS:
            self.daily_stats.current_consecutive_losses += 1
            self.daily_stats.max_consecutive_losses = max(
                self.daily_stats.max_consecutive_losses,
                self.daily_stats.current_consecutive_losses
            )
        else:
            self.daily_stats.current_consecutive_losses = 0
    
    def get_stats(self) -> DailyStats:
        return self.daily_stats

# ================= MAIN ENGINE =================

class NamiEngine:
    def __init__(self):
        self._validate_config()
        
        self.data_fetcher = DataFetcher(TWELVEDATA_KEY)
        self.telegram = TelegramManager(TELEGRAM_TOKEN, CHAT_ID)
        self.session_manager = SessionManager()
        self.portfolio_manager = PortfolioRiskManager()
        self.trade_manager = TradeManager()
        self.strategy_engine = StrategyAggregationEngine()
        self.liquidity_engine = InstitutionalLiquidityEngine()
        self.orderflow_engine = OrderflowDetectionEngine()
        self.risk_engine = DynamicRiskEngine()
        self.notification_system = EventTriggerEngine(self.telegram)
        self.whale_detector = WhaleDetectionEngine()
        
        self.running = False
        self.account_size = 10000.0
        
        logger.info(f"NamiEngine v3.2 initialized | 16 Strategies | Personality: {PERSONALITY.upper()}")
    
    def _validate_config(self):
        missing = []
        if not TELEGRAM_TOKEN:
            missing.append("TELEGRAM_TOKEN")
        if not CHAT_ID:
            missing.append("CHAT_ID")
        if not TWELVEDATA_KEY:
            missing.append("TWELVEDATA_KEY")
        
        if missing:
            raise ValueError(f"Missing: {', '.join(missing)}")
    
    async def run(self):
        self.running = True
        
        await self.telegram.send_message(
            f"🚀 <b>NamiEngine v3.2 LIVE</b>\n"
            f"<i>16 Strategies | Institutional Grade | UX Optimized</i>\n\n"
            f"Personality: <code>{PERSONALITY.upper()}</code>\n"
            f"Min Score: <code>{MIN_SCORE}</code> | "
            f"Max Risk: <code>{MAX_PORTFOLIO_RISK_PCT}%</code>\n"
            f"Session: <code>{'Overlap Only' if SESSION_OVERLAP_ONLY else 'All Sessions'}</code>\n\n"
            f"Monitoring {len(WATCHLIST)} instruments with full strategy suite..."
        )
        
        tasks = [
            self._scanning_loop(),
            self._trade_management_loop(),
            self._reporting_loop(),
            self._ux_enhancement_loop()
        ]
        
        await asyncio.gather(*tasks)
    
    async def _scanning_loop(self):
        while self.running:
            try:
                if not self.session_manager.is_trading_allowed():
                    await asyncio.sleep(300)
                    continue
                
                start_time = datetime.now(pytz.UTC)
                self.portfolio_manager.update_state(self.trade_manager.active_trades, self.account_size)
                
                await self._scan_watchlist()
                
                elapsed = (datetime.now(pytz.UTC) - start_time).total_seconds()
                await asyncio.sleep(max(0, SCAN_INTERVAL - elapsed))
                
            except Exception as e:
                logger.error(f"Scanning error: {e}")
                await asyncio.sleep(60)
    
    async def _scan_watchlist(self):
        semaphore = asyncio.Semaphore(3)
        
        async def scan_symbol(symbol: str):
            async with semaphore:
                return await self._analyze_symbol(symbol)
        
        tasks = [scan_symbol(symbol) for symbol in WATCHLIST.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for symbol, result in zip(WATCHLIST.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Error scanning {symbol}: {result}")
                continue
            
            if result:
                await self._process_signal(result)
    
    async def _analyze_symbol(self, symbol: str) -> Optional[Signal]:
        can_trade, reason = self.portfolio_manager.can_add_position(symbol, "BUY", 1.0)
        if not can_trade:
            return None
        
        data = {}
        for tf_name, tf_interval in TIMEFRAMES.items():
            if tf_name in ["micro", "signal", "trend", "higher"]:
                df = await self.data_fetcher.fetch_time_series(symbol, tf_interval)
                if df is not None:
                    data[tf_name] = df
        
        if "signal" not in data or "trend" not in data:
            return None
        
        self.liquidity_engine.update(symbol, data["signal"])
        liquidity_profile = self.liquidity_engine.get_profile(symbol)
        
        orderflow = self.orderflow_engine.analyze(data["signal"], data.get("micro"))
        
        weighted_score, direction, primary_strategy, adx, triggered_strategies, regime, raw_score = \
            self.strategy_engine.calculate_score(
                data["signal"], data["trend"], data.get("higher"),
                symbol in ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
            )
        
        if direction == "NEUTRAL" or weighted_score < (MIN_SCORE * 1.5 if PERSONALITY == "nami" else MIN_SCORE * 2.0):
            return None
        
        min_weighted = MIN_SCORE * 1.5 if PERSONALITY == "nami" else MIN_SCORE * 2.0
        if weighted_score < min_weighted:
            return None
        
        if adx < ADX_THRESHOLD:
            return None
        
        market_context = self._build_market_context(data, symbol, regime)
        
        entry, sl, tp, tp1, atr, rr, sl_type = self.risk_engine.calculate_risk_levels(
            data["signal"], direction, market_context, liquidity_profile, 0.7
        )
        
        if rr < MIN_RR_RATIO:
            return None
        
        position_size, r_risk = self.risk_engine.calculate_position_size(
            self.account_size, MAX_PORTFOLIO_RISK_PCT / 3, entry, sl, 0.7, self.portfolio_manager.state
        )
        
        if r_risk <= 0:
            return None
        
        for active_trade in self.trade_manager.active_trades:
            corr = self.portfolio_manager.get_correlation(symbol, active_trade.signal.symbol)
            if abs(corr) > 0.7:
                return None
        
        signal_id = f"{symbol.replace('/', '').replace(':', '_')}_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}"
        
        prob_model = ProbabilityModel(
            base_probability=0.7,
            confluence_score=weighted_score / 10,
            regime_adjustment=0.05,
            liquidity_adjustment=0.02,
            timing_adjustment=0.03,
            historical_calibration=0.0
        )
        
        confidence = min(95, int(50 + weighted_score * 5)) if PERSONALITY == "nami" else min(95, int(60 + weighted_score * 4))
        
        return Signal(
            id=signal_id,
            symbol=symbol,
            direction=direction,
            score=weighted_score,
            confidence=confidence,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            atr_value=atr,
            risk_reward=rr,
            adx_value=adx,
            strategy_type=primary_strategy or "Multi-Strategy",
            triggered_strategies=triggered_strategies,
            timestamp=datetime.now(pytz.UTC),
            timeframe=TIMEFRAMES["signal"],
            market_regime=regime,
            weighted_score=weighted_score,
            spread_at_signal=market_context.spread_estimate,
            slippage_estimate=entry * SLIPPAGE_PCT,
            take_profit_1=tp1,
            probability_model=prob_model,
            market_context=market_context,
            liquidity_profile=liquidity_profile,
            orderflow_signature=orderflow,
            position_size_risk=r_risk
        )
    
    def _build_market_context(self, data: Dict[str, pd.DataFrame], symbol: str, regime: MarketRegime) -> MarketContext:
        df = data["signal"]
        analyzer = TechnicalAnalyzer()
        
        atr = analyzer.calculate_atr(df, 14).iloc[-1]
        adx = analyzer.calculate_adx(df, 14).iloc[-1]
        atr_history = analyzer.calculate_atr(df, 14).tail(50)
        atr_pct = (atr_history <= atr).mean() * 100
        
        vol_regime = VolatilityRegime.NORMAL
        if atr_pct < 10:
            vol_regime = VolatilityRegime.VERY_LOW
        elif atr_pct < 30:
            vol_regime = VolatilityRegime.LOW
        elif atr_pct > 90:
            vol_regime = VolatilityRegime.EXTREME
        elif atr_pct >
