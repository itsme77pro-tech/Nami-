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
from collections import deque
import pytz
from abc import ABC, abstractmethod

# ================= CONFIGURATION =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

PERSONALITY = os.getenv("PERSONALITY", "nami").lower()
DATA_SOURCE = os.getenv("DATA_SOURCE", "twelvedata").lower()

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
HISTORY_DIR = DATA_DIR / "history"
HISTORY_DIR.mkdir(exist_ok=True)

# ================= SYSTEM PARAMETERS =================

class SystemConfig:
    # Execution Windows (UTC)
    LONDON_START = 7
    LONDON_END = 10
    NY_START = 13
    NY_END = 16
    KILL_ZONE_OVERLAP = (14, 16)  # Highest probability
    
    # Quality Thresholds
    MIN_CONFIDENCE_SCORE = 70
    MIN_RR_RATIO = 2.0
    MAX_SPREAD_PCT = 0.0005  # 5 pips for forex
    MAX_SLIPPAGE_PCT = 0.001
    
    # Risk Management
    BASE_RISK_PCT = 0.01  # 1% base risk
    MAX_DAILY_RISK = 0.03  # 3% max daily loss
    MAX_CONSECUTIVE_LOSSES = 3
    DRAWDOWN_THRESHOLD = 0.05  # 5% equity drop triggers defensive mode
    
    # Volatility Regimes
    VOLATILITY_LOW = 0.005
    VOLATILITY_HIGH = 0.02
    
    # Timeframes
    TF_EXECUTION = "15min"
    TF_TREND = "1h"
    TF_STRUCTURE = "4h"
    TF_BIAS = "1d"
    
    # Trade Management
    MAX_TRADE_DURATION_MIN = 240  # 4 hours
    PARTIAL_TP_RATIO = 0.5
    PARTIAL_TP_LEVEL = 1.0  # Close 50% at 1R
    RUNNER_TP_MULTIPLIER = 3.0

# ================= ENUMS =================

class MarketRegime(Enum):
    TRENDING_UP = auto()
    TRENDING_DOWN = auto()
    RANGING = auto()
    COMPRESSION = auto()
    EXPANSION = auto()
    MANIPULATION = auto()
    LOW_VOLATILITY = auto()

class SessionType(Enum):
    ASIA = "asia"
    LONDON = "london"
    NEW_YORK = "new_york"
    OVERLAP = "overlap"
    OFF_HOURS = "off_hours"

class StructureType(Enum):
    BOS_BULLISH = "bos_bullish"  # Break of Structure
    BOS_BEARISH = "bos_bearish"
    CHOCH_BULLISH = "choch_bullish"  # Change of Character
    CHOCH_BEARISH = "choch_bearish"
    NONE = "none"

class LiquidityType(Enum):
    EQUAL_HIGHS = "equal_highs"
    EQUAL_LOWS = "equal_lows"
    PDH = "pdh"  # Previous Day High
    PDL = "pdl"  # Previous Day Low
    SESSION_HIGH = "session_high"
    SESSION_LOW = "session_low"
    SWEEP_HIGH = "sweep_high"
    SWEEP_LOW = "sweep_low"
    POOL = "liquidity_pool"

class TradeStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"  # Partial TP hit
    WIN = "win"
    LOSS = "loss"
    EXPIRED = "expired"
    CLOSED_MANUAL = "manual"

# ================= DATA CLASSES =================

@dataclass
class LiquidityZone:
    level: float
    type: LiquidityType
    strength: int  # 1-10 based on touches/volume
    timestamp: datetime
    is_swept: bool = False
    sweep_timestamp: Optional[datetime] = None

@dataclass
class MarketStructure:
    type: StructureType
    swing_point: float
    confirmation_price: float
    timestamp: datetime
    timeframe: str
    strength: int  # 1-10

@dataclass
class Signal:
    id: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    confidence_score: int
    entry_price: float
    stop_loss: float
    take_profit: float
    take_profit_runner: Optional[float]
    atr_value: float
    risk_reward: float
    liquidity_sweep: LiquidityZone
    structure_break: MarketStructure
    session: SessionType
    regime: MarketRegime
    htf_bias: str
    volatility_regime: str
    spread_at_entry: float
    score_components: Dict[str, int]
    timestamp: datetime
    kill_zone: bool = False
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat(),
            'liquidity_sweep': {
                'level': self.liquidity_sweep.level,
                'type': self.liquidity_sweep.type.value,
                'strength': self.liquidity_sweep.strength
            },
            'structure_break': {
                'type': self.structure_break.type.value,
                'strength': self.structure_break.strength
            }
        }

@dataclass
class Trade:
    signal: Signal
    status: TradeStatus = TradeStatus.PENDING
    position_size: float = 0.0
    partial_closed: bool = False
    partial_pnl: float = 0.0
    runner_pnl: float = 0.0
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    max_profit_reached: float = 0.0
    max_drawdown_reached: float = 0.0
    candles_held: int = 0
    close_reason: str = ""
    
    @property
    def total_pnl(self) -> float:
        return self.partial_pnl + self.runner_pnl
    
    @property
    def is_closed(self) -> bool:
        return self.status in [TradeStatus.WIN, TradeStatus.LOSS, 
                              TradeStatus.EXPIRED, TradeStatus.CLOSED_MANUAL]

@dataclass
class PerformanceMetrics:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    partials: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    max_drawdown: float = 0.0
    current_drawdown: float = 0.0
    consecutive_losses: int = 0
    win_rate_by_session: Dict[str, List[bool]] = field(default_factory=dict)
    win_rate_by_regime: Dict[str, List[bool]] = field(default_factory=dict)
    equity_curve: List[Tuple[datetime, float]] = field(default_factory=list)
    
    @property
    def win_rate(self) -> float:
        closed = self.wins + self.losses
        return (self.wins / closed * 100) if closed > 0 else 0.0
    
    @property
    def profit_factor(self) -> float:
        return abs(self.gross_profit / self.gross_loss) if self.gross_loss != 0 else float('inf')
    
    @property
    def net_profit(self) -> float:
        return self.gross_profit + self.gross_loss  # losses are negative

# ================= LIQUIDITY MAP GENERATOR =================

class LiquidityMap:
    """Detects and tracks institutional liquidity levels"""
    
    def __init__(self, lookback: int = 100):
        self.lookback = lookback
        self.zones: List[LiquidityZone] = []
        self.equal_threshold = 0.0005  # 5 pips for forex
    
    def detect_equal_levels(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
        """Detect equal highs and lows"""
        highs = df['high'].values
        lows = df['low'].values
        
        # Find clusters of similar highs/lows
        equal_highs = self._find_clusters(highs, self.equal_threshold)
        equal_lows = self._find_clusters(lows, self.equal_threshold)
        
        strongest_high = max(equal_highs, key=lambda x: x[1])[0] if equal_highs else None
        strongest_low = max(equal_lows, key=lambda x: x[1])[0] if equal_lows else None
        
        return strongest_high, strongest_low
    
    def _find_clusters(self, prices: np.ndarray, threshold: float) -> List[Tuple[float, int]]:
        """Find price clusters with frequency"""
        clusters = []
        for price in prices:
            found = False
            for i, (level, count) in enumerate(clusters):
                if abs(price - level) / level < threshold:
                    clusters[i] = (level + (price - level) * 0.1, count + 1)  # Weighted update
                    found = True
                    break
            if not found:
                clusters.append((price, 1))
        return [c for c in clusters if c[1] >= 2]  # Min 2 touches
    
    def detect_session_levels(self, df: pd.DataFrame, session: SessionType) -> Tuple[Optional[float], Optional[float]]:
        """Detect Asian session high/low for London breakout strategy"""
        # Filter by session time
        df['hour'] = df.index.hour
        if session == SessionType.ASIA:
            mask = (df['hour'] >= 0) & (df['hour'] < 7)
        elif session == SessionType.LONDON:
            mask = (df['hour'] >= 7) & (df['hour'] < 13)
        else:
            mask = (df['hour'] >= 13) & (df['hour'] < 21)
        
        session_data = df[mask]
        if len(session_data) == 0:
            return None, None
        
        return session_data['high'].max(), session_data['low'].min()
    
    def detect_sweep(self, df: pd.DataFrame, direction: str, level: float) -> bool:
        """Detect if price swept a liquidity level"""
        recent = df.tail(3)
        if direction == "BUY":
            # Price went below level then closed above
            swept = (recent['low'].min() < level) and (recent['close'].iloc[-1] > level)
        else:
            swept = (recent['high'].max() > level) and (recent['close'].iloc[-1] < level)
        return swept
    
    def get_nearest_liquidity(self, price: float, direction: str) -> Optional[LiquidityZone]:
        """Get nearest liquidity pool for TP targeting"""
        relevant = [z for z in self.zones if not z.is_swept]
        if direction == "BUY":
            targets = [z for z in relevant if z.level > price]
            return min(targets, key=lambda x: x.level) if targets else None
        else:
            targets = [z for z in relevant if z.level < price]
            return max(targets, key=lambda x: x.level) if targets else None

# ================= MARKET STRUCTURE DETECTOR =================

class StructureDetector:
    """Detects BOS/CHoCH and market structure shifts"""
    
    def __init__(self):
        self.swing_lookback = 5
    
    def find_swing_points(self, df: pd.DataFrame) -> Tuple[List[int], List[int]]:
        """Find swing highs and lows"""
        highs = df['high'].values
        lows = df['low'].values
        
        swing_highs = []
        swing_lows = []
        
        for i in range(self.swing_lookback, len(df) - self.swing_lookback):
            # Swing high
            if all(highs[i] > highs[i-j] for j in range(1, self.swing_lookback+1)) and \
               all(highs[i] > highs[i+j] for j in range(1, self.swing_lookback+1)):
                swing_highs.append(i)
            
            # Swing low
            if all(lows[i] < lows[i-j] for j in range(1, self.swing_lookback+1)) and \
               all(lows[i] < lows[i+j] for j in range(1, self.swing_lookback+1)):
                swing_lows.append(i)
        
        return swing_highs, swing_lows
    
    def detect_structure_break(self, df: pd.DataFrame, direction: str) -> Optional[MarketStructure]:
        """Detect BOS or CHoCH"""
        swing_highs, swing_lows = self.find_swing_points(df)
        
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return None
        
        recent = df.tail(10)
        current_close = recent['close'].iloc[-1]
        
        if direction == "BUY":
            # Check for bullish BOS (break above last swing high)
            if swing_highs and current_close > df['high'].iloc[swing_highs[-1]]:
                return MarketStructure(
                    type=StructureType.BOS_BULLISH,
                    swing_point=df['high'].iloc[swing_highs[-1]],
                    confirmation_price=current_close,
                    timestamp=df.index[-1],
                    timeframe="15min",
                    strength=8
                )
            # Check for CHoCH (higher low after downtrend)
            elif len(swing_lows) >= 2:
                if df['low'].iloc[swing_lows[-1]] > df['low'].iloc[swing_lows[-2]]:
                    return MarketStructure(
                        type=StructureType.CHOCH_BULLISH,
                        swing_point=df['low'].iloc[swing_lows[-1]],
                        confirmation_price=current_close,
                        timestamp=df.index[-1],
                        timeframe="15min",
                        strength=7
                    )
        else:
            # Bearish structures
            if swing_lows and current_close < df['low'].iloc[swing_lows[-1]]:
                return MarketStructure(
                    type=StructureType.BOS_BEARISH,
                    swing_point=df['low'].iloc[swing_lows[-1]],
                    confirmation_price=current_close,
                    timestamp=df.index[-1],
                    timeframe="15min",
                    strength=8
                )
            elif len(swing_highs) >= 2:
                if df['high'].iloc[swing_highs[-1]] < df['high'].iloc[swing_highs[-2]]:
                    return MarketStructure(
                        type=StructureType.CHOCH_BEARISH,
                        swing_point=df['high'].iloc[swing_highs[-1]],
                        confirmation_price=current_close,
                        timestamp=df.index[-1],
                        timeframe="15min",
                        strength=7
                    )
        
        return None
    
    def is_retest_valid(self, df: pd.DataFrame, structure: MarketStructure, direction: str) -> bool:
        """Check if price retested structure level with rejection"""
        recent = df.tail(5)
        
        if direction == "BUY":
            # Price should touch/break structure then reject up
            touched = recent['low'].min() <= structure.swing_point * 1.001
            rejected = recent['close'].iloc[-1] > recent['open'].iloc[-1]  # Bullish candle
            above_structure = recent['close'].iloc[-1] > structure.swing_point
        else:
            touched = recent['high'].max() >= structure.swing_point * 0.999
            rejected = recent['close'].iloc[-1] < recent['open'].iloc[-1]  # Bearish candle
            above_structure = recent['close'].iloc[-1] < structure.swing_point
        
        return touched and rejected and above_structure

# ================= MARKET REGIME CLASSIFIER =================

class RegimeClassifier:
    """Classifies market conditions for strategy selection"""
    
    def __init__(self):
        self.trend_ema_fast = 20
        self.trend_ema_slow = 50
        self.bb_period = 20
    
    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ADX for trend strength"""
        high = df['high']
        low = df['low']
        close = df['close']
        
        plus_dm = high.diff()
        minus_dm = -low.diff()
        
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
        
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)
        
        atr = tr.ewm(span=period, adjust=False).mean()
        plus_di = 100 * plus_dm.ewm(span=period, adjust=False).mean() / atr
        minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / atr
        
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx = dx.ewm(span=period, adjust=False).mean()
        
        return adx.iloc[-1] if not adx.empty else 0
    
    def calculate_volatility(self, df: pd.DataFrame) -> float:
        """Calculate current volatility regime"""
        returns = df['close'].pct_change().dropna()
        return returns.std() * np.sqrt(252)  # Annualized volatility
    
    def detect_compression(self, df: pd.DataFrame) -> bool:
        """Detect Bollinger Band squeeze"""
        sma = df['close'].rolling(self.bb_period).mean()
        std = df['close'].rolling(self.bb_period).std()
        upper = sma + (std * 2)
        lower = sma - (std * 2)
        
        bandwidth = (upper - lower) / sma
        current_width = bandwidth.iloc[-1]
        avg_width = bandwidth.rolling(20).mean().iloc[-1]
        
        return current_width < avg_width * 0.6  # 60% of average = squeeze
    
    def classify(self, df: pd.DataFrame, df_htf: pd.DataFrame) -> MarketRegime:
        """Classify current market regime"""
        adx = self.calculate_adx(df)
        volatility = self.calculate_volatility(df)
        is_compression = self.detect_compression(df)
        
        # Trend determination
        ema_fast = df['close'].ewm(span=self.trend_ema_fast, adjust=False).mean().iloc[-1]
        ema_slow = df['close'].ewm(span=self.trend_ema_slow, adjust=False).mean().iloc[-1]
        
        # HTF trend
        ema_fast_htf = df_htf['close'].ewm(span=self.trend_ema_fast, adjust=False).mean().iloc[-1]
        ema_slow_htf = df_htf['close'].ewm(span=self.trend_ema_slow, adjust=False).mean().iloc[-1]
        
        trend_up = ema_fast > ema_slow and ema_fast_htf > ema_slow_htf
        trend_down = ema_fast < ema_slow and ema_fast_htf < ema_slow_htf
        
        if is_compression:
            return MarketRegime.COMPRESSION
        elif adx < 20:
            if volatility < SystemConfig.VOLATILITY_LOW:
                return MarketRegime.LOW_VOLATILITY
            return MarketRegime.RANGING
        elif adx > 40:
            if volatility > SystemConfig.VOLATILITY_HIGH:
                return MarketRegime.EXPANSION
            if trend_up:
                return MarketRegime.TRENDING_UP
            elif trend_down:
                return MarketRegime.TRENDING_DOWN
        
        return MarketRegime.RANGING

# ================= SESSION MANAGER =================

class SessionManager:
    """Manages trading sessions and kill zones"""
    
    def __init__(self):
        self.timezone = pytz.UTC
    
    def get_current_session(self) -> SessionType:
        """Determine current trading session"""
        now = datetime.now(self.timezone)
        hour = now.hour
        
        london = SystemConfig.LONDON_START <= hour < SystemConfig.LONDON_END
        ny = SystemConfig.NY_START <= hour < SystemConfig.NY_END
        
        if london and ny:
            return SessionType.OVERLAP
        elif london:
            return SessionType.LONDON
        elif ny:
            return SessionType.NEW_YORK
        elif 0 <= hour < 7:
            return SessionType.ASIA
        else:
            return SessionType.OFF_HOURS
    
    def is_kill_zone(self) -> bool:
        """Check if in high-probability overlap window"""
        now = datetime.now(self.timezone)
        hour = now.hour
        return SystemConfig.KILL_ZONE_OVERLAP[0] <= hour < SystemConfig.KILL_ZONE_OVERLAP[1]
    
    def is_tradeable_session(self) -> bool:
        """Only trade London and NY sessions"""
        session = self.get_current_session()
        return session in [SessionType.LONDON, SessionType.NEW_YORK, SessionType.OVERLAP]
    
    def get_session_quality_score(self) -> int:
        """Score session timing (0-20)"""
        if not self.is_tradeable_session():
            return 0
        
        session = self.get_current_session()
        if session == SessionType.OVERLAP:
            return 20  # Maximum quality
        elif session == SessionType.LONDON:
            return 15
        elif session == SessionType.NEW_YORK:
            return 12
        return 0

# ================= CONFIDENCE SCORING ENGINE =================

class ConfidenceScorer:
    """Calculates trade confidence score 0-100"""
    
    def __init__(self):
        self.weights = {
            'liquidity_sweep': 20,
            'structure_break': 20,
            'session_timing': 15,
            'htf_bias': 15,
            'volatility_regime': 10,
            'risk_reward': 10,
            'retest_quality': 10
        }
    
    def calculate(self, 
                  liquidity_sweep: bool, 
                  sweep_strength: int,
                  structure: MarketStructure,
                  session_score: int,
                  htf_aligned: bool,
                  regime: MarketRegime,
                  rr_ratio: float,
                  retest_valid: bool) -> Tuple[int, Dict[str, int]]:
        """Calculate composite confidence score"""
        
        components = {}
        
        # Liquidity sweep (0-20)
        if liquidity_sweep:
            components['liquidity_sweep'] = min(20, 10 + sweep_strength)
        else:
            components['liquidity_sweep'] = 0
        
        # Structure break (0-20)
        if structure:
            components['structure_break'] = min(20, 10 + structure.strength)
        else:
            components['structure_break'] = 0
        
        # Session timing (0-15)
        components['session_timing'] = session_score
        
        # HTF bias alignment (0-15)
        components['htf_bias'] = 15 if htf_aligned else 0
        
        # Volatility regime (0-10)
        if regime in [MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN]:
            components['volatility_regime'] = 10
        elif regime == MarketRegime.EXPANSION:
            components['volatility_regime'] = 8
        elif regime == MarketRegime.COMPRESSION:
            components['volatility_regime'] = 6
        else:
            components['volatility_regime'] = 2
        
        # Risk:Reward (0-10)
        if rr_ratio >= 3.0:
            components['risk_reward'] = 10
        elif rr_ratio >= 2.5:
            components['risk_reward'] = 8
        elif rr_ratio >= 2.0:
            components['risk_reward'] = 6
        else:
            components['risk_reward'] = max(0, int(rr_ratio * 3))
        
        # Retest quality (0-10)
        components['retest_quality'] = 10 if retest_valid else 0
        
        total_score = sum(components.values())
        return min(100, total_score), components

# ================= ADAPTIVE RISK MANAGER =================

class AdaptiveRiskManager:
    """Volatility-based position sizing with equity protection"""
    
    def __init__(self, initial_equity: float = 10000.0):
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.peak_equity = initial_equity
        self.daily_risk_used = 0.0
        self.last_reset = datetime.now().date()
        self.defensive_mode = False
        self.loss_streak = 0
        self.trade_history: deque = deque(maxlen=50)
    
    def update_equity(self, pnl: float):
        """Update equity and check drawdown"""
        self.current_equity += pnl
        self.peak_equity = max(self.peak_equity, self.current_equity)
        
        drawdown = (self.peak_equity - self.current_equity) / self.peak_equity
        
        if drawdown > SystemConfig.DRAWDOWN_THRESHOLD:
            self.defensive_mode = True
            logging.warning(f"Defensive mode activated. Drawdown: {drawdown:.2%}")
        
        if pnl < 0:
            self.loss_streak += 1
            if self.loss_streak >= SystemConfig.MAX_CONSECUTIVE_LOSSES:
                self.defensive_mode = True
        else:
            self.loss_streak = max(0, self.loss_streak - 1)
            if self.loss_streak == 0 and drawdown < SystemConfig.DRAWDOWN_THRESHOLD * 0.5:
                self.defensive_mode = False
    
    def check_daily_reset(self):
        """Reset daily risk counter"""
        today = datetime.now().date()
        if today != self.last_reset:
            self.daily_risk_used = 0.0
            self.last_reset = today
    
    def calculate_position_size(self, 
                               entry: float, 
                               stop_loss: float, 
                               atr: float,
                               volatility: float) -> Tuple[float, float]:
        """Calculate adaptive position size"""
        self.check_daily_reset()
        
        # Check daily risk limit
        if self.daily_risk_used >= SystemConfig.MAX_DAILY_RISK:
            return 0.0, 0.0
        
        # Base risk percentage
        risk_pct = SystemConfig.BASE_RISK_PCT
        
        # Adjust for defensive mode
        if self.defensive_mode:
            risk_pct *= 0.5  # Halve risk in defensive mode
        
        # Adjust for volatility (reduce size in high vol)
        vol_adjustment = 1.0
        if volatility > SystemConfig.VOLATILITY_HIGH:
            vol_adjustment = 0.7
        elif volatility < SystemConfig.VOLATILITY_LOW:
            vol_adjustment = 0.8  # Also reduce in low vol (chop)
        
        # Adjust for loss streak
        if self.loss_streak > 0:
            streak_adjustment = max(0.3, 1.0 - (self.loss_streak * 0.2))
            risk_pct *= streak_adjustment
        
        adjusted_risk = risk_pct * vol_adjustment
        available_risk = min(adjusted_risk, SystemConfig.MAX_DAILY_RISK - self.daily_risk_used)
        
        # Calculate position size
        risk_amount = self.current_equity * available_risk
        risk_per_unit = abs(entry - stop_loss)
        
        if risk_per_unit == 0:
            return 0.0, 0.0
        
        position_size = risk_amount / risk_per_unit
        self.daily_risk_used += available_risk
        
        return position_size, available_risk
    
    def get_sl_placement(self, 
                        direction: str, 
                        df: pd.DataFrame, 
                        atr: float,
                        structure: MarketStructure) -> float:
        """Place SL beyond liquidity wick or structure"""
        recent = df.tail(5)
        
        if direction == "BUY":
            # SL below recent low or structure
            recent_low = recent['low'].min()
            structure_level = structure.swing_point if structure else recent_low
            sl_candidate = min(recent_low, structure_level) - (atr * 0.5)
            return sl_candidate
        else:
            recent_high = recent['high'].max()
            structure_level = structure.swing_point if structure else recent_high
            sl_candidate = max(recent_high, structure_level) + (atr * 0.5)
            return sl_candidate

# ================= CORRELATION GUARD =================

class CorrelationGuard:
    """Prevents correlated trade stacking"""
    
    def __init__(self):
        # Currency correlations (simplified)
        self.correlation_groups = {
            'usd_majors': ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD'],
            'usd_crosses': ['USD/JPY', 'USD/CHF', 'USD/CAD'],
            'euro_crosses': ['EUR/GBP', 'EUR/JPY', 'EUR/CHF'],
            'crypto': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
        }
        self.active_directions: Dict[str, str] = {}
    
    def can_trade(self, symbol: str, direction: str, active_trades: List[Trade]) -> bool:
        """Check if trade would create correlated risk"""
        # Find which group
        group = None
        for g_name, symbols in self.correlation_groups.items():
            if symbol in symbols:
                group = g_name
                break
        
        if not group:
            return True
        
        # Check for opposing direction in same group
        for trade in active_trades:
            if not trade.is_closed:
                trade_group = None
                for g_name, symbols in self.correlation_groups.items():
                    if trade.signal.symbol in symbols:
                        trade_group = g_name
                        break
                
                if trade_group == group:
                    # Same group - check direction
                    if trade.signal.direction != direction:
                        # Opposing directions in correlated pairs = cancel out or double risk
                        return False
        
        return True
    
    def get_heat_score(self, active_trades: List[Trade]) -> float:
        """Calculate portfolio heat (0-100)"""
        if not active_trades:
            return 0.0
        
        total_risk = sum(t.signal.risk_reward for t in active_trades if not t.is_closed)
        return min(100, total_risk * 10)

# ================= EXECUTION ENGINE =================

class ExecutionEngine:
    """Handles trade execution with spread/slippage filters"""
    
    def __init__(self):
        self.max_spread = SystemConfig.MAX_SPREAD_PCT
        self.max_slippage = SystemConfig.MAX_SLIPPAGE_PCT
    
    async def check_spread(self, symbol: str, fetcher: 'DataFetcher') -> Tuple[bool, float]:
        """Check if spread is acceptable"""
        # Get current quote
        quote = await fetcher.get_quote(symbol)
        if not quote:
            return False, 0.0
        
        spread = (quote['ask'] - quote['bid']) / ((quote['ask'] + quote['bid']) / 2)
        return spread <= self.max_spread, spread
    
    def calculate_entry_zone(self, 
                            direction: str, 
                            structure: MarketStructure,
                            df: pd.DataFrame) -> Tuple[float, float]:
        """Calculate optimal entry zone with retest confirmation"""
        current = df['close'].iloc[-1]
        
        if direction == "BUY":
            # Entry at structure retest or 50% of last candle
            ideal_entry = structure.swing_point if structure else current
            max_entry = current * 1.002  # Max 20 pips above
            return ideal_entry, max_entry
        else:
            ideal_entry = structure.swing_point if structure else current
            max_entry = current * 0.998  # Max 20 pips below
            return ideal_entry, max_entry
    
    def calculate_tp_levels(self, 
                           direction: str, 
                           entry: float, 
                           sl: float,
                           liquidity_map: LiquidityMap) -> Tuple[float, Optional[float]]:
        """Calculate partial TP and runner TP based on liquidity"""
        risk = abs(entry - sl)
        
        # Partial TP at 1R
        partial_tp = entry + risk if direction == "BUY" else entry - risk
        
        # Runner TP at next liquidity level or 3R
        nearest_liq = liquidity_map.get_nearest_liquidity(entry, direction)
        if nearest_liq:
            runner_tp = nearest_liq.level
            # Ensure minimum 2:1
            runner_risk_reward = abs(runner_tp - entry) / risk
            if runner_risk_reward < 2.0:
                runner_tp = entry + (risk * 3) if direction == "BUY" else entry - (risk * 3)
        else:
            runner_tp = entry + (risk * SystemConfig.RUNNER_TP_MULTIPLIER) if direction == "BUY" \
                       else entry - (risk * SystemConfig.RUNNER_TP_MULTIPLIER)
        
        return partial_tp, runner_tp

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
            
            async with session.post(url, data=payload) as response:
                result = await response.json()
                return result.get("ok", False)
        except Exception as e:
            logging.error(f"Telegram error: {e}")
            return False
    
    def format_signal(self, signal: Signal) -> str:
        emoji = "🎯" if signal.kill_zone else "⚡"
        regime_emoji = {
            MarketRegime.TRENDING_UP: "📈",
            MarketRegime.TRENDING_DOWN: "📉",
            MarketRegime.RANGING: "↔️",
            MarketRegime.COMPRESSION: "⚠️",
            MarketRegime.EXPANSION: "💥",
            MarketRegime.LOW_VOLATILITY: "😴"
        }.get(signal.regime, "⚪")
        
        confidence_bar = "█" * (signal.confidence_score // 10) + "░" * (10 - signal.confidence_score // 10)
        
        return f"""
{emoji} <b>INSTITUTIONAL SETUP DETECTED</b> {emoji}

<b>Quality Score:</b> <code>{signal.confidence_score}/100</code> [{confidence_bar}]
<b>Regime:</b> {regime_emoji} {signal.regime.value.replace('_', ' ').title()}
<b>Session:</b> {signal.session.value.upper()} {'🔥 KILL ZONE' if signal.kill_zone else ''}

<b>Symbol:</b> <code>{signal.symbol}</code> | <b>Direction:</b> {"🟢 LONG" if signal.direction == "BUY" else "🔴 SHORT"}
<b>Strategy:</b> {signal.structure_break.type.value.upper()} | <b>HTF Bias:</b> {signal.htf_bias}

<b>Execution Levels:</b>
├ Entry: <code>{signal.entry_price:.5f}</code>
├ Stop: <code>{signal.stop_loss:.5f}</code> ({abs(signal.entry_price - signal.stop_loss)/signal.entry_price*100:.2f}%)
├ TP1 (50%): <code>{signal.take_profit:.5f}</code> (1:1)
└ TP2 (Runner): <code>{signal.take_profit_runner:.5f}</code> (1:{signal.risk_reward:.1f})

<b>Risk Metrics:</b>
├ ATR: {signal.atr_value:.5f} | Spread: {signal.spread_at_entry*10000:.1f} pips
└ R:R Ratio: 1:{signal.risk_reward:.2f}

<b>Score Breakdown:</b>
{chr(10).join(f"• {k.replace('_', ' ').title()}: {v}/20" for k, v in signal.score_components.items() if v > 0)}

<i>Liquidity {signal.liquidity_sweep.type.value.replace('_', ' ').title()} swept @ {signal.liquidity_sweep.level:.5f}</i>
<code>ID: {signal.id}</code>
"""

    def format_daily_report(self, metrics: PerformanceMetrics, date: str) -> str:
        emoji = "👑" if metrics.net_profit > 0 else "⚠️" if metrics.net_profit > -metrics.initial_equity * 0.01 else "🛑"
        
        return f"""
{emoji} <b>DAILY PERFORMANCE REPORT</b> {emoji}
<i>{date}</i>

<b>Trade Statistics:</b>
├ Total Trades: {metrics.total_trades}
├ Wins: {metrics.wins} | Losses: {metrics.losses} | Partials: {metrics.partials}
└ Win Rate: {metrics.win_rate:.1f}%

<b>Financial Performance:</b>
├ Gross Profit: +${metrics.gross_profit:.2f}
├ Gross Loss: ${metrics.gross_loss:.2f}
├ Net P&L: ${metrics.net_profit:+.2f}
└ Profit Factor: {metrics.profit_factor:.2f}

<b>Risk Metrics:</b>
├ Max Drawdown: {metrics.max_drawdown:.2%}
├ Current Drawdown: {metrics.current_drawdown:.2%}
└ Consecutive Losses: {metrics.consecutive_losses}

<b>Session Performance:</b>
{chr(10).join(f"• {session}: {sum(trades)/len(trades)*100:.0f}% WR ({len(trades)} trades)" 
              for session, trades in metrics.win_rate_by_session.items() if trades)}

<i>System {"DEFENSIVE" if metrics.consecutive_losses >= 2 else "NORMAL"} mode | 
Equity: ${metrics.equity_curve[-1][1]:.2f if metrics.equity_curve else 0}</i>
"""

# ================= DATA FETCHER =================

class DataFetcher:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter = asyncio.Semaphore(8)  # TwelveData free tier limit
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"Accept": "application/json"}
            )
        return self._session
    
    async def fetch_time_series(self, 
                                symbol: str, 
                                interval: str, 
                                outputsize: int = 500) -> Optional[pd.DataFrame]:
        async with self._rate_limiter:
            session = await self._get_session()
            url = f"{self.base_url}/time_series"
            params = {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": self.api_key
            }
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        return None
                    
                    data = await response.json()
                    if "values" not in data or not data["values"]:
                        return None
                    
                    return self._process_data(data["values"])
            except Exception as e:
                logging.error(f"Fetch error {symbol}: {e}")
                return None
    
    async def get_quote(self, symbol: str) -> Optional[Dict]:
        """Get real-time quote for spread calculation"""
        async with self._rate_limiter:
            session = await self._get_session()
            url = f"{self.base_url}/quote"
            params = {"symbol": symbol, "apikey": self.api_key}
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'bid': float(data.get('bid', 0)),
                            'ask': float(data.get('ask', 0)),
                            'price': float(data.get('price', 0))
                        }
            except:
                pass
            return None
    
    def _process_data(self, values: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame(values)
        
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df.dropna(subset=["open", "high", "low", "close"], inplace=True)
        
        return df
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# ================= PERFORMANCE OPTIMIZER =================

class PerformanceOptimizer:
    """Self-learning engine that optimizes parameters based on trade history"""
    
    def __init__(self, history_file: Path = HISTORY_DIR / "trade_history.json"):
        self.history_file = history_file
        self.trade_history: List[Dict] = []
        self.optimal_params = {
            'best_sessions': [],
            'best_pairs': [],
            'best_regimes': [],
            'optimal_rr': 2.0,
            'min_confidence': 70
        }
        self.load_history()
    
    def load_history(self):
        """Load historical trade data"""
        if self.history_file.exists():
            with open(self.history_file, 'r') as f:
                self.trade_history = json.load(f)
            self._analyze_performance()
    
    def save_trade(self, trade: Trade):
        """Record trade for analysis"""
        record = {
            'timestamp': trade.signal.timestamp.isoformat(),
            'symbol': trade.signal.symbol,
            'direction': trade.signal.direction,
            'session': trade.signal.session.value,
            'regime': trade.signal.regime.value,
            'confidence': trade.signal.confidence_score,
            'rr': trade.signal.risk_reward,
            'pnl': trade.total_pnl,
            'status': trade.status.value,
            'components': trade.signal.score_components
        }
        self.trade_history.append(record)
        
        # Save periodically
        if len(self.trade_history) % 10 == 0:
            self._persist_history()
            self._analyze_performance()
    
    def _persist_history(self):
        """Save to disk"""
        with open(self.history_file, 'w') as f:
            json.dump(self.trade_history[-1000:], f)  # Keep last 1000
    
    def _analyze_performance(self):
        """Analyze patterns in winning vs losing trades"""
        if len(self.trade_history) < 20:
            return
        
        df = pd.DataFrame(self.trade_history)
        
        # Analyze by session
        session_performance = df.groupby('session')['pnl'].agg(['sum', 'count', 'mean'])
        self.optimal_params['best_sessions'] = session_performance[
            session_performance['mean'] > 0
        ].index.tolist()
        
        # Analyze by regime
        regime_performance = df.groupby('regime')['pnl'].agg(['sum', 'count', 'mean'])
        self.optimal_params['best_regimes'] = regime_performance[
            regime_performance['mean'] > 0
        ].index.tolist()
        
        # Analyze by confidence score
        df['confidence_bucket'] = pd.cut(df['confidence'], bins=[0, 60, 70, 80, 90, 100])
        conf_performance = df.groupby('confidence_bucket')['pnl'].mean()
        best_conf = conf_performance.idxmax()
        if best_conf:
            self.optimal_params['min_confidence'] = int(best_conf.left)
        
        # Analyze by R:R
        df['rr_bucket'] = pd.cut(df['rr'], bins=[0, 1.5, 2.0, 2.5, 3.0, 5.0])
        rr_performance = df.groupby('rr_bucket')['pnl'].mean()
        best_rr = rr_performance.idxmax()
        if best_rr:
            self.optimal_params['optimal_rr'] = float((best_rr.left + best_rr.right) / 2)
    
    def get_recommendations(self) -> Dict:
        """Get current optimization recommendations"""
        return self.optimal_params
    
    def should_trade(self, signal: Signal) -> Tuple[bool, str]:
        """Filter based on historical performance"""
        # Avoid trading in consistently losing regimes
        if signal.regime.value in self.optimal_params.get('worst_regimes', []):
            return False, f"Avoiding {signal.regime.value} based on historical performance"
        
        # Require higher confidence in defensive markets
        if signal.regime == MarketRegime.RANGING and signal.confidence_score < 80:
            return False, "Low confidence range trade rejected"
        
        return True, "Passed optimization filter"

# ================= MAIN TRADING ENGINE =================

class InstitutionalTradingEngine:
    """Main orchestrator for institutional-grade trading"""
    
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.fetcher = DataFetcher(TWELVEDATA_KEY)
        
        # Core components
        self.liquidity_map = LiquidityMap()
        self.structure_detector = StructureDetector()
        self.regime_classifier = RegimeClassifier()
        self.session_manager = SessionManager()
        self.confidence_scorer = ConfidenceScorer()
        self.risk_manager = AdaptiveRiskManager()
        self.correlation_guard = CorrelationGuard()
        self.execution_engine = ExecutionEngine()
        self.optimizer = PerformanceOptimizer()
        
        # State
        self.active_trades: List[Trade] = []
        self.metrics = PerformanceMetrics()
        self.watchlist = [
            "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD",
            "BTC/USDT", "ETH/USDT"
        ]
        self.running = False
    
    def generate_signal_id(self, symbol: str) -> str:
        return f"{symbol.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    async def analyze_symbol(self, symbol: str) -> Optional[Signal]:
        """Full institutional analysis pipeline"""
        
        # 1. Session filter
        if not self.session_manager.is_tradeable_session():
            return None
        
        # 2. Fetch multi-timeframe data
        df_15m = await self.fetcher.fetch_time_series(symbol, SystemConfig.TF_EXECUTION, 500)
        df_1h = await self.fetcher.fetch_time_series(symbol, SystemConfig.TF_TREND, 300)
        df_4h = await self.fetcher.fetch_time_series(symbol, SystemConfig.TF_STRUCTURE, 200)
        df_1d = await self.fetcher.fetch_time_series(symbol, SystemConfig.TF_BIAS, 100)
        
        if any(df is None or len(df) < 50 for df in [df_15m, df_1h, df_4h, df_1d]):
            return None
        
        # 3. Market regime classification
        regime = self.regime_classifier.classify(df_15m, df_4h)
        if regime in [MarketRegime.LOW_VOLATILITY, MarketRegime.MANIPULATION]:
            logging.info(f"{symbol}: Avoiding {regime.value} regime")
            return None
        
        # 4. Higher timeframe bias
        htf_bias = self._determine_htf_bias(df_1h, df_4h, df_1d)
        
        # 5. Liquidity analysis
        eq_high, eq_low = self.liquidity_map.detect_equal_levels(df_15m)
        session_high, session_low = self.liquidity_map.detect_session_levels(
            df_15m, self.session_manager.get_current_session()
        )
        
        # Determine direction based on sweep and structure
        direction = None
        sweep_level = None
        sweep_type = None
        sweep_strength = 0
        
        # Check for liquidity sweeps
        if eq_high and self.liquidity_map.detect_sweep(df_15m, "SELL", eq_high):
            direction = "SELL"
            sweep_level = eq_high
            sweep_type = LiquidityType.EQUAL_HIGHS
            sweep_strength = 8
        elif eq_low and self.liquidity_map.detect_sweep(df_15m, "BUY", eq_low):
            direction = "BUY"
            sweep_level = eq_low
            sweep_type = LiquidityType.EQUAL_LOWS
            sweep_strength = 8
        elif session_high and self.liquidity_map.detect_sweep(df_15m, "SELL", session_high):
            direction = "SELL"
            sweep_level = session_high
            sweep_type = LiquidityType.SESSION_HIGH
            sweep_strength = 6
        elif session_low and self.liquidity_map.detect_sweep(df_15m, "BUY", session_low):
            direction = "BUY"
            sweep_level = session_low
            sweep_type = LiquidityType.SESSION_LOW
            sweep_strength = 6
        
        if not direction or not sweep_level:
            return None
        
        # 6. Structure break confirmation
        structure = self.structure_detector.detect_structure_break(df_15m, direction)
        if not structure:
            logging.info(f"{symbol}: No structure break confirmed")
            return None
        
        # 7. Retest validation (don't chase)
        if not self.structure_detector.is_retest_valid(df_15m, structure, direction):
            logging.info(f"{symbol}: Invalid retest - possible fakeout")
            return None
        
        # 8. HTF alignment check
        htf_aligned = self._check_htf_alignment(direction, htf_bias)
        
        # 9. Spread check
        spread_ok, spread = await self.execution_engine.check_spread(symbol, self.fetcher)
        if not spread_ok:
            logging.info(f"{symbol}: Spread too high ({spread:.4%})")
            return None
        
        # 10. Calculate levels
        atr = self._calculate_atr(df_15m)
        entry = df_15m['close'].iloc[-1]
        sl = self.risk_manager.get_sl_placement(direction, df_15m, atr, structure)
        
        # Calculate TP levels
        partial_tp, runner_tp = self.execution_engine.calculate_tp_levels(
            direction, entry, sl, self.liquidity_map
        )
        
        risk = abs(entry - sl)
        reward = abs(runner_tp - entry)
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < SystemConfig.MIN_RR_RATIO:
            logging.info(f"{symbol}: R:R {rr_ratio:.2f} below minimum")
            return None
        
        # 11. Confidence scoring
        session_score = self.session_manager.get_session_quality_score()
        confidence, components = self.confidence_scorer.calculate(
            liquidity_sweep=True,
            sweep_strength=sweep_strength,
            structure=structure,
            session_score=session_score,
            htf_aligned=htf_aligned,
            regime=regime,
            rr_ratio=rr_ratio,
            retest_valid=True
        )
        
        if confidence < SystemConfig.MIN_CONFIDENCE_SCORE:
            logging.info(f"{symbol}: Confidence {confidence} below threshold")
            return None
        
        # 12. Correlation check
        if not self.correlation_guard.can_trade(symbol, direction, self.active_trades):
            logging.info(f"{symbol}: Correlation guard blocked trade")
            return None
        
        # 13. Optimization check
        should_trade, reason = self.optimizer.should_trade(None)  # Will check regime filters
        
        # Create signal
        liquidity_zone = LiquidityZone(
            level=sweep_level,
            type=sweep_type,
            strength=sweep_strength,
            timestamp=datetime.now(),
            is_swept=True,
            sweep_timestamp=datetime.now()
        )
        
        signal = Signal(
            id=self.generate_signal_id(symbol),
            symbol=symbol,
            direction=direction,
            confidence_score=confidence,
            entry_price=entry,
            stop_loss=sl,
            take_profit=partial_tp,
            take_profit_runner=runner_tp,
            atr_value=atr,
            risk_reward=rr_ratio,
            liquidity_sweep=liquidity_zone,
            structure_break=structure,
            session=self.session_manager.get_current_session(),
            regime=regime,
            htf_bias=htf_bias,
            volatility_regime="normal",  # Simplified
            spread_at_entry=spread,
            score_components=components,
            timestamp=datetime.now(),
            kill_zone=self.session_manager.is_kill_zone()
        )
        
        return signal
    
    def _determine_htf_bias(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame, df_1d: pd.DataFrame) -> str:
        """Determine higher timeframe directional bias"""
        def ema_trend(df: pd.DataFrame) -> str:
            if len(df) < 50:
                return "neutral"
            ema20 = df['close'].ewm(span=20, adjust=False).mean().iloc[-1]
            ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            return "bullish" if ema20 > ema50 else "bearish"
        
        trend_1h = ema_trend(df_1h)
        trend_4h = ema_trend(df_4h)
        trend_1d = ema_trend(df_1d)
        
        # Weight: Daily > 4H > 1H
        if trend_1d == trend_4h == trend_1h:
            return f"strong_{trend_1d}"
        elif trend_1d == trend_4h:
            return f"{trend_1d}_bias"
        else:
            return "mixed"
    
    def _check_htf_alignment(self, direction: str, htf_bias: str) -> bool:
        """Check if trade direction aligns with HTF bias"""
        if "strong_bullish" in htf_bias and direction == "BUY":
            return True
        if "strong_bearish" in htf_bias and direction == "SELL":
            return True
        if "bullish_bias" in htf_bias and direction == "BUY":
            return True
        if "bearish_bias" in htf_bias and direction == "SELL":
            return True
        return False
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(span=period, adjust=False).mean().iloc[-1]
        return atr
    
    async def execute_trade(self, signal: Signal) -> bool:
        """Execute trade with full risk management"""
        # Calculate position size
        volatility = self.regime_classifier.calculate_volatility(
            await self.fetcher.fetch_time_series(signal.symbol, "15min", 100) or pd.DataFrame()
        )
        
        position_size, risk_pct = self.risk_manager.calculate_position_size(
            signal.entry_price, signal.stop_loss, signal.atr_value, volatility
        )
        
        if position_size <= 0:
            logging.warning("Risk limits reached, skipping trade")
            return False
        
        # Create trade
        trade = Trade(
            signal=signal,
            position_size=position_size
        )
        
        self.active_trades.append(trade)
        
        # Notify
        await self.telegram.send_message(self.telegram.format_signal(signal))
        
        logging.info(f"Trade executed: {signal.symbol} {signal.direction} "
                    f"Size: {position_size:.4f} Risk: {risk_pct:.2%}")
        
        return True
    
    async def manage_active_trades(self):
        """Monitor and manage open positions"""
        for trade in list(self.active_trades):
            if trade.is_closed:
                continue
            
            # Fetch recent data
            df = await self.fetcher.fetch_time_series(
                trade.signal.symbol, SystemConfig.TF_EXECUTION, 100
            )
            if df is None or len(df) < 5:
                continue
            
            current_price = df['close'].iloc[-1]
            high = df['high'].iloc[-1]
            low = df['low'].iloc[-1]
            trade.candles_held += 1
            
            # Update max profit/drawdown
            if trade.signal.direction == "BUY":
                current_pnl = (current_price - trade.signal.entry_price) / \
                             (trade.signal.entry_price - trade.signal.stop_loss)
            else:
                current_pnl = (trade.signal.entry_price - current_price) / \
                             (trade.signal.stop_loss - trade.signal.entry_price)
            
            trade.max_profit_reached = max(trade.max_profit_reached, current_pnl)
            trade.max_drawdown_reached = min(trade.max_drawdown_reached, current_pnl)
            
            # Check partial TP
            if not trade.partial_closed:
                if trade.signal.direction == "BUY":
                    hit_partial = high >= trade.signal.take_profit
                else:
                    hit_partial = low <= trade.signal.take_profit
                
                if hit_partial:
                    trade.partial_closed = True
                    trade.partial_pnl = 1.0 * 0.5  # 1R * 50% position
                    await self.telegram.send_message(
                        f"🎯 <b>PARTIAL TP HIT</b>\n{trade.signal.symbol} @ {trade.signal.take_profit:.5f}\n"
                        f"Closed 50% for +{trade.partial_pnl:.2f}R"
                    )
            
            # Check runner TP
            if trade.signal.take_profit_runner:
                if trade.signal.direction == "BUY":
                    hit_runner = high >= trade.signal.take_profit_runner
                else:
                    hit_runner = low <= trade.signal.take_profit_runner
                
                if hit_runner:
                    runner_r = abs(trade.signal.take_profit_runner - trade.signal.entry_price) / \
                              abs(trade.signal.entry_price - trade.signal.stop_loss)
                    trade.runner_pnl = runner_r * 0.5  # 50% of position
                    trade.status = TradeStatus.WIN
                    trade.close_reason = "Runner TP hit"
                    await self._close_trade(trade, trade.signal.take_profit_runner)
                    continue
            
            # Check SL
            if trade.signal.direction == "BUY":
                hit_sl = low <= trade.signal.stop_loss
            else:
                hit_sl = high >= trade.signal.stop_loss
            
            if hit_sl:
                trade.status = TradeStatus.LOSS
                trade.close_reason = "Stop loss hit"
                await self._close_trade(trade, trade.signal.stop_loss)
                continue
            
            # Time-based exit (stagnant trade)
            if trade.candles_held >= SystemConfig.MAX_TRADE_DURATION_MIN / 15:  # 15min candles
                if current_pnl > 0:
                    # Close at market if profitable but stagnant
                    trade.status = TradeStatus.WIN
                    trade.close_reason = "Time exit (profitable)"
                    trade.runner_pnl = current_pnl * 0.5 if trade.partial_closed else current_pnl
                else:
                    trade.status = TradeStatus.EXPIRED
                    trade.close_reason = "Time exit (stagnant)"
                await self._close_trade(trade, current_price)
    
    async def _close_trade(self, trade: Trade, exit_price: float):
        """Close trade and update metrics"""
        trade.exit_price = exit_price
        trade.exit_time = datetime.now()
        
        # Calculate total P&L in R
        total_r = trade.total_pnl
        
        # Update risk manager
        pnl_dollars = total_r * trade.position_size * abs(trade.signal.entry_price - trade.signal.stop_loss)
        self.risk_manager.update_equity(pnl_dollars)
        
        # Update metrics
        self.metrics.total_trades += 1
        if trade.status == TradeStatus.WIN:
            self.metrics.wins += 1
            self.metrics.gross_profit += total_r
        elif trade.status == TradeStatus.LOSS:
            self.metrics.losses += 1
            self.metrics.gross_loss -= total_r  # Losses negative
            self.metrics.consecutive_losses += 1
        elif trade.status == TradeStatus.EXPIRED:
            if total_r > 0:
                self.metrics.wins += 1
                self.metrics.gross_profit += total_r
            else:
                self.metrics.losses += 1
                self.metrics.gross_loss += total_r
        
        # Update drawdown
        self.metrics.current_drawdown = (self.risk_manager.peak_equity - self.risk_manager.current_equity) / \
                                       self.risk_manager.peak_equity
        self.metrics.max_drawdown = max(self.metrics.max_drawdown, self.metrics.current_drawdown)
        
        # Session tracking
        session_key = trade.signal.session.value
        if session_key not in self.metrics.win_rate_by_session:
            self.metrics.win_rate_by_session[session_key] = []
        self.metrics.win_rate_by_session[session_key].append(trade.status == TradeStatus.WIN)
        
        # Save to optimizer
        self.optimizer.save_trade(trade)
        
        # Remove from active
        if trade in self.active_trades:
            self.active_trades.remove(trade)
        
        # Notify
        emoji = "✅" if trade.status == TradeStatus.WIN else "❌" if trade.status == TradeStatus.LOSS else "⏱️"
        await self.telegram.send_message(
            f"{emoji} <b>TRADE CLOSED</b>\n"
            f"{trade.signal.symbol} {trade.signal.direction}\n"
            f"Result: {trade.status.value.upper()} | P&L: {total_r:+.2f}R\n"
            f"Reason: {trade.close_reason}\n"
            f"Duration: {trade.candles_held * 15}min"
        )
        
        logging.info(f"Trade closed: {trade.signal.id} | {trade.status.value} | {total_r:+.2f}R")
    
    async def run(self):
        """Main execution loop"""
        self.running = True
        
        await self.telegram.send_message(
            "🏛️ <b>INSTITUTIONAL TRADING ENGINE ACTIVATED</b>\n\n"
            f"<b>Configuration:</b>\n"
            f"• Min Confidence: {SystemConfig.MIN_CONFIDENCE_SCORE}/100\n"
            f"• Min R:R: 1:{SystemConfig.MIN_RR_RATIO}\n"
            f"• Max Daily Risk: {SystemConfig.MAX_DAILY_RISK:.1%}\n"
            f"• Kill Zone Only: {self.session_manager.is_kill_zone()}\n"
            f"• Defensive Mode: {self.risk_manager.defensive_mode}\n\n"
            f"<i>Scanning {len(self.watchlist)} instruments...</i>"
        )
        
        while self.running:
            try:
                # Trading hours check
                if not self.session_manager.is_tradeable_session():
                    await asyncio.sleep(300)  # Check every 5 min
                    continue
                
                # Manage existing trades first
                await self.manage_active_trades()
                
                # Scan for new opportunities
                for symbol in self.watchlist:
                    # Skip if max heat reached
                    if self.correlation_guard.get_heat_score(self.active_trades) > 50:
                        break
                    
                    # Skip if already in trade on this symbol
                    if any(t.signal.symbol == symbol and not t.is_closed for t in self.active_trades):
                        continue
                    
                    try:
                        signal = await self.analyze_symbol(symbol)
                        if signal:
                            await self.execute_trade(signal)
                    except Exception as e:
                        logging.error(f"Error analyzing {symbol}: {e}")
                    
                    await asyncio.sleep(1)  # Rate limiting
                
                # Daily report check
                now = datetime.now()
                if now.hour == 21 and now.minute < 5:
                    await self._send_daily_report()
                
                await asyncio.sleep(60)  # 1-minute scan interval
                
            except Exception as e:
                logging.error(f"Main loop error: {e}")
                await asyncio.sleep(60)
    
    async def _send_daily_report(self):
        """Send daily performance summary"""
        today = datetime.now().strftime("%Y-%m-%d")
        msg = self.telegram.format_daily_report(self.metrics, today)
        await self.telegram.send_message(msg)
        
        # Reset daily metrics
        self.metrics.daily_risk_used = 0
    
    async def shutdown(self):
        """Graceful shutdown"""
        self.running = False
        await self.fetcher.close()
        await self.telegram.close()

# ================= ENTRY POINT =================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(DATA_DIR / "trading_engine.log"),
            logging.StreamHandler()
        ]
    )
    
    # Validate environment
    required = ["TELEGRAM_TOKEN", "CHAT_ID", "TWELVEDATA_KEY"]
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        logging.error(f"Missing environment variables: {', '.join(missing)}")
        exit(1)
    
    engine = InstitutionalTradingEngine()
    
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logging.info("Shutdown requested...")
        asyncio.run(engine.shutdown())
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise
