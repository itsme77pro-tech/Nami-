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
import traceback

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
    KILL_ZONE_OVERLAP = (14, 16)
    
    # Quality Thresholds
    MIN_CONFIDENCE_SCORE = 70
    MIN_RR_RATIO = 2.0
    MAX_SPREAD_PCT = 0.0005
    MAX_SLIPPAGE_PCT = 0.001
    
    # Risk Management
    BASE_RISK_PCT = 0.01
    MAX_DAILY_RISK = 0.03
    MAX_CONSECUTIVE_LOSSES = 3
    DRAWDOWN_THRESHOLD = 0.05
    
    # Volatility Regimes
    VOLATILITY_LOW = 0.005
    VOLATILITY_HIGH = 0.02
    
    # Timeframes
    TF_EXECUTION = "15min"
    TF_TREND = "1h"
    TF_STRUCTURE = "4h"
    TF_BIAS = "1d"
    
    # Trade Management
    MAX_TRADE_DURATION_MIN = 240
    PARTIAL_TP_RATIO = 0.5
    PARTIAL_TP_LEVEL = 1.0
    RUNNER_TP_MULTIPLIER = 3.0
    
    # Scanning
    SCAN_INTERVAL_SECONDS = 60
    MAX_CONCURRENT_SCANS = 3  # Limit concurrent API calls
    API_RETRY_DELAY = 5
    MAX_API_RETRIES = 3

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
    BOS_BULLISH = "bos_bullish"
    BOS_BEARISH = "bos_bearish"
    CHOCH_BULLISH = "choch_bullish"
    CHOCH_BEARISH = "choch_bearish"
    NONE = "none"

class LiquidityType(Enum):
    EQUAL_HIGHS = "equal_highs"
    EQUAL_LOWS = "equal_lows"
    PDH = "pdh"
    PDL = "pdl"
    SESSION_HIGH = "session_high"
    SESSION_LOW = "session_low"
    SWEEP_HIGH = "sweep_high"
    SWEEP_LOW = "sweep_low"
    POOL = "liquidity_pool"

class TradeStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    WIN = "win"
    LOSS = "loss"
    EXPIRED = "expired"
    CLOSED_MANUAL = "manual"

# ================= DATA CLASSES =================

@dataclass
class LiquidityZone:
    level: float
    type: LiquidityType
    strength: int
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
    strength: int

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
    initial_equity: float = 10000.0
    
    @property
    def win_rate(self) -> float:
        closed = self.wins + self.losses
        return (self.wins / closed * 100) if closed > 0 else 0.0
    
    @property
    def profit_factor(self) -> float:
        return abs(self.gross_profit / self.gross_loss) if self.gross_loss != 0 else float('inf')
    
    @property
    def net_profit(self) -> float:
        return self.gross_profit + self.gross_loss

# ================= LIQUIDITY MAP GENERATOR =================

class LiquidityMap:
    def __init__(self, lookback: int = 100):
        self.lookback = lookback
        self.zones: List[LiquidityZone] = []
        self.equal_threshold = 0.0005
    
    def detect_equal_levels(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
        try:
            highs = df['high'].values
            lows = df['low'].values
            
            equal_highs = self._find_clusters(highs, self.equal_threshold)
            equal_lows = self._find_clusters(lows, self.equal_threshold)
            
            strongest_high = max(equal_highs, key=lambda x: x[1])[0] if equal_highs else None
            strongest_low = max(equal_lows, key=lambda x: x[1])[0] if equal_lows else None
            
            return strongest_high, strongest_low
        except Exception as e:
            logging.error(f"Error detecting equal levels: {e}")
            return None, None
    
    def _find_clusters(self, prices: np.ndarray, threshold: float) -> List[Tuple[float, int]]:
        try:
            clusters = []
            for price in prices:
                found = False
                for i, (level, count) in enumerate(clusters):
                    if abs(price - level) / level < threshold:
                        clusters[i] = (level + (price - level) * 0.1, count + 1)
                        found = True
                        break
                if not found:
                    clusters.append((price, 1))
            return [c for c in clusters if c[1] >= 2]
        except Exception as e:
            logging.error(f"Error finding clusters: {e}")
            return []
    
    def detect_session_levels(self, df: pd.DataFrame, session: SessionType) -> Tuple[Optional[float], Optional[float]]:
        try:
            df = df.copy()
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
        except Exception as e:
            logging.error(f"Error detecting session levels: {e}")
            return None, None
    
    def detect_sweep(self, df: pd.DataFrame, direction: str, level: float) -> bool:
        try:
            if len(df) < 3:
                return False
            
            recent = df.tail(3)
            if direction == "BUY":
                swept = (recent['low'].min() < level) and (recent['close'].iloc[-1] > level)
            else:
                swept = (recent['high'].max() > level) and (recent['close'].iloc[-1] < level)
            return swept
        except Exception as e:
            logging.error(f"Error detecting sweep: {e}")
            return False
    
    def get_nearest_liquidity(self, price: float, direction: str) -> Optional[LiquidityZone]:
        try:
            relevant = [z for z in self.zones if not z.is_swept]
            if direction == "BUY":
                targets = [z for z in relevant if z.level > price]
                return min(targets, key=lambda x: x.level) if targets else None
            else:
                targets = [z for z in relevant if z.level < price]
                return max(targets, key=lambda x: x.level) if targets else None
        except Exception as e:
            logging.error(f"Error getting nearest liquidity: {e}")
            return None

# ================= MARKET STRUCTURE DETECTOR =================

class StructureDetector:
    def __init__(self):
        self.swing_lookback = 5
    
    def find_swing_points(self, df: pd.DataFrame) -> Tuple[List[int], List[int]]:
        try:
            if len(df) < self.swing_lookback * 2 + 1:
                return [], []
            
            highs = df['high'].values
            lows = df['low'].values
            
            swing_highs = []
            swing_lows = []
            
            for i in range(self.swing_lookback, len(df) - self.swing_lookback):
                # Swing high
                is_swing_high = True
                for j in range(1, self.swing_lookback + 1):
                    if highs[i] <= highs[i-j] or highs[i] <= highs[i+j]:
                        is_swing_high = False
                        break
                if is_swing_high:
                    swing_highs.append(i)
                
                # Swing low
                is_swing_low = True
                for j in range(1, self.swing_lookback + 1):
                    if lows[i] >= lows[i-j] or lows[i] >= lows[i+j]:
                        is_swing_low = False
                        break
                if is_swing_low:
                    swing_lows.append(i)
            
            return swing_highs, swing_lows
        except Exception as e:
            logging.error(f"Error finding swing points: {e}")
            return [], []
    
    def detect_structure_break(self, df: pd.DataFrame, direction: str) -> Optional[MarketStructure]:
        try:
            swing_highs, swing_lows = self.find_swing_points(df)
            
            if len(swing_highs) < 2 or len(swing_lows) < 2:
                return None
            
            recent = df.tail(10)
            if len(recent) == 0:
                return None
            
            current_close = recent['close'].iloc[-1]
            
            if direction == "BUY":
                if swing_highs and current_close > df['high'].iloc[swing_highs[-1]]:
                    return MarketStructure(
                        type=StructureType.BOS_BULLISH,
                        swing_point=df['high'].iloc[swing_highs[-1]],
                        confirmation_price=current_close,
                        timestamp=df.index[-1],
                        timeframe="15min",
                        strength=8
                    )
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
        except Exception as e:
            logging.error(f"Error detecting structure break: {e}")
            return None
    
    def is_retest_valid(self, df: pd.DataFrame, structure: MarketStructure, direction: str) -> bool:
        try:
            if len(df) < 5:
                return False
            
            recent = df.tail(5)
            
            if direction == "BUY":
                touched = recent['low'].min() <= structure.swing_point * 1.001
                rejected = recent['close'].iloc[-1] > recent['open'].iloc[-1]
                above_structure = recent['close'].iloc[-1] > structure.swing_point
            else:
                touched = recent['high'].max() >= structure.swing_point * 0.999
                rejected = recent['close'].iloc[-1] < recent['open'].iloc[-1]
                above_structure = recent['close'].iloc[-1] < structure.swing_point
            
            return touched and rejected and above_structure
        except Exception as e:
            logging.error(f"Error checking retest: {e}")
            return False

# ================= MARKET REGIME CLASSIFIER =================

class RegimeClassifier:
    def __init__(self):
        self.trend_ema_fast = 20
        self.trend_ema_slow = 50
        self.bb_period = 20
    
    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        try:
            if len(df) < period + 10:
                return 0.0
            
            high = df['high']
            low = df['low']
            close = df['close']
            
            plus_dm = high.diff()
            minus_dm = -low.diff()
            
            plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
            minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
            
            tr1 = high - low
            tr2 = (high - close.shift()).abs()
            tr3 = (low - close.shift()).abs()
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            atr = tr.ewm(span=period, adjust=False).mean()
            plus_di = 100 * plus_dm.ewm(span=period, adjust=False).mean() / atr
            minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / atr
            
            dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
            adx = dx.ewm(span=period, adjust=False).mean()
            
            return float(adx.iloc[-1]) if not adx.empty else 0.0
        except Exception as e:
            logging.error(f"Error calculating ADX: {e}")
            return 0.0
    
    def calculate_volatility(self, df: pd.DataFrame) -> float:
        try:
            if len(df) < 20:
                return 0.0
            returns = df['close'].pct_change().dropna()
            if len(returns) == 0:
                return 0.0
            return float(returns.std() * np.sqrt(252))
        except Exception as e:
            logging.error(f"Error calculating volatility: {e}")
            return 0.0
    
    def detect_compression(self, df: pd.DataFrame) -> bool:
        try:
            if len(df) < self.bb_period + 20:
                return False
            
            sma = df['close'].rolling(self.bb_period).mean()
            std = df['close'].rolling(self.bb_period).std()
            upper = sma + (std * 2)
            lower = sma - (std * 2)
            
            bandwidth = (upper - lower) / sma
            current_width = bandwidth.iloc[-1]
            avg_width = bandwidth.rolling(20).mean().iloc[-1]
            
            return current_width < avg_width * 0.6
        except Exception as e:
            logging.error(f"Error detecting compression: {e}")
            return False
    
    def classify(self, df: pd.DataFrame, df_htf: pd.DataFrame) -> MarketRegime:
        try:
            if len(df) < 50 or len(df_htf) < 50:
                return MarketRegime.RANGING
            
            adx = self.calculate_adx(df)
            volatility = self.calculate_volatility(df)
            is_compression = self.detect_compression(df)
            
            ema_fast = df['close'].ewm(span=self.trend_ema_fast, adjust=False).mean().iloc[-1]
            ema_slow = df['close'].ewm(span=self.trend_ema_slow, adjust=False).mean().iloc[-1]
            
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
        except Exception as e:
            logging.error(f"Error classifying regime: {e}")
            return MarketRegime.RANGING

# ================= SESSION MANAGER =================

class SessionManager:
    def __init__(self):
        self.timezone = pytz.UTC
    
    def get_current_session(self) -> SessionType:
        try:
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
        except Exception as e:
            logging.error(f"Error getting session: {e}")
            return SessionType.OFF_HOURS
    
    def is_kill_zone(self) -> bool:
        try:
            now = datetime.now(self.timezone)
            hour = now.hour
            return SystemConfig.KILL_ZONE_OVERLAP[0] <= hour < SystemConfig.KILL_ZONE_OVERLAP[1]
        except Exception as e:
            logging.error(f"Error checking kill zone: {e}")
            return False
    
    def is_tradeable_session(self) -> bool:
        session = self.get_current_session()
        return session in [SessionType.LONDON, SessionType.NEW_YORK, SessionType.OVERLAP]
    
    def get_session_quality_score(self) -> int:
        if not self.is_tradeable_session():
            return 0
        
        session = self.get_current_session()
        if session == SessionType.OVERLAP:
            return 20
        elif session == SessionType.LONDON:
            return 15
        elif session == SessionType.NEW_YORK:
            return 12
        return 0

# ================= CONFIDENCE SCORING ENGINE =================

class ConfidenceScorer:
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
        
        components = {}
        
        if liquidity_sweep:
            components['liquidity_sweep'] = min(20, 10 + sweep_strength)
        else:
            components['liquidity_sweep'] = 0
        
        if structure:
            components['structure_break'] = min(20, 10 + structure.strength)
        else:
            components['structure_break'] = 0
        
        components['session_timing'] = session_score
        components['htf_bias'] = 15 if htf_aligned else 0
        
        if regime in [MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN]:
            components['volatility_regime'] = 10
        elif regime == MarketRegime.EXPANSION:
            components['volatility_regime'] = 8
        elif regime == MarketRegime.COMPRESSION:
            components['volatility_regime'] = 6
        else:
            components['volatility_regime'] = 2
        
        if rr_ratio >= 3.0:
            components['risk_reward'] = 10
        elif rr_ratio >= 2.5:
            components['risk_reward'] = 8
        elif rr_ratio >= 2.0:
            components['risk_reward'] = 6
        else:
            components['risk_reward'] = max(0, int(rr_ratio * 3))
        
        components['retest_quality'] = 10 if retest_valid else 0
        
        total_score = sum(components.values())
        return min(100, total_score), components

# ================= ADAPTIVE RISK MANAGER =================

class AdaptiveRiskManager:
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
        today = datetime.now().date()
        if today != self.last_reset:
            self.daily_risk_used = 0.0
            self.last_reset = today
    
    def calculate_position_size(self, 
                               entry: float, 
                               stop_loss: float, 
                               atr: float,
                               volatility: float) -> Tuple[float, float]:
        self.check_daily_reset()
        
        if self.daily_risk_used >= SystemConfig.MAX_DAILY_RISK:
            return 0.0, 0.0
        
        risk_pct = SystemConfig.BASE_RISK_PCT
        
        if self.defensive_mode:
            risk_pct *= 0.5
        
        vol_adjustment = 1.0
        if volatility > SystemConfig.VOLATILITY_HIGH:
            vol_adjustment = 0.7
        elif volatility < SystemConfig.VOLATILITY_LOW:
            vol_adjustment = 0.8
        
        if self.loss_streak > 0:
            streak_adjustment = max(0.3, 1.0 - (self.loss_streak * 0.2))
            risk_pct *= streak_adjustment
        
        adjusted_risk = risk_pct * vol_adjustment
        available_risk = min(adjusted_risk, SystemConfig.MAX_DAILY_RISK - self.daily_risk_used)
        
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
        try:
            recent = df.tail(5)
            
            if direction == "BUY":
                recent_low = recent['low'].min()
                structure_level = structure.swing_point if structure else recent_low
                sl_candidate = min(recent_low, structure_level) - (atr * 0.5)
                return sl_candidate
            else:
                recent_high = recent['high'].max()
                structure_level = structure.swing_point if structure else recent_high
                sl_candidate = max(recent_high, structure_level) + (atr * 0.5)
                return sl_candidate
        except Exception as e:
            logging.error(f"Error placing SL: {e}")
            # Fallback to ATR-based
            current = df['close'].iloc[-1]
            if direction == "BUY":
                return current - (atr * 1.5)
            else:
                return current + (atr * 1.5)

# ================= CORRELATION GUARD =================

class CorrelationGuard:
    def __init__(self):
        self.correlation_groups = {
            'usd_majors': ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD'],
            'usd_crosses': ['USD/JPY', 'USD/CHF', 'USD/CAD'],
            'euro_crosses': ['EUR/GBP', 'EUR/JPY', 'EUR/CHF'],
            'crypto': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
        }
        self.active_directions: Dict[str, str] = {}
    
    def can_trade(self, symbol: str, direction: str, active_trades: List[Trade]) -> bool:
        group = None
        for g_name, symbols in self.correlation_groups.items():
            if symbol in symbols:
                group = g_name
                break
        
        if not group:
            return True
        
        for trade in active_trades:
            if not trade.is_closed:
                trade_group = None
                for g_name, symbols in self.correlation_groups.items():
                    if trade.signal.symbol in symbols:
                        trade_group = g_name
                        break
                
                if trade_group == group:
                    if trade.signal.direction != direction:
                        return False
        
        return True
    
    def get_heat_score(self, active_trades: List[Trade]) -> float:
        if not active_trades:
            return 0.0
        
        total_risk = sum(t.signal.risk_reward for t in active_trades if not t.is_closed)
        return min(100, total_risk * 10)

# ================= EXECUTION ENGINE =================

class ExecutionEngine:
    def __init__(self):
        self.max_spread = SystemConfig.MAX_SPREAD_PCT
        self.max_slippage = SystemConfig.MAX_SLIPPAGE_PCT
    
    async def check_spread(self, symbol: str, fetcher: 'DataFetcher') -> Tuple[bool, float]:
        quote = await fetcher.get_quote(symbol)
        if not quote:
            return False, 999.0
        
        spread = (quote['ask'] - quote['bid']) / ((quote['ask'] + quote['bid']) / 2)
        return spread <= self.max_spread, spread
    
    def calculate_tp_levels(self, 
                           direction: str, 
                           entry: float, 
                           sl: float,
                           liquidity_map: LiquidityMap) -> Tuple[float, Optional[float]]:
        risk = abs(entry - sl)
        
        partial_tp = entry + risk if direction == "BUY" else entry - risk
        
        nearest_liq = liquidity_map.get_nearest_liquidity(entry, direction)
        if nearest_liq:
            runner_tp = nearest_liq.level
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
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._sender_task: Optional[asyncio.Task] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
        return self._session
    
    async def _message_sender(self):
        """Background task to send messages with rate limiting"""
        while True:
            try:
                message = await self._message_queue.get()
                await self._send_single_message(message)
                await asyncio.sleep(0.5)  # Rate limit: 2 msg/sec max
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Message sender error: {e}")
                await asyncio.sleep(1)
    
    async def start(self):
        """Start background sender"""
        if self._sender_task is None or self._sender_task.done():
            self._sender_task = asyncio.create_task(self._message_sender())
    
    async def stop(self):
        """Stop background sender"""
        if self._sender_task and not self._sender_task.done():
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
    
    async def send_message(self, message: str) -> bool:
        """Queue message for sending"""
        if not self.token or not self.chat_id:
            return False
        
        try:
            await self._message_queue.put(message)
            return True
        except Exception as e:
            logging.error(f"Failed to queue message: {e}")
            return False
    
    async def _send_single_message(self, message: str) -> bool:
        """Actually send message to Telegram"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message[:4000],  # Telegram limit
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with session.post(url, data=payload) as response:
                result = await response.json()
                if not result.get("ok"):
                    logging.error(f"Telegram API error: {result}")
                    return False
                return True
        except Exception as e:
            logging.error(f"Telegram send error: {e}")
            return False
    
    async def close(self):
        await self.stop()
        if self._session and not self._session.closed:
            await self._session.close()
    
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
        
        components_text = "\n".join(
            f"• {k.replace('_', ' ').title()}: {v}/20" 
            for k, v in signal.score_components.items() if v > 0
        )
        
        return f"""{emoji} <b>INSTITUTIONAL SETUP</b> {emoji}

<b>Quality:</b> <code>{signal.confidence_score}/100</code> [{confidence_bar}]
<b>Regime:</b> {regime_emoji} {signal.regime.value.replace('_', ' ').title()}
<b>Session:</b> {signal.session.value.upper()} {'🔥 KILL ZONE' if signal.kill_zone else ''}

<b>Symbol:</b> <code>{signal.symbol}</code> | <b>Dir:</b> {"🟢 LONG" if signal.direction == "BUY" else "🔴 SHORT"}
<b>Strategy:</b> {signal.structure_break.type.value.upper()}

<b>Levels:</b>
├ Entry: <code>{signal.entry_price:.5f}</code>
├ SL: <code>{signal.stop_loss:.5f}</code>
├ TP1: <code>{signal.take_profit:.5f}</code>
└ TP2: <code>{signal.take_profit_runner:.5f}</code> (1:{signal.risk_reward:.1f})

<b>Components:</b>
{components_text}

<i>Sweep: {signal.liquidity_sweep.type.value.replace('_', ' ').title()} @ {signal.liquidity_sweep.level:.5f}</i>
<code>{signal.id}</code>"""

    def format_daily_report(self, metrics: PerformanceMetrics, date: str) -> str:
        emoji = "👑" if metrics.net_profit > 0 else "⚠️" if metrics.net_profit > -metrics.initial_equity * 0.01 else "🛑"
        
        sessions_text = "\n".join(
            f"• {session}: {sum(trades)/len(trades)*100:.0f}% WR" 
            for session, trades in metrics.win_rate_by_session.items() if trades
        ) or "No session data"
        
        return f"""{emoji} <b>DAILY REPORT</b> {emoji} <i>{date}</i>

<b>Stats:</b> {metrics.total_trades} trades | {metrics.wins}W/{metrics.losses}L
<b>Win Rate:</b> {metrics.win_rate:.1f}%
<b>Net P&L:</b> ${metrics.net_profit:+.2f}
<b>Max DD:</b> {metrics.max_drawdown:.2%}

<b>Sessions:</b>
{sessions_text}

<i>Equity: ${metrics.initial_equity + metrics.net_profit:.2f}</i>"""

# ================= DATA FETCHER =================

class DataFetcher:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(SystemConfig.MAX_CONCURRENT_SCANS)
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._cache_ttl = timedelta(seconds=30)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5),
                headers={"Accept": "application/json"}
            )
        return self._session
    
    def _get_cache_key(self, symbol: str, interval: str) -> str:
        return f"{symbol}_{interval}"
    
    def _get_cached(self, key: str) -> Optional[pd.DataFrame]:
        if key in self._cache:
            df, timestamp = self._cache[key]
            if datetime.now() - timestamp < self._cache_ttl:
                return df
            del self._cache[key]
        return None
    
    async def fetch_time_series(self, 
                                symbol: str, 
                                interval: str, 
                                outputsize: int = 500) -> Optional[pd.DataFrame]:
        cache_key = self._get_cache_key(symbol, interval)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        async with self._semaphore:
            for attempt in range(SystemConfig.MAX_API_RETRIES):
                try:
                    session = await self._get_session()
                    url = f"{self.base_url}/time_series"
                    params = {
                        "symbol": symbol,
                        "interval": interval,
                        "outputsize": outputsize,
                        "apikey": self.api_key
                    }
                    
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status != 200:
                            if response.status == 429:  # Rate limited
                                await asyncio.sleep(SystemConfig.API_RETRY_DELAY * (attempt + 1))
                                continue
                            logging.warning(f"HTTP {response.status} for {symbol}")
                            return None
                        
                        data = await response.json()
                        
                        if "values" not in data or not data["values"]:
                            logging.warning(f"No data for {symbol}")
                            return None
                        
                        df = self._process_data(data["values"])
                        if df is not None and len(df) > 0:
                            self._cache[cache_key] = (df, datetime.now())
                        return df
                        
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout fetching {symbol}, attempt {attempt + 1}")
                    if attempt < SystemConfig.MAX_API_RETRIES - 1:
                        await asyncio.sleep(SystemConfig.API_RETRY_DELAY)
                except Exception as e:
                    logging.error(f"Fetch error {symbol}: {e}")
                    if attempt < SystemConfig.MAX_API_RETRIES - 1:
                        await asyncio.sleep(SystemConfig.API_RETRY_DELAY * (attempt + 1))
            
            return None
    
    async def get_quote(self, symbol: str) -> Optional[Dict]:
        async with self._semaphore:
            try:
                session = await self._get_session()
                url = f"{self.base_url}/quote"
                params = {"symbol": symbol, "apikey": self.api_key}
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'bid': float(data.get('bid', 0) or 0),
                            'ask': float(data.get('ask', 0) or 0),
                            'price': float(data.get('price', 0) or 0)
                        }
            except Exception as e:
                logging.error(f"Quote error for {symbol}: {e}")
            return None
    
    def _process_data(self, values: List[dict]) -> Optional[pd.DataFrame]:
        try:
            if not values:
                return None
            
            df = pd.DataFrame(values)
            
            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.set_index("datetime", inplace=True)
            df.sort_index(inplace=True)
            df.dropna(subset=["open", "high", "low", "close"], inplace=True)
            
            return df if len(df) > 0 else None
        except Exception as e:
            logging.error(f"Data processing error: {e}")
            return None
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# ================= PERFORMANCE OPTIMIZER =================

class PerformanceOptimizer:
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
        self._load_history()
    
    def _load_history(self):
        try:
            if self.history_file.exists():
                with open(self.history_file, 'r') as f:
                    self.trade_history = json.load(f)
                self._analyze_performance()
        except Exception as e:
            logging.error(f"Error loading history: {e}")
            self.trade_history = []
    
    def save_trade(self, trade: Trade):
        try:
            record = {
                'timestamp': trade.signal.timestamp.isoformat(),
                'symbol': trade.signal.symbol,
                'direction': trade.signal.direction,
                'session': trade.signal.session.value,
                'regime': trade.signal.regime.value,
                'confidence': trade.signal.confidence_score,
                'rr': trade.signal.risk_reward,
                'pnl': trade.total_pnl,
                'status': trade.status.value
            }
            self.trade_history.append(record)
            
            if len(self.trade_history) % 10 == 0:
                self._persist_history()
                self._analyze_performance()
        except Exception as e:
            logging.error(f"Error saving trade: {e}")
    
    def _persist_history(self):
        try:
            with open(self.history_file, 'w') as f:
                json.dump(self.trade_history[-1000:], f)
        except Exception as e:
            logging.error(f"Error persisting history: {e}")
    
    def _analyze_performance(self):
        try:
            if len(self.trade_history) < 20:
                return
            
            df = pd.DataFrame(self.trade_history)
            
            session_perf = df.groupby('session')['pnl'].mean()
            self.optimal_params['best_sessions'] = session_perf[session_perf > 0].index.tolist()
            
            regime_perf = df.groupby('regime')['pnl'].mean()
            self.optimal_params['best_regimes'] = regime_perf[regime_perf > 0].index.tolist()
        except Exception as e:
            logging.error(f"Error analyzing performance: {e}")
    
    def should_trade(self, signal: Signal) -> Tuple[bool, str]:
        if signal.regime.value in getattr(self, 'worst_regimes', []):
            return False, f"Avoiding {signal.regime.value}"
        
        if signal.regime == MarketRegime.RANGING and signal.confidence_score < 80:
            return False, "Low confidence range trade"
        
        return True, "Passed"

# ================= MAIN TRADING ENGINE =================

class InstitutionalTradingEngine:
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.fetcher = DataFetcher(TWELVEDATA_KEY)
        
        self.liquidity_map = LiquidityMap()
        self.structure_detector = StructureDetector()
        self.regime_classifier = RegimeClassifier()
        self.session_manager = SessionManager()
        self.confidence_scorer = ConfidenceScorer()
        self.risk_manager = AdaptiveRiskManager()
        self.correlation_guard = CorrelationGuard()
        self.execution_engine = ExecutionEngine()
        self.optimizer = PerformanceOptimizer()
        
        self.active_trades: List[Trade] = []
        self.metrics = PerformanceMetrics()
        self.watchlist = [
            "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD",
            "BTC/USDT", "ETH/USDT"
        ]
        self.running = False
        self._scan_lock = asyncio.Lock()
        self._last_scan_time = datetime.min
    
    def generate_signal_id(self, symbol: str) -> str:
        return f"{symbol.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    async def analyze_symbol(self, symbol: str) -> Optional[Signal]:
        try:
            if not self.session_manager.is_tradeable_session():
                return None
            
            # Fetch all timeframes concurrently
            tasks = [
                self.fetcher.fetch_time_series(symbol, SystemConfig.TF_EXECUTION, 500),
                self.fetcher.fetch_time_series(symbol, SystemConfig.TF_TREND, 300),
                self.fetcher.fetch_time_series(symbol, SystemConfig.TF_STRUCTURE, 200),
                self.fetcher.fetch_time_series(symbol, SystemConfig.TF_BIAS, 100)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            df_15m, df_1h, df_4h, df_1d = [r if not isinstance(r, Exception) else None for r in results]
            
            if any(df is None or len(df) < 50 for df in [df_15m, df_1h, df_4h, df_1d]):
                return None
            
            # Regime classification
            regime = self.regime_classifier.classify(df_15m, df_4h)
            if regime in [MarketRegime.LOW_VOLATILITY, MarketRegime.MANIPULATION]:
                logging.debug(f"{symbol}: Avoiding {regime.value}")
                return None
            
            # HTF bias
            htf_bias = self._determine_htf_bias(df_1h, df_4h, df_1d)
            
            # Liquidity analysis
            eq_high, eq_low = self.liquidity_map.detect_equal_levels(df_15m)
            session_high, session_low = self.liquidity_map.detect_session_levels(
                df_15m, self.session_manager.get_current_session()
            )
            
            # Determine direction from sweep
            direction = None
            sweep_level = None
            sweep_type = None
            sweep_strength = 0
            
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
            
            # Structure confirmation
            structure = self.structure_detector.detect_structure_break(df_15m, direction)
            if not structure:
                return None
            
            if not self.structure_detector.is_retest_valid(df_15m, structure, direction):
                return None
            
            # HTF alignment
            htf_aligned = self._check_htf_alignment(direction, htf_bias)
            
            # Spread check
            spread_ok, spread = await self.execution_engine.check_spread(symbol, self.fetcher)
            if not spread_ok:
                logging.debug(f"{symbol}: Spread too high ({spread:.4%})")
                return None
            
            # Calculate levels
            atr = self._calculate_atr(df_15m)
            entry = float(df_15m['close'].iloc[-1])
            sl = self.risk_manager.get_sl_placement(direction, df_15m, atr, structure)
            
            partial_tp, runner_tp = self.execution_engine.calculate_tp_levels(
                direction, entry, sl, self.liquidity_map
            )
            
            risk = abs(entry - sl)
            reward = abs(runner_tp - entry) if runner_tp else 0
            rr_ratio = reward / risk if risk > 0 else 0
            
            if rr_ratio < SystemConfig.MIN_RR_RATIO:
                return None
            
            # Confidence scoring
            session_score = self.session_manager.get_session_quality_score()
            confidence, components = self.confidence_scorer.calculate(
                True, sweep_strength, structure, session_score,
                htf_aligned, regime, rr_ratio, True
            )
            
            if confidence < SystemConfig.MIN_CONFIDENCE_SCORE:
                return None
            
            # Correlation check
            if not self.correlation_guard.can_trade(symbol, direction, self.active_trades):
                return None
            
            # Create signal
            liquidity_zone = LiquidityZone(
                level=sweep_level, type=sweep_type, strength=sweep_strength,
                timestamp=datetime.now(), is_swept=True, sweep_timestamp=datetime.now()
            )
            
            return Signal(
                id=self.generate_signal_id(symbol),
                symbol=symbol, direction=direction, confidence_score=confidence,
                entry_price=entry, stop_loss=sl, take_profit=partial_tp,
                take_profit_runner=runner_tp, atr_value=atr, risk_reward=rr_ratio,
                liquidity_sweep=liquidity_zone, structure_break=structure,
                session=self.session_manager.get_current_session(),
                regime=regime, htf_bias=htf_bias, volatility_regime="normal",
                spread_at_entry=spread, score_components=components,
                timestamp=datetime.now(), kill_zone=self.session_manager.is_kill_zone()
            )
            
        except Exception as e:
            logging.error(f"Error analyzing {symbol}: {e}\n{traceback.format_exc()}")
            return None
    
    def _determine_htf_bias(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame, df_1d: pd.DataFrame) -> str:
        try:
            def ema_trend(df: pd.DataFrame) -> str:
                if len(df) < 50:
                    return "neutral"
                ema20 = df['close'].ewm(span=20, adjust=False).mean().iloc[-1]
                ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
                return "bullish" if ema20 > ema50 else "bearish"
            
            t1h, t4h, t1d = ema_trend(df_1h), ema_trend(df_4h), ema_trend(df_1d)
            
            if t1d == t4h == t1h:
                return f"strong_{t1d}"
            elif t1d == t4h:
                return f"{t1d}_bias"
            return "mixed"
        except Exception as e:
            logging.error(f"HTF bias error: {e}")
            return "mixed"
    
    def _check_htf_alignment(self, direction: str, htf_bias: str) -> bool:
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
        try:
            if len(df) < period + 1:
                return 0.0
            
            high, low, close = df['high'], df['low'], df['close']
            
            tr1 = high - low
            tr2 = (high - close.shift()).abs()
            tr3 = (low - close.shift()).abs()
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.ewm(span=period, adjust=False).mean().iloc[-1]
            
            return float(atr)
        except Exception as e:
            logging.error(f"ATR calculation error: {e}")
            return 0.0
    
    async def execute_trade(self, signal: Signal) -> bool:
        try:
            df = await self.fetcher.fetch_time_series(signal.symbol, "15min", 100)
            volatility = self.regime_classifier.calculate_volatility(df) if df is not None else 0.01
            
            position_size, risk_pct = self.risk_manager.calculate_position_size(
                signal.entry_price, signal.stop_loss, signal.atr_value, volatility
            )
            
            if position_size <= 0:
                logging.warning("Risk limits reached")
                return False
            
            trade = Trade(signal=signal, position_size=position_size)
            self.active_trades.append(trade)
            
            await self.telegram.send_message(self.telegram.format_signal(signal))
            
            logging.info(f"Trade: {signal.symbol} {signal.direction} @ {signal.entry_price:.5f}")
            return True
            
        except Exception as e:
            logging.error(f"Execute error: {e}")
            return False
    
    async def manage_active_trades(self):
        try:
            for trade in list(self.active_trades):
                if trade.is_closed:
                    continue
                
                df = await self.fetcher.fetch_time_series(
                    trade.signal.symbol, SystemConfig.TF_EXECUTION, 50
                )
                if df is None or len(df) < 5:
                    continue
                
                current_price = float(df['close'].iloc[-1])
                high = float(df['high'].iloc[-1])
                low = float(df['low'].iloc[-1])
                trade.candles_held += 1
                
                # P&L calculation
                if trade.signal.direction == "BUY":
                    current_pnl = (current_price - trade.signal.entry_price) / \
                                 (trade.signal.entry_price - trade.signal.stop_loss)
                    hit_partial = high >= trade.signal.take_profit
                    hit_runner = high >= trade.signal.take_profit_runner if trade.signal.take_profit_runner else False
                    hit_sl = low <= trade.signal.stop_loss
                else:
                    current_pnl = (trade.signal.entry_price - current_price) / \
                                 (trade.signal.stop_loss - trade.signal.entry_price)
                    hit_partial = low <= trade.signal.take_profit
                    hit_runner = low <= trade.signal.take_profit_runner if trade.signal.take_profit_runner else False
                    hit_sl = high >= trade.signal.stop_loss
                
                trade.max_profit_reached = max(trade.max_profit_reached, current_pnl)
                trade.max_drawdown_reached = min(trade.max_drawdown_reached, current_pnl)
                
                # Partial TP
                if not trade.partial_closed and hit_partial:
                    trade.partial_closed = True
                    trade.partial_pnl = 1.0 * 0.5
                    await self.telegram.send_message(
                        f"🎯 <b>PARTIAL</b> {trade.signal.symbol} @ {trade.signal.take_profit:.5f}"
                    )
                
                # Runner TP
                if hit_runner:
                    runner_r = abs(trade.signal.take_profit_runner - trade.signal.entry_price) / \
                              abs(trade.signal.entry_price - trade.signal.stop_loss)
                    trade.runner_pnl = runner_r * 0.5
                    trade.status = TradeStatus.WIN
                    await self._close_trade(trade, trade.signal.take_profit_runner)
                    continue
                
                # SL
                if hit_sl:
                    trade.status = TradeStatus.LOSS
                    await self._close_trade(trade, trade.signal.stop_loss)
                    continue
                
                # Time exit
                max_candles = SystemConfig.MAX_TRADE_DURATION_MIN // 15
                if trade.candles_held >= max_candles:
                    if current_pnl > 0:
                        trade.status = TradeStatus.WIN
                        trade.runner_pnl = current_pnl * (0.5 if trade.partial_closed else 1.0)
                    else:
                        trade.status = TradeStatus.EXPIRED
                    await self._close_trade(trade, current_price)
                    
        except Exception as e:
            logging.error(f"Manage trades error: {e}")
    
    async def _close_trade(self, trade: Trade, exit_price: float):
        try:
            trade.exit_price = exit_price
            trade.exit_time = datetime.now()
            
            total_r = trade.total_pnl
            
            # Update metrics
            self.metrics.total_trades += 1
            if trade.status == TradeStatus.WIN:
                self.metrics.wins += 1
                self.metrics.gross_profit += total_r
                self.metrics.consecutive_losses = 0
            elif trade.status == TradeStatus.LOSS:
                self.metrics.losses += 1
                self.metrics.gross_loss -= abs(total_r)
                self.metrics.consecutive_losses += 1
            else:
                if total_r > 0:
                    self.metrics.wins += 1
                    self.metrics.gross_profit += total_r
                else:
                    self.metrics.losses += 1
                    self.metrics.gross_loss += abs(total_r)
            
            # Update risk manager
            pnl_dollars = total_r * trade.position_size * abs(trade.signal.entry_price - trade.signal.stop_loss)
            self.risk_manager.update_equity(pnl_dollars)
            
            # Update drawdown
            current_eq = self.metrics.initial_equity + self.metrics.net_profit
            peak_eq = self.metrics.initial_equity + max(0, self.metrics.net_profit)
            self.metrics.current_drawdown = (peak_eq - current_eq) / peak_eq if peak_eq > 0 else 0
            self.metrics.max_drawdown = max(self.metrics.max_drawdown, self.metrics.current_drawdown)
            
            # Session tracking
            session_key = trade.signal.session.value
            if session_key not in self.metrics.win_rate_by_session:
                self.metrics.win_rate_by_session[session_key] = []
            self.metrics.win_rate_by_session[session_key].append(trade.status == TradeStatus.WIN)
            
            # Save and cleanup
            self.optimizer.save_trade(trade)
            if trade in self.active_trades:
                self.active_trades.remove(trade)
            
            # Notify
            emoji = "✅" if trade.status == TradeStatus.WIN else "❌"
            await self.telegram.send_message(
                f"{emoji} <b>CLOSED</b> {trade.signal.symbol} {trade.status.value.upper()} | {total_r:+.2f}R"
            )
            
        except Exception as e:
            logging.error(f"Close trade error: {e}")
    
    async def run(self):
        self.running = True
        
        # Start telegram sender
        await self.telegram.start()
        
        await self.telegram.send_message(
            "🏛️ <b>ENGINE STARTED</b>\n"
            f"Watchlist: {len(self.watchlist)} pairs\n"
            f"Min Confidence: {SystemConfig.MIN_CONFIDENCE_SCORE}\n"
            f"Session: {self.session_manager.get_current_session().value}"
        )
        
        try:
            while self.running:
                try:
                    # Check if should trade
                    if not self.session_manager.is_tradeable_session():
                        await asyncio.sleep(300)
                        continue
                    
                    # Manage existing trades
                    await self.manage_active_trades()
                    
                    # Scan for new signals (with lock to prevent overlap)
                    async with self._scan_lock:
                        if datetime.now() - self._last_scan_time < timedelta(seconds=SystemConfig.SCAN_INTERVAL_SECONDS):
                            await asyncio.sleep(1)
                            continue
                        
                        self._last_scan_time = datetime.now()
                        
                        for symbol in self.watchlist:
                            # Skip checks
                            if self.correlation_guard.get_heat_score(self.active_trades) > 50:
                                break
                            
                            if any(t.signal.symbol == symbol and not t.is_closed for t in self.active_trades):
                                continue
                            
                            # Analyze with timeout
                            try:
                                signal = await asyncio.wait_for(
                                    self.analyze_symbol(symbol), 
                                    timeout=30.0
                                )
                                if signal:
                                    await self.execute_trade(signal)
                            except asyncio.TimeoutError:
                                logging.warning(f"Timeout analyzing {symbol}")
                            except Exception as e:
                                logging.error(f"Scan error {symbol}: {e}")
                            
                            await asyncio.sleep(0.5)  # Small delay between symbols
                    
                    # Daily report
                    now = datetime.now()
                    if now.hour == 21 and now.minute == 0:
                        await self._send_daily_report()
                        await asyncio.sleep(60)  # Prevent duplicate reports
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Main loop error: {e}\n{traceback.format_exc()}")
                    await asyncio.sleep(10)
                    
        finally:
            await self.shutdown()
    
    async def _send_daily_report(self):
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            msg = self.telegram.format_daily_report(self.metrics, today)
            await self.telegram.send_message(msg)
        except Exception as e:
            logging.error(f"Daily report error: {e}")
    
    async def shutdown(self):
        logging.info("Shutting down...")
        self.running = False
        await self.telegram.stop()
        await self.fetcher.close()

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
        logging.error(f"Missing: {', '.join(missing)}")
        exit(1)
    
    engine = InstitutionalTradingEngine()
    
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.error(f"Fatal: {e}\n{traceback.format_exc()}")
