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
from typing import Optional, Tuple, List, Dict, Literal, Set, Callable
from datetime import datetime, timedelta, time
from enum import Enum, auto
from pathlib import Path
from collections import deque, defaultdict
import pytz
from abc import ABC, abstractmethod
import traceback

# ================= CONFIGURATION =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")

PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# ================= SYSTEM PARAMETERS =================

class SystemConfig:
    LONDON_START = 7
    LONDON_END = 10
    NY_START = 13
    NY_END = 16
    MIN_CONFIDENCE_SCORE = 70
    MIN_RR_RATIO = 2.0
    BASE_RISK_PCT = 0.01
    MAX_DAILY_RISK = 0.03
    SCAN_INTERVAL_SECONDS = 60
    MAX_CONCURRENT_SCANS = 3

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

class StrategyType(Enum):
    LIQUIDITY_SWEEP = "liquidity_sweep"
    BREAKOUT_RETEST = "breakout_retest"
    ORDER_BLOCK = "order_block"
    KILL_ZONE_MOMENTUM = "kill_zone_momentum"
    NONE = "none"

class TradeStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    WIN = "win"
    LOSS = "loss"
    EXPIRED = "expired"

# ================= DATA CLASSES =================

@dataclass
class LiquidityZone:
    level: float
    type: LiquidityType
    strength: int
    timestamp: datetime
    is_swept: bool = False

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
    strategy_type: StrategyType
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
    score_components: Dict[str, float]
    timestamp: datetime
    kill_zone: bool = False

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
    candles_held: int = 0
    
    @property
    def total_pnl(self) -> float:
        return self.partial_pnl + self.runner_pnl
    
    @property
    def is_closed(self) -> bool:
        return self.status in [TradeStatus.WIN, TradeStatus.LOSS, TradeStatus.EXPIRED]

# ================= LIQUIDITY MAP =================

class LiquidityMap:
    def __init__(self, lookback: int = 100):
        self.lookback = lookback
        self.equal_threshold = 0.0005
        self.session_levels: Dict[str, Dict] = {}
    
    def detect_equal_levels(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
        try:
            highs = df['high'].values
            lows = df['low'].values
            
            # Find clusters
            high_clusters = []
            for price in highs:
                found = False
                for i, (level, count) in enumerate(high_clusters):
                    if abs(price - level) / level < self.equal_threshold:
                        high_clusters[i] = (level * 0.7 + price * 0.3, count + 1)
                        found = True
                        break
                if not found:
                    high_clusters.append((price, 1))
            
            low_clusters = []
            for price in lows:
                found = False
                for i, (level, count) in enumerate(low_clusters):
                    if abs(price - level) / level < self.equal_threshold:
                        low_clusters[i] = (level * 0.7 + price * 0.3, count + 1)
                        found = True
                        break
                if not found:
                    low_clusters.append((price, 1))
            
            # Filter for minimum 2 touches
            strong_highs = [c for c in high_clusters if c[1] >= 2]
            strong_lows = [c for c in low_clusters if c[1] >= 2]
            
            strongest_high = max(strong_highs, key=lambda x: x[1])[0] if strong_highs else None
            strongest_low = max(strong_lows, key=lambda x: x[1])[0] if strong_lows else None
            
            return strongest_high, strongest_low
        except Exception as e:
            logging.error(f"Error detecting equal levels: {e}")
            return None, None
    
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
            
            return float(session_data['high'].max()), float(session_data['low'].min())
        except Exception as e:
            logging.error(f"Error detecting session levels: {e}")
            return None, None
    
    def detect_sweep(self, df: pd.DataFrame, direction: str, level: float) -> Tuple[bool, float]:
        try:
            if len(df) < 3:
                return False, 0.0
            
            recent = df.tail(3)
            current = recent.iloc[-1]
            
            if direction == "BUY":
                wick_low = float(recent['low'].min())
                swept = wick_low < level and float(current['close']) > level
                
                if swept:
                    wick_depth = (level - wick_low) / level
                    quality = min(10, wick_depth * 1000)
                    return True, quality
            else:
                wick_high = float(recent['high'].max())
                swept = wick_high > level and float(current['close']) < level
                
                if swept:
                    wick_depth = (wick_high - level) / level
                    quality = min(10, wick_depth * 1000)
                    return True, quality
            
            return False, 0.0
        except Exception as e:
            logging.error(f"Error detecting sweep: {e}")
            return False, 0.0
    
    def get_optimal_tp(self, symbol: str, direction: str, entry: float, min_rr: float = 2.0) -> Optional[float]:
        try:
            if symbol not in self.session_levels:
                return None
            
            levels = self.session_levels[symbol]
            targets = []
            
            if direction == "BUY":
                for key in ['equal_highs', 'session_high', 'pdh']:
                    level = levels.get(key)
                    if level and level > entry * 1.001:
                        rr = (level - entry) / (entry * 0.001)  # Simplified risk calc
                        if rr >= min_rr:
                            targets.append((level, rr))
                
                return min(targets, key=lambda x: x[1])[0] if targets else None
            else:
                for key in ['equal_lows', 'session_low', 'pdl']:
                    level = levels.get(key)
                    if level and level < entry * 0.999:
                        rr = (entry - level) / (entry * 0.001)
                        if rr >= min_rr:
                            targets.append((level, rr))
                
                return max(targets, key=lambda x: x[1])[0] if targets else None
        except Exception as e:
            logging.error(f"Error getting TP: {e}")
            return None

# ================= STRUCTURE DETECTOR =================

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
            
            current_close = float(recent['close'].iloc[-1])
            
            if direction == "BUY":
                if swing_highs and current_close > float(df['high'].iloc[swing_highs[-1]]):
                    return MarketStructure(
                        type=StructureType.BOS_BULLISH,
                        swing_point=float(df['high'].iloc[swing_highs[-1]]),
                        confirmation_price=current_close,
                        timestamp=df.index[-1],
                        timeframe="15min",
                        strength=8
                    )
                elif len(swing_lows) >= 2:
                    if float(df['low'].iloc[swing_lows[-1]]) > float(df['low'].iloc[swing_lows[-2]]):
                        return MarketStructure(
                            type=StructureType.CHOCH_BULLISH,
                            swing_point=float(df['low'].iloc[swing_lows[-1]]),
                            confirmation_price=current_close,
                            timestamp=df.index[-1],
                            timeframe="15min",
                            strength=7
                        )
            else:
                if swing_lows and current_close < float(df['low'].iloc[swing_lows[-1]]):
                    return MarketStructure(
                        type=StructureType.BOS_BEARISH,
                        swing_point=float(df['low'].iloc[swing_lows[-1]]),
                        confirmation_price=current_close,
                        timestamp=df.index[-1],
                        timeframe="15min",
                        strength=8
                    )
                elif len(swing_highs) >= 2:
                    if float(df['high'].iloc[swing_highs[-1]]) < float(df['high'].iloc[swing_highs[-2]]):
                        return MarketStructure(
                            type=StructureType.CHOCH_BEARISH,
                            swing_point=float(df['high'].iloc[swing_highs[-1]]),
                            confirmation_price=current_close,
                            timestamp=df.index[-1],
                            timeframe="15min",
                            strength=7
                        )
            
            return None
        except Exception as e:
            logging.error(f"Error detecting structure: {e}")
            return None
    
    def is_retest_valid(self, df: pd.DataFrame, structure: MarketStructure, direction: str) -> Tuple[bool, float]:
        try:
            if len(df) < 5:
                return False, 0.0
            
            recent = df.tail(5)
            current = recent.iloc[-1]
            
            if direction == "BUY":
                touched = float(recent['low'].min()) <= structure.swing_point * 1.001
                if not touched:
                    return False, 0.0
                
                bullish_close = float(current['close']) > float(current['open'])
                above_structure = float(current['close']) > structure.swing_point
                
                if not (bullish_close and above_structure):
                    return False, 0.0
                
                quality = 5.0
                rejection_wick = float(current['close']) - float(current['low'])
                body = abs(float(current['close']) - float(current['open']))
                if rejection_wick > body * 1.5:
                    quality += 3
                if structure.strength >= 7:
                    quality += 2
                
                return True, min(10, quality)
            else:
                touched = float(recent['high'].max()) >= structure.swing_point * 0.999
                if not touched:
                    return False, 0.0
                
                bearish_close = float(current['close']) < float(current['open'])
                below_structure = float(current['close']) < structure.swing_point
                
                if not (bearish_close and below_structure):
                    return False, 0.0
                
                quality = 5.0
                rejection_wick = float(current['high']) - float(current['close'])
                body = abs(float(current['close']) - float(current['open']))
                if rejection_wick > body * 1.5:
                    quality += 3
                if structure.strength >= 7:
                    quality += 2
                
                return True, min(10, quality)
        except Exception as e:
            logging.error(f"Error checking retest: {e}")
            return False, 0.0

# ================= REGIME CLASSIFIER =================

class RegimeClassifier:
    def __init__(self):
        self.trend_ema_fast = 20
        self.trend_ema_slow = 50
    
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
            logging.error(f"ADX error: {e}")
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
            return 0.0
    
    def classify(self, df: pd.DataFrame, df_htf: pd.DataFrame) -> Tuple[MarketRegime, Dict]:
        try:
            if len(df) < 50 or len(df_htf) < 50:
                return MarketRegime.RANGING, {}
            
            adx = self.calculate_adx(df)
            volatility = self.calculate_volatility(df)
            
            ema_fast = float(df['close'].ewm(span=self.trend_ema_fast, adjust=False).mean().iloc[-1])
            ema_slow = float(df['close'].ewm(span=self.trend_ema_slow, adjust=False).mean().iloc[-1])
            
            ema_fast_htf = float(df_htf['close'].ewm(span=self.trend_ema_fast, adjust=False).mean().iloc[-1])
            ema_slow_htf = float(df_htf['close'].ewm(span=self.trend_ema_slow, adjust=False).mean().iloc[-1])
            
            trend_up = ema_fast > ema_slow and ema_fast_htf > ema_slow_htf
            trend_down = ema_fast < ema_slow and ema_fast_htf < ema_slow_htf
            
            metadata = {
                'adx': adx,
                'volatility': volatility,
                'ema_fast': ema_fast,
                'ema_slow': ema_slow
            }
            
            if adx < 15:
                if volatility < 0.005:
                    return MarketRegime.LOW_VOLATILITY, metadata
                return MarketRegime.RANGING, metadata
            
            if adx > 40 and volatility > 0.02:
                return MarketRegime.EXPANSION, metadata
            
            if adx > 25:
                if trend_up:
                    return MarketRegime.TRENDING_UP, metadata
                elif trend_down:
                    return MarketRegime.TRENDING_DOWN, metadata
            
            if volatility > 0.03 and adx < 20:
                return MarketRegime.MANIPULATION, metadata
            
            return MarketRegime.RANGING, metadata
            
        except Exception as e:
            logging.error(f"Classification error: {e}")
            return MarketRegime.RANGING, {}

# ================= SESSION MANAGER =================

class SessionManager:
    def __init__(self):
        self.timezone = pytz.UTC
    
    def get_current_session(self) -> SessionType:
        try:
            now = datetime.now(self.timezone)
            hour = now.hour
            
            london = 7 <= hour < 10
            ny = 13 <= hour < 16
            
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
            logging.error(f"Session error: {e}")
            return SessionType.OFF_HOURS
    
    def is_kill_zone(self) -> bool:
        try:
            now = datetime.now(self.timezone)
            return 14 <= now.hour < 16
        except:
            return False
    
    def is_tradeable_session(self) -> bool:
        return self.get_current_session() in [SessionType.LONDON, SessionType.NEW_YORK, SessionType.OVERLAP]
    
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

# ================= CONFIDENCE SCORER =================

class ConfidenceScorer:
    def __init__(self):
        self.weights = {
            'liquidity_sweep': 15,
            'sweep_quality': 10,
            'structure_break': 15,
            'retest_quality': 10,
            'session_timing': 10,
            'kill_zone': 10,
            'htf_alignment': 10,
            'regime_quality': 10,
            'risk_reward': 10
        }
    
    def calculate(self, 
                  sweep_detected: bool,
                  sweep_quality: float,
                  structure: MarketStructure,
                  retest_valid: bool,
                  retest_quality: float,
                  session_score: int,
                  kill_zone: bool,
                  htf_aligned: bool,
                  regime: MarketRegime,
                  rr_ratio: float) -> Tuple[int, Dict[str, float]]:
        
        components = {}
        
        if sweep_detected:
            components['liquidity_sweep'] = 15
            components['sweep_quality'] = min(10, sweep_quality)
        else:
            components['liquidity_sweep'] = 0
            components['sweep_quality'] = 0
        
        if structure:
            components['structure_break'] = 15 if structure.type in [StructureType.BOS_BULLISH, StructureType.BOS_BEARISH] else 12
            components['retest_quality'] = retest_quality if retest_valid else 0
        else:
            components['structure_break'] = 0
            components['retest_quality'] = 0
        
        components['session_timing'] = session_score / 2
        components['kill_zone'] = 10 if kill_zone else 0
        components['htf_alignment'] = 10 if htf_aligned else 0
        
        regime_scores = {
            MarketRegime.TRENDING_UP: 10,
            MarketRegime.TRENDING_DOWN: 10,
            MarketRegime.EXPANSION: 8,
            MarketRegime.COMPRESSION: 6,
            MarketRegime.RANGING: 3,
            MarketRegime.LOW_VOLATILITY: 1,
            MarketRegime.MANIPULATION: 0
        }
        components['regime_quality'] = regime_scores.get(regime, 0)
        
        if rr_ratio >= 3.0:
            components['risk_reward'] = 10
        elif rr_ratio >= 2.5:
            components['risk_reward'] = 8
        elif rr_ratio >= 2.0:
            components['risk_reward'] = 6
        else:
            components['risk_reward'] = max(0, rr_ratio * 3)
        
        total = sum(components.values())
        return min(100, int(total)), components

# ================= STRATEGY SELECTOR =================

class StrategySelector:
    def get_optimal_strategy(self, regime: MarketRegime, kill_zone: bool) -> List[StrategyType]:
        if kill_zone and regime in [MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN]:
            return [StrategyType.KILL_ZONE_MOMENTUM, StrategyType.LIQUIDITY_SWEEP]
        
        strategies = {
            MarketRegime.TRENDING_UP: [StrategyType.LIQUIDITY_SWEEP, StrategyType.BREAKOUT_RETEST],
            MarketRegime.TRENDING_DOWN: [StrategyType.LIQUIDITY_SWEEP, StrategyType.BREAKOUT_RETEST],
            MarketRegime.RANGING: [StrategyType.ORDER_BLOCK],
            MarketRegime.COMPRESSION: [StrategyType.ORDER_BLOCK],
            MarketRegime.EXPANSION: [StrategyType.KILL_ZONE_MOMENTUM],
            MarketRegime.LOW_VOLATILITY: [],
            MarketRegime.MANIPULATION: []
        }
        
        return strategies.get(regime, [StrategyType.NONE])

# ================= RISK MANAGER =================

class AdaptiveRiskManager:
    def __init__(self, initial_equity: float = 10000.0):
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.peak_equity = initial_equity
        self.daily_risk_used = 0.0
        self.last_reset = datetime.now().date()
        self.defensive_mode = False
        self.loss_streak = 0
    
    def update_equity(self, pnl: float):
        self.current_equity += pnl
        self.peak_equity = max(self.peak_equity, self.current_equity)
        
        drawdown = (self.peak_equity - self.current_equity) / self.peak_equity
        if drawdown > 0.05:
            self.defensive_mode = True
        
        if pnl < 0:
            self.loss_streak += 1
            if self.loss_streak >= 3:
                self.defensive_mode = True
        else:
            self.loss_streak = max(0, self.loss_streak - 1)
            if self.loss_streak == 0 and drawdown < 0.025:
                self.defensive_mode = False
    
    def check_daily_reset(self):
        today = datetime.now().date()
        if today != self.last_reset:
            self.daily_risk_used = 0.0
            self.last_reset = today
    
    def calculate_position_size(self, entry: float, stop_loss: float, atr: float, volatility: float) -> Tuple[float, float]:
        self.check_daily_reset()
        
        if self.daily_risk_used >= 0.03:
            return 0.0, 0.0
        
        risk_pct = 0.01
        if self.defensive_mode:
            risk_pct *= 0.5
        
        if volatility > 0.02:
            risk_pct *= 0.7
        
        if self.loss_streak > 0:
            risk_pct *= max(0.3, 1.0 - (self.loss_streak * 0.2))
        
        available_risk = min(risk_pct, 0.03 - self.daily_risk_used)
        if available_risk <= 0:
            return 0.0, 0.0
        
        risk_amount = self.current_equity * available_risk
        risk_per_unit = abs(entry - stop_loss)
        
        if risk_per_unit == 0:
            return 0.0, 0.0
        
        position_size = risk_amount / risk_per_unit
        self.daily_risk_used += available_risk
        
        return position_size, available_risk
    
    def get_sl_placement(self, direction: str, df: pd.DataFrame, atr: float, structure: MarketStructure) -> float:
        try:
            recent = df.tail(5)
            current = float(df['close'].iloc[-1])
            
            if direction == "BUY":
                swing_low = float(recent['low'].min())
                structure_level = structure.swing_point if structure else swing_low
                lowest = min(swing_low, structure_level)
                return lowest - (atr * 0.5)
            else:
                swing_high = float(recent['high'].max())
                structure_level = structure.swing_point if structure else swing_high
                highest = max(swing_high, structure_level)
                return highest + (atr * 0.5)
        except Exception as e:
            current = float(df['close'].iloc[-1])
            if direction == "BUY":
                return current - (atr * 1.5)
            else:
                return current + (atr * 1.5)

# ================= CORRELATION GUARD =================

class CorrelationGuard:
    def __init__(self):
        self.groups = {
            'usd_majors': ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD'],
            'usd_crosses': ['USD/JPY', 'USD/CHF', 'USD/CAD'],
            'crypto': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
        }
    
    def can_trade(self, symbol: str, direction: str, active_trades: List[Trade]) -> bool:
        group = None
        for g_name, symbols in self.groups.items():
            if symbol in symbols:
                group = g_name
                break
        
        if not group:
            return True
        
        same_group = 0
        for trade in active_trades:
            if trade.is_closed:
                continue
            
            trade_group = None
            for g_name, symbols in self.groups.items():
                if trade.signal.symbol in symbols:
                    trade_group = g_name
                    break
            
            if trade_group == group:
                same_group += 1
                if trade.signal.direction != direction:
                    return False
        
        return same_group < 2
    
    def get_heat_score(self, active_trades: List[Trade]) -> float:
        if not active_trades:
            return 0.0
        total_risk = sum(t.signal.risk_reward for t in active_trades if not t.is_closed)
        return min(100, total_risk * 8)

# ================= EXECUTION ENGINE =================

class ExecutionEngine:
    def __init__(self):
        self.max_spread = 0.0005
    
    async def check_spread(self, symbol: str, fetcher: 'DataFetcher') -> Tuple[bool, float]:
        quote = await fetcher.get_quote(symbol)
        if not quote:
            return False, 999.0
        
        mid = (quote['ask'] + quote['bid']) / 2
        spread = (quote['ask'] - quote['bid']) / mid
        return spread <= self.max_spread, spread
    
    def calculate_tp_levels(self, direction: str, entry: float, sl: float, liquidity_map: LiquidityMap, symbol: str) -> Tuple[float, float]:
        risk = abs(entry - sl)
        
        tp1 = entry + risk if direction == "BUY" else entry - risk
        
        tp2_liq = liquidity_map.get_optimal_tp(symbol, direction, entry, 2.0)
        tp2_rr = entry + (risk * 3) if direction == "BUY" else entry - (risk * 3)
        tp2 = tp2_liq if tp2_liq else tp2_rr
        
        return tp1, tp2

# ================= DATA FETCHER =================

class DataFetcher:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(3)
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
        return self._session
    
    def _get_cache_key(self, symbol: str, interval: str) -> str:
        return f"{symbol}_{interval}"
    
    def _get_cached(self, key: str) -> Optional[pd.DataFrame]:
        if key in self._cache:
            df, timestamp = self._cache[key]
            if datetime.now() - timestamp < timedelta(seconds=30):
                return df
            del self._cache[key]
        return None
    
    async def fetch_time_series(self, symbol: str, interval: str, outputsize: int = 500) -> Optional[pd.DataFrame]:
        cache_key = self._get_cache_key(symbol, interval)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        async with self._semaphore:
            for attempt in range(3):
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
                            if response.status == 429:
                                await asyncio.sleep(5 * (attempt + 1))
                                continue
                            return None
                        
                        data = await response.json()
                        if "values" not in data or not data["values"]:
                            return None
                        
                        df = self._process_data(data["values"])
                        if df is not None and len(df) > 0:
                            self._cache[cache_key] = (df, datetime.now())
                        return df
                        
                except asyncio.TimeoutError:
                    if attempt < 2:
                        await asyncio.sleep(5)
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(5 * (attempt + 1))
            
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
                logging.error(f"Quote error: {e}")
            return None
    
    def _process_data(self, values: List[dict]) -> Optional[pd.DataFrame]:
        try:
            if not values:
                return None
            
            df = pd.DataFrame(values)
            
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.set_index("datetime", inplace=True)
            df.sort_index(inplace=True)
            df.dropna(subset=["open", "high", "low", "close"], inplace=True)
            
            return df if len(df) > 0 else None
        except Exception as e:
            logging.error(f"Process error: {e}")
            return None
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# ================= TELEGRAM CLIENT =================

class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=5)
            )
        return self._session
    
    async def _sender(self):
        while True:
            try:
                msg = await self._queue.get()
                await self._send(msg)
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Sender error: {e}")
                await asyncio.sleep(1)
    
    async def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._sender())
    
    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def send_message(self, message: str) -> bool:
        if not self.token or not self.chat_id:
            return False
        try:
            await self._queue.put(message)
            return True
        except Exception as e:
            logging.error(f"Queue error: {e}")
            return False
    
    async def _send(self, message: str):
        try:
            session = await self._get_session()
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message[:4000],
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with session.post(url, data=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                result = await response.json()
                if not result.get("ok"):
                    logging.error(f"Telegram error: {result}")
        except Exception as e:
            logging.error(f"Send error: {e}")
    
    async def close(self):
        await self.stop()
        if self._session and not self._session.closed:
            await self._session.close()
    
    def format_signal(self, signal: Signal) -> str:
        emoji = "🔥" if signal.kill_zone else "⚡"
        dir_emoji = "🟢" if signal.direction == "BUY" else "🔴"
        
        regime_emoji = {
            MarketRegime.TRENDING_UP: "📈",
            MarketRegime.TRENDING_DOWN: "📉",
            MarketRegime.RANGING: "↔️",
            MarketRegime.COMPRESSION: "⏳",
            MarketRegime.EXPANSION: "💥",
            MarketRegime.LOW_VOLATILITY: "😴",
            MarketRegime.MANIPULATION: "⚠️"
        }.get(signal.regime, "⚪")
        
        conf_bar = "█" * (signal.confidence_score // 10) + "░" * (10 - signal.confidence_score // 10)
        
        comp_lines = [f"  {k.replace('_', ' ').title()}: {v:.0f}" for k, v in signal.score_components.items() if v > 0]
        
        return f"""{emoji} <b>SETUP</b> {dir_emoji}

<b>Score:</b> <code>{signal.confidence_score}/100</code> [{conf_bar}]
<b>Strategy:</b> {signal.strategy_type.value.replace('_', ' ').title()}
<b>Regime:</b> {regime_emoji} {signal.regime.value.replace('_', ' ').title()}
<b>Session:</b> {signal.session.value.upper()} {'🔥 KILL ZONE' if signal.kill_zone else ''}

<b>{signal.symbol}</b> | {signal.direction}

<b>Levels:</b>
├ Entry: <code>{signal.entry_price:.5f}</code>
├ SL:    <code>{signal.stop_loss:.5f}</code>
├ TP1:   <code>{signal.take_profit:.5f}</code>
└ TP2:   <code>{signal.take_profit_runner:.5f}</code> (1:{signal.risk_reward:.1f})

<b>Components:</b>
{chr(10).join(comp_lines) if comp_lines else '  None'}

<i>{signal.liquidity_sweep.type.value.replace('_', ' ').title()} @ {signal.liquidity_sweep.level:.5f}</i>
<code>{signal.id}</code>"""

# ================= MAIN ENGINE =================

class InstitutionalTradingEngine:
    def __init__(self):
        self.telegram = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.fetcher = DataFetcher(TWELVEDATA_KEY)
        
        self.liquidity_map = LiquidityMap()
        self.structure_detector = StructureDetector()
        self.regime_classifier = RegimeClassifier()
        self.session_manager = SessionManager()
        self.confidence_scorer = ConfidenceScorer()
        self.strategy_selector = StrategySelector()
        self.risk_manager = AdaptiveRiskManager()
        self.correlation_guard = CorrelationGuard()
        self.execution_engine = ExecutionEngine()
        
        self.active_trades: List[Trade] = []
        self.watchlist = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "BTC/USDT", "ETH/USDT"]
        self.running = False
        self._scan_lock = asyncio.Lock()
        self._last_scan = datetime.min
    
    def generate_id(self, symbol: str) -> str:
        return f"{symbol.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    async def analyze_symbol(self, symbol: str) -> Optional[Signal]:
        try:
            if not self.session_manager.is_tradeable_session():
                return None
            
            # Fetch data concurrently
            tasks = [
                self.fetcher.fetch_time_series(symbol, "15min", 500),
                self.fetcher.fetch_time_series(symbol, "1h", 300),
                self.fetcher.fetch_time_series(symbol, "4h", 200)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            df_15m, df_1h, df_4h = [r if not isinstance(r, Exception) else None for r in results]
            
            if any(df is None or len(df) < 50 for df in [df_15m, df_1h, df_4h]):
                return None
            
            # Regime classification
            regime, metadata = self.regime_classifier.classify(df_15m, df_4h)
            if regime in [MarketRegime.LOW_VOLATILITY, MarketRegime.MANIPULATION]:
                return None
            
            # HTF bias
            htf_bias = self._get_htf_bias(df_1h, df_4h)
            
            # Liquidity detection
            eq_high, eq_low = self.liquidity_map.detect_equal_levels(df_15m)
            session_high, session_low = self.liquidity_map.detect_session_levels(
                df_15m, self.session_manager.get_current_session()
            )
            
            # Store levels
            self.liquidity_map.session_levels[symbol] = {
                'equal_highs': eq_high, 'equal_lows': eq_low,
                'session_high': session_high, 'session_low': session_low
            }
            
            # Determine direction from sweep
            direction = None
            sweep_level = None
            sweep_type = None
            sweep_quality = 0
            
            if eq_high:
                swept, quality = self.liquidity_map.detect_sweep(df_15m, "SELL", eq_high)
                if swept:
                    direction = "SELL"
                    sweep_level = eq_high
                    sweep_type = LiquidityType.EQUAL_HIGHS
                    sweep_quality = quality
            
            if not direction and eq_low:
                swept, quality = self.liquidity_map.detect_sweep(df_15m, "BUY", eq_low)
                if swept:
                    direction = "BUY"
                    sweep_level = eq_low
                    sweep_type = LiquidityType.EQUAL_LOWS
                    sweep_quality = quality
            
            if not direction:
                return None
            
            # Structure confirmation
            structure = self.structure_detector.detect_structure_break(df_15m, direction)
            if not structure:
                return None
            
            # Retest validation
            retest_valid, retest_quality = self.structure_detector.is_retest_valid(df_15m, structure, direction)
            if not retest_valid:
                return None
            
            # HTF alignment
            htf_aligned = self._check_htf_alignment(direction, htf_bias)
            
            # Spread check
            spread_ok, spread = await self.execution_engine.check_spread(symbol, self.fetcher)
            if not spread_ok:
                return None
            
            # Calculate levels
            atr = self._calculate_atr(df_15m)
            entry = float(df_15m['close'].iloc[-1])
            sl = self.risk_manager.get_sl_placement(direction, df_15m, atr, structure)
            
            tp1, tp2 = self.execution_engine.calculate_tp_levels(direction, entry, sl, self.liquidity_map, symbol)
            
            risk = abs(entry - sl)
            rr = abs(tp2 - entry) / risk if risk > 0 else 0
            
            if rr < SystemConfig.MIN_RR_RATIO:
                return None
            
            # Strategy selection
            kill_zone = self.session_manager.is_kill_zone()
            strategies = self.strategy_selector.get_optimal_strategy(regime, kill_zone)
            strategy = strategies[0] if strategies else StrategyType.NONE
            
            # Confidence scoring
            session_score = self.session_manager.get_session_quality_score()
            confidence, components = self.confidence_scorer.calculate(
                True, sweep_quality, structure, retest_valid, retest_quality,
                session_score, kill_zone, htf_aligned, regime, rr
            )
            
            if confidence < SystemConfig.MIN_CONFIDENCE_SCORE:
                return None
            
            # Correlation check
            if not self.correlation_guard.can_trade(symbol, direction, self.active_trades):
                return None
            
            # Create signal
            liquidity_zone = LiquidityZone(
                level=sweep_level, type=sweep_type, strength=int(sweep_quality),
                timestamp=datetime.now(), is_swept=True
            )
            
            return Signal(
                id=self.generate_id(symbol),
                symbol=symbol,
                direction=direction,
                confidence_score=confidence,
                strategy_type=strategy,
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp1,
                take_profit_runner=tp2,
                atr_value=atr,
                risk_reward=rr,
                liquidity_sweep=liquidity_zone,
                structure_break=structure,
                session=self.session_manager.get_current_session(),
                regime=regime,
                htf_bias=htf_bias,
                score_components=components,
                timestamp=datetime.now(),
                kill_zone=kill_zone
            )
            
        except Exception as e:
            logging.error(f"Analyze error {symbol}: {e}")
            return None
    
    def _get_htf_bias(self, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> str:
        try:
            def trend(df):
                if len(df) < 50:
                    return "neutral"
                ema20 = df['close'].ewm(span=20, adjust=False).mean().iloc[-1]
                ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
                return "bullish" if ema20 > ema50 else "bearish"
            
            t1h, t4h = trend(df_1h), trend(df_4h)
            
            if t1h == t4h:
                return f"strong_{t1h}"
            return f"{t1h}_bias"
        except:
            return "mixed"
    
    def _check_htf_alignment(self, direction: str, bias: str) -> bool:
        if "strong_bullish" in bias and direction == "BUY":
            return True
        if "strong_bearish" in bias and direction == "SELL":
            return True
        if "bullish" in bias and direction == "BUY":
            return True
        if "bearish" in bias and direction == "SELL":
            return True
        return False
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        try:
            high, low, close = df['high'], df['low'], df['close']
            
            tr1 = high - low
            tr2 = (high - close.shift()).abs()
            tr3 = (low - close.shift()).abs()
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.ewm(span=period, adjust=False).mean().iloc[-1]
            
            return float(atr)
        except:
            return 0.0
    
    async def execute_trade(self, signal: Signal) -> bool:
        try:
            df = await self.fetcher.fetch_time_series(signal.symbol, "15min", 100)
            vol = self.regime_classifier.calculate_volatility(df) if df is not None else 0.01
            
            size, risk = self.risk_manager.calculate_position_size(
                signal.entry_price, signal.stop_loss, signal.atr_value, vol
            )
            
            if size <= 0:
                return False
            
            trade = Trade(signal=signal, position_size=size)
            self.active_trades.append(trade)
            
            await self.telegram.send_message(self.telegram.format_signal(signal))
            logging.info(f"Trade: {signal.symbol} {signal.direction} @ {signal.entry_price:.5f}")
            return True
            
        except Exception as e:
            logging.error(f"Execute error: {e}")
            return False
    
    async def manage_trades(self):
        try:
            for trade in list(self.active_trades):
                if trade.is_closed:
                    continue
                
                df = await self.fetcher.fetch_time_series(trade.signal.symbol, "15min", 50)
                if df is None or len(df) < 5:
                    continue
                
                current = float(df['close'].iloc[-1])
                high = float(df['high'].iloc[-1])
                low = float(df['low'].iloc[-1])
                trade.candles_held += 1
                
                # P&L in R
                if trade.signal.direction == "BUY":
                    current_r = (current - trade.signal.entry_price) / (trade.signal.entry_price - trade.signal.stop_loss)
                    hit_tp1 = high >= trade.signal.take_profit
                    hit_tp2 = high >= trade.signal.take_profit_runner if trade.signal.take_profit_runner else False
                    hit_sl = low <= trade.signal.stop_loss
                else:
                    current_r = (trade.signal.entry_price - current) / (trade.signal.stop_loss - trade.signal.entry_price)
                    hit_tp1 = low <= trade.signal.take_profit
                    hit_tp2 = low <= trade.signal.take_profit_runner if trade.signal.take_profit_runner else False
                    hit_sl = high >= trade.signal.stop_loss
                
                # Partial close
                if not trade.partial_closed and hit_tp1:
                    trade.partial_closed = True
                    trade.partial_pnl = 0.5  # 1R * 50%
                    await self.telegram.send_message(
                        f"🎯 PARTIAL {trade.signal.symbol} @ {trade.signal.take_profit:.5f} (+0.5R)"
                    )
                
                # Full close
                if hit_tp2:
                    runner_r = abs(trade.signal.take_profit_runner - trade.signal.entry_price) / \
                              abs(trade.signal.entry_price - trade.signal.stop_loss)
                    trade.runner_pnl = runner_r * 0.5
                    trade.status = TradeStatus.WIN
                    await self._close_trade(trade, trade.signal.take_profit_runner)
                    continue
                
                if hit_sl:
                    trade.status = TradeStatus.LOSS
                    await self._close_trade(trade, trade.signal.stop_loss)
                    continue
                
                # Time exit (4 hours = 16 candles of 15min)
                if trade.candles_held >= 16:
                    if current_r > 0:
                        trade.status = TradeStatus.WIN
                        trade.runner_pnl = current_r * (0.5 if trade.partial_closed else 1.0)
                    else:
                        trade.status = TradeStatus.EXPIRED
                    await self._close_trade(trade, current)
                    
        except Exception as e:
            logging.error(f"Manage error: {e}")
    
    async def _close_trade(self, trade: Trade, exit_price: float):
        try:
            trade.exit_price = exit_price
            trade.exit_time = datetime.now()
            
            total_r = trade.total_pnl
            
            # Update risk manager
            pnl_dollars = total_r * trade.position_size * abs(trade.signal.entry_price - trade.signal.stop_loss)
            self.risk_manager.update_equity(pnl_dollars)
            
            if trade in self.active_trades:
                self.active_trades.remove(trade)
            
            emoji = "✅" if trade.status == TradeStatus.WIN else "❌"
            await self.telegram.send_message(
                f"{emoji} CLOSED {trade.signal.symbol} {trade.status.value.upper()} | {total_r:+.2f}R"
            )
            
            logging.info(f"Closed: {trade.signal.id} | {trade.status.value} | {total_r:+.2f}R")
            
        except Exception as e:
            logging.error(f"Close error: {e}")
    
    async def run(self):
        self.running = True
        await self.telegram.start()
        
        await self.telegram.send_message(
            f"🏛️ <b>ENGINE STARTED</b>\n"
            f"Pairs: {len(self.watchlist)}\n"
            f"Min Score: {SystemConfig.MIN_CONFIDENCE_SCORE}\n"
            f"Session: {self.session_manager.get_current_session().value}"
        )
        
        try:
            while self.running:
                try:
                    # Trading hours check
                    if not self.session_manager.is_tradeable_session():
                        await asyncio.sleep(300)
                        continue
                    
                    # Manage existing trades
                    await self.manage_trades()
                    
                    # Scan for new signals
                    async with self._scan_lock:
                        if datetime.now() - self._last_scan < timedelta(seconds=SystemConfig.SCAN_INTERVAL_SECONDS):
                            await asyncio.sleep(1)
                            continue
                        
                        self._last_scan = datetime.now()
                        
                        for symbol in self.watchlist:
                            # Skip if heat too high
                            if self.correlation_guard.get_heat_score(self.active_trades) > 50:
                                break
                            
                            # Skip if already in trade
                            if any(t.signal.symbol == symbol and not t.is_closed for t in self.active_trades):
                                continue
                            
                            try:
                                signal = await asyncio.wait_for(
                                    self.analyze_symbol(symbol),
                                    timeout=30.0
                                )
                                if signal:
                                    await self.execute_trade(signal)
                            except asyncio.TimeoutError:
                                logging.warning(f"Timeout: {symbol}")
                            except Exception as e:
                                logging.error(f"Scan error {symbol}: {e}")
                            
                            await asyncio.sleep(0.5)
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Loop error: {e}\n{traceback.format_exc()}")
                    await asyncio.sleep(10)
                    
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        logging.info("Shutting down...")
        self.running = False
        await self.telegram.close()
        await self.fetcher.close()

# ================= ENTRY POINT =================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(DATA_DIR / "engine.log"),
            logging.StreamHandler()
        ]
    )
    
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
