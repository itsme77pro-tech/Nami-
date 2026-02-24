import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
import csv
import json
import pickle
from dataclasses import dataclass, field, asdict
from typing import Optional, Tuple, List, Dict, Literal, Callable, Any
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
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

# System Config
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR = DATA_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

WATCHLIST = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
    "BTC/USDT", "ETH/USDT", "SOL/USDT"
]

TIMEFRAMES = {
    "micro": "5min",
    "signal": "15min",
    "trend": "1h",
    "higher": "4h",
    "daily": "1day"
}

# Module Toggles
MODULES = {
    "liquidity_intelligence": True,
    "market_context": True,
    "execution_optimizer": True,
    "risk_management": True,
    "ai_learning": True,
    "profit_enhancement": True
}

# Risk Configuration
RISK_CONFIG = {
    "base_risk_per_trade": 0.01,  # 1% base risk
    "max_daily_loss": 0.03,       # 3% daily loss cap
    "max_drawdown": 0.10,         # 10% max drawdown
    "min_confidence_score": 70,   # Minimum trade quality score
    "max_trades_per_session": 5,  # Overtrading protection
    "trade_cooldown_minutes": 30, # Post-loss cooldown
    "volatility_lookback": 20,    # ATR lookback period
    "partial_tp_ratio": 0.5,      # Close 50% at first TP
    "runner_risk_reward": 3.0     # Trail runner to 3R
}

# Session Configuration (London & NY)
SESSION_CONFIG = {
    "london": {"start": time(8, 0), "end": time(11, 0), "timezone": "Europe/London"},
    "ny": {"start": time(9, 30), "end": time(11, 30), "timezone": "America/New_York"},
    "overlap": {"start": time(13, 0), "end": time(16, 0), "timezone": "UTC"}  # London-NY overlap
}

# News Filter (High Impact Events)
NEWS_BLACKOUT_MINUTES = 30  # No trading 30min before/after major news

# ================= ENUMS & DATA CLASSES =================

class MarketRegime(Enum):
    TRENDING_UP = auto()
    TRENDING_DOWN = auto()
    RANGING = auto()
    COMPRESSION = auto()
    EXPANSION = auto()
    MANIPULATION = auto()
    LOW_VOLATILITY = auto()

class TradeDirection(Enum):
    LONG = 1
    SHORT = -1
    NEUTRAL = 0

class StructureType(Enum):
    BOS = "Break of Structure"
    CHOCH = "Change of Character"
    NONE = "No Structure Break"

class LiquidityType(Enum):
    EQUAL_HIGHS = "Equal Highs"
    EQUAL_LOWS = "Equal Lows"
    SWING_HIGH = "Swing High"
    SWING_LOW = "Swing Low"
    PDH = "Previous Day High"
    PDL = "Previous Day Low"
    POOL = "Liquidity Pool"

@dataclass
class LiquidityZone:
    price: float
    type: LiquidityType
    strength: int  # 1-10 based on touches/volume
    timestamp: datetime
    is_swept: bool = False
    sweep_timestamp: Optional[datetime] = None

@dataclass
class MarketStructure:
    trend: TradeDirection
    structure_break: StructureType
    break_level: float
    break_timestamp: datetime
    confirming_candle: bool = False

@dataclass
class TradeScore:
    liquidity_sweep: int = 0      # 0-25
    structure_break: int = 0      # 0-20
    session_timing: int = 0       # 0-15
    htf_bias: int = 0            # 0-15
    volatility_fit: int = 0       # 0-10
    risk_reward: int = 0          # 0-10
    institutional_footprint: int = 0  # 0-5
    
    @property
    def total(self) -> int:
        return sum([
            self.liquidity_sweep, self.structure_break, self.session_timing,
            self.htf_bias, self.volatility_fit, self.risk_reward, self.institutional_footprint
        ])
    
    def is_valid(self) -> bool:
        return self.total >= RISK_CONFIG["min_confidence_score"]

@dataclass
class Trade:
    id: str
    symbol: str
    direction: TradeDirection
    entry_price: float
    stop_loss: float
    take_profit: float
    position_size: float
    score: TradeScore
    timestamp: datetime
    session: str
    regime: MarketRegime
    partial_tp_hit: bool = False
    runner_active: bool = False
    close_price: Optional[float] = None
    close_time: Optional[datetime] = None
    pnl: Optional[float] = None
    status: str = "OPEN"

@dataclass
class PerformanceMetrics:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    avg_trade_pnl: float = 0.0
    avg_winner: float = 0.0
    avg_loser: float = 0.0
    best_session: str = ""
    best_pair: str = ""
    best_strategy: str = ""
    current_streak: int = 0
    defensive_mode: bool = False

# ================= CORE DATA STRUCTURES =================

class CircularBuffer:
    def __init__(self, size: int):
        self.size = size
        self.buffer = deque(maxlen=size)
    
    def add(self, item):
        self.buffer.append(item)
    
    def get_all(self) -> List:
        return list(self.buffer)
    
    def is_full(self) -> bool:
        return len(self.buffer) == self.size

class TradeHistory:
    def __init__(self):
        self.trades: List[Trade] = []
        self.daily_pnl: Dict[str, float] = {}
        self.equity_curve: List[Tuple[datetime, float]] = []
        self.session_performance: Dict[str, List[float]] = {
            "london": [], "ny": [], "overlap": [], "asian": []
        }
        self.pair_performance: Dict[str, List[float]] = {pair: [] for pair in WATCHLIST}
        
    def add_trade(self, trade: Trade):
        self.trades.append(trade)
        self._update_metrics(trade)
    
    def _update_metrics(self, trade: Trade):
        date_key = trade.timestamp.strftime("%Y-%m-%d")
        if date_key not in self.daily_pnl:
            self.daily_pnl[date_key] = 0.0
        if trade.pnl:
            self.daily_pnl[date_key] += trade.pnl
            self.session_performance[trade.session].append(trade.pnl)
            self.pair_performance[trade.symbol].append(trade.pnl)
    
    def get_recent_trades(self, n: int = 10) -> List[Trade]:
        return self.trades[-n:] if len(self.trades) >= n else self.trades
    
    def get_daily_loss(self, date: Optional[str] = None) -> float:
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        return abs(min(0, self.daily_pnl.get(date, 0.0)))
    
    def get_win_rate(self, last_n: int = 50) -> float:
        recent = [t for t in self.trades[-last_n:] if t.pnl is not None]
        if not recent:
            return 0.0
        winners = sum(1 for t in recent if t.pnl > 0)
        return winners / len(recent)

# ================= LIQUIDITY INTELLIGENCE MODULE =================

class LiquidityIntelligence:
    def __init__(self):
        self.liquidity_map: Dict[str, List[LiquidityZone]] = {pair: [] for pair in WATCHLIST}
        self.recent_sweeps: CircularBuffer = CircularBuffer(100)
        
    def analyze_liquidity(self, df: pd.DataFrame, symbol: str) -> List[LiquidityZone]:
        """Detect equal highs/lows, swing points, and liquidity pools"""
        zones = []
        highs = df['high'].values
        lows = df['low'].values
        
        # Detect Equal Highs/Lows (within 0.1% tolerance)
        tolerance = 0.001
        
        for i in range(2, len(df) - 2):
            # Equal Highs
            if abs(highs[i] - highs[i-1]) / highs[i] < tolerance:
                strength = self._calculate_zone_strength(df, i, 'high')
                zones.append(LiquidityZone(
                    price=highs[i],
                    type=LiquidityType.EQUAL_HIGHS,
                    strength=strength,
                    timestamp=df.index[i]
                ))
            
            # Equal Lows
            if abs(lows[i] - lows[i-1]) / lows[i] < tolerance:
                strength = self._calculate_zone_strength(df, i, 'low')
                zones.append(LiquidityZone(
                    price=lows[i],
                    type=LiquidityType.EQUAL_LOWS,
                    strength=strength,
                    timestamp=df.index[i]
                ))
        
        # Detect Swing Points (3-bar pattern)
        for i in range(3, len(df) - 3):
            # Swing High
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                zones.append(LiquidityZone(
                    price=highs[i],
                    type=LiquidityType.SWING_HIGH,
                    strength=5,
                    timestamp=df.index[i]
                ))
            
            # Swing Low
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                zones.append(LiquidityZone(
                    price=lows[i],
                    type=LiquidityType.SWING_LOW,
                    strength=5,
                    timestamp=df.index[i]
                ))
        
        # Update liquidity map
        self.liquidity_map[symbol] = zones
        return zones
    
    def _calculate_zone_strength(self, df: pd.DataFrame, idx: int, level_type: str) -> int:
        """Calculate liquidity zone strength based on touches and volume"""
        price = df[level_type].iloc[idx]
        touches = 0
        volume_sum = 0
        
        for i in range(max(0, idx-10), min(len(df), idx+10)):
            if level_type == 'high':
                if abs(df['high'].iloc[i] - price) / price < 0.001:
                    touches += 1
                    volume_sum += df.get('volume', pd.Series([0]*len(df))).iloc[i]
            else:
                if abs(df['low'].iloc[i] - price) / price < 0.001:
                    touches += 1
                    volume_sum += df.get('volume', pd.Series([0]*len(df))).iloc[i]
        
        # Score 1-10 based on touches and relative volume
        base_score = min(touches * 2, 6)
        volume_score = 4 if volume_sum > df.get('volume', pd.Series([0]*len(df))).mean() * 2 else 2
        return min(base_score + volume_score, 10)
    
    def detect_sweep(self, current_price: float, recent_candle: pd.Series, zones: List[LiquidityZone]) -> Optional[LiquidityZone]:
        """Detect if price swept liquidity with rejection"""
        for zone in zones:
            if zone.is_swept:
                continue
                
            # Bullish sweep (swept lows)
            if zone.type in [LiquidityType.EQUAL_LOWS, LiquidityType.SWING_LOW, LiquidityType.PDL]:
                if recent_candle['low'] < zone.price and current_price > zone.price:
                    # Check for rejection (close above sweep level)
                    if recent_candle['close'] > zone.price:
                        zone.is_swept = True
                        zone.sweep_timestamp = datetime.now()
                        self.recent_sweeps.add(zone)
                        return zone
            
            # Bearish sweep (swept highs)
            if zone.type in [LiquidityType.EQUAL_HIGHS, LiquidityType.SWING_HIGH, LiquidityType.PDH]:
                if recent_candle['high'] > zone.price and current_price < zone.price:
                    if recent_candle['close'] < zone.price:
                        zone.is_swept = True
                        zone.sweep_timestamp = datetime.now()
                        self.recent_sweeps.add(zone)
                        return zone
        
        return None
    
    def get_nearest_liquidity(self, price: float, direction: TradeDirection, symbol: str) -> Optional[LiquidityZone]:
        """Find next liquidity target for take profit"""
        zones = self.liquidity_map.get(symbol, [])
        valid_zones = [z for z in zones if not z.is_swept]
        
        if direction == TradeDirection.LONG:
            # Find next resistance above price
            targets = [z for z in valid_zones if z.price > price]
            return min(targets, key=lambda x: x.price) if targets else None
        else:
            # Find next support below price
            targets = [z for z in valid_zones if z.price < price]
            return max(targets, key=lambda x: x.price) if targets else None

# ================= MARKET CONTEXT MODULE =================

class MarketContextEngine:
    def __init__(self):
        self.htf_bias: Dict[str, TradeDirection] = {}
        self.regime_history: CircularBuffer = CircularBuffer(50)
        self.volatility_regime: Dict[str, float] = {}
        
    def analyze_multi_timeframe(self, dfs: Dict[str, pd.DataFrame]) -> TradeDirection:
        """Analyze H1/H4/D1 alignment for trend bias"""
        scores = []
        
        for tf_name in ["trend", "higher", "daily"]:
            if tf_name not in dfs:
                continue
                
            df = dfs[tf_name]
            if len(df) < 20:
                continue
                
            # Simple EMA alignment
            ema20 = df['close'].ewm(span=20).mean().iloc[-1]
            ema50 = df['close'].ewm(span=50).mean().iloc[-1] if len(df) >= 50 else ema20
            
            current_price = df['close'].iloc[-1]
            
            if current_price > ema20 > ema50:
                scores.append(1)  # Bullish
            elif current_price < ema20 < ema50:
                scores.append(-1)  # Bearish
            else:
                scores.append(0)  # Neutral
        
        avg_score = np.mean(scores) if scores else 0
        
        if avg_score > 0.3:
            return TradeDirection.LONG
        elif avg_score < -0.3:
            return TradeDirection.SHORT
        return TradeDirection.NEUTRAL
    
    def classify_regime(self, df: pd.DataFrame) -> MarketRegime:
        """Classify current market regime"""
        if len(df) < 20:
            return MarketRegime.RANGING
            
        # Calculate indicators
        atr = self._calculate_atr(df)
        adx = self._calculate_adx(df)
        bb_width = self._calculate_bb_width(df)
        
        # Volatility analysis
        recent_range = (df['high'].iloc[-5:].max() - df['low'].iloc[-5:].min()) / df['close'].iloc[-1]
        avg_range = atr / df['close'].iloc[-1]
        
        # Trend strength
        price_change = abs(df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20]
        
        # Regime classification logic
        if adx < 20:
            if bb_width < 0.02:
                return MarketRegime.COMPRESSION
            return MarketRegime.RANGING
        
        if adx > 30 and price_change > 0.05:
            trend_dir = TradeDirection.LONG if df['close'].iloc[-1] > df['close'].iloc[-20] else TradeDirection.SHORT
            return MarketRegime.TRENDING_UP if trend_dir == TradeDirection.LONG else MarketRegime.TRENDING_DOWN
        
        if recent_range > avg_range * 2:
            return MarketRegime.EXPANSION
        
        if recent_range < avg_range * 0.5:
            return MarketRegime.LOW_VOLATILITY
        
        # Manipulation detection (wick analysis)
        last_candle = df.iloc[-1]
        body = abs(last_candle['close'] - last_candle['open'])
        upper_wick = last_candle['high'] - max(last_candle['close'], last_candle['open'])
        lower_wick = min(last_candle['close'], last_candle['open']) - last_candle['low']
        
        if (upper_wick > body * 3 or lower_wick > body * 3) and body / last_candle['close'] < 0.001:
            return MarketRegime.MANIPULATION
        
        return MarketRegime.RANGING
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(period).mean().iloc[-1]
    
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        """Simplified ADX calculation"""
        plus_dm = df['high'].diff()
        minus_dm = df['low'].diff().abs()
        
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        tr = self._calculate_atr(df, period)
        plus_di = 100 * (plus_dm.rolling(period).mean() / tr)
        minus_di = 100 * (minus_dm.rolling(period).mean() / tr)
        
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        return dx.rolling(period).mean().iloc[-1]
    
    def _calculate_bb_width(self, df: pd.DataFrame, period: int = 20) -> float:
        """Calculate Bollinger Band width"""
        sma = df['close'].rolling(period).mean()
        std = df['close'].rolling(period).std()
        upper = sma + (std * 2)
        lower = sma - (std * 2)
        return ((upper - lower) / sma).iloc[-1]
    
    def is_tradeable_session(self) -> Tuple[bool, str]:
        """Check if current time is within trading sessions"""
        now = datetime.now(pytz.UTC)
        
        # Check London session
        london_tz = pytz.timezone(SESSION_CONFIG["london"]["timezone"])
        london_now = now.astimezone(london_tz)
        london_start = SESSION_CONFIG["london"]["start"]
        london_end = SESSION_CONFIG["london"]["end"]
        
        if london_start <= london_now.time() <= london_end:
            return True, "london"
        
        # Check NY session
        ny_tz = pytz.timezone(SESSION_CONFIG["ny"]["timezone"])
        ny_now = now.astimezone(ny_tz)
        ny_start = SESSION_CONFIG["ny"]["start"]
        ny_end = SESSION_CONFIG["ny"]["end"]
        
        if ny_start <= ny_now.time() <= ny_end:
            return True, "ny"
        
        # Check overlap
        overlap_start = SESSION_CONFIG["overlap"]["start"]
        overlap_end = SESSION_CONFIG["overlap"]["end"]
        
        if overlap_start <= now.time() <= overlap_end:
            return True, "overlap"
        
        return False, "asian"
    
    def check_news_filter(self) -> bool:
        """Check if currently in news blackout period"""
        # Placeholder - integrate with economic calendar API
        # Returns True if safe to trade (no upcoming news)
        return True

# ================= EXECUTION OPTIMIZER MODULE =================

class ExecutionOptimizer:
    def __init__(self, liquidity_module: LiquidityIntelligence):
        self.liquidity = liquidity_module
        self.spread_threshold = 0.0005  # 5 pips for FX
        
    def calculate_optimal_entry(self, 
                               sweep_zone: LiquidityZone,
                               direction: TradeDirection,
                               df: pd.DataFrame) -> Optional[Tuple[float, float, float]]:
        """Calculate entry, SL, and TP based on structure"""
        current_price = df['close'].iloc[-1]
        
        # Wait for retest after sweep
        if not self._confirm_retest(sweep_zone, current_price, direction):
            return None
        
        # ATR-based stop loss
        atr = self._calculate_atr(df)
        
        if direction == TradeDirection.LONG:
            # Entry at retest of broken level or 50% of sweep candle
            entry = current_price
            # SL below liquidity wick or structural low
            recent_lows = df['low'].tail(5).min()
            stop_loss = min(sweep_zone.price - atr * 0.5, recent_lows - atr * 0.2)
            
            # TP at next liquidity pool
            tp_zone = self.liquidity.get_nearest_liquidity(entry, direction, "symbol")
            if tp_zone:
                take_profit = tp_zone.price
            else:
                take_profit = entry + (entry - stop_loss) * 2  # 1:2 RR minimum
        else:
            entry = current_price
            recent_highs = df['high'].tail(5).max()
            stop_loss = max(sweep_zone.price + atr * 0.5, recent_highs + atr * 0.2)
            
            tp_zone = self.liquidity.get_nearest_liquidity(entry, direction, "symbol")
            if tp_zone:
                take_profit = tp_zone.price
            else:
                take_profit = entry - (stop_loss - entry) * 2
        
        # Validate R:R
        risk = abs(entry - stop_loss)
        reward = abs(take_profit - entry)
        
        if reward / risk < 1.5:  # Minimum 1:1.5 R:R
            return None
            
        return entry, stop_loss, take_profit
    
    def _confirm_retest(self, zone: LiquidityZone, current_price: float, direction: TradeDirection) -> bool:
        """Confirm price retested broken level with rejection"""
        if direction == TradeDirection.LONG:
            # Price should come back to zone and reject upward
            return current_price >= zone.price * 0.998 and current_price <= zone.price * 1.002
        else:
            return current_price <= zone.price * 1.002 and current_price >= zone.price * 0.998
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(period).mean().iloc[-1]
    
    def check_spread(self, bid: float, ask: float) -> bool:
        """Check if spread is acceptable"""
        spread = (ask - bid) / ((ask + bid) / 2)
        return spread <= self.spread_threshold
    
    def detect_institutional_footprint(self, df: pd.DataFrame) -> int:
        """Detect displacement, imbalance, volume spikes (0-5 score)"""
        score = 0
        
        if len(df) < 3:
            return score
            
        last_three = df.tail(3)
        
        # Displacement detection (large body candles)
        for _, candle in last_three.iterrows():
            body = abs(candle['close'] - candle['open'])
            range_size = candle['high'] - candle['low']
            if body / range_size > 0.8 and range_size > df['high'].diff().mean() * 2:
                score += 2
        
        # Volume spike (if volume data available)
        if 'volume' in df.columns:
            vol_avg = df['volume'].tail(20).mean()
            if df['volume'].iloc[-1] > vol_avg * 2:
                score += 2
        
        # Imbalance (gap between candles)
        for i in range(1, len(last_three)):
            prev_close = last_three.iloc[i-1]['close']
            curr_open = last_three.iloc[i]['open']
            gap = abs(curr_open - prev_close) / prev_close
            if gap > 0.001:  # 10 pips
                score += 1
        
        return min(score, 5)

# ================= RISK MANAGEMENT MODULE =================

class RiskManager:
    def __init__(self, history: TradeHistory):
        self.history = history
        self.daily_loss = 0.0
        self.current_drawdown = 0.0
        self.peak_equity = 0.0
        self.current_equity = 0.0
        self.last_trade_time: Optional[datetime] = None
        self.trades_today = 0
        self.defensive_mode = False
        self.loss_cluster_count = 0
        
    def update_equity(self, equity: float):
        """Update equity and calculate drawdown"""
        self.current_equity = equity
        if equity > self.peak_equity:
            self.peak_equity = equity
        else:
            self.current_drawdown = (self.peak_equity - equity) / self.peak_equity
        
        # Check defensive mode triggers
        self._check_defensive_triggers()
    
    def _check_defensive_triggers(self):
        """Activate defensive mode on drawdown or loss clusters"""
        if self.current_drawdown > RISK_CONFIG["max_drawdown"] * 0.5:
            self.defensive_mode = True
        
        recent_trades = self.history.get_recent_trades(5)
        losses = sum(1 for t in recent_trades if t.pnl and t.pnl < 0)
        if losses >= 3:
            self.loss_cluster_count += 1
            if self.loss_cluster_count >= 2:
                self.defensive_mode = True
        else:
            self.loss_cluster_count = max(0, self.loss_cluster_count - 1)
    
    def can_trade(self) -> Tuple[bool, str]:
        """Comprehensive trade permission check"""
        now = datetime.now()
        
        # Daily loss cap
        if self.history.get_daily_loss() > RISK_CONFIG["max_daily_loss"] * self.current_equity:
            return False, "Daily loss cap reached"
        
        # Max drawdown
        if self.current_drawdown > RISK_CONFIG["max_drawdown"]:
            return False, "Max drawdown exceeded"
        
        # Trade frequency governor
        if self.trades_today >= RISK_CONFIG["max_trades_per_session"]:
            return False, "Max trades per session reached"
        
        # Cooldown after loss
        if self.last_trade_time:
            recent_trades = self.history.get_recent_trades(1)
            if recent_trades and recent_trades[0].pnl and recent_trades[0].pnl < 0:
                cooldown_end = self.last_trade_time + timedelta(minutes=RISK_CONFIG["trade_cooldown_minutes"])
                if now < cooldown_end:
                    return False, f"Cooldown active until {cooldown_end}"
        
        # Defensive mode restrictions
        if self.defensive_mode:
            # Only allow A+ setups in defensive mode
            return True, "DEFENSIVE_MODE"
        
        return True, "OK"
    
    def calculate_position_size(self, 
                               entry: float, 
                               stop_loss: float, 
                               confidence_score: int,
                               volatility: float) -> float:
        """Adaptive position sizing based on volatility and score"""
        risk_amount = self.current_equity * RISK_CONFIG["base_risk_per_trade"]
        
        # Adjust for confidence (reduce size for lower scores)
        confidence_multiplier = confidence_score / 100
        
        # Adjust for volatility (reduce size in high vol)
        vol_adjustment = 1.0
        if volatility > 0.02:  # 2% daily vol
            vol_adjustment = 0.7
        
        # Defensive mode reduction
        defensive_mult = 0.5 if self.defensive_mode else 1.0
        
        adjusted_risk = risk_amount * confidence_multiplier * vol_adjustment * defensive_mult
        
        risk_per_unit = abs(entry - stop_loss)
        position_size = adjusted_risk / risk_per_unit if risk_per_unit > 0 else 0
        
        return position_size
    
    def register_trade(self, trade: Trade):
        """Track trade for risk management"""
        self.last_trade_time = datetime.now()
        self.trades_today += 1
    
    def reset_daily_stats(self):
        """Reset daily counters"""
        self.trades_today = 0
        self.daily_loss = 0.0
        # Gradually exit defensive mode
        if self.defensive_mode and self.current_drawdown < RISK_CONFIG["max_drawdown"] * 0.3:
            self.defensive_mode = False

# ================= AI LEARNING MODULE =================

class AILearningEngine:
    def __init__(self, history: TradeHistory):
        self.history = history
        self.parameter_history: List[Dict] = []
        self.optimal_params = {
            "min_confidence": RISK_CONFIG["min_confidence_score"],
            "atr_multiplier_sl": 1.5,
            "session_weights": {"london": 1.0, "ny": 1.0, "overlap": 1.2, "asian": 0.0}
        }
        
    def analyze_performance(self):
        """Analyze trade history for optimization opportunities"""
        if len(self.history.trades) < 20:
            return
        
        # Session analysis
        for session, pnls in self.history.session_performance.items():
            if pnls:
                avg_pnl = np.mean(pnls)
                self.optimal_params["session_weights"][session] = 1.0 + (avg_pnl * 10)
        
        # Pair analysis
        pair_scores = {}
        for pair, pnls in self.history.pair_performance.items():
            if pnls:
                win_rate = sum(1 for p in pnls if p > 0) / len(pnls)
                avg_pnl = np.mean(pnls)
                pair_scores[pair] = win_rate * avg_pnl
        
        best_pair = max(pair_scores, key=pair_scores.get) if pair_scores else WATCHLIST[0]
        
        # Strategy optimization
        winning_trades = [t for t in self.history.trades if t.pnl and t.pnl > 0]
        if winning_trades:
            avg_score = np.mean([t.score.total for t in winning_trades])
            # Adjust minimum confidence toward winning trade average
            self.optimal_params["min_confidence"] = max(65, min(85, int(avg_score * 0.9)))
        
        # Store parameter history
        self.parameter_history.append({
            "timestamp": datetime.now(),
            "params": self.optimal_params.copy(),
            "performance": self._calculate_recent_performance()
        })
    
    def _calculate_recent_performance(self) -> Dict:
        """Calculate recent performance metrics"""
        recent = self.history.get_recent_trades(20)
        if not recent:
            return {}
        
        pnls = [t.pnl for t in recent if t.pnl is not None]
        return {
            "win_rate": sum(1 for p in pnls if p > 0) / len(pnls) if pnls else 0,
            "profit_factor": abs(sum(p for p in pnls if p > 0) / sum(p for p in pnls if p < 0)) if any(p < 0 for p in pnls) else float('inf'),
            "avg_pnl": np.mean(pnls) if pnls else 0
        }
    
    def get_optimized_parameters(self) -> Dict:
        """Return current optimized parameters"""
        return self.optimal_params
    
    def suggest_improvements(self) -> List[str]:
        """Generate suggestions based on performance analysis"""
        suggestions = []
        perf = self._calculate_recent_performance()
        
        if perf.get("win_rate", 0) < 0.4:
            suggestions.append("Win rate low - tighten entry filters, increase confidence threshold")
        
        if perf.get("profit_factor", 0) < 1.5:
            suggestions.append("Profit factor weak - review R:R ratios, extend TP targets")
        
        recent = self.history.get_recent_trades(10)
        if recent:
            regime_performance = {}
            for trade in recent:
                if trade.pnl:
                    regime_performance.setdefault(trade.regime, []).append(trade.pnl)
            
            bad_regimes = [r for r, pnls in regime_performance.items() if np.mean(pnls) < 0]
            if bad_regimes:
                suggestions.append(f"Avoid trading in regimes: {[r.name for r in bad_regimes]}")
        
        return suggestions

# ================= MAIN TRADING ENGINE =================

class InstitutionalTradingBot:
    def __init__(self):
        self.liquidity = LiquidityIntelligence()
        self.context = MarketContextEngine()
        self.execution = ExecutionOptimizer(self.liquidity)
        self.history = TradeHistory()
        self.risk = RiskManager(self.history)
        self.ai = AILearningEngine(self.history)
        
        self.active_trades: List[Trade] = []
        self.equity = 10000.0  # Starting equity
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(DATA_DIR / "trading_bot.log"),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger("InstitutionalBot")
    
    async def run(self):
        """Main execution loop"""
        self.logger.info("Institutional Trading Bot Initialized")
        
        while True:
            try:
                await self._trading_cycle()
                await asyncio.sleep(60)  # 1-minute cycle
            except Exception as e:
                self.logger.error(f"Error in trading cycle: {e}")
                await asyncio.sleep(300)  # 5-min cooldown on error
    
    async def _trading_cycle(self):
        """Single trading cycle"""
        # Update risk management
        self.risk.update_equity(self.equity)
        
        # Check if we can trade
        can_trade, reason = self.risk.can_trade()
        if not can_trade:
            self.logger.info(f"Trading paused: {reason}")
            return
        
        # Check session
        is_session, session_name = self.context.is_tradeable_session()
        if not is_session and not MODULES.get("ignore_sessions", False):
            return
        
        # Check news filter
        if not self.context.check_news_filter():
            self.logger.info("News blackout active")
            return
        
        # Process each symbol
        for symbol in WATCHLIST:
            await self._analyze_symbol(symbol, session_name)
        
        # Manage existing trades
        self._manage_open_trades()
        
        # AI optimization
        if len(self.history.trades) % 10 == 0:
            self.ai.analyze_performance()
    
    async def _analyze_symbol(self, symbol: str, session: str):
        """Analyze single symbol for trade opportunities"""
        # Fetch multi-timeframe data
        dfs = await self._fetch_data(symbol)
        if not dfs or "signal" not in dfs:
            return
        
        df_signal = dfs["signal"]
        if len(df_signal) < 50:
            return
        
        # Market context analysis
        htf_bias = self.context.analyze_multi_timeframe(dfs)
        regime = self.context.classify_regime(df_signal)
        
        # Skip unfavorable regimes
        if regime in [MarketRegime.MANIPULATION, MarketRegime.LOW_VOLATILITY, MarketRegime.COMPRESSION]:
            return
        
        # Liquidity analysis
        liquidity_zones = self.liquidity.analyze_liquidity(df_signal, symbol)
        
        # Detect sweep on latest candle
        latest = df_signal.iloc[-1]
        current_price = latest['close']
        
        sweep = self.liquidity.detect_sweep(current_price, latest, liquidity_zones)
        if not sweep:
            return
        
        # Determine direction based on sweep type
        direction = TradeDirection.LONG if sweep.type in [LiquidityType.EQUAL_LOWS, LiquidityType.SWING_LOW] else TradeDirection.SHORT
        
        # Check HTF alignment
        if htf_bias != TradeDirection.NEUTRAL and htf_bias != direction:
            return  # Don't fight HTF trend
        
        # Calculate entry parameters
        entry_data = self.execution.calculate_optimal_entry(sweep, direction, df_signal)
        if not entry_data:
            return
        
        entry, stop_loss, take_profit = entry_data
        
        # Calculate trade score
        score = self._calculate_trade_score(
            sweep, htf_bias, regime, session, 
            entry, stop_loss, take_profit, df_signal
        )
        
        if not score.is_valid():
            self.logger.info(f"Trade rejected - Score: {score.total}/100")
            return
        
        # Calculate position size
        volatility = self.context._calculate_atr(df_signal) / current_price
        position_size = self.risk.calculate_position_size(entry, stop_loss, score.total, volatility)
        
        if position_size <= 0:
            return
        
        # Execute trade
        trade = Trade(
            id=f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            symbol=symbol,
            direction=direction,
            entry_price=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            position_size=position_size,
            score=score,
            timestamp=datetime.now(),
            session=session,
            regime=regime
        )
        
        await self._execute_trade(trade)
    
    def _calculate_trade_score(self, 
                              sweep: LiquidityZone,
                              htf_bias: TradeDirection,
                              regime: MarketRegime,
                              session: str,
                              entry: float,
                              sl: float,
                              tp: float,
                              df: pd.DataFrame) -> TradeScore:
        """Calculate comprehensive trade quality score"""
        score = TradeScore()
        
        # Liquidity sweep score (0-25)
        score.liquidity_sweep = min(25, sweep.strength * 2 + 15 if sweep.is_swept else 0)
        
        # Structure break score (0-20)
        # Check for BOS/CHOCH in recent candles
        recent_high = df['high'].tail(5).max()
        recent_low = df['low'].tail(5).min()
        if sweep.type in [LiquidityType.EQUAL_HIGHS, LiquidityType.SWING_HIGH]:
            if df['close'].iloc[-1] < recent_low:  # CHOCH
                score.structure_break = 20
            elif df['close'].iloc[-1] < sweep.price:  # BOS
                score.structure_break = 15
        else:
            if df['close'].iloc[-1] > recent_high:
                score.structure_break = 20
            elif df['close'].iloc[-1] > sweep.price:
                score.structure_break = 15
        
        # Session timing (0-15)
        session_weights = self.ai.optimal_params["session_weights"]
        score.session_timing = int(15 * session_weights.get(session, 0.5))
        
        # HTF bias (0-15)
        if htf_bias != TradeDirection.NEUTRAL:
            score.htf_bias = 15
        
        # Volatility fit (0-10)
        atr = self.context._calculate_atr(df)
        vol_score = 10 if 0.005 < atr / entry < 0.02 else 5
        score.volatility_fit = vol_score
        
        # Risk:Reward (0-10)
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = reward / risk if risk > 0 else 0
        score.risk_reward = min(10, int(rr * 3))
        
        # Institutional footprint (0-5)
        score.institutional_footprint = self.execution.detect_institutional_footprint(df)
        
        return score
    
    async def _execute_trade(self, trade: Trade):
        """Execute trade with risk management"""
        self.logger.info(f"Executing {trade.direction.name} on {trade.symbol} | Score: {trade.score.total}/100")
        
        # Register with risk manager
        self.risk.register_trade(trade)
        
        # Add to active trades
        self.active_trades.append(trade)
        self.history.add_trade(trade)
        
        # Send notification
        await self._send_notification(trade)
    
    def _manage_open_trades(self):
        """Manage open positions (TP, SL, partial close)"""
        for trade in self.active_trades[:]:
            # Simulate price monitoring (in real implementation, fetch current price)
            current_price = self._get_current_price(trade.symbol)
            
            if trade.status != "OPEN":
                continue
            
            # Check stop loss
            if trade.direction == TradeDirection.LONG and current_price <= trade.stop_loss:
                self._close_trade(trade, current_price, "STOP_LOSS")
            elif trade.direction == TradeDirection.SHORT and current_price >= trade.stop_loss:
                self._close_trade(trade, current_price, "STOP_LOSS")
            
            # Check take profit
            elif trade.direction == TradeDirection.LONG and current_price >= trade.take_profit:
                if not trade.partial_tp_hit:
                    self._partial_close(trade, current_price)
                else:
                    self._close_trade(trade, current_price, "TAKE_PROFIT")
            
            elif trade.direction == TradeDirection.SHORT and current_price <= trade.take_profit:
                if not trade.partial_tp_hit:
                    self._partial_close(trade, current_price)
                else:
                    self._close_trade(trade, current_price, "TAKE_PROFIT")
            
            # Time-based exit for stagnant trades
            trade_duration = datetime.now() - trade.timestamp
            if trade_duration > timedelta(hours=4) and not trade.partial_tp_hit:
                self._close_trade(trade, current_price, "TIME_EXIT")
    
    def _partial_close(self, trade: Trade, price: float):
        """Close partial position and trail runner"""
        trade.partial_tp_hit = True
        trade.runner_active = True
        
        # Adjust stop loss to breakeven or better
        if trade.direction == TradeDirection.LONG:
            trade.stop_loss = max(trade.stop_loss, trade.entry_price)
        else:
            trade.stop_loss = min(trade.stop_loss, trade.entry_price)
        
        self.logger.info(f"Partial TP hit on {trade.symbol}, runner active")
    
    def _close_trade(self, trade: Trade, close_price: float, reason: str):
        """Close trade and update P&L"""
        trade.close_price = close_price
        trade.close_time = datetime.now()
        trade.status = "CLOSED"
        
        # Calculate P&L
        if trade.direction == TradeDirection.LONG:
            trade.pnl = (close_price - trade.entry_price) * trade.position_size
        else:
            trade.pnl = (trade.entry_price - close_price) * trade.position_size
        
        # Update equity
        self.equity += trade.pnl
        
        self.logger.info(f"Closed {trade.symbol} | Reason: {reason} | P&L: ${trade.pnl:.2f}")
        
        # Remove from active trades
        self.active_trades.remove(trade)
        
        # Update history
        self.history.add_trade(trade)
    
    async def _fetch_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch multi-timeframe data from API"""
        # Placeholder - implement actual data fetching
        # Return dict with keys: micro, signal, trend, higher, daily
        return {}
    
    def _get_current_price(self, symbol: str) -> float:
        """Get current market price"""
        # Placeholder
        return 0.0
    
    async def _send_notification(self, trade: Trade):
        """Send Telegram notification"""
        if not TELEGRAM_TOKEN or not CHAT_ID:
            return
        
        message = (
            f"🎯 <b>New Trade Executed</b>\n\n"
            f"Symbol: {trade.symbol}\n"
            f"Direction: {trade.direction.name}\n"
            f"Entry: {trade.entry_price:.5f}\n"
            f"SL: {trade.stop_loss:.5f}\n"
            f"TP: {trade.take_profit:.5f}\n"
            f"Size: {trade.position_size:.2f}\n"
            f"Score: {trade.score.total}/100\n"
            f"Regime: {trade.regime.name}\n"
            f"Session: {trade.session}"
        )
        
        # Implement actual Telegram API call here
        self.logger.info(f"Notification: {message}")

# ================= UTILITY FUNCTIONS =================

def save_state(bot: InstitutionalTradingBot):
    """Save bot state to disk"""
    state = {
        "history": bot.history,
        "ai_params": bot.ai.optimal_params,
        "equity": bot.equity,
        "timestamp": datetime.now()
    }
    with open(DATA_DIR / "bot_state.pkl", "wb") as f:
        pickle.dump(state, f)

def load_state() -> Optional[InstitutionalTradingBot]:
    """Load bot state from disk"""
    state_file = DATA_DIR / "bot_state.pkl"
    if state_file.exists():
        with open(state_file, "rb") as f:
            state = pickle.load(f)
        # Reconstruct bot with saved state
        bot = InstitutionalTradingBot()
        bot.history = state["history"]
        bot.ai.optimal_params = state["ai_params"]
        bot.equity = state["equity"]
        return bot
    return None

# ================= MAIN ENTRY =================

async def main():
    """Main entry point"""
    # Try to load existing state
    bot = load_state()
    if not bot:
        bot = InstitutionalTradingBot()
    
    try:
        await bot.run()
    except KeyboardInterrupt:
        save_state(bot)
        print("Bot stopped and state saved")

if __name__ == "__main__":
    asyncio.run(main())
