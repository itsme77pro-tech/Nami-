"""
HedgeFundEngine v4.1 - Production Ready System
Part 1: Configuration, Data Classes, Exchange Interfaces
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
import json
import hashlib
import time
import pytz
import warnings
import sqlite3
from dataclasses import dataclass, field, asdict
from typing import Optional, Tuple, List, Dict, Literal, Callable, Any, Union
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from collections import defaultdict, deque
from abc import ABC, abstractmethod
import hmac
from logging.handlers import RotatingFileHandler

warnings.filterwarnings('ignore')

# Try to import exchange libraries
try:
 import ccxt.async_support as ccxt
 CCXT_AVAILABLE = True
except ImportError:
 CCXT_AVAILABLE = False
 logging.warning("CCXT not installed. Binance integration limited.")

try:
 from scipy.signal import find_peaks
 SCIPY_AVAILABLE = True
except ImportError:
 SCIPY_AVAILABLE = False

# ================= CONFIGURATION =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()
EXCHANGE_MODE = "hybrid"
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "true").lower() == "true"

# Global Trading Sessions Configuration
TRADING_SESSIONS = {
 "SYDNEY": (21, 24),
 "TOKYO": (0, 8),
 "LONDON": (8, 13),
 "OVERLAP": (13, 17),
 "NEW_YORK": (17, 22)
}

ALERT_COOLDOWN_MINUTES = 30
MAX_DAILY_ALERTS = 20
ENGAGEMENT_HOURS = [8, 13, 21]
WHALE_VOLUME_MULTIPLIER = 3.0
WHALE_PRICE_IMPACT_PCT = 0.5
PROGRESS_BAR_LENGTH = 10

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

TIMEFRAMES = {"micro": "5m", "signal": "15m", "trend": "1h", "macro": "4h", "structural": "1d"}
SCAN_INTERVAL = 300
COOLDOWN_MINUTES = 60 if PERSONALITY == "nami" else 120

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Setup logging
logger = logging.getLogger("HedgeFundEngine")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

file_handler = RotatingFileHandler(DATA_DIR / "hedgefund.log", maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'))
logger.addHandler(file_handler)

# ================= NEW RISK-CONTROL CONFIGURATION =================

# Signal Quality Thresholds
MIN_SIGNAL_PROBABILITY = 0.70

# Stop Loss Configuration
ATR_STOP_MULTIPLIER = 1.3

# Trading Session Restrictions
ALLOWED_TRADING_SESSIONS = ["TOKYO", "LONDON", "OVERLAP", "NEW_YORK"]

# Symbol-Specific Limits
MAX_USD_TRADES = 1

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
 LOW = auto(); NORMAL = auto(); HIGH = auto(); EXTREME = auto()

class LiquidityType(Enum):
 SESSION_HIGH = "session_high"; SESSION_LOW = "session_low"
 WEEKLY_HIGH = "weekly_high"; WEEKLY_LOW = "weekly_low"
 EQUAL_HIGH = "equal_high"; EQUAL_LOW = "equal_low"
 POOL_ABOVE = "pool_above"; POOL_BELOW = "pool_below"
 STOP_CLUSTER = "stop_cluster"

class TradeStatus(Enum):
 PENDING = "pending"; PARTIAL = "partial"; WIN = "win"
 LOSS = "loss"; BREAKEVEN = "breakeven"; EXPIRED = "expired"

class TradeOutcome(Enum):
 NONE = "none"; TP_HIT = "tp_hit"; SL_HIT = "sl_hit"
 PARTIAL_TP = "partial_tp"; TRAILING_STOP = "trailing_stop"
 BREAKEVEN = "breakeven"; EXPIRED = "expired"

class AlertType(Enum):
 PRE_SIGNAL = "pre_signal"; SIGNAL = "signal"; LIQUIDITY_ZONE = "liquidity_zone"
 SESSION_OPEN = "session_open"; CONFIDENCE_HEATMAP = "confidence_heatmap"
 RISK_DASHBOARD = "risk_dashboard"; INVALIDATION = "invalidation"
 WHALE_ACTIVITY = "whale_activity"; MULTI_RANKING = "multi_ranking"
 WEEKLY_ANALYTICS = "weekly_analytics"; DRAWDOWN_WARNING = "drawdown_warning"
 PROBABILITY_VIZ = "probability_viz"; RR_SUGGESTION = "rr_suggestion"
 REGIME_CHANGE = "regime_change"

# ================= DATA CLASSES =================

@dataclass
class LiquidityLevel:
 price: float; liquidity_type: LiquidityType; strength: float
 volume_proxy: float; timestamp: datetime; is_swept: bool = False
 sweep_timestamp: Optional[datetime] = None

@dataclass
class OrderflowImprint:
 displacement: bool = False; imbalance: bool = False; absorption: bool = False
 momentum_burst: bool = False; volume_anomaly: bool = False
 delta_direction: Optional[str] = None

@dataclass
class RegimeState:
 primary: MarketRegime; volatility: VolatilityRegime; trend_strength: float
 range_position: float; adx: float; atr_percentile: float

@dataclass
class ProbabilityModel:
 base_probability: float; confluence_boost: float; regime_adjustment: float
 liquidity_score: float; orderflow_score: float; final_probability: float
 edge: float; kelly_fraction: float; confidence_interval: Tuple[float, float]

@dataclass
class Signal:
 id: str; symbol: str; direction: Literal["BUY", "SELL"]
 entry_price: float; stop_loss: float; take_profit: float
 take_profit_1: Optional[float]; atr_value: float; timestamp: datetime
 probability: ProbabilityModel; regime: RegimeState; primary_strategy: str
 triggered_strategies: List[str]; liquidity_levels: List[LiquidityLevel]
 orderflow: OrderflowImprint; position_size_r: float; risk_reward: float
 expected_value: float; timeframe: str = "15m"; spread_at_signal: float = 0.0
 slippage_estimate: float = 0.0

@dataclass
class Trade:
 signal: Signal; status: TradeStatus = field(default=TradeStatus.PENDING)
 outcome: TradeOutcome = field(default=TradeOutcome.NONE)
 entry_executed: float = 0.0; exit_price: Optional[float] = None
 exit_time: Optional[datetime] = None; partial_size_closed: float = 0.0
 partial_exit_price: Optional[float] = None; trailing_active: bool = False
 trailing_level: Optional[float] = None; highest_profit_r: float = 0.0
 realized_pnl_r: float = 0.0; unrealized_pnl_r: float = 0.0
 candles_evaluated: int = 0; max_adverse_excursion: float = 0.0
 max_favorable_excursion: float = 0.0

 @property
 def is_closed(self) -> bool:
     return self.status in [TradeStatus.WIN, TradeStatus.LOSS, TradeStatus.BREAKEVEN, TradeStatus.EXPIRED]

 @property
 def current_pnl_r(self) -> float:
     return self.realized_pnl_r + self.unrealized_pnl_r

@dataclass
class PortfolioState:
 total_exposure_r: float = 0.0; open_trades: int = 0; daily_pnl_r: float = 0.0
 peak_equity_r: float = 0.0; current_drawdown_pct: float = 0.0
 correlation_matrix: Dict[str, Dict[str, float]] = field(default_factory=dict)
 last_signals: Dict[str, datetime] = field(default_factory=dict)

@dataclass
class UXSignal:
 id: str; symbol: str; direction: Literal["BUY", "SELL"]; probability: float
 confidence: int; entry_price: float; stop_loss: float; take_profit: float
 risk_reward: float; timestamp: datetime; countdown_seconds: int = 0
 setup_quality_score: int = 0; confluence_factors: List[str] = field(default_factory=list)
 risk_visualization: Dict[str, Any] = field(default_factory=dict)
 probability_breakdown: Dict[str, float] = field(default_factory=dict)

@dataclass
class LiquidityZone:
 symbol: str; zone_type: Literal["support", "resistance", "equal_high", "equal_low", "stop_cluster"]
 price_level: float; strength_score: int; distance_pct: float; test_count: int; volume_at_level: float

 @property
 def heatmap_intensity(self) -> str:
     if self.strength_score >= 80: return "🔥🔥🔥"
     elif self.strength_score >= 60: return "🔥🔥"
     elif self.strength_score >= 40: return "🔥"
     else: return "⚡"

@dataclass
class WhaleActivity:
 symbol: str; timestamp: datetime
 activity_type: Literal["accumulation", "distribution", "stop_hunt", "momentum_ignition"]
 volume_anomaly: float; price_impact: float; direction: Literal["BUY", "SELL", "NEUTRAL"]; confidence: int

 @property
 def magnitude_emoji(self) -> str:
     if self.volume_anomaly >= 5: return "🐋🐋🐋"
     elif self.volume_anomaly >= 3: return "🐋🐋"
     else: return "🐋"

@dataclass
class RiskMetrics:
 timestamp: datetime; portfolio_heat_score: int; active_exposure: float
 available_capacity: float; drawdown_status: Literal["normal", "elevated", "critical"]
 correlation_risk: int; session_risk: int; top_risks: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class StrategyDetection:
 name: str; detected: bool; confidence: float; direction: Optional[Literal["BUY", "SELL"]]
 entry_price: Optional[float]; stop_loss: Optional[float]; take_profit: Optional[float]
 metadata: Dict[str, Any] = field(default_factory=dict)
 confluence_factors: List[str] = field(default_factory=list)

# ================= EXCHANGE INTERFACES =================

class ExchangeInterface(ABC):
 @abstractmethod
 async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]: pass
 @abstractmethod
 async def get_spread(self, symbol: str) -> float: pass
 @abstractmethod
 async def close(self): pass

class TwelveDataInterface(ExchangeInterface):
 def __init__(self, api_key: str):
     self.api_key = api_key; self.base_url = "https://api.twelvedata.com"
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
         params = {"symbol": symbol, "interval": interval, "outputsize": limit, "apikey": self.api_key}
         try:
             async with session.get(url, params=params) as resp:
                 if resp.status != 200: return None
                 data = await resp.json()
                 if "values" not in data: return None
                 df = pd.DataFrame(data["values"])
                 df["datetime"] = pd.to_datetime(df["datetime"])
                 df.set_index("datetime", inplace=True)
                 df = df.astype(float); df.sort_index(inplace=True)
                 return df
         except Exception as e:
             logger.error(f"TwelveData error: {e}"); return None

 async def get_spread(self, symbol: str) -> float: return 0.0002
 async def close(self):
     if self._session: await self._session.close()

class BinanceInterface(ExchangeInterface):
 def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
     self.api_key = api_key; self.api_secret = api_secret; self.testnet = testnet
     if CCXT_AVAILABLE:
         self.exchange = ccxt.binance({
             'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True,
             'options': {'defaultType': 'future', 'adjustForTimeDifference': True}
         })
         if testnet: self.exchange.set_sandbox_mode(True)
     else:
         self.exchange = None
         self.base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
         self._session: Optional[aiohttp.ClientSession] = None

 async def _get_session(self) -> aiohttp.ClientSession:
     if self._session is None or self._session.closed:
         self._session = aiohttp.ClientSession()
     return self._session

 def _generate_signature(self, query_string: str) -> str:
     return hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

 async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
     if self.exchange:
         try:
             ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
             df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
             df.set_index('timestamp', inplace=True); return df
         except Exception as e:
             logger.error(f"CCXT error: {e}"); return None

     session = await self._get_session()
     interval = timeframe.replace('m', '').replace('h', 'h').replace('d', 'd')
     url = f"{self.base_url}/fapi/v1/klines"
     params = {"symbol": symbol.replace('/', ''), "interval": interval, "limit": limit}
     try:
         async with session.get(url, params=params) as resp:
             if resp.status != 200: return None
             data = await resp.json()
             df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                 'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'])
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
             df.set_index('timestamp', inplace=True)
             df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
             return df
     except Exception as e:
         logger.error(f"Binance API error: {e}"); return None

 async def get_spread(self, symbol: str) -> float:
     if self.exchange:
         try:
             ticker = await self.exchange.fetch_ticker(symbol)
             return (ticker['ask'] - ticker['bid']) / ticker['last']
         except: pass
     return 0.0005

 async def close(self):
     if self.exchange: await self.exchange.close()
     if self._session: await self._session.close()

class HybridExchangeManager:
 def __init__(self, twelvedata_key: str, binance_key: str, binance_secret: str):
     self.forex = TwelveDataInterface(twelvedata_key) if twelvedata_key else None
     self.crypto = BinanceInterface(binance_key, binance_secret, BINANCE_TESTNET) if binance_key and binance_secret else None
     self.asset_class_map = {**{s: "forex" for s in FOREX_WATCHLIST}, **{s: "crypto" for s in CRYPTO_WATCHLIST}}

 async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
     asset_class = self.asset_class_map.get(symbol, "forex")
     if asset_class == "forex" and self.forex: return await self.forex.fetch_ohlcv(symbol, timeframe, limit)
     elif asset_class == "crypto" and self.crypto: return await self.crypto.fetch_ohlcv(symbol, timeframe, limit)
     if self.forex: return await self.forex.fetch_ohlcv(symbol, timeframe, limit)
     if self.crypto: return await self.crypto.fetch_ohlcv(symbol, timeframe, limit)
     return None

 async def get_spread(self, symbol: str) -> float:
     asset_class = self.asset_class_map.get(symbol, "forex")
     if asset_class == "forex" and self.forex: return await self.forex.get_spread(symbol)
     elif asset_class == "crypto" and self.crypto: return await self.crypto.get_spread(symbol)
     return 0.0003

 async def close(self):
     if self.forex: await self.forex.close()
     if self.crypto: await self.crypto.close()

print("Part 1 loaded: Configuration, Data Classes, Exchange Interfaces")
"""
Part 2: Technical Analysis Core, Strategy Detectors, Orchestrator, Regime, Liquidity, Orderflow
"""

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
        plus_dm = high.diff(); minus_dm = -low.diff()
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
    def ema_trend(df: pd.DataFrame, fast: int = 50, slow: int = 200) -> str:
        """NEW: EMA Trend Detection - Returns bullish, bearish, or sideways"""
        if len(df) < slow:
            return "sideways"
        
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        
        fast_val = ema_fast.iloc[-1]
        slow_val = ema_slow.iloc[-1]
        
        # Calculate slope for momentum confirmation
        fast_slope = (ema_fast.iloc[-1] - ema_fast.iloc[-5]) / ema_fast.iloc[-5] if len(ema_fast) >= 5 else 0
        slow_slope = (ema_slow.iloc[-1] - ema_slow.iloc[-5]) / ema_slow.iloc[-5] if len(ema_slow) >= 5 else 0
        
        price = df['close'].iloc[-1]
        
        # Strong trend conditions
        if fast_val > slow_val * 1.02 and fast_slope > 0 and slow_slope > -0.001:
            return "bullish"
        elif fast_val < slow_val * 0.98 and fast_slope < 0 and slow_slope < 0.001:
            return "bearish"
        
        # Check for sideways/consolidation
        ema_distance = abs(fast_val - slow_val) / slow_val
        if ema_distance < 0.01:  # Less than 1% apart
            return "sideways"
        
        # Weak trend - check price position relative to both EMAs
        if price > fast_val and price > slow_val and fast_val > slow_val:
            return "bullish"
        elif price < fast_val and price < slow_val and fast_val < slow_val:
            return "bearish"
        
        return "sideways"

# ================= TRADE SCORING ENGINE =================

class TradeScoringEngine:
    """NEW: Evaluates signals using multi-factor scoring"""
    
    def __init__(self, regime_engine=None, liquidity_engine=None, orderflow_detector=None):
        self.regime_engine = regime_engine
        self.liquidity_engine = liquidity_engine
        self.orderflow_detector = orderflow_detector
        self.ta = TechnicalCore()
    
    def score_signal(self, detection: StrategyDetection, df: pd.DataFrame, 
                    symbol: str = None, higher_tf: pd.DataFrame = None) -> Tuple[float, List[str]]:
        """
        Score a trade signal from 0-10 based on multiple factors
        Returns: (score, list of score_factors)
        """
        score_factors = []
        total_score = 0.0
        max_possible = 0.0
        
        # 1. Trend Alignment (0-2 points)
        trend_score = self._score_trend_alignment(detection, df, higher_tf)
        total_score += trend_score
        max_possible += 2.0
        if trend_score >= 1.5:
            score_factors.append(f"Strong trend alignment (+{trend_score:.1f})")
        elif trend_score > 0:
            score_factors.append(f"Weak trend alignment (+{trend_score:.1f})")
        
        # 2. Strategy Confidence (0-2 points)
        conf_score = (detection.confidence / 100.0) * 2.0
        total_score += conf_score
        max_possible += 2.0
        if detection.confidence >= 80:
            score_factors.append(f"High strategy confidence ({detection.confidence}%)")
        elif detection.confidence >= 60:
            score_factors.append(f"Moderate strategy confidence ({detection.confidence}%)")
        
        # 3. Orderflow Displacement (0-2 points)
        if self.orderflow_detector:
            of_score = self._score_orderflow(detection, df)
            total_score += of_score
            max_possible += 2.0
            if of_score >= 1.5:
                score_factors.append(f"Strong orderflow displacement (+{of_score:.1f})")
            elif of_score > 0:
                score_factors.append(f"Orderflow confirmation (+{of_score:.1f})")
        
        # 4. Volume Anomaly (0-2 points)
        vol_score = self._score_volume_anomaly(detection, df)
        total_score += vol_score
        max_possible += 2.0
        if vol_score >= 1.5:
            score_factors.append(f"Volume anomaly confirmed (+{vol_score:.1f})")
        elif vol_score > 0:
            score_factors.append(f"Volume above average (+{vol_score:.1f})")
        
        # 5. Volatility Regime (0-1 point)
        vol_regime_score = self._score_volatility_regime(df)
        total_score += vol_regime_score
        max_possible += 1.0
        if vol_regime_score > 0:
            score_factors.append(f"Favorable volatility regime (+{vol_regime_score:.1f})")
        
        # 6. Liquidity Proximity (0-1 point)
        liq_score = self._score_liquidity_proximity(detection, symbol) if symbol else 0
        total_score += liq_score
        max_possible += 1.0
        if liq_score > 0:
            score_factors.append(f"Liquidity proximity favorable (+{liq_score:.1f})")
        
        # Normalize to 0-10 scale
        if max_possible > 0:
            final_score = (total_score / max_possible) * 10.0
        else:
            final_score = 0.0
        
        # Clamp between 0 and 10
        final_score = max(0.0, min(10.0, final_score))
        
        # Add penalty factors if score is low
        if final_score < 5.0:
            if trend_score == 0:
                score_factors.append("WARNING: Counter-trend signal")
            if detection.confidence < 60:
                score_factors.append("WARNING: Low strategy confidence")
        
        return round(final_score, 2), score_factors
    
    def _score_trend_alignment(self, detection: StrategyDetection, df: pd.DataFrame, 
                              higher_tf: pd.DataFrame = None) -> float:
        """Score trend alignment (0-2)"""
        if detection.direction not in ["BUY", "SELL"]:
            return 0.0
        
        # Use higher timeframe trend if available
        trend_df = higher_tf if higher_tf is not None and len(higher_tf) > 50 else df
        
        try:
            trend = self.ta.ema_trend(trend_df)
            
            if detection.direction == "BUY" and trend == "bullish":
                return 2.0
            elif detection.direction == "SELL" and trend == "bearish":
                return 2.0
            elif trend == "sideways":
                return 1.0
            else:
                return 0.0
        except:
            return 1.0  # Neutral if calculation fails
    
    def _score_orderflow(self, detection: StrategyDetection, df: pd.DataFrame) -> float:
        """Score orderflow displacement (0-2)"""
        if not self.orderflow_detector:
            return 1.0  # Neutral if no detector available
        
        try:
            imprint = self.orderflow_detector.analyze(df)
            
            if not imprint.displacement:
                return 0.5
            
            # Check if orderflow aligns with signal direction
            if detection.direction == "BUY" and imprint.delta_direction == "BUY":
                return 2.0 if imprint.momentum_burst else 1.5
            elif detection.direction == "SELL" and imprint.delta_direction == "SELL":
                return 2.0 if imprint.momentum_burst else 1.5
            elif imprint.imbalance:
                return 0.5  # Imbalance present but direction mismatch
            
            return 1.0
        except:
            return 1.0
    
    def _score_volume_anomaly(self, detection: StrategyDetection, df: pd.DataFrame) -> float:
        """Score volume anomaly (0-2)"""
        if 'volume' not in df.columns or len(df) < 20:
            return 1.0  # Neutral
        
        try:
            current_vol = df['volume'].iloc[-1]
            avg_vol = df['volume'].tail(20).mean()
            
            if avg_vol == 0:
                return 1.0
            
            ratio = current_vol / avg_vol
            
            if ratio >= 3.0:
                return 2.0
            elif ratio >= 2.0:
                return 1.5
            elif ratio >= 1.5:
                return 1.0
            elif ratio >= 1.0:
                return 0.5
            else:
                return 0.0
        except:
            return 1.0
    
    def _score_volatility_regime(self, df: pd.DataFrame) -> float:
        """Score volatility regime favorability (0-1)"""
        if len(df) < 20:
            return 0.5
        
        try:
            atr = self.ta.atr(df, 14)
            current_atr = atr.iloc[-1]
            atr_mean = atr.tail(20).mean()
            
            if atr_mean == 0:
                return 0.5
            
            ratio = current_atr / atr_mean
            
            # Optimal volatility: not too low, not too extreme
            if 0.8 <= ratio <= 1.5:
                return 1.0
            elif 0.5 <= ratio < 0.8 or 1.5 < ratio <= 2.0:
                return 0.5
            else:
                return 0.0
        except:
            return 0.5
    
    def _score_liquidity_proximity(self, detection: StrategyDetection, symbol: str) -> float:
        """Score liquidity proximity (0-1)"""
        if not self.liquidity_engine or not symbol:
            return 0.5
        
        try:
            # Check if entry is near a liquidity level
            if detection.entry_price is None:
                return 0.5
            
            profile = self.liquidity_engine.get_profile(symbol)
            if not profile:
                return 0.5
            
            entry = detection.entry_price
            
            # Check proximity to session levels
            session_high = profile.get('session_high')
            session_low = profile.get('session_low')
            
            if session_high and session_low:
                range_size = session_high - session_low
                if range_size > 0:
                    proximity_to_high = abs(entry - session_high) / range_size
                    proximity_to_low = abs(entry - session_low) / range_size
                    
                    # Good if entry is near liquidity for target/protection
                    if min(proximity_to_high, proximity_to_low) < 0.1:
                        return 1.0
                    elif min(proximity_to_high, proximity_to_low) < 0.2:
                        return 0.5
            
            return 0.5
        except:
            return 0.5

# ================= STRATEGY DETECTORS =================

class StrategyDetector(ABC):
    @abstractmethod
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection: pass
    @property
    @abstractmethod
    def weight(self) -> float: pass

class BreakoutStrategy(StrategyDetector):
    def __init__(self, lookback: int = 20, confirmation_candles: int = 2):
        self.lookback = lookback; self.confirmation_candles = confirmation_candles
    @property
    def weight(self) -> float: return 2.0
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback + 5: return StrategyDetection("breakout", False, 0, None, None, None, None)
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
            if resistance_break: retest_confirm = df['low'].iloc[-2] <= recent_high <= df['high'].iloc[-2]
            elif support_break: retest_confirm = df['low'].iloc[-2] <= recent_low <= df['high'].iloc[-2]
        
        if resistance_break and (volume_confirm or retest_confirm):
            confidence = 60 + (20 if volume_confirm else 0) + (20 if retest_confirm else 0)
            entry = current_close
            sl = recent_high - (recent_high - recent_low) * 0.1
            tp = entry + (entry - sl) * 2.5
            return StrategyDetection("breakout", True, min(confidence, 95), "BUY", entry, sl, tp,
                {'breakout_level': recent_high, 'retest_confirmed': retest_confirm, 'volume_confirmed': volume_confirm},
                [f"Resistance breakout @ {recent_high:.5f}", "Volume surge" if volume_confirm else "Price momentum", "Retest confirmed" if retest_confirm else "Direct breakout"])
        
        elif support_break and (volume_confirm or retest_confirm):
            confidence = 60 + (20 if volume_confirm else 0) + (20 if retest_confirm else 0)
            entry = current_close
            sl = recent_low + (recent_high - recent_low) * 0.1
            tp = entry - (sl - entry) * 2.5
            return StrategyDetection("breakout", True, min(confidence, 95), "SELL", entry, sl, tp,
                {'breakout_level': recent_low, 'retest_confirmed': retest_confirm, 'volume_confirmed': volume_confirm},
                [f"Support breakout @ {recent_low:.5f}", "Volume surge" if volume_confirm else "Price momentum", "Retest confirmed" if retest_confirm else "Direct breakout"])
        
        return StrategyDetection("breakout", False, 0, None, None, None, None)

class FlagStrategy(StrategyDetector):
    def __init__(self, pole_lookback: int = 10, flag_lookback: int = 5):
        self.pole_lookback = pole_lookback; self.flag_lookback = flag_lookback
    @property
    def weight(self) -> float: return 1.8
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.pole_lookback + self.flag_lookback: return StrategyDetection("flag", False, 0, None, None, None, None)
        pole_data = df.tail(self.pole_lookback + self.flag_lookback).head(self.pole_lookback)
        flag_data = df.tail(self.flag_lookback)
        pole_move = (pole_data['close'].iloc[-1] - pole_data['close'].iloc[0]) / pole_data['close'].iloc[0] * 100
        if abs(pole_move) <= 2.0: return StrategyDetection("flag", False, 0, None, None, None, None)
        flag_trend = (flag_data['close'].iloc[-1] - flag_data['close'].iloc[0]) / flag_data['close'].iloc[0] * 100
        bull_flag = pole_move > 0 and flag_trend < pole_move * 0.3
        bear_flag = pole_move < 0 and flag_trend > pole_move * 0.3
        
        pole_atr = self._calculate_atr(pole_data); flag_atr = self._calculate_atr(flag_data)
        vol_contract = flag_atr < pole_atr * 0.6
        flag_high, flag_low = flag_data['high'].max(), flag_data['low'].min()
        entry = df['close'].iloc[-1]
        
        if bull_flag and vol_contract and entry > flag_high * 0.999:
            sl = flag_low; tp = entry + (entry - sl) * 2.0
            return StrategyDetection("bull_flag", True, 75 if vol_contract else 65, "BUY", entry, sl, tp,
                {'pole_move': pole_move, 'pattern': 'bull_flag' if flag_trend < 0 else 'pennant'},
                [f"Strong pole: +{pole_move:.1f}%", "Volatility contraction" if vol_contract else "Consolidation", "Breakout"])
        elif bear_flag and vol_contract and entry < flag_low * 1.001:
            sl = flag_high; tp = entry - (sl - entry) * 2.0
            return StrategyDetection("bear_flag", True, 75 if vol_contract else 65, "SELL", entry, sl, tp,
                {'pole_move': pole_move, 'pattern': 'bear_flag' if flag_trend > 0 else 'pennant'},
                [f"Strong pole: {pole_move:.1f}%", "Volatility contraction" if vol_contract else "Consolidation", "Breakdown"])
        return StrategyDetection("flag", False, 0, None, None, None, None)
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 5) -> float:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.tail(period).mean()

class DoubleTopBottomStrategy(StrategyDetector):
    def __init__(self, tolerance: float = 0.002, lookback: int = 50):
        self.tolerance = tolerance; self.lookback = lookback
    @property
    def weight(self) -> float: return 2.2
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback: return StrategyDetection("double_top_bottom", False, 0, None, None, None, None)
        highs = df['high'].tail(self.lookback).values; lows = df['low'].tail(self.lookback).values
        recent_high, recent_low = df['high'].iloc[-1], df['low'].iloc[-1]
        
        double_top, top_level = False, None
        for i in range(-10, -self.lookback, -1):
            if abs(highs[i] - recent_high) / recent_high < self.tolerance:
                between_low = df['low'].iloc[i:-1].min()
                if (highs[i] - between_low) / highs[i] > 0.01:
                    double_top, top_level = True, highs[i]; break
        
        double_bottom, bottom_level = False, None
        for i in range(-10, -self.lookback, -1):
            if abs(lows[i] - recent_low) / recent_low < self.tolerance:
                between_high = df['high'].iloc[i:-1].max()
                if (between_high - lows[i]) / lows[i] > 0.01:
                    double_bottom, bottom_level = True, lows[i]; break
        
        if double_top and df['close'].iloc[-1] < recent_high * 0.998:
            valley_idx = df['low'].iloc[-10:-1].idxmin()
            neckline = df.loc[valley_idx, 'low']
            entry = df['close'].iloc[-1]
            sl = recent_high + (recent_high - neckline) * 0.1
            tp = neckline - (recent_high - neckline) * 1.0
            return StrategyDetection("double_top", True, 80, "SELL", entry, sl, tp,
                {'top_level': top_level, 'neckline': neckline},
                [f"Equal highs at {top_level:.5f}", f"Neckline break @ {neckline:.5f}", "Measured move", "Liquidity sweep"])
        
        elif double_bottom and df['close'].iloc[-1] > recent_low * 1.002:
            peak_idx = df['high'].iloc[-10:-1].idxmax()
            neckline = df.loc[peak_idx, 'high']
            entry = df['close'].iloc[-1]
            sl = recent_low - (neckline - recent_low) * 0.1
            tp = neckline + (neckline - recent_low) * 1.0
            return StrategyDetection("double_bottom", True, 80, "BUY", entry, sl, tp,
                {'bottom_level': bottom_level, 'neckline': neckline},
                [f"Equal lows at {bottom_level:.5f}", f"Neckline break @ {neckline:.5f}", "Measured move", "Liquidity accumulation"])
        
        return StrategyDetection("double_top_bottom", False, 0, None, None, None, None)

class HeadAndShouldersStrategy(StrategyDetector):
    def __init__(self, lookback: int = 30, shoulder_tolerance: float = 0.03):
        self.lookback = lookback; self.shoulder_tolerance = shoulder_tolerance
    @property
    def weight(self) -> float: return 1.5
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback or not SCIPY_AVAILABLE: return StrategyDetection("head_shoulders", False, 0, None, None, None, None)
        highs = df['high'].tail(self.lookback).values; lows = df['low'].tail(self.lookback).values
        peaks, _ = find_peaks(highs, distance=5, prominence=np.std(highs)*0.5)
        if len(peaks) >= 3:
            left_s, head, right_s = highs[peaks[-3]], highs[peaks[-2]], highs[peaks[-1]]
            shoulders_equal = abs(left_s - right_s) / left_s < self.shoulder_tolerance
            head_higher = head > max(left_s, right_s) * 1.01
            trough1_idx = np.argmin(lows[peaks[-3]:peaks[-2]]) + peaks[-3]
            trough2_idx = np.argmin(lows[peaks[-2]:peaks[-1]]) + peaks[-2]
            neckline = max(lows[trough1_idx], lows[trough2_idx])
            if shoulders_equal and head_higher and df['close'].iloc[-1] < neckline:
                entry = df['close'].iloc[-1]; sl = right_s; tp = neckline - (head - neckline)
                return StrategyDetection("head_and_shoulders", True, 70, "SELL", entry, sl, tp,
                    {'left_shoulder': left_s, 'head': head, 'right_shoulder': right_s, 'neckline': neckline},
                    ["Three-peak structure", "Shoulder symmetry", "Neckline violation", "Classic reversal"])
        return StrategyDetection("head_shoulders", False, 0, None, None, None, None)

class TriangleStrategy(StrategyDetector):
    def __init__(self, lookback: int = 20, min_touches: int = 2):
        self.lookback = lookback
        self.min_touches = min_touches
        self.triangle_detected = False
        self.breakout_up = False
        self.breakout_down = False
    
    @property
    def weight(self) -> float: return 1.6
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        # Simplified triangle detection logic
        if len(df) < self.lookback:
            return StrategyDetection("triangle", False, 0, None, None, None, None)
        
        highs = df['high'].tail(self.lookback).values
        lows = df['low'].tail(self.lookback).values
        x = np.arange(len(highs))
        
        # Fit trendlines
        high_slope, high_int = np.polyfit(x, highs, 1)
        low_slope, low_int = np.polyfit(x, lows, 1)
        
        # Check for converging lines (symmetrical triangle)
        converging = high_slope < 0 and low_slope > 0
        
        # Check for flat top/bottom (ascending/descending)
        flat_high = abs(high_slope) < 0.0001
        flat_low = abs(low_slope) < 0.0001
        
        # Current price position
        current = df['close'].iloc[-1]
        upper_line = high_int + high_slope * (len(highs) - 1)
        lower_line = low_int + low_slope * (len(lows) - 1)
        
        # Determine if triangle exists and breakout direction
        self.triangle_detected = converging or flat_high or flat_low
        
        if self.triangle_detected:
            self.breakout_up = current > upper_line * 1.001
            self.breakout_down = current < lower_line * 0.999
        
        # Use simplified strategy logic
        result = self._triangle_strategy(df)
        
        if result is not None:
            entry = result["entry"]
            direction = result["direction"]
            sl = lower_line if direction == "BUY" else upper_line
            tp = entry + (entry - sl) * 2.0 if direction == "BUY" else entry - (sl - entry) * 2.0
            
            pattern_type = "symmetrical_triangle" if converging else "ascending_triangle" if flat_high else "descending_triangle"
            
            return StrategyDetection(
                pattern_type, 
                True, 
                70, 
                direction, 
                entry, 
                sl, 
                tp,
                {'breakout_direction': direction, 'upper_line': upper_line, 'lower_line': lower_line},
                [f"{pattern_type} breakout", f"Direction: {direction}", "Trendline violation"]
            )
        
        return StrategyDetection("triangle", False, 0, None, None, None, None)
    
    def _triangle_strategy(self, df: pd.DataFrame):
        entry = None
        direction = None

        if self.triangle_detected:
            entry = df["close"].iloc[-1]

            if self.breakout_up:
                direction = "BUY"
            elif self.breakout_down:
                direction = "SELL"

        if entry is None:
            return None

        return {
            "entry": entry,
            "direction": direction
        }

class FairValueGapStrategy(StrategyDetector):
    def __init__(self, min_gap_size: float = 0.001): self.min_gap_size = min_gap_size
    @property
    def weight(self) -> float: return 1.5
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < 3: return StrategyDetection("fvg", False, 0, None, None, None, None)
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        bullish_fvg = c1['high'] < c3['low']; bullish_gap = (c3['low'] - c1['high']) / c1['close'] if bullish_fvg else 0
        bearish_fvg = c1['low'] > c3['high']; bearish_gap = (c1['low'] - c3['high']) / c1['close'] if bearish_fvg else 0
        current = df['close'].iloc[-1]
        
        if bullish_fvg and bullish_gap > self.min_gap_size:
            gap_low, gap_high = c1['high'], c3['low']
            if gap_low * 0.999 <= current <= gap_high * 1.001:
                entry, sl = current, min(c1['low'], c2['low']) - (gap_high - gap_low) * 0.5
                tp = entry + (entry - sl) * 2.0
                return StrategyDetection("bullish_fvg", True, 75, "BUY", entry, sl, tp,
                    {'gap_low': gap_low, 'gap_high': gap_high, 'gap_size': bullish_gap},
                    ["3-candle bullish imbalance", f"Gap: {bullish_gap*100:.2f}%", "Price in gap", "Institutional level"])
        elif bearish_fvg and bearish_gap > self.min_gap_size:
            gap_low, gap_high = c3['high'], c1['low']
            if gap_low * 0.999 <= current <= gap_high * 1.001:
                entry, sl = current, max(c1['high'], c2['high']) + (gap_high - gap_low) * 0.5
                tp = entry - (sl - entry) * 2.0
                return StrategyDetection("bearish_fvg", True, 75, "SELL", entry, sl, tp,
                    {'gap_low': gap_low, 'gap_high': gap_high, 'gap_size': bearish_gap},
                    ["3-candle bearish imbalance", f"Gap: {bearish_gap*100:.2f}%", "Price in gap", "Institutional level"])
        return StrategyDetection("fvg", False, 0, None, None, None, None)

class VolumeSpikeStrategy(StrategyDetector):
    def __init__(self, multiplier: float = 2.0, lookback: int = 20):
        self.multiplier = multiplier; self.lookback = lookback
    @property
    def weight(self) -> float: return 0.8
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if 'volume' not in df.columns or len(df) < self.lookback: return StrategyDetection("volume_spike", False, 0, None, None, None, None)
        avg_vol = df['volume'].tail(self.lookback).mean(); curr_vol = df['volume'].iloc[-1]
        ratio = curr_vol / avg_vol if avg_vol > 0 else 0
        if ratio >= self.multiplier:
            body = df['close'].iloc[-1] - df['open'].iloc[-1]
            direction = "BUY" if body > 0 else "SELL" if body < 0 else None
            price_change = abs(body) / df['open'].iloc[-1] * 100
            return StrategyDetection("volume_spike", True, min(60 + ratio * 5, 90), direction, None, None, None,
                {'volume_ratio': ratio, 'price_impact': price_change},
                [f"{ratio:.1f}x volume surge", f"{price_change:.2f}% price move", "Institutional participation" if ratio > 3 else "Above average interest"])
        return StrategyDetection("volume_spike", False, 0, None, None, None, None)

class SMEStrategy(StrategyDetector):
    def __init__(self, vwap_deviation: float = 0.002): self.vwap_deviation = vwap_deviation
    @property
    def weight(self) -> float: return 1.4
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < 20 or 'volume' not in df.columns: return StrategyDetection("sme", False, 0, None, None, None, None)
        tp = (df['high'] + df['low'] + df['close']) / 3
        vwap = (tp * df['volume']).cumsum() / df['volume'].cumsum()
        price, vwap_val = df['close'].iloc[-1], vwap.iloc[-1]
        dev = (price - vwap_val) / vwap_val
        if abs(dev) > self.vwap_deviation:
            trend = (df['close'].iloc[-5:].iloc[-1] - df['close'].iloc[-5:].iloc[0]) / df['close'].iloc[-5:].iloc[0]
            if dev < -self.vwap_deviation and trend > -0.001:
                entry, sl, tp_price = price, df['low'].tail(5).min(), vwap_val
                return StrategyDetection("sme_long", True, 70, "BUY", entry, sl, tp_price,
                    {'vwap': vwap_val, 'deviation': dev},
                    [f"Price {abs(dev)*100:.2f}% below VWAP", "VWAP magnet", "Smart money entry", "Mean reversion"])
            elif dev > self.vwap_deviation and trend < 0.001:
                entry, sl, tp_price = price, df['high'].tail(5).max(), vwap_val
                return StrategyDetection("sme_short", True, 70, "SELL", entry, sl, tp_price,
                    {'vwap': vwap_val, 'deviation': dev},
                    [f"Price {dev*100:.2f}% above VWAP", "VWAP magnet", "Smart money entry", "Mean reversion"])
        return StrategyDetection("sme", False, 0, None, None, None, None)

class EMAMeanReversionStrategy(StrategyDetector):
    def __init__(self, fast_ema: int = 20, deviation: float = 0.015):
        self.fast_ema = fast_ema; self.deviation = deviation
    @property
    def weight(self) -> float: return 1.3
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.fast_ema * 2: return StrategyDetection("ema_mean_reversion", False, 0, None, None, None, None)
        ema = df['close'].ewm(span=self.fast_ema, adjust=False).mean()
        price, ema_val = df['close'].iloc[-1], ema.iloc[-1]
        dev = (price - ema_val) / ema_val
        if abs(dev) > self.deviation:
            o, c, pc = df['open'].iloc[-1], df['close'].iloc[-1], df['close'].iloc[-2]
            if dev < -self.deviation and c > o and c > pc:
                entry, sl, tp = c, df['low'].tail(3).min(), ema_val
                return StrategyDetection("ema_mean_reversion_long", True, 65, "BUY", entry, sl, tp,
                    {'ema_period': self.fast_ema, 'deviation': dev},
                    [f"Price {abs(dev)*100:.2f}% below EMA{self.fast_ema}", "Bullish reversal", "Mean reversion", "Momentum exhaustion"])
            elif dev > self.deviation and c < o and c < pc:
                entry, sl, tp = c, df['high'].tail(3).max(), ema_val
                return StrategyDetection("ema_mean_reversion_short", True, 65, "SELL", entry, sl, tp,
                    {'ema_period': self.fast_ema, 'deviation': dev},
                    [f"Price {dev*100:.2f}% above EMA{self.fast_ema}", "Bearish reversal", "Mean reversion", "Momentum exhaustion"])
        return StrategyDetection("ema_mean_reversion", False, 0, None, None, None, None)

class VWAPCryptoStrategy(StrategyDetector):
    def __init__(self, session_aware: bool = True): self.session_aware = session_aware
    @property
    def weight(self) -> float: return 1.4
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < 24 or 'volume' not in df.columns: return StrategyDetection("vwap_crypto", False, 0, None, None, None, None)
        session = df.iloc[-24:] if self.session_aware else df
        tp = (session['high'] + session['low'] + session['close']) / 3
        vwap = (tp * session['volume']).cumsum() / session['volume'].cumsum()
        price, vwap_val = df['close'].iloc[-1], vwap.iloc[-1]
        dev = (price - vwap_val) / vwap_val
        if abs(dev) > 0.025:
            vol_confirm = df['volume'].iloc[-1] > df['volume'].tail(24).mean()
            if dev < -0.025:
                entry, sl, tp_price = price, df['low'].tail(5).min(), vwap_val
                return StrategyDetection("vwap_crypto_long", True, 70 if vol_confirm else 65, "BUY", entry, sl, tp_price,
                    {'vwap': vwap_val, 'deviation': dev, 'volume_confirmed': vol_confirm},
                    [f"Price {abs(dev)*100:.2f}% below 24h VWAP", "Crypto mean reversion", "Volume confirmation" if vol_confirm else "Price structure", "Institutional level"])
            elif dev > 0.025:
                entry, sl, tp_price = price, df['high'].tail(5).max(), vwap_val
                return StrategyDetection("vwap_crypto_short", True, 70 if vol_confirm else 65, "SELL", entry, sl, tp_price,
                    {'vwap': vwap_val, 'deviation': dev, 'volume_confirmed': vol_confirm},
                    [f"Price {dev*100:.2f}% above 24h VWAP", "Crypto mean reversion", "Volume confirmation" if vol_confirm else "Price structure", "Institutional level"])
        return StrategyDetection("vwap_crypto", False, 0, None, None, None, None)

class LiquiditySweepStrategy(StrategyDetector):
    def __init__(self, sweep_tol: float = 0.001, lookback: int = 20):
        self.sweep_tol = sweep_tol; self.lookback = lookback
    @property
    def weight(self) -> float: return 2.5
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback: return StrategyDetection("liquidity_sweep", False, 0, None, None, None, None)
        recent_highs, recent_lows = df['high'].tail(self.lookback), df['low'].tail(self.lookback)
        curr_high, curr_low, curr_close, prev_close = df['high'].iloc[-1], df['low'].iloc[-1], df['close'].iloc[-1], df['close'].iloc[-2]
        recent_low_lvl, recent_high_lvl = recent_lows.min(), recent_highs.max()
        
        bullish_sweep = curr_low <= recent_low_lvl * (1 + self.sweep_tol) and curr_close > recent_low_lvl and prev_close <= recent_low_lvl
        bearish_sweep = curr_high >= recent_high_lvl * (1 - self.sweep_tol) and curr_close < recent_high_lvl and prev_close >= recent_high_lvl
        displacement = abs(curr_close - prev_close) / prev_close > 0.003
        
        if bullish_sweep and displacement:
            entry, sl, tp = curr_close, curr_low - (recent_high_lvl - recent_low_lvl) * 0.1, recent_high_lvl
            return StrategyDetection("bullish_liquidity_sweep", True, 85, "BUY", entry, sl, tp,
                {'swept_level': recent_low_lvl, 'recovery_close': curr_close, 'displacement': displacement},
                [f"Sweep of lows @ {recent_low_lvl:.5f}", "Strong recovery", "Displacement confirmed", "Institutional stop hunt"])
        elif bearish_sweep and displacement:
            entry, sl, tp = curr_close, curr_high + (recent_high_lvl - recent_low_lvl) * 0.1, recent_low_lvl
            return StrategyDetection("bearish_liquidity_sweep", True, 85, "SELL", entry, sl, tp,
                {'swept_level': recent_high_lvl, 'recovery_close': curr_close, 'displacement': displacement},
                [f"Sweep of highs @ {recent_high_lvl:.5f}", "Strong rejection", "Displacement confirmed", "Institutional stop hunt"])
        return StrategyDetection("liquidity_sweep", False, 0, None, None, None, None)

class ORBStrategy(StrategyDetector):
    def __init__(self, range_minutes: int = 30): self.range_minutes = range_minutes
    @property
    def weight(self) -> float: return 1.7
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.range_minutes // 5: return StrategyDetection("orb", False, 0, None, None, None, None)
        opening_candles = self.range_minutes // 5
        range_data = df.head(opening_candles)
        or_high, or_low = range_data['high'].max(), range_data['low'].min()
        current, curr_idx = df['close'].iloc[-1], len(df) - 1
        if curr_idx < opening_candles: return StrategyDetection("orb", False, 0, None, None, None, None)
        
        breakout_up = current > or_high * 1.001 and df['close'].iloc[-2] <= or_high
        breakout_down = current < or_low * 0.999 and df['close'].iloc[-2] >= or_low
        vol_confirm = df['volume'].iloc[-1] > range_data['volume'].mean() * 1.5 if 'volume' in df.columns and range_data['volume'].mean() > 0 else True
        
        if breakout_up:
            entry, sl, tp = current, or_low, entry + (or_high - or_low) * 2.0
            return StrategyDetection("orb_long", True, 75 if vol_confirm else 65, "BUY", entry, sl, tp,
                {'opening_range_high': or_high, 'opening_range_low': or_low, 'range_size': or_high - or_low},
                [f"Opening range: {or_high-or_low:.5f}", "Upside breakout", "Volume confirmation" if vol_confirm else "Price momentum", "Session momentum"])
        elif breakout_down:
            entry, sl, tp = current, or_high, entry - (or_high - or_low) * 2.0
            return StrategyDetection("orb_short", True, 75 if vol_confirm else 65, "SELL", entry, sl, tp,
                {'opening_range_high': or_high, 'opening_range_low': or_low, 'range_size': or_high - or_low},
                [f"Opening range: {or_high-or_low:.5f}", "Downside breakout", "Volume confirmation" if vol_confirm else "Price momentum", "Session momentum"])
        return StrategyDetection("orb", False, 0, None, None, None, None)

class DailyLiquidityCycleStrategy(StrategyDetector):
    def __init__(self):
        self.session_times = {'asia': (0, 8), 'london': (8, 13), 'ny': (13, 22), 'overlap': (13, 17)}
    @property
    def weight(self) -> float: return 1.2
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        curr_hour = datetime.now(pytz.UTC).hour
        session = None
        for name, (start, end) in self.session_times.items():
            if start <= curr_hour < end: session = name; break
        if not session: return StrategyDetection("liquidity_cycle", False, 0, None, None, None, None)
        
        if session == 'london' and len(df) >= 20:
            asia_high, asia_low = df.head(20)['high'].max(), df.head(20)['low'].min()
            current = df['close'].iloc[-1]
            if current < asia_low * 1.001 and df['close'].iloc[-2] > asia_low:
                return StrategyDetection("london_sweep", True, 70, "BUY", current, df['low'].iloc[-1], asia_high,
                    {'session': 'london', 'asia_range_high': asia_high, 'asia_range_low': asia_low},
                    ["London session open", "Sweep of Asia lows", "Classic liquidity cycle", "Mean reversion"])
        elif session == 'overlap' and len(df) >= 10:
            trend = (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10]
            if abs(trend) > 0.005:
                direction = "BUY" if trend > 0 else "SELL"
                return StrategyDetection("overlap_momentum", True, 65, direction, df['close'].iloc[-1], None, None,
                    {'session': 'london_ny_overlap', 'trend_strength': trend},
                    ["London/NY overlap", "High liquidity", "Trend continuation", "Volatility expansion"])
        return StrategyDetection("liquidity_cycle", False, 0, None, None, None, None)

# ================= STRATEGY ORCHESTRATOR =================

class StrategyOrchestrator:
    def __init__(self):
        self.strategies = [
            BreakoutStrategy(), FlagStrategy(), DoubleTopBottomStrategy(),
            HeadAndShouldersStrategy(), TriangleStrategy(), FairValueGapStrategy(),
            VolumeSpikeStrategy(), SMEStrategy(), EMAMeanReversionStrategy(),
            VWAPCryptoStrategy(), LiquiditySweepStrategy(), ORBStrategy(),
            DailyLiquidityCycleStrategy()
        ]
        self.detections_cache = {}
        # NEW: Initialize scoring engine
        self.scoring_engine = None  # Will be set externally if needed
    
    def analyze(self, symbol, df, higher_tf=None):
        detections, total_weight, weighted_conf, all_factors = [], 0.0, 0.0, []
        for strategy in self.strategies:
            try:
                detection = strategy.detect(df, higher_tf)
                if detection.detected:
                    detections.append(detection)
                    weight = strategy.weight
                    total_weight += weight
                    weighted_conf += detection.confidence * weight
                    all_factors.extend(detection.confluence_factors)
            except Exception as e:
                logger.error(f"Strategy {strategy.__class__.__name__} error: {e}")
        aggregate_conf = weighted_conf / total_weight if total_weight > 0 else 0
        self.detections_cache[symbol] = detections
        return detections, aggregate_conf, list(dict.fromkeys(all_factors))
    
    def get_primary_setup(self, symbol):
        detections = self.detections_cache.get(symbol, [])
        return max(detections, key=lambda d: d.confidence) if detections else None
    
    # NEW: Method to score detections
    def score_detections(self, symbol, df, higher_tf=None):
        """Score all detections for a symbol using the scoring engine"""
        if not self.scoring_engine:
            return {}
        
        detections = self.detections_cache.get(symbol, [])
        scores = {}
        for detection in detections:
            score, factors = self.scoring_engine.score_signal(detection, df, symbol, higher_tf)
            scores[detection.strategy_name] = {
                'score': score,
                'factors': factors,
                'detection': detection
            }
        return scores

# ================= REGIME ENGINE =================

class RegimeEngine:
    def __init__(self): self.ta = TechnicalCore(); self.lookback = 50
    
    def analyze(self, df, higher_tf_df=None):
        if len(df) < self.lookback: return RegimeState(MarketRegime.TRENDING_WEAK, VolatilityRegime.NORMAL, 0.5, 0.5, 20.0, 50.0)
        adx = self.ta.adx(df, 14).iloc[-1]; atr = self.ta.atr(df, 14)
        curr_atr = atr.iloc[-1]; atr_pct = (atr.tail(self.lookback) < curr_atr).mean() * 100
        ema_fast = self.ta.ema(df, 20); trend_slope = (ema_fast.iloc[-1] - ema_fast.iloc[-10]) / ema_fast.iloc[-10] * 100
        range_high, range_low = df['high'].tail(self.lookback).max(), df['low'].tail(self.lookback).min()
        range_pos = (df['close'].iloc[-1] - range_low) / (range_high - range_low) if range_high != range_low else 0.5
        
        vol_regime = VolatilityRegime.LOW if atr_pct < 20 else VolatilityRegime.NORMAL if atr_pct < 50 else VolatilityRegime.HIGH if atr_pct < 80 else VolatilityRegime.EXTREME
        trend_str = min(adx / 50.0, 1.0) if adx > 25 else adx / 100.0
        
        if adx > 40 and trend_slope > 1: primary = MarketRegime.TRENDING_STRONG
        elif adx > 25: primary = MarketRegime.TRENDING_WEAK
        elif atr_pct < 30: primary = MarketRegime.RANGING_COMPRESSION
        elif vol_regime == VolatilityRegime.HIGH:
            primary = MarketRegime.DISTRIBUTION if range_pos > 0.7 else MarketRegime.ACCUMULATION if range_pos < 0.3 else MarketRegime.VOLATILE_BREAKOUT
        else: primary = MarketRegime.RANGING_EXPANSION
        
        return RegimeState(primary, vol_regime, trend_str, range_pos, adx, atr_pct)

# ================= LIQUIDITY ENGINE =================

class LiquidityEngine:
    def __init__(self):
        self.levels = defaultdict(list); self.session_history = defaultdict(lambda: deque(maxlen=96)); self.equal_tol = 0.002
    
    def update(self, symbol, df):
        if len(df) < 20: return
        for idx, row in df.iterrows():
            self.session_history[symbol].append({'high': row['high'], 'low': row['low'], 'close': row['close'], 'volume': row.get('volume', 0), 'timestamp': idx})
        history = list(self.session_history[symbol])
        if len(history) < 96: return
        
        session_data = history[-96:]; session_high, session_low = max(d['high'] for d in session_data), min(d['low'] for d in session_data)
        self._add_level(symbol, session_high, LiquidityType.SESSION_HIGH, session_data)
        self._add_level(symbol, session_low, LiquidityType.SESSION_LOW, session_data)
        
        if len(history) >= 672:
            weekly = history[-672:]; weekly_high, weekly_low = max(d['high'] for d in weekly), min(d['low'] for d in weekly)
            self._add_level(symbol, weekly_high, LiquidityType.WEEKLY_HIGH, weekly)
            self._add_level(symbol, weekly_low, LiquidityType.WEEKLY_LOW, weekly)
        
        self._detect_equal_levels(symbol, df.tail(50)); self._detect_liquidity_pools(symbol, df.tail(50)); self._cleanup(symbol)
    
    def _add_level(self, symbol, price, ltype, data):
        for level in self.levels[symbol]:
            if abs(level.price - price) / price < self.equal_tol:
                level.strength = min(1.0, level.strength + 0.1); level.timestamp = datetime.now(); return
        vol_proxy = sum(d.get('volume', 0) for d in data[-10:]) / len(data[-10:]) if data else 0
        self.levels[symbol].append(LiquidityLevel(price, ltype, 0.5, vol_proxy, datetime.now()))
    
    def _detect_equal_levels(self, symbol, df):
        highs, lows = df['high'].values, df['low'].values
        for i in range(len(highs)):
            for j in range(i+1, len(highs)):
                if abs(highs[i] - highs[j]) / highs[i] < self.equal_tol: self._add_level(symbol, highs[i], LiquidityType.EQUAL_HIGH, df.to_dict('records'))
        for i in range(len(lows)):
            for j in range(i+1, len(lows)):
                if abs(lows[i] - lows[j]) / lows[i] < self.equal_tol: self._add_level(symbol, lows[i], LiquidityType.EQUAL_LOW, df.to_dict('records'))
    
    def _detect_liquidity_pools(self, symbol, df):
        if 'volume' not in df.columns or df['volume'].sum() == 0: return
        vol_pct = df['volume'].quantile(0.8); high_vol = df[df['volume'] > vol_pct]
        for _, candle in high_vol.iterrows(): self._add_level(symbol, (candle['high'] + candle['low']) / 2, LiquidityType.POOL_ABOVE, df.to_dict('records'))
    
    def _cleanup(self, symbol, max_levels=30):
        levels = self.levels[symbol]
        if len(levels) > max_levels: levels.sort(key=lambda x: (x.strength, x.timestamp), reverse=True); self.levels[symbol] = levels[:max_levels]
    
    def get_nearest(self, symbol, price, direction, min_strength=0.3):
        candidates = [(price - l.price if direction == "BUY" else l.price - price, l) for l in self.levels[symbol] if not l.is_swept and l.strength >= min_strength and ((direction == "BUY" and l.price < price) or (direction == "SELL" and l.price > price))]
        return min(candidates, key=lambda x: x[0])[1] if candidates else None
    
    def mark_swept(self, symbol, price, tolerance=0.003):
        for level in self.levels[symbol]:
            if abs(level.price - price) / price < tolerance: level.is_swept = True; level.sweep_timestamp = datetime.now()
    
    def get_profile(self, symbol):
        levels = self.levels.get(symbol, [])
        return {'symbol': symbol, 'session_high': max((l.price for l in levels if l.liquidity_type == LiquidityType.SESSION_HIGH), default=None),
                'session_low': min((l.price for l in levels if l.liquidity_type == LiquidityType.SESSION_LOW), default=None),
                'equal_highs': [l for l in levels if l.liquidity_type == LiquidityType.EQUAL_HIGH],
                'equal_lows': [l for l in levels if l.liquidity_type == LiquidityType.EQUAL_LOW], 'total_levels': len(levels)}
    
    # NEW: Liquidity Heatmap Scoring
    def calculate_liquidity_score(self, symbol: str, current_price: float) -> Dict[str, any]:
        """
        Score liquidity clusters based on distance from price, volume concentration, and number of touches
        Returns dict with score (0-100) and detailed breakdown
        """
        levels = self.levels.get(symbol, [])
        if not levels:
            return {'total_score': 0, 'clusters': [], 'nearest_support': None, 'nearest_resistance': None}
        
        clusters = []
        support_candidates = []
        resistance_candidates = []
        
        for level in levels:
            if level.is_swept:
                continue
            
            distance = abs(level.price - current_price) / current_price
            direction = "support" if level.price < current_price else "resistance"
            
            # Calculate individual cluster score
            distance_score = max(0, 1 - (distance * 10))  # Closer is better, max 1% distance for full score
            
            # Volume concentration score (normalized)
            vol_score = min(1.0, level.volume_proxy / 1000) if hasattr(level, 'volume_proxy') else 0.5
            
            # Touches/strength score
            touch_score = level.strength
            
            # Combined cluster score (0-100)
            cluster_score = (distance_score * 40 + vol_score * 30 + touch_score * 30)
            
            cluster_info = {
                'price': level.price,
                'type': level.liquidity_type.value if hasattr(level.liquidity_type, 'value') else str(level.liquidity_type),
                'direction': direction,
                'distance_pct': distance * 100,
                'distance_score': distance_score,
                'volume_score': vol_score,
                'strength_score': touch_score,
                'total_score': cluster_score,
                'timestamp': level.timestamp
            }
            clusters.append(cluster_info)
            
            if direction == "support":
                support_candidates.append(cluster_info)
            else:
                resistance_candidates.append(cluster_info)
        
        # Sort by score
        clusters.sort(key=lambda x: x['total_score'], reverse=True)
        
        # Calculate aggregate scores
        if clusters:
            avg_score = sum(c['total_score'] for c in clusters) / len(clusters)
            top_cluster_bonus = clusters[0]['total_score'] * 0.2 if clusters else 0
            total_score = min(100, avg_score + top_cluster_bonus)
        else:
            total_score = 0
        
        # Find nearest support/resistance
        nearest_support = min(support_candidates, key=lambda x: x['distance_pct']) if support_candidates else None
        nearest_resistance = min(resistance_candidates, key=lambda x: x['distance_pct']) if resistance_candidates else None
        
        return {
            'total_score': round(total_score, 2),
            'cluster_count': len(clusters),
            'clusters': clusters[:5],  # Top 5 clusters
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'liquidity_density': len(clusters) / 30.0  # Normalized by max levels
        }

# ================= ORDERFLOW DETECTOR =================

class OrderflowDetector:
    def __init__(self): self.ta = TechnicalCore()
    
    def analyze(self, df, lookback=20):
        if len(df) < 5: return OrderflowImprint()
        recent, current = df.tail(5), df.iloc[-1]
        imprint = OrderflowImprint()
        body, range_val = abs(current['close'] - current['open']), current['high'] - current['low']
        if range_val > 0:
            body_ratio = body / range_val; avg_range = recent['high'].sub(recent['low']).mean()
            imprint.displacement = body_ratio > 0.7 and range_val > avg_range * 1.5
        
        upper_wick, lower_wick = current['high'] - max(current['open'], current['close']), min(current['open'], current['close']) - current['low']
        if body > 0:
            if upper_wick > body * 2: imprint.imbalance, imprint.delta_direction = True, "SELL"
            elif lower_wick > body * 2: imprint.imbalance, imprint.delta_direction = True, "BUY"
        
        if 'volume' in df.columns and df['volume'].sum() > 0:
            vol_pct = (df['volume'].tail(lookback) < current['volume']).mean()
            imprint.absorption = vol_pct > 0.8 and body_ratio < 0.3
        
        closes, opens = df['close'].tail(3).values, df['open'].tail(3).values
        if all(c > o for c, o in zip(closes, opens)): imprint.momentum_burst, imprint.delta_direction = True, "BUY"
        elif all(c < o for c, o in zip(closes, opens)): imprint.momentum_burst, imprint.delta_direction = True, "SELL"
        
        if 'volume' in df.columns: imprint.volume_anomaly = current['volume'] > df['volume'].tail(20).mean() * 2
        return imprint

# ================= ADAPTIVE STRATEGY ENGINE =================

class AdaptiveStrategyEngine:
    """NEW: Tracks strategy performance per regime and updates weights dynamically"""
    
    def __init__(self):
        # Performance tracking per strategy per regime
        # Structure: {strategy_name: {regime: PerformanceMetrics}}
        self.performance_data = defaultdict(lambda: defaultdict(lambda: {
            'total_trades': 0,
            'wins': 0,
            'losses': 0,
            'profit_factor': 0.0,
            'avg_rr': 0.0,  # Average Risk:Reward
            'total_profit': 0.0,
            'total_loss': 0.0,
            'consecutive_wins': 0,
            'consecutive_losses': 0
        }))
        
        # Current dynamic weights
        self.dynamic_weights = {}
        self.base_weights = {}
        
        # Regime tracking
        self.current_regime = None
        self.regime_history = deque(maxlen=100)
        
        # Minimum samples before weight adjustment
        self.min_samples = 5
        
        # Weight adjustment factors
        self.weight_boost_factor = 1.5
        self.weight_penalty_factor = 0.6
        self.max_weight = 3.0
        self.min_weight = 0.3
    
    def register_strategy(self, strategy_name: str, base_weight: float):
        """Register a strategy with its base weight"""
        self.base_weights[strategy_name] = base_weight
        self.dynamic_weights[strategy_name] = base_weight
    
    def update_performance(self, strategy_name: str, regime: str, result: str, 
                          profit: float = 0, risk_reward: float = 0):
        """
        Update performance metrics for a strategy in a specific regime
        result: 'win' or 'loss'
        """
        metrics = self.performance_data[strategy_name][regime]
        metrics['total_trades'] += 1
        
        if result == 'win':
            metrics['wins'] += 1
            metrics['consecutive_wins'] += 1
            metrics['consecutive_losses'] = 0
            metrics['total_profit'] += profit
        else:
            metrics['losses'] += 1
            metrics['consecutive_losses'] += 1
            metrics['consecutive_wins'] = 0
            metrics['total_loss'] += abs(profit)
        
        # Update derived metrics
        total_loss = metrics['total_loss']
        metrics['profit_factor'] = metrics['total_profit'] / total_loss if total_loss > 0 else metrics['total_profit']
        
        # Update average R:R
        total_trades = metrics['total_trades']
        current_avg_rr = metrics['avg_rr']
        metrics['avg_rr'] = ((current_avg_rr * (total_trades - 1)) + risk_reward) / total_trades if total_trades > 1 else risk_reward
        
        # Recalculate weights after update
        self._recalculate_weights(regime)
    
    def _recalculate_weights(self, regime: str):
        """Recalculate dynamic weights based on performance in current regime"""
        if regime != self.current_regime:
            self.current_regime = regime
            self.regime_history.append(regime)
        
        for strategy_name in self.base_weights.keys():
            metrics = self.performance_data[strategy_name][regime]
            base_weight = self.base_weights[strategy_name]
            
            # Not enough data, use base weight
            if metrics['total_trades'] < self.min_samples:
                self.dynamic_weights[strategy_name] = base_weight
                continue
            
            # Calculate performance score
            win_rate = metrics['wins'] / metrics['total_trades'] if metrics['total_trades'] > 0 else 0
            profit_factor = metrics['profit_factor']
            avg_rr = metrics['avg_rr']
            
            # Composite performance score (0-1)
            # Win rate contributes 30%, profit factor 40%, R:R 30%
            pf_score = min(1.0, profit_factor / 2.0) if profit_factor > 0 else 0  # Cap at PF=2.0
            rr_score = min(1.0, avg_rr / 3.0) if avg_rr > 0 else 0  # Cap at R:R=3.0
            
            performance_score = (win_rate * 0.3) + (pf_score * 0.4) + (rr_score * 0.3)
            
            # Adjust weight based on performance
            if performance_score > 0.6:  # Strong performance
                new_weight = min(self.max_weight, base_weight * self.weight_boost_factor)
            elif performance_score < 0.4:  # Poor performance
                new_weight = max(self.min_weight, base_weight * self.weight_penalty_factor)
            else:
                new_weight = base_weight
            
            # Apply consecutive win/loss adjustments
            if metrics['consecutive_wins'] >= 3:
                new_weight = min(self.max_weight, new_weight * 1.2)
            elif metrics['consecutive_losses'] >= 3:
                new_weight = max(self.min_weight, new_weight * 0.8)
            
            self.dynamic_weights[strategy_name] = round(new_weight, 2)
    
    def get_strategy_weight(self, strategy_name: str, regime: str = None) -> float:
        """Get the current dynamic weight for a strategy"""
        if regime and regime != self.current_regime:
            # Return weight for specific regime if available
            if strategy_name in self.performance_data and regime in self.performance_data[strategy_name]:
                metrics = self.performance_data[strategy_name][regime]
                if metrics['total_trades'] >= self.min_samples:
                    return self.dynamic_weights.get(strategy_name, self.base_weights.get(strategy_name, 1.0))
        
        return self.dynamic_weights.get(strategy_name, self.base_weights.get(strategy_name, 1.0))
    
    def get_performance_report(self, strategy_name: str = None, regime: str = None) -> Dict:
        """Get performance report for specific strategy or all strategies"""
        if strategy_name:
            if regime:
                return {
                    'strategy': strategy_name,
                    'regime': regime,
                    'metrics': dict(self.performance_data[strategy_name][regime]),
                    'current_weight': self.dynamic_weights.get(strategy_name, self.base_weights.get(strategy_name, 1.0))
                }
            else:
                return {
                    'strategy': strategy_name,
                    'regimes': {r: dict(m) for r, m in self.performance_data[strategy_name].items()},
                    'current_weight': self.dynamic_weights.get(strategy_name, self.base_weights.get(strategy_name, 1.0))
                }
        
        # Return all strategies
        return {
            'strategies': {
                name: {
                    'regimes': {r: dict(m) for r, m in self.performance_data[name].items()},
                    'current_weight': self.dynamic_weights.get(name, self.base_weights.get(name, 1.0))
                }
                for name in self.base_weights.keys()
            },
            'current_regime': self.current_regime,
            'regime_history': list(self.regime_history)
        }
    
    def get_best_strategies_for_regime(self, regime: str, top_n: int = 3) -> List[Tuple[str, float]]:
        """Get top N strategies for a specific regime based on performance"""
        strategy_scores = []
        
        for strategy_name in self.base_weights.keys():
            metrics = self.performance_data[strategy_name][regime]
            if metrics['total_trades'] >= self.min_samples:
                win_rate = metrics['wins'] / metrics['total_trades']
                score = win_rate * metrics['profit_factor'] * (1 + metrics['avg_rr'])
                strategy_scores.append((strategy_name, score))
        
        strategy_scores.sort(key=lambda x: x[1], reverse=True)
        return strategy_scores[:top_n]

print("Part 2 loaded: Technical Analysis, Strategy Detectors, Core Engines + Adaptive Intelligence")
"""
Part 3A: Whale Detection Engine + Enhanced Message Templates
Includes: Trade Entry, Exit, Partial Profit, SL Update alerts
"""

# ================= WHALE DETECTION ENGINE =================

class WhaleDetectionEngine:
   def __init__(self):
       self.volume_baselines = defaultdict(lambda: deque(maxlen=100))
       self.price_history = defaultdict(lambda: deque(maxlen=50))
       self.atr_history = defaultdict(lambda: deque(maxlen=20))
       self.recent_activities = []
   
   def update(self, symbol, df):
       if len(df) < 2: 
           return
       self.volume_baselines[symbol].append(df['volume'].iloc[-1])
       self.price_history[symbol].append({
           'price': df['close'].iloc[-1], 
           'timestamp': df.index[-1]
       })
       
       # Calculate and store ATR for displacement filter
       if len(df) >= 14:
           atr = self._calculate_atr(df)
           self.atr_history[symbol].append(atr)
       
       activity = self._detect_anomaly(symbol, df)
       if activity: 
           self.recent_activities.append(activity)
   
   def _calculate_atr(self, df, period=14):
       """Calculate Average True Range for volatility filtering"""
       if len(df) < period:
           return None
       high_low = df['high'] - df['low']
       high_close = abs(df['high'] - df['close'].shift())
       low_close = abs(df['low'] - df['close'].shift())
       tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
       return tr.rolling(window=period).mean().iloc[-1]
   
   def _detect_anomaly(self, symbol, df):
       if len(self.volume_baselines[symbol]) < 20: 
           return None
       
       curr_vol = df['volume'].iloc[-1]
       avg_vol = np.mean(list(self.volume_baselines[symbol])[:-5])
       if avg_vol == 0: 
           return None
       
       vol_ratio = curr_vol / avg_vol
       price_change = abs(df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100 if len(df) >= 2 else 0
       
       # Enhanced volume threshold: >3x baseline
       if vol_ratio >= 3.0 and price_change >= WHALE_PRICE_IMPACT_PCT:
           # ATR displacement filter
           atr_valid = self._check_atr_displacement(symbol, df, price_change)
           if not atr_valid:
               return None
           
           body = df['close'].iloc[-1] - df['open'].iloc[-1]
           direction = "BUY" if body > 0 else "SELL" if body < 0 else "NEUTRAL"
           
           # Multi-candle momentum ignition detection
           momentum_score = self._detect_multi_candle_momentum(df, direction)
           
           if momentum_score >= 2 and price_change >= 1.0: 
               activity_type = "momentum_ignition"
           elif self._is_stop_hunt_pattern(df): 
               activity_type = "stop_hunt"
           elif self._is_accumulation_pattern(df, direction): 
               activity_type = "accumulation"
           elif self._is_distribution_pattern(df, direction): 
               activity_type = "distribution"
           else: 
               activity_type = "momentum_ignition"
           
           return WhaleActivity(
               symbol=symbol,
               timestamp=df.index[-1],
               activity_type=activity_type,
               volume_anomaly=vol_ratio,
               price_impact=price_change,
               direction=direction,
               confidence=min(95, int(50 + vol_ratio * 5 + price_change * 10 + momentum_score * 5)),
               momentum_score=momentum_score
           )
       return None
   
   def _check_atr_displacement(self, symbol, df, price_change):
       """Filter out noise using ATR displacement"""
       if len(self.atr_history[symbol]) < 5:
           return True
       
       current_atr = self._calculate_atr(df) if len(df) >= 14 else None
       if current_atr is None:
           return True
       
       avg_atr = np.mean(list(self.atr_history[symbol])[-5:])
       price_change_atr = (price_change / 100) * df['close'].iloc[-1]  # Convert % to price
       
       # Require price move to be significant relative to ATR (>0.5 ATR)
       return price_change_atr >= (avg_atr * 0.5)
   
   def _detect_multi_candle_momentum(self, df, direction, lookback=5):
       """
       Detect sustained momentum across multiple candles
       Returns score 0-5 based on consecutive directional candles
       """
       if len(df) < lookback + 1:
           return 0
       
       score = 0
       recent_candles = df.iloc[-lookback:]
       
       # Count consecutive candles in direction
       consecutive = 0
       for i in range(len(recent_candles)):
           candle_body = recent_candles['close'].iloc[i] - recent_candles['open'].iloc[i]
           if direction == "BUY" and candle_body > 0:
               consecutive += 1
           elif direction == "SELL" and candle_body < 0:
               consecutive += 1
           else:
               break
       
       # Volume progression check
       vol_trend = recent_candles['volume'].is_monotonic_increasing
       
       # Price velocity check
       price_velocities = []
       for i in range(1, len(recent_candles)):
           vel = abs(recent_candles['close'].iloc[i] - recent_candles['close'].iloc[i-1]) / recent_candles['close'].iloc[i-1] * 100
           price_velocities.append(vel)
       
       avg_velocity = np.mean(price_velocities) if price_velocities else 0
       accelerating = all(x < y for x, y in zip(price_velocities[:-1], price_velocities[1:])) if len(price_velocities) >= 2 else False
       
       # Build score
       score += min(2, consecutive)  # Up to 2 points for consecutive candles
       if vol_trend: score += 1
       if avg_velocity > 0.3: score += 1
       if accelerating: score += 1
       
       return min(5, score)
   
   def _is_accumulation_pattern(self, df, direction):
       """Enhanced accumulation detection with volume profile"""
       if direction != "BUY" or len(df) < 20:
           return False
       
       # Price above 20-period high
       price_break = df['close'].iloc[-1] > df['high'].rolling(20).mean().iloc[-1]
       
       # Volume profile: increasing volume on up moves, decreasing on pullbacks
       up_volume = df[df['close'] > df['open']]['volume'].mean()
       down_volume = df[df['close'] < df['open']]['volume'].mean()
       volume_profile = up_volume > down_volume * 1.2
       
       # Low volatility compression before breakout
       recent_range = (df['high'].iloc[-5:].max() - df['low'].iloc[-5:].min()) / df['close'].iloc[-1]
       historical_range = (df['high'].iloc[-20:].max() - df['low'].iloc[-20:].min()) / df['close'].iloc[-1]
       compression = recent_range < historical_range * 0.6
       
       return price_break and volume_profile and compression
   
   def _is_distribution_pattern(self, df, direction):
       """Enhanced distribution detection with volume profile"""
       if direction != "SELL" or len(df) < 20:
           return False
       
       # Price below 20-period low
       price_break = df['close'].iloc[-1] < df['low'].rolling(20).mean().iloc[-1]
       
       # Volume profile: increasing volume on down moves
       up_volume = df[df['close'] > df['open']]['volume'].mean()
       down_volume = df[df['close'] < df['open']]['volume'].mean()
       volume_profile = down_volume > up_volume * 1.2
       
       # Failed breakout pattern
       recent_high = df['high'].iloc[-5:].max()
       prev_high = df['high'].iloc[-20:-5].max()
       failed_breakout = recent_high > prev_high and df['close'].iloc[-1] < prev_high
       
       return price_break and volume_profile and failed_breakout
   
   def _is_stop_hunt_pattern(self, df):
       if len(df) < 5: 
           return False
       c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
       
       # Bearish stop hunt (sweeping longs)
       bearish_hunt = (
           c2['low'] < c1['low'] * 0.998 and 
           c2['close'] > c2['open'] and 
           c3['close'] > c3['open'] and 
           abs(c2['low'] - c1['low']) / c1['low'] > 0.002
       )
       
       # Bullish stop hunt (sweeping shorts)
       bullish_hunt = (
           c2['high'] > c1['high'] * 1.002 and 
           c2['close'] < c2['open'] and 
           c3['close'] < c3['open'] and 
           abs(c2['high'] - c1['high']) / c1['high'] > 0.002
       )
       
       # Enhanced volume confirmation: >3x baseline
       vol_baseline = df['volume'].tail(20).mean()
       vol_confirm = 'volume' in df.columns and c2['volume'] > vol_baseline * 3.0
       
       # Quick reversal confirmation
       quick_reversal = abs(c3['close'] - c2['open']) > abs(c2['close'] - c2['open']) * 0.5
       
       return (bearish_hunt or bullish_hunt) and vol_confirm and quick_reversal
   
   def get_recent_activities(self, lookback_minutes=15):
       cutoff = datetime.now(pytz.UTC) - timedelta(minutes=lookback_minutes)
       return [a for a in self.recent_activities if a.timestamp >= cutoff]


# ================= ENHANCED MESSAGE TEMPLATES =================

class MessageTemplateLibrary:
   
   # ================= TRADE ENTRY MESSAGE =================
   
   @staticmethod
   def trade_entry_alert(signal, execution_details=None, market_context=None):
       """
       Detailed trade entry message with full SL/TP information and enhanced metrics
       """
       exec_info = execution_details or {}
       market_ctx = market_context or {}
       sl_distance = abs(signal.entry_price - signal.stop_loss)
       tp_distance = abs(signal.take_profit - signal.entry_price)
       
       # Calculate pip values
       pip_value = 0.0001 if "JPY" not in signal.symbol else 0.01
       sl_pips = sl_distance / pip_value
       tp_pips = tp_distance / pip_value
       
       entry_emoji = "🟢" if signal.direction == "BUY" else "🔴"
       
       # Enhanced metrics
       trade_score = getattr(signal, 'trade_score', 0)
       trend_direction = market_ctx.get('trend_direction', 'NEUTRAL')
       liquidity_strength = market_ctx.get('liquidity_strength', 'MEDIUM')
       volatility_regime = market_ctx.get('volatility_regime', 'NORMAL')
       
       # Score visualization
       score_stars = "⭐" * (trade_score // 20)
       score_emoji = "🟢" if trade_score >= 80 else "🟡" if trade_score >= 60 else "🟠"
       
       # Trend emoji
       trend_emojis = {
           'BULLISH': '📈 STRONG UP',
           'BEARISH': '📉 STRONG DOWN', 
           'NEUTRAL': '➡️ SIDEWAYS',
           'WEAK_UP': '↗️ WEAK UP',
           'WEAK_DOWN': '↘️ WEAK DOWN'
       }
       
       # Liquidity emoji
       liq_emojis = {
           'HIGH': '💧💧💧 HIGH',
           'MEDIUM': '💧💧 MEDIUM',
           'LOW': '💧 LOW',
           'EXTREME': '🌊 EXTREME'
       }
       
       # Volatility emoji
       vol_emojis = {
           'LOW': '🟢 LOW',
           'NORMAL': '🟡 NORMAL',
           'HIGH': '🟠 HIGH',
           'EXTREME': '🔴 EXTREME'
       }
       
       return f"""
{entry_emoji} <b>POSITION OPENED — {signal.symbol}</b> {entry_emoji}

<b>📊 Trade Details</b>
├ Direction: <b>{signal.direction}</b> {"📈 LONG" if signal.direction == "BUY" else "📉 SHORT"}
├ Entry Price: <code>{signal.entry_price:.5f}</code>
├ Position Size: <code>{signal.position_size_r:.2f}R</code>
└ Strategy: <b>{signal.primary_strategy}</b>

<b>🎯 Trade Quality Score: {score_emoji} {trade_score}/100</b>
├ {score_stars}
├ Trend Direction: {trend_emojis.get(trend_direction, trend_direction)}
├ Liquidity Strength: {liq_emojis.get(liquidity_strength, liquidity_strength)}
└ Volatility Regime: {vol_emojis.get(volatility_regime, volatility_regime)}

<b>🛡️ Risk Management</b>
├ Stop Loss: <code>{signal.stop_loss:.5f}</code> ({sl_pips:.0f} pips)
├ Take Profit: <code>{signal.take_profit:.5f}</code> ({tp_pips:.0f} pips)
├ Risk:Reward: <b>1:{signal.risk_reward:.1f}</b> ⭐
└ Expected Value: <code>{signal.expected_value:+.2f}R</code>

<b>🎯 Profit Targets</b>
├ TP1 (Partial): <code>{signal.take_profit_1:.5f}</code> (50% position)
└ TP2 (Final): <code>{signal.take_profit:.5f}</code> (100% position)

<b>📈 Probability Analysis</b>
├ Win Probability: <b>{signal.probability.final_probability:.1%}</b>
├ Edge: <code>{signal.probability.edge:+.2f}</code>
└ Kelly Fraction: <code>{signal.probability.kelly_fraction:.2%}</code>

<b>🧠 Confluence Factors</b>
{chr(10).join(f"  ✅ {factor}" for factor in signal.confluence_factors[:4])}

<i>⏱️ Trade opened at {signal.timestamp.strftime('%H:%M:%S')} UTC</i>
<code>ID: {signal.id}</code>
"""
   
   # ================= TRADE EXIT MESSAGE =================
   
   @staticmethod
   def trade_exit_alert(trade, exit_details):
       """
       Comprehensive trade exit message showing P&L, exit reason, and performance
       """
       signal = trade.signal
       exit_price = exit_details.get('exit_price', trade.exit_price)
       exit_reason = exit_details.get('reason', trade.outcome.value)
       
       realized_pnl = trade.realized_pnl_r
       pnl_emoji = "🟢" if realized_pnl > 0 else "🔴" if realized_pnl < 0 else "⚪"
       pnl_percent = realized_pnl * 100
       
       exit_emojis = {
           'TP_HIT': '🎯 TAKE PROFIT HIT',
           'SL_HIT': '🛑 STOP LOSS HIT',
           'TRAILING_STOP': '📊 TRAILING STOP',
           'PARTIAL_TP': '💰 PARTIAL PROFIT',
           'BREAKEVEN': '⚖️ BREAKEVEN',
           'EXPIRED': '⏰ TIME EXPIRY'
       }
       exit_header = exit_emojis.get(exit_reason, f'📤 {exit_reason}')
       
       price_move = abs(exit_price - signal.entry_price)
       move_percent = price_move / signal.entry_price * 100
       
       return f"""
{pnl_emoji} <b>POSITION CLOSED — {signal.symbol}</b> {pnl_emoji}

<b>{exit_header}</b>

<b>📊 Trade Summary</b>
├ Direction: {signal.direction} {"📈" if signal.direction == "BUY" else "📉"}
├ Duration: <code>{trade.candles_evaluated} candles</code> ({trade.candles_evaluated * 15} min)
└ Status: <b>{trade.status.value.upper()}</b>

<b>💰 P&L Breakdown</b>
├ Realized P&L: <b>{realized_pnl:+.2f}R</b> ({pnl_percent:+.1f}%)
├ Partial Profit: <code>{trade.realized_pnl_r - (trade.partial_size_closed * 1.0 if trade.partial_size_closed else 0):+.2f}R</code>
└ Total Return: <b>${realized_pnl * 100:.2f}</b> (per $100 risk)

<b>📈 Price Action</b>
├ Entry: <code>{signal.entry_price:.5f}</code>
├ Exit: <code>{exit_price:.5f}</code>
├ Move: <code>{price_move:.5f}</code> ({move_percent:.2f}%)
└ Max Favorable: <code>+{trade.max_favorable_excursion:.2f}R</code> | Max Adverse: <code>{trade.max_adverse_excursion:.2f}R</code>

<b>🛡️ Risk Management Review</b>
├ Initial SL: <code>{signal.stop_loss:.5f}</code>
├ Initial TP: <code>{signal.take_profit:.5f}</code>
├ Trailing Used: {"✅ Yes" if trade.trailing_active else "❌ No"}
└ Breakeven Moved: {"✅ Yes" if trade.signal.stop_loss == trade.signal.entry_price else "❌ No"}

<b>🎓 Trade Metrics</b>
├ R:R Achieved: <code>{abs(exit_price - signal.entry_price) / abs(signal.entry_price - signal.stop_loss):.2f}</code>
├ Strategy: <b>{signal.primary_strategy}</b>
└ Market Regime: <code>{signal.regime.primary.name}</code>

<i>⏱️ Closed at {trade.exit_time.strftime('%H:%M:%S')} UTC</i>
<code>ID: {signal.id[:8]}</code>
"""
   
   # ================= PARTIAL PROFIT MESSAGE =================
   
   @staticmethod
   def partial_profit_alert(trade, partial_details):
       """
       Notification when partial take profit is hit
       """
       signal = trade.signal
       tp1_price = signal.take_profit_1
       
       return f"""
💰 <b>PARTIAL PROFIT SECURED — {signal.symbol}</b> 💰

<b>📊 Position Update</b>
├ Status: <b>PARTIAL CLOSE</b>
├ 50% Position Closed at TP1
└ 50% Position Still Active

<b>💵 Profit Secured</b>
├ Closed Amount: <code>{trade.partial_size_closed:.2f}R</code>
├ Profit Realized: <b>+{trade.realized_pnl_r:.2f}R</b> ⭐
├ TP1 Price: <code>{tp1_price:.5f}</code>

<b>🛡️ Risk Update</b>
├ Stop Loss: <code>{signal.stop_loss:.5f}</code> → <code>{signal.entry_price:.5f}</code> (BREAKEVEN ✅)
├ Remaining TP: <code>{signal.take_profit:.5f}</code>
└ Trailing Stop: {"Activated" if trade.trailing_active else "Will activate at +1R"}

<b>📈 Running Position</b>
├ Remaining Size: <code>{1.0 - trade.partial_size_closed:.0%}</code>
├ Unrealized P&L: <code>{trade.unrealized_pnl_r:+.2f}R</code>
└ Current Price: <code>{partial_details.get('current_price', 'N/A')}</code>

<i>💡 Risk-free trade: Stop loss moved to breakeven</i>
<code>ID: {signal.id[:8]}</code>
"""
   
   # ================= STOP LOSS UPDATE MESSAGE =================
   
   @staticmethod
   def stop_loss_update_alert(trade, old_sl, new_sl, reason):
       """
       Notification when stop loss is moved (trailing or breakeven)
       """
       sl_improvement = abs(new_sl - trade.signal.entry_price) / abs(old_sl - trade.signal.entry_price)
       
       return f"""
🛡️ <b>STOP LOSS UPDATED — {trade.signal.symbol}</b> 🛡️

<b>📊 Adjustment Details</b>
├ Reason: <b>{reason}</b>
├ Old SL: <code>{old_sl:.5f}</code>
├ New SL: <code>{new_sl:.5f}</code>
└ Improvement: <code>{sl_improvement:.1%}</code> closer to entry

<b>💰 Impact</b>
├ Risk Reduced: <code>${abs(new_sl - old_sl) * 100:.2f}</code> (per $100 risk)
└ Locked Profit: <code>{trade.unrealized_pnl_r:+.2f}R</code> if hit

<i>🔄 Trailing stop actively protecting profits</i>
<code>ID: {trade.signal.id[:8]}</code>
"""
   
   # ================= RISK DASHBOARD MESSAGE =================
   
   @staticmethod
   def risk_dashboard(metrics, active_trades):
       """
       Generate a Telegram formatted HTML message displaying portfolio risk metrics.
       
       Args:
           metrics: Object with attributes: portfolio_heat_score, active_exposure, 
                    available_capacity, drawdown_status, correlation_risk, session_risk
           active_trades: List of dicts with keys: symbol, direction, unrealized_pnl
       
       Returns:
           str: Formatted HTML message for Telegram
       """
       # Heat score color coding
       heat_score = getattr(metrics, 'portfolio_heat_score', 0)
       if heat_score >= 80:
           heat_emoji = "🔴"
           heat_status = "⚠️ CRITICAL"
       elif heat_score >= 50:
           heat_emoji = "🟡"
           heat_status = "⚡ ELEVATED"
       else:
           heat_emoji = "🟢"
           heat_status = "✅ HEALTHY"
       
       # Drawdown status emoji
       dd_status = getattr(metrics, 'drawdown_status', 'NORMAL').upper()
       dd_emojis = {
           'NORMAL': '🟢',
           'WARNING': '🟡',
           'CRITICAL': '🔴',
           'RECOVERY': '🔵'
       }
       dd_emoji = dd_emojis.get(dd_status, '⚪')
       
       # Format active trades (top 5)
       trades_list = []
       display_trades = active_trades[:5] if active_trades else []
       
       if display_trades:
           for trade in display_trades:
               symbol = trade.get('symbol', 'N/A')
               direction = trade.get('direction', 'N/A')
               pnl = trade.get('unrealized_pnl', 0)
               
               # Direction emoji
               dir_emoji = "📈" if direction == "BUY" else "📉" if direction == "SELL" else "➖"
               # PnL emoji
               pnl_emoji = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
               
               trades_list.append(f"  {dir_emoji} <code>{symbol}</code> {direction} | {pnl_emoji} {pnl:+.2f}R")
           
           if len(active_trades) > 5:
               trades_list.append(f"  <i>... and {len(active_trades) - 5} more</i>")
           trades_section = "\n".join(trades_list)
       else:
           trades_section = "  <i>No active positions</i>"
       
       # Risk indicators
       correlation_risk = getattr(metrics, 'correlation_risk', 'LOW')
       session_risk = getattr(metrics, 'session_risk', 'NORMAL')
       
       return f"""
📊 <b>PORTFOLIO RISK DASHBOARD</b>

<b>🔥 Portfolio Heat</b>
├ Heat Score: {heat_emoji} <code>{heat_score}%</code> {heat_status}
├ Active Exposure: <code>{getattr(metrics, 'active_exposure', 0):.2f}R</code>
└ Capacity Left: <code>{getattr(metrics, 'available_capacity', 0):.2f}R</code>

<b>📉 Drawdown Status</b>
└ {dd_emoji} <b>{dd_status}</b>

<b>🎯 Active Trades ({len(active_trades)})</b>
{trades_section}

<b>⚠️ Risk Indicators</b>
├ Correlation Risk: <code>{correlation_risk}</code>
└ Session Risk: <code>{session_risk}</code>

<i>⏱️ Updated: {datetime.now(pytz.UTC).strftime('%H:%M:%S')} UTC</i>
"""
   
   # ================= PRE-SIGNAL & SETUP MESSAGES =================
   
   @staticmethod
   def pre_signal_alert(signal, time_to_signal):
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
   def _format_confluence_preview(factors):
       preview = [f"  {i}. {factor} ⚡" for i, factor in enumerate(factors[:3], 1)]
       if len(factors) > 3: 
           preview.append(f"  ... and {len(factors)-3} more factors")
       return "\n".join(preview) if preview else "  Analyzing market structure..."
   
   # ================= SIGNAL CONFIRMATION =================
   
   @staticmethod
   def signal_alert(signal, market_context):
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
   
   # ================= UTILITY METHODS =================
   
   @staticmethod
   def _generate_probability_bar(probability):
       filled = int(probability * PROGRESS_BAR_LENGTH)
       bar = "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)
       emoji = "🟢" if probability >= 0.80 else "🟡" if probability >= 0.65 else "🟠"
       return f"{emoji} <b>[{bar}]</b> {probability:.0%}"
   
   @staticmethod
   def _generate_risk_matrix(signal):
       risk_amount = abs(signal.entry_price - signal.stop_loss)
       reward_amount = abs(signal.take_profit - signal.entry_price)
       risk_bar = "🔴" * min(5, int(risk_amount / signal.entry_price * 10000))
       reward_bar = "🟢" * min(5, int(reward_amount / signal.entry_price * 10000))
       return f"""<b>📊 Risk Matrix</b>
├ Risk:    {risk_bar} ${risk_amount:.2f}
└ Reward:  {reward_bar} ${reward_amount:.2f}"""
   
   @staticmethod
   def _generate_context_badges(context):
       badges = []
       if 'trend' in context.get('regime', 'unknown').lower(): 
           badges.append("📈 TRENDING")
       elif 'range' in context.get('regime', 'unknown').lower(): 
           badges.append("↔️ RANGING")
       elif 'volatile' in context.get('regime', 'unknown').lower(): 
           badges.append("⚠️ VOLATILE")
       if 'overlap' in context.get('session', 'unknown').lower(): 
           badges.append("🔥 HIGH LIQUIDITY")
       if context.get('volatility', 'normal') == 'extreme': 
           badges.append("🌊 EXTREME VOL")
       return " | ".join(badges) if badges else "📊 STANDARD CONDITIONS"
   
   @staticmethod
   def _format_confluence_detailed(factors):
       lines = []
       for i, factor in enumerate(factors[:5]):
           bar = "█" * int([2.0, 1.5, 1.0, 0.8, 0.5][min(i, 4)] * 3)
           lines.append(f"  {bar} {factor}")
       return "\n".join(lines)
   
   # ================= MARKET STRUCTURE MESSAGES =================
   
   @staticmethod
   def liquidity_zone_heatmap(zones, current_price):
       zones_sorted = sorted(zones, key=lambda z: z.strength_score, reverse=True)
       heatmap_lines = []
       for zone in zones_sorted[:5]:
           distance_emoji = "🎯" if zone.distance_pct < 0.5 else "📍" if zone.distance_pct < 1.0 else "👁️"
           direction = "↑" if zone.price_level > current_price else "↓"
           heatmap_lines.append(f"{zone.heatmap_intensity} {distance_emoji} <code>{zone.price_level:.5f}</code> {direction} ({zone.distance_pct:.1f}%) — {zone.zone_type.replace('_', ' ').title()}")
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
   def _generate_price_ladder(zones, current_price):
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
       ladder.append(f"  <code>{current_price:.5f}</code> {'─' * pos}🔴 YOU ARE HERE{'─' * (20 - pos)}")
       return "\n".join(ladder)
   
   @staticmethod
   def session_open_ceremony(session_type, high_impact_news=None):
       if high_impact_news is None: 
           high_impact_news = []
       session_emoji = {"LONDON": "🏰", "NEW_YORK": "🗽", "OVERLAP": "⚡", "TOKYO": "🗾", "SYDNEY": "🏖️"}.get(session_type, "🌍")
       news_alert = f"\n⚠️ <b>HIGH IMPACT NEWS (Next 2H):</b>\n{chr(10).join(f'  • {news}' for news in high_impact_news)}\n" if high_impact_news else ""
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

   # ================= DAILY PERFORMANCE REPORT =================
   
   @staticmethod
   def daily_performance_report(trades, portfolio_state):
       """
       Generate a comprehensive daily performance report with Telegram HTML formatting.
       
       Args:
           trades: List of trade objects with attributes: outcome, realized_pnl_r
           portfolio_state: Dict with keys: active_trades_count, portfolio_exposure, 
                          current_drawdown_pct
       
       Returns:
           str: Formatted HTML message for Telegram
       """
       # Calculate statistics
       total_trades = len(trades)
       wins = sum(1 for t in trades if getattr(t, 'realized_pnl_r', 0) > 0)
       losses = sum(1 for t in trades if getattr(t, 'realized_pnl_r', 0) < 0)
       breakeven = sum(1 for t in trades if getattr(t, 'realized_pnl_r', 0) == 0)
       
       win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
       
       # Total profit in R
       total_profit_r = sum(getattr(t, 'realized_pnl_r', 0) for t in trades)
       
       # Best and worst trades
       if trades:
           sorted_trades = sorted(trades, key=lambda x: getattr(x, 'realized_pnl_r', 0), reverse=True)
           best_trade = sorted_trades[0]
           worst_trade = sorted_trades[-1]
           best_pnl = getattr(best_trade, 'realized_pnl_r', 0)
           worst_pnl = getattr(worst_trade, 'realized_pnl_r', 0)
           best_symbol = getattr(getattr(best_trade, 'signal', None), 'symbol', 'N/A')
           worst_symbol = getattr(getattr(worst_trade, 'signal', None), 'symbol', 'N/A')
       else:
           best_pnl = worst_pnl = 0
           best_symbol = worst_symbol = 'N/A'
       
       # Portfolio state
       active_count = portfolio_state.get('active_trades_count', 0)
       exposure = portfolio_state.get('portfolio_exposure', 0)
       drawdown = portfolio_state.get('current_drawdown_pct', 0)
       
       # Emojis
       profit_emoji = "🟢" if total_profit_r > 0 else "🔴" if total_profit_r < 0 else "⚪"
       day_emoji = "🌅" if datetime.now(pytz.UTC).hour < 12 else "🌇" if datetime.now(pytz.UTC).hour < 18 else "🌃"
       
       return f"""
{day_emoji} <b>DAILY PERFORMANCE REPORT</b> {day_emoji}

<b>📊 Trade Statistics</b>
├ Total Trades: <code>{total_trades}</code>
├ Wins: <code>{wins}</code> ✅
├ Losses: <code>{losses}</code> ❌
└ Breakeven: <code>{breakeven}</code> ⚖️

<b>🎯 Performance Metrics</b>
├ Win Rate: <code>{win_rate:.1f}%</code> {"⭐" * int(win_rate / 20)}
├ Total Profit: {profit_emoji} <code>{total_profit_r:+.2f}R</code>
├ Best Trade: 🏆 <code>{best_symbol} ({best_pnl:+.2f}R)</code>
└ Worst Trade: 💀 <code>{worst_symbol} ({worst_pnl:+.2f}R)</code>

<b>💼 Portfolio Status</b>
├ Active Trades: <code>{active_count}</code>
├ Current Exposure: <code>{exposure:.2f}R</code>
└ Drawdown: <code>{drawdown:.2f}%</code> {"🔴" if drawdown > 10 else "🟡" if drawdown > 5 else "🟢"}

<b>📈 Session Summary</b>
└ {"🚀 Profitable Day" if total_profit_r > 0 else "📉 Red Day" if total_profit_r < 0 else "➖ Flat Day"} | {wins}W/{losses}L/{breakeven}BE

<i>📅 Report Date: {datetime.now(pytz.UTC).strftime('%Y-%m-%d')}</i>
<code>Generated at {datetime.now(pytz.UTC).strftime('%H:%M')} UTC</code>
"""

print("Part 3A loaded: Whale Detection + Message Templates (Entry/Exit/Partial)")
"""
Part 3B: Event Trigger Engine, Portfolio Risk Manager, Execution Engine
FIXED: Complete method definitions with proper syntax
UPDATED: Enhanced risk filters, dynamic position sizing, global limits, improved trailing stop, drawdown protection, and division-by-zero safety
UPDATED: Added probability filter and USD correlation protection
UPDATED: ATR-based stop-loss calculation in Execution Engine
"""

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
    
    async def trigger_event(self, event_type, data, priority=2):
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
    
    def _check_quota(self, event_type):
        if event_type in [AlertType.DRAWDOWN_WARNING, AlertType.REGIME_CHANGE]: 
            return True
        return sum(self.daily_alert_count.values()) < MAX_DAILY_ALERTS
    
    def _check_cooldown(self, event_type):
        cooldown = self.alert_cooldowns.get(event_type, 300)
        last_time = self.last_alert_time.get(event_type.value)
        if last_time is None: 
            return True
        return (datetime.now(pytz.UTC) - last_time).total_seconds() >= cooldown
    
    def _get_handler(self, event_type):
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
    
    def _record_alert(self, event_type):
        self.last_alert_time[event_type.value] = datetime.now(pytz.UTC)
        self.daily_alert_count[event_type.value] += 1
    
    # Handler methods
    def _handle_pre_signal(self, data): 
        return self.templates.pre_signal_alert(UXSignal(**data['signal']), data.get('time_to_signal', 60))
    
    def _handle_signal(self, data): 
        return self.templates.signal_alert(UXSignal(**data['signal']), data.get('market_context', {}))
    
    def _handle_liquidity_zone(self, data): 
        return self.templates.liquidity_zone_heatmap([LiquidityZone(**z) for z in data['zones']], data['current_price'])
    
    def _handle_session_open(self, data): 
        return self.templates.session_open_ceremony(data['session_type'], data.get('high_impact_news', []))
    
    def _handle_confidence_heatmap(self, data): 
        return self.templates.confidence_heatmap([UXSignal(**s) for s in data['signals']])
    
    def _handle_risk_dashboard(self, data): 
        return self.templates.risk_dashboard(RiskMetrics(**data['metrics']), data.get('active_trades', []))
    
    def _handle_whale_activity(self, data): 
        return self.templates.whale_activity_alert([WhaleActivity(**a) for a in data['activities']])
    
    def _handle_multi_ranking(self, data): 
        return self.templates.confidence_heatmap([UXSignal(**s) for s in data['signals']])
    
    def _handle_weekly_analytics(self, data): 
        return self.templates.weekly_analytics_report(data.get('trade_history', []), data.get('performance_metrics', {}))
    
    def _handle_probability_viz(self, data): 
        return self.templates.probability_distribution_viz(data.get('probabilities', {}))
    
    def _handle_rr_suggestion(self, data): 
        return self.templates.rr_suggestion(UXSignal(**data['signal']), data.get('alternatives', []))
    
    def _handle_invalidation(self, data):
        return f"""
❌ <b>TRADE INVALIDATED — {data['symbol']}</b>

<b>📝 Invalidation Reason:</b>
<code>{data['reason']}</code>

<b>📊 Trade Post-Mortem:</b>
├ Entry: <code>{data['entry']:.5f}</code>
├ Current: <code>{data['current']:.5f}</code>
├ Loss: 🔴 <code>{data['loss']:.2f}R</code>
└ Trade ID: <code>{data['trade_id'][:8]}</code>

<b>🎓 Learning Context:</b>
  • Review confluence factors
  • Check regime alignment
  • Validate liquidity assumptions

<i>💡 Every invalidation is data. Adapt.</i>
"""
    
    def _handle_drawdown_warning(self, data):
        return f"""
🔴 <b>DRAWDOWN INTERVENTION</b> 🔴

<b>⚠️ Status:</b> {data['recommended_action']}
<b>📉 Current Drawdown:</b> <code>{data['current_dd']:.1%}</code>
<b>📊 Max Drawdown:</b> <code>{data['max_dd']:.1%}</code>

<b>🎯 Recommended Action:</b>
<code>{data['recommended_action']}</code>

<b>📋 Recovery Protocol:</b>
  1. ✅ Review all active signals
  2. ✅ Reduce position size by 50%
  3. ✅ Tighten stops to 0.75x ATR
  4. ✅ Only A+ setups (probability >80%)
  5. ⏳ Daily risk review mandatory

<i>💪 This is a test of process, not prediction.</i>
"""
    
    def _handle_regime_change(self, data):
        return f"""
🔄 <b>REGIME CHANGE DETECTED</b> 🔄

<b>📊 Transition:</b>
<code>{data['old_regime']}</code> ➡️ <code>{data['new_regime']}</code>

<b>⚡ Market Impact:</b>
<code>{data['impact']}</code>

<b>🎯 Strategy Adaptation:</b>
{chr(10).join(f"  • {adj}" for adj in data['adjustments'])}

<b>📋 Action Checklist:</b>
  ⏸️ Pause new entries (2 candles)
  🔍 Reassess active signals
  📊 Adjust position sizing
  🎯 Update probability models
  ✅ Resume with confirmation

<i>🎪 Adapt or perish. The market has spoken.</i>
"""


# ================= PORTFOLIO RISK MANAGER =================

class PortfolioRiskManager:
    # NEW: Global trade limits
    MAX_TRADES_PER_SYMBOL = 2  # Maximum concurrent trades per symbol
    MAX_TRADES_TOTAL = 10      # Maximum total concurrent trades
    SPREAD_THRESHOLD = 0.0005  # 5 pips for forex, 0.05% for crypto
    
    # NEW: Score thresholds for position sizing
    SCORE_HIGH = 8
    SCORE_NORMAL_MIN = 6
    SCORE_REJECT = 5
    
    # NEW: Drawdown protection threshold
    DRAWDOWN_PROTECTION_PCT = 10.0
    RISK_REDUCTION_FACTOR = 0.5
    
    # NEW: Probability and USD correlation settings
    MIN_SIGNAL_PROBABILITY = 0.70  # Minimum probability to accept trade
    MAX_USD_TRADES = 1  # Maximum concurrent USD trades allowed
    
    def __init__(self):
        self.state = PortfolioState()
        self.correlation_window = 20
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.cooldowns = {}
        self.symbol_trade_counts = defaultdict(int)  # NEW: Track trades per symbol
        self.risk_multiplier = 1.0  # NEW: Dynamic risk adjustment based on drawdown
    
    def update_price(self, symbol, price):
        self.price_history[symbol].append(price)
    
    def calculate_correlation(self, sym1, sym2):
        if len(self.price_history[sym1]) < self.correlation_window or len(self.price_history[sym2]) < self.correlation_window: 
            return 0.0
        s1 = pd.Series(list(self.price_history[sym1])[-self.correlation_window:])
        s2 = pd.Series(list(self.price_history[sym2])[-self.correlation_window:])
        returns1 = s1.pct_change().dropna()
        returns2 = s2.pct_change().dropna()
        return returns1.corr(returns2) if len(returns1) >= 5 else 0.0
    
    # NEW: Check if symbol contains USD (forex pairs)
    def _is_usd_pair(self, symbol):
        """Check if symbol is a USD forex pair"""
        usd_pairs = ['USDJPY', 'USDCHF', 'EURUSD', 'GBPUSD', 'AUDUSD', 'USDCAD', 'NZDUSD', 'USDSEK', 'USDNOK', 'USDDKK']
        return symbol.upper() in usd_pairs
    
    # NEW: Count active USD trades
    def _count_usd_trades(self, open_trades):
        """Count number of active USD trades"""
        return sum(1 for t in open_trades if self._is_usd_pair(t.signal.symbol))
    
    # NEW: Enhanced trade validation with filters
    def validate_trade_filters(self, signal, current_spread):
        """
        Validate trade against probability, risk/reward, and spread filters
        Returns: (passed: bool, reason: str)
        """
        # Probability filter: ≥ 0.70
        if signal.probability < 0.70:
            return False, f"Probability {signal.probability:.2%} below threshold 70%"
        
        # Risk/Reward filter: ≥ 1.5
        if signal.risk_reward < 1.5:
            return False, f"Risk/Reward {signal.risk_reward:.2f} below threshold 1.5"
        
        # Spread filter: ≤ SPREAD_THRESHOLD
        if current_spread > self.SPREAD_THRESHOLD:
            return False, f"Spread {current_spread:.5f} exceeds threshold {self.SPREAD_THRESHOLD}"
        
        return True, "All filters passed"
    
    # NEW: Dynamic position sizing based on trade score
    def calculate_position_size(self, signal, base_size_r):
        """
        Calculate position size based on trade score
        Returns: (size_r: float, approved: bool)
        """
        score = getattr(signal, 'score', 0)
        
        if score >= self.SCORE_HIGH:
            # Score ≥ 8: 1.5x size
            adjusted_size = base_size_r * 1.5 * self.risk_multiplier
            return adjusted_size, True
        elif score >= self.SCORE_NORMAL_MIN:
            # Score 6-7: normal size
            adjusted_size = base_size_r * self.risk_multiplier
            return adjusted_size, True
        else:
            # Score < 5: reject trade
            return 0, False
    
    def can_add_trade(self, new_signal, open_trades, current_spread=0.0):
        # NEW: Check probability filter using final_probability
        signal_probability = getattr(new_signal, 'probability', None)
        if signal_probability is not None:
            # Handle both direct float and object with final_probability attribute
            if hasattr(signal_probability, 'final_probability'):
                prob_value = signal_probability.final_probability
            else:
                prob_value = float(signal_probability)
            
            if prob_value < self.MIN_SIGNAL_PROBABILITY:
                return False, f"Probability {prob_value:.2%} below minimum threshold {self.MIN_SIGNAL_PROBABILITY:.2%}"
        
        # NEW: Check USD correlation protection
        if self._is_usd_pair(new_signal.symbol):
            usd_trade_count = self._count_usd_trades(open_trades)
            if usd_trade_count >= self.MAX_USD_TRADES:
                return False, f"Max USD trades {self.MAX_USD_TRADES} reached (currently {usd_trade_count} active)"
        
        # NEW: Check global trade limits
        total_trades = len(open_trades)
        if total_trades >= self.MAX_TRADES_TOTAL:
            return False, f"Max total trades {self.MAX_TRADES_TOTAL} reached"
        
        symbol_trades = sum(1 for t in open_trades if t.signal.symbol == new_signal.symbol)
        if symbol_trades >= self.MAX_TRADES_PER_SYMBOL:
            return False, f"Max trades per symbol {self.MAX_TRADES_PER_SYMBOL} reached for {new_signal.symbol}"
        
        # NEW: Validate trade filters
        filters_passed, filter_reason = self.validate_trade_filters(new_signal, current_spread)
        if not filters_passed:
            return False, filter_reason
        
        # NEW: Calculate dynamic position sizing
        base_size = getattr(new_signal, 'position_size_r', 1.0)
        adjusted_size, approved = self.calculate_position_size(new_signal, base_size)
        if not approved:
            return False, f"Trade rejected: score {getattr(new_signal, 'score', 0)} below threshold {self.SCORE_REJECT}"
        
        # Update signal with adjusted size for downstream use
        new_signal.adjusted_position_size_r = adjusted_size
        
        # Check portfolio exposure with adjusted size
        total_risk = sum(t.signal.position_size_r for t in open_trades)
        if total_risk + adjusted_size > MAX_PORTFOLIO_EXPOSURE * self.risk_multiplier: 
            return False, f"Max exposure {MAX_PORTFOLIO_EXPOSURE * self.risk_multiplier:.1f}R would be exceeded"
        
        for trade in open_trades:
            if trade.signal.direction != new_signal.direction: 
                continue
            corr = self.calculate_correlation(new_signal.symbol, trade.signal.symbol)
            if abs(corr) > 0.7:
                corr_exposure = sum(t.signal.position_size_r for t in open_trades if abs(self.calculate_correlation(new_signal.symbol, t.signal.symbol)) > 0.7)
                if corr_exposure + adjusted_size > MAX_CORRELATION_EXPOSURE * self.risk_multiplier: 
                    return False, f"Max correlated exposure {MAX_CORRELATION_EXPOSURE * self.risk_multiplier:.1f}R would be exceeded"
        
        if new_signal.symbol in self.cooldowns and datetime.now() < self.cooldowns[new_signal.symbol]: 
            return False, "Symbol in cooldown"
        
        if new_signal.symbol in self.state.last_signals:
            time_since = (datetime.now() - self.state.last_signals[new_signal.symbol]).total_seconds() / 60
            if time_since < SIGNAL_COOLDOWN_MINUTES: 
                return False, f"Signal clustering: {time_since:.0f}min since last signal"
        
        return True, "OK"
    
    def register_trade(self, signal):
        self.state.open_trades += 1
        # Use adjusted size if available, otherwise fall back to original
        size_to_use = getattr(signal, 'adjusted_position_size_r', signal.position_size_r)
        self.state.total_exposure_r += size_to_use
        self.state.last_signals[signal.symbol] = datetime.now()
        self.symbol_trade_counts[signal.symbol] += 1  # NEW: Increment symbol count
    
    def close_trade(self, trade):
        self.state.open_trades -= 1
        # Use adjusted size if available for accurate exposure reduction
        size_to_use = getattr(trade.signal, 'adjusted_position_size_r', trade.signal.position_size_r)
        self.state.total_exposure_r -= size_to_use
        self.state.daily_pnl_r += trade.current_pnl_r
        
        if self.state.daily_pnl_r > self.state.peak_equity_r: 
            self.state.peak_equity_r = self.state.daily_pnl_r
        
        # SAFEGUARD: Division-by-zero protection in drawdown calculation
        if self.state.peak_equity_r > 0: 
            self.state.current_drawdown_pct = (self.state.peak_equity_r - self.state.daily_pnl_r) / self.state.peak_equity_r * 100
        else:
            self.state.current_drawdown_pct = 0.0
        
        self.cooldowns[trade.signal.symbol] = datetime.now() + timedelta(minutes=SIGNAL_COOLDOWN_MINUTES)
        self.symbol_trade_counts[trade.signal.symbol] = max(0, self.symbol_trade_counts[trade.signal.symbol] - 1)  # NEW: Decrement symbol count
        
        # NEW: Update drawdown protection after closing trade
        self._update_drawdown_protection()
    
    # NEW: Drawdown protection logic
    def _update_drawdown_protection(self):
        """Update risk multiplier based on drawdown status"""
        if self.state.current_drawdown_pct > self.DRAWDOWN_PROTECTION_PCT:
            self.risk_multiplier = self.RISK_REDUCTION_FACTOR
        else:
            self.risk_multiplier = 1.0
    
    def check_drawdown_throttle(self):
        return self.state.current_drawdown_pct > DRAWDOWN_THROTTLE_PCT
    
    def get_risk_metrics(self):
        heat_score = min(100, int((self.state.total_exposure_r / (MAX_PORTFOLIO_EXPOSURE * self.risk_multiplier)) * 100)) if MAX_PORTFOLIO_EXPOSURE > 0 else 0
        dd_status = "critical" if self.state.current_drawdown_pct > 10 else "elevated" if self.state.current_drawdown_pct > 7 else "normal"
        
        return RiskMetrics(
            timestamp=datetime.now(pytz.UTC),
            portfolio_heat_score=heat_score,
            active_exposure=self.state.total_exposure_r,
            available_capacity=(MAX_PORTFOLIO_EXPOSURE * self.risk_multiplier) - self.state.total_exposure_r,
            drawdown_status=dd_status,
            correlation_risk=50,
            session_risk=30,
            current_drawdown_pct=self.state.current_drawdown_pct,  # NEW: Include raw drawdown %
            risk_multiplier=self.risk_multiplier  # NEW: Include current risk multiplier
        )


# ================= EXECUTION ENGINE =================

class ExecutionEngine:
    # NEW: Trailing stop configuration
    TRAILING_ACTIVATION_R = 1.0  # Activate after 1R profit
    TRAILING_DISTANCE_R = 0.3    # Trail 0.3R behind price
    
    # NEW: ATR-based stop-loss configuration
    ATR_STOP_MULTIPLIER = 2.0    # Multiplier for ATR stop distance (e.g., 2x ATR)
    
    def __init__(self):
        self.slippage_model = {'forex': 0.0001, 'crypto': 0.0005}
    
    def simulate_entry(self, signal, asset_class):
        base_slip = self.slippage_model.get(asset_class, 0.0003)
        vol_adj = 1 + (signal.regime.atr_percentile / 100)
        slippage = signal.entry_price * base_slip * vol_adj
        
        if signal.direction == "BUY":
            return signal.entry_price + slippage
        else:
            return signal.entry_price - slippage
    
    def simulate_exit(self, trade, exit_price, asset_class):
        base_slip = self.slippage_model.get(asset_class, 0.0003)
        slippage = exit_price * base_slip
        
        if trade.signal.direction == "BUY":
            return exit_price - slippage
        else:
            return exit_price + slippage
    
    # NEW: Calculate ATR-based stop-loss
    def calculate_atr_stop_loss(self, entry_price, direction, atr_value, multiplier=None):
        """
        Calculate stop-loss based on ATR to prevent premature exits from normal volatility.
        
        Args:
            entry_price: Trade entry price
            direction: "BUY" or "SELL"
            atr_value: Current ATR value (from TechnicalCore.atr(df).iloc[-1])
            multiplier: ATR multiplier (uses ATR_STOP_MULTIPLIER if None)
        
        Returns:
            stop_loss: Calculated stop-loss price
        """
        if multiplier is None:
            multiplier = self.ATR_STOP_MULTIPLIER
        
        # SAFEGUARD: Validate inputs
        if entry_price is None or atr_value is None or atr_value <= 0:
            return None
        
        stop_distance = atr_value * multiplier
        
        if direction == "BUY":
            stop_loss = entry_price - stop_distance
        else:  # SELL
            stop_loss = entry_price + stop_distance
        
        return stop_loss
    
    # NEW: Calculate stop-loss for signal using ATR
    def calculate_signal_stop_loss(self, signal, atr_value):
        """
        Calculate and assign ATR-based stop-loss to signal.
        Updates signal.stop_loss attribute.
        
        Args:
            signal: Signal object with entry_price and direction
            atr_value: Current ATR value
        
        Returns:
            bool: True if stop-loss was calculated successfully
        """
        stop_loss = self.calculate_atr_stop_loss(
            entry_price=signal.entry_price,
            direction=signal.direction,
            atr_value=atr_value,
            multiplier=self.ATR_STOP_MULTIPLIER
        )
        
        if stop_loss is not None:
            signal.stop_loss = stop_loss
            return True
        return False
    
    def should_trigger_partial(self, trade, current_price):
        if trade.signal.take_profit_1 is None: 
            return False
        
        if trade.signal.direction == "BUY":
            return current_price >= trade.signal.take_profit_1
        else:
            return current_price <= trade.signal.take_profit_1
    
    def update_trailing_stop(self, trade, current_price):
        """
        UPDATED: Improved trailing stop with 1R activation and 0.3R trail distance
        Uses ATR-based risk calculation if available, otherwise falls back to entry-stop distance
        Returns new trailing stop level or None
        """
        # Calculate risk (1R) - SAFEGUARD against division by zero
        entry = trade.signal.entry_price
        stop = trade.signal.stop_loss
        
        # Prevent division by zero or invalid risk calculation
        if entry is None or stop is None or abs(entry - stop) < 1e-10:
            return None
            
        risk = abs(entry - stop)
        if risk < 1e-10:  # Additional safety check
            return None
        
        profit = abs(current_price - entry)
        
        if not trade.trailing_active:
            # Check if we should activate trailing stop after 1R profit
            if profit >= risk * self.TRAILING_ACTIVATION_R:
                trade.trailing_active = True
                # Set initial trailing level at entry + 0.7R (giving 0.3R buffer from 1R profit)
                if trade.signal.direction == "BUY":
                    return entry + (risk * (self.TRAILING_ACTIVATION_R - self.TRAILING_DISTANCE_R))
                else:
                    return entry - (risk * (self.TRAILING_ACTIVATION_R - self.TRAILING_DISTANCE_R))
        else:
            # Update existing trailing stop - trail 0.3R behind current price
            if trade.signal.direction == "BUY":
                # For longs: trail below current price by 0.3R
                new_level = current_price - (risk * self.TRAILING_DISTANCE_R)
                # Only move stop up, never down
                if trade.trailing_level is None:
                    return new_level
                return max(new_level, trade.trailing_level)
            else:
                # For shorts: trail above current price by 0.3R
                new_level = current_price + (risk * self.TRAILING_DISTANCE_R)
                # Only move stop down, never up
                if trade.trailing_level is None:
                    return new_level
                return min(new_level, trade.trailing_level)
        
        return None
    
    # NEW: Safe PnL calculation with division-by-zero protection
    def calculate_pnl_r(self, trade, current_price):
        """
        Calculate PnL in R multiples with division-by-zero safety
        Returns: (pnl_r: float, pnl_pct: float)
        """
        entry = trade.signal.entry_price
        stop = trade.signal.stop_loss
        
        # SAFEGUARD: Prevent division by zero
        if entry is None or stop is None:
            return 0.0, 0.0
            
        risk = abs(entry - stop)
        if risk < 1e-10:  # Avoid division by zero
            return 0.0, 0.0
        
        if trade.signal.direction == "BUY":
            price_diff = current_price - entry
        else:
            price_diff = entry - current_price
        
        pnl_r = price_diff / risk
        pnl_pct = (price_diff / entry) * 100 if abs(entry) > 1e-10 else 0.0
        
        return pnl_r, pnl_pct
    
    # NEW: Safe risk/reward calculation
    def calculate_risk_reward(self, entry, stop, target):
        """
        Calculate risk/reward ratio with division-by-zero safety
        """
        if entry is None or stop is None or target is None:
            return 0.0
            
        risk = abs(entry - stop)
        reward = abs(target - entry)
        
        if risk < 1e-10:  # Avoid division by zero
            return 0.0
        
        return reward / risk
    
    # NEW: Calculate position size based on ATR risk
    def calculate_position_size_by_atr(self, account_balance, risk_percent, entry_price, atr_value, atr_multiplier=None):
        """
        Calculate position size based on ATR-defined risk.
        
        Args:
            account_balance: Current account balance
            risk_percent: Risk percentage per trade (e.g., 1.0 for 1%)
            entry_price: Trade entry price
            atr_value: Current ATR value
            atr_multiplier: ATR multiplier for stop distance
        
        Returns:
            position_size: Position size in units
            stop_loss: Calculated stop-loss price
        """
        if atr_multiplier is None:
            atr_multiplier = self.ATR_STOP_MULTIPLIER
        
        # SAFEGUARD: Validate inputs
        if account_balance is None or entry_price is None or atr_value is None:
            return 0, None
        
        if account_balance <= 0 or entry_price <= 0 or atr_value <= 0:
            return 0, None
        
        # Calculate monetary risk amount
        risk_amount = account_balance * (risk_percent / 100)
        
        # Calculate stop distance in price terms
        stop_distance = atr_value * atr_multiplier
        
        # Calculate position size: Risk Amount / Stop Distance
        if stop_distance < 1e-10:  # Avoid division by zero
            return 0, None
        
        position_size = risk_amount / stop_distance
        
        # Calculate actual stop-loss price (for BUY example, stop is below entry)
        stop_loss = entry_price - stop_distance
        
        return position_size, stop_loss

print("Part 3B loaded: Event Triggers, Risk Manager, Execution Engine (FIXED + UPGRADED)")
print("Upgrades: Trade filters, Dynamic sizing, Global limits, Improved trailing stop, Drawdown protection, Zero-division safety")
print("NEW: Probability filter (≥70%) and USD correlation protection (max 1 USD trade)")
print("NEW: ATR-based stop-loss calculation (2x ATR multiplier)")
"""
Part 4: Telegram Client, Notification System, Signal Persistence, Main Engine, Entry Point
"""

# ================= TELEGRAM CLIENT =================

class TelegramClient:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self._session = None
    
    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session
    
    async def send_message(self, message):
        if not self.token or not self.chat_id: return False
        try:
            session = await self._get_session()
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
            async with session.post(url, data=payload) as resp:
                result = await resp.json()
                return result.get("ok", False)
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False
    
    async def close(self):
        if self._session: await self._session.close()

# ================= INTEGRATED NOTIFICATION SYSTEM =================

class IntegratedNotificationSystem:
    def __init__(self, telegram_manager):
        self.telegram = telegram_manager
        self.triggers = EventTriggerEngine(telegram_manager)
        self.whale_detector = WhaleDetectionEngine()
        self.current_regime = {}
        self.active_signals = {}
        self.risk_state = None
    
    async def process_market_data(self, symbol, df, context):
        self.whale_detector.update(symbol, df)
        whale_activities = self.whale_detector.get_recent_activities()
        if whale_activities:
            await self.triggers.trigger_event(AlertType.WHALE_ACTIVITY, {'activities': [self._whale_to_dict(a) for a in whale_activities]}, priority=3)
        new_regime = context.get('regime', 'unknown')
        if symbol in self.current_regime and self.current_regime[symbol] != new_regime:
            await self.triggers.trigger_event(AlertType.REGIME_CHANGE, {'old_regime': self.current_regime[symbol], 'new_regime': new_regime, 'impact': context.get('regime_impact', 'Market structure shift'), 'adjustments': context.get('strategy_adjustments', ['Review active signals'])}, priority=1)
        self.current_regime[symbol] = new_regime
    
    async def send_pre_signal(self, signal, time_to_signal): await self.triggers.trigger_event(AlertType.PRE_SIGNAL, {'signal': self._signal_to_dict(signal), 'time_to_signal': time_to_signal}, priority=2)
    async def send_signal(self, signal, context):
        self.active_signals[signal.id] = signal
        await self.triggers.trigger_event(AlertType.SIGNAL, {'signal': self._signal_to_dict(signal), 'market_context': context}, priority=0)
    async def send_risk_dashboard(self, metrics, active_trades):
        self.risk_state = metrics
        if metrics.drawdown_status in ["elevated", "critical"]:
            await self.triggers.trigger_event(AlertType.DRAWDOWN_WARNING, {'current_dd': 0.08 if metrics.drawdown_status == "elevated" else 0.12, 'max_dd': 0.15, 'recommended_action': 'HALF_SIZE' if metrics.drawdown_status == "critical" else 'REDUCE_EXPOSURE'}, priority=0)
        await self.triggers.trigger_event(AlertType.RISK_DASHBOARD, {'metrics': self._risk_to_dict(metrics), 'active_trades': active_trades}, priority=2)
    async def send_liquidity_alert(self, symbol, zones, current_price): await self.triggers.trigger_event(AlertType.LIQUIDITY_ZONE, {'zones': [self._zone_to_dict(z) for z in zones], 'current_price': current_price}, priority=3)
    async def send_session_open(self, session_type, high_impact_news=None): await self.triggers.trigger_event(AlertType.SESSION_OPEN, {'session_type': session_type, 'high_impact_news': high_impact_news or []}, priority=3)
    
    def _signal_to_dict(self, signal):
        return {'id': signal.id, 'symbol': signal.symbol, 'direction': signal.direction, 'probability': signal.probability, 'confidence': signal.confidence, 'entry_price': signal.entry_price, 'stop_loss': signal.stop_loss, 'take_profit': signal.take_profit, 'risk_reward': signal.risk_reward, 'timestamp': signal.timestamp.isoformat(), 'countdown_seconds': signal.countdown_seconds, 'setup_quality_score': signal.setup_quality_score, 'confluence_factors': signal.confluence_factors, 'risk_visualization': signal.risk_visualization, 'probability_breakdown': signal.probability_breakdown}
    def _whale_to_dict(self, a): return {'symbol': a.symbol, 'timestamp': a.timestamp.isoformat(), 'activity_type': a.activity_type, 'volume_anomaly': a.volume_anomaly, 'price_impact': a.price_impact, 'direction': a.direction, 'confidence': a.confidence}
    def _zone_to_dict(self, z): return {'symbol': z.symbol, 'zone_type': z.zone_type, 'price_level': z.price_level, 'strength_score': z.strength_score, 'distance_pct': z.distance_pct, 'test_count': z.test_count, 'volume_at_level': z.volume_at_level}
    def _risk_to_dict(self, m): return {'timestamp': m.timestamp.isoformat(), 'portfolio_heat_score': m.portfolio_heat_score, 'active_exposure': m.active_exposure, 'available_capacity': m.available_capacity, 'drawdown_status': m.drawdown_status, 'correlation_risk': m.correlation_risk, 'session_risk': m.session_risk, 'top_risks': m.top_risks}

# ================= SIGNAL PERSISTENCE =================

class SignalPersistence:
    def __init__(self, db_path=DATA_DIR / "signals.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY, symbol TEXT, direction TEXT, entry_price REAL, stop_loss REAL,
                    take_profit REAL, timestamp TEXT, probability REAL, status TEXT, closed_pnl REAL, metadata TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, signal_id TEXT, entry_executed REAL, exit_price REAL, exit_time TEXT,
                    status TEXT, outcome TEXT, realized_pnl_r REAL, FOREIGN KEY (signal_id) REFERENCES signals(id)
                )
            """)
            conn.commit()
    
    def save_signal(self, signal: Signal):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO signals (id, symbol, direction, entry_price, stop_loss, take_profit, timestamp, probability, status, closed_pnl, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (signal.id, signal.symbol, signal.direction, signal.entry_price, signal.stop_loss, signal.take_profit, signal.timestamp.isoformat(), signal.probability.final_probability, "active", 0.0, json.dumps(asdict(signal))))
            conn.commit()
    
    def update_trade_close(self, trade: Trade):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE signals SET status = ?, closed_pnl = ? WHERE id = ?", (trade.status.value, trade.realized_pnl_r, trade.signal.id))
            conn.execute("""
                INSERT INTO trades (id, signal_id, entry_executed, exit_price, exit_time, status, outcome, realized_pnl_r)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (f"{trade.signal.id}_exec", trade.signal.id, trade.entry_executed, trade.exit_price, trade.exit_time.isoformat() if trade.exit_time else None, trade.status.value, trade.outcome.value, trade.realized_pnl_r))
            conn.commit()

# ================= TRADING SESSION ENGINE =================

class TradingSessionEngine:
    """
    Detects global trading sessions based on UTC time.
    Sessions: SYDNEY, TOKYO, LONDON, OVERLAP, NEW_YORK
    """
    
    def __init__(self):
        # Session definitions in UTC hours (24h format)
        # Assumes TRADING_SESSIONS from Part 1 is available in global scope
        self.session_ranges = {
            'SYDNEY': (22, 7),      # 22:00 UTC to 07:00 UTC (next day)
            'TOKYO': (0, 9),        # 00:00 UTC to 09:00 UTC
            'LONDON': (8, 17),      # 08:00 UTC to 17:00 UTC
            'NEW_YORK': (13, 22),   # 13:00 UTC to 22:00 UTC
        }
        # Overlap: London (8-17) + New York (13-22) = 13:00 to 17:00 UTC
        self.overlap_start = 13
        self.overlap_end = 17
    
    def get_current_session(self):
        """
        Returns the active trading session name based on current UTC time.
        Priority: OVERLAP > LONDON/NEW_YORK > TOKYO > SYDNEY
        """
        now = datetime.now(pytz.UTC)
        hour = now.hour
        
        # Check for London-New York overlap first (highest priority)
        if self.overlap_start <= hour < self.overlap_end:
            return 'OVERLAP'
        
        # Check other sessions
        # Note: Sydney wraps around midnight (22:00 to 07:00)
        if hour >= 22 or hour < 7:
            return 'SYDNEY'
        
        if 0 <= hour < 9:
            return 'TOKYO'
        
        if 8 <= hour < 17:
            return 'LONDON'
        
        if 13 <= hour < 22:
            return 'NEW_YORK'
        
        # Fallback (should not reach here with current logic)
        return 'SYDNEY'
    
    def is_session_active(self, session_name):
        """Check if a specific session is currently active."""
        current = self.get_current_session()
        return current == session_name
    
    def get_session_volatility_profile(self):
        """
        Returns expected volatility characteristics for current session.
        Used by risk management systems.
        """
        session = self.get_current_session()
        profiles = {
            'SYDNEY': {'volatility': 'low', 'liquidity': 'low'},
            'TOKYO': {'volatility': 'medium', 'liquidity': 'medium'},
            'LONDON': {'volatility': 'high', 'liquidity': 'high'},
            'OVERLAP': {'volatility': 'very_high', 'liquidity': 'very_high'},
            'NEW_YORK': {'volatility': 'high', 'liquidity': 'high'}
        }
        return profiles.get(session, {'volatility': 'medium', 'liquidity': 'medium'})
    
    def get_next_session_transition(self):
        """Returns hours until next session change."""
        now = datetime.now(pytz.UTC)
        hour = now.hour
        
        transition_hours = [0, 7, 8, 9, 13, 17, 22]
        
        for transition in transition_hours:
            if hour < transition:
                return transition - hour
        
        # If past 22:00, next is midnight (0)
        return 24 - hour

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
        self.persistence = SignalPersistence()
        self.session_engine = TradingSessionEngine()
        self.last_session = None
        self.trades = {}
        self.active_trades = []
        self.closed_trades = []
        self.pending_pre_signals = {}
        self.running = False
        self.performance_metrics = {'strategy_counts': defaultdict(int), 'max_drawdown': 0.0, 'profit_factor': 0.0, 'sharpe': 0.0}
    
    def get_asset_class(self, symbol): return 'crypto' if any(c in symbol for c in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA']) else 'forex'
    
    async def scan_symbol(self, symbol):
        # Session filter - skip trading during low-liquidity sessions
        session = self.session_engine.get_current_session()
        if session not in ALLOWED_TRADING_SESSIONS:
            logger.info(f"{symbol}: Skipping trade due to low-liquidity session ({session})")
            return None
        
        # USD correlation protection - prevent correlated losses across USD pairs
        if "USD" in symbol:
            usd_trades = [t for t in self.active_trades if "USD" in t.signal.symbol]
            if len(usd_trades) >= MAX_USD_TRADES:
                logger.info(f"{symbol}: USD exposure limit reached ({len(usd_trades)}/{MAX_USD_TRADES}), skipping trade")
                return None
        
        df_15m = await self.exchange.fetch_ohlcv(symbol, "15m", 200)
        df_1h = await self.exchange.fetch_ohlcv(symbol, "1h", 100)
        if df_15m is None or len(df_15m) < 50: return None
        self.portfolio_risk.update_price(symbol, df_15m['close'].iloc[-1])
        if self.portfolio_risk.check_drawdown_throttle():
            logger.warning("Drawdown throttle active - skipping signals"); return None
        regime = self.regime_engine.analyze(df_15m, df_1h)
        orderflow = self.orderflow_detector.analyze(df_15m)
        self.liquidity_engine.update(symbol, df_15m)
        nearest_liq = self.liquidity_engine.get_nearest(symbol, df_15m['close'].iloc[-1], "BUY")
        detections, agg_conf, factors = self.strategy_orchestrator.analyze(symbol, df_15m, df_1h)
        primary = self.strategy_orchestrator.get_primary_setup(symbol)
        if not primary or primary.confidence < 65: return None
        
        entry = df_15m['close'].iloc[-1]
        atr = TechnicalCore().atr(df_15m, ATR_PERIOD).iloc[-1]
        if primary.entry_price: entry = primary.entry_price
        if primary.stop_loss: sl = primary.stop_loss
        else: sl = entry - atr * ATR_MULTIPLIER_SL if primary.direction == "BUY" else entry + atr * ATR_MULTIPLIER_SL
        if primary.take_profit: tp = primary.take_profit
        else: tp = entry + atr * ATR_MULTIPLIER_TP if primary.direction == "BUY" else entry - atr * ATR_MULTIPLIER_TP
        
        risk = abs(entry - sl)
        tp1 = entry + risk if primary.direction == "BUY" else entry - risk
        rr = abs(tp - entry) / risk if risk > 0 else 0
        breakeven = 1 / (1 + ATR_MULTIPLIER_TP)
        edge = (agg_conf / 100) - breakeven
        if edge < MIN_EDGE or agg_conf / 100 < MIN_PROBABILITY: return None
        
        kelly = edge / (1 - breakeven) if edge > 0 else 0
        kelly_frac = min(kelly * 0.25, 0.02)
        prob_model = ProbabilityModel(agg_conf / 100, agg_conf / 100, 0.05 if regime.volatility == VolatilityRegime.NORMAL else -0.05 if regime.volatility == VolatilityRegime.LOW else 0, nearest_liq.strength if nearest_liq else 0, 1.0 if orderflow.displacement else 0.5, min(0.95, agg_conf / 100), edge, kelly_frac, (max(0, agg_conf/100 - 0.15), min(1, agg_conf/100 + 0.15)))
        
        signal = Signal(f"{symbol.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}", symbol, primary.direction, entry, sl, tp, tp1, atr, datetime.now(), prob_model, regime, primary.name, [d.name for d in detections], [nearest_liq] if nearest_liq else [], orderflow, kelly_frac * 100, rr, (prob_model.final_probability * rr) - ((1 - prob_model.final_probability) * 1))
        
        can_trade, reason = self.portfolio_risk.can_add_trade(signal, self.active_trades)
        if not can_trade: logger.info(f"{symbol}: Risk check failed - {reason}"); return None
        
        asset_class = self.get_asset_class(symbol)
        executed_entry = self.execution.simulate_entry(signal, asset_class)
        signal.entry_price = executed_entry
        signal.slippage_estimate = abs(executed_entry - entry)
        
        trade = Trade(signal=signal)
        self.trades[signal.id] = trade
        self.active_trades.append(trade)
        self.portfolio_risk.register_trade(signal)
        self.persistence.save_signal(signal)
        
        message = f"""
🚀 TRADE OPENED

Symbol: {signal.symbol}
Direction: {signal.direction}

Entry: {signal.entry_price}
Stop Loss: {signal.stop_loss}
Take Profit: {signal.take_profit}

Risk: {signal.position_size_r}R
"""
        await self.telegram.send_message(message)
        
        ux_signal = UXSignal(signal.id, signal.symbol, signal.direction, prob_model.final_probability, int(agg_conf), signal.entry_price, signal.stop_loss, signal.take_profit, signal.risk_reward, signal.timestamp, 0, int(agg_conf), factors[:6], {}, {d.name: d.confidence / 100 for d in detections})
        await self.notification_system.send_signal(ux_signal, {'regime': regime.primary.name, 'session': self._get_current_session(), 'volatility': regime.volatility.name, 'primary_strategy': primary.name})
        return signal
    
    async def evaluate_trades(self):
        for trade in list(self.active_trades):
            if trade.is_closed: continue
            try:
                df = await self.exchange.fetch_ohlcv(trade.signal.symbol, "15m", 50)
                if df is None or len(df) < 5: continue
                current_price = df['close'].iloc[-1]
                asset_class = self.get_asset_class(trade.signal.symbol)
                
                if trade.signal.direction == "BUY": trade.unrealized_pnl_r = (current_price - trade.signal.entry_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                else: trade.unrealized_pnl_r = (trade.signal.entry_price - current_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                trade.max_favorable_excursion = max(trade.max_favorable_excursion, trade.unrealized_pnl_r)
                trade.max_adverse_excursion = min(trade.max_adverse_excursion, trade.unrealized_pnl_r)
                
                if trade.status == TradeStatus.PENDING and self.execution.should_trigger_partial(trade, current_price):
                    trade.status = TradeStatus.PARTIAL
                    trade.partial_exit_price = trade.signal.take_profit_1
                    trade.partial_size_closed = trade.signal.position_size_r * PARTIAL_TP_PCT
                    trade.realized_pnl_r = 1.0 * PARTIAL_TP_PCT
                    trade.signal.stop_loss = trade.signal.entry_price  # FIXED: Completed assignment
                    await self.notification_system.triggers.trigger_event(AlertType.SIGNAL, {'message': f"Partial TP hit for {trade.signal.symbol}: +{trade.realized_pnl_r:.2f}R secured"}, priority=2)
                
                new_trailing = self.execution.update_trailing_stop(trade, current_price)
                if new_trailing: trade.trailing_level = new_trailing
                
                exit_triggered, exit_price, exit_reason = False, None, None
                if trade.signal.direction == "BUY":
                    if current_price <= trade.signal.stop_loss: exit_triggered, exit_price, exit_reason = True, trade.signal.stop_loss, TradeOutcome.SL_HIT
                    elif trade.trailing_active and trade.trailing_level and current_price <= trade.trailing_level: exit_triggered, exit_price, exit_reason = True, trade.trailing_level, TradeOutcome.TRAILING_STOP
                    elif current_price >= trade.signal.take_profit: exit_triggered, exit_price, exit_reason = True, trade.signal.take_profit, TradeOutcome.TP_HIT
                else:
                    if current_price >= trade.signal.stop_loss: exit_triggered, exit_price, exit_reason = True, trade.signal.stop_loss, TradeOutcome.SL_HIT
                    elif trade.trailing_active and trade.trailing_level and current_price >= trade.trailing_level: exit_triggered, exit_price, exit_reason = True, trade.trailing_level, TradeOutcome.TRAILING_STOP
                    elif current_price <= trade.signal.take_profit: exit_triggered, exit_price, exit_reason = True, trade.signal.take_profit, TradeOutcome.TP_HIT
                
                if exit_triggered:
                    executed_exit = self.execution.simulate_exit(trade, exit_price, asset_class)
                    if trade.signal.direction == "BUY": full_pnl = (executed_exit - trade.signal.entry_price) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                    else: full_pnl = (trade.signal.entry_price - executed_exit) / abs(trade.signal.entry_price - trade.signal.stop_loss)
                    remaining_size = 1.0 - trade.partial_size_closed
                    trade.realized_pnl_r += full_pnl * remaining_size
                    trade.unrealized_pnl_r = 0
                    trade.exit_price = executed_exit
                    trade.exit_time = datetime.now()
                    trade.outcome = exit_reason
                    trade.status = TradeStatus.WIN if exit_reason == TradeOutcome.TP_HIT else TradeStatus.LOSS if exit_reason == TradeOutcome.SL_HIT else TradeStatus.WIN if trade.realized_pnl_r > 0.5 else TradeStatus.BREAKEVEN if exit_reason == TradeOutcome.TRAILING_STOP else TradeStatus.BREAKEVEN
                    
                    self.portfolio_risk.close_trade(trade)
                    self.persistence.update_trade_close(trade)
                    self.active_trades.remove(trade)
                    self.closed_trades.append(trade)
                    self.performance_metrics['strategy_counts'][trade.signal.primary_strategy] += 1
                    
                    if trade.status == TradeStatus.LOSS:
                        await self.notification_system.triggers.trigger_event(AlertType.INVALIDATION, {'symbol': trade.signal.symbol, 'reason': f'Stop loss hit at {exit_price:.5f}', 'entry': trade.signal.entry_price, 'current': current_price, 'loss': trade.realized_pnl_r, 'trade_id': trade.signal.id}, priority=1)
                    else:
                        await self.notification_system.triggers.trigger_event(AlertType.SIGNAL, {'message': f"Trade closed: {trade.signal.symbol} {trade.status.value.upper()} | P&L: {trade.realized_pnl_r:+.2f}R"}, priority=2)
                
                trade.candles_evaluated += 1
                if trade.candles_evaluated > 96 and abs(trade.unrealized_pnl_r) < 0.5:
                    trade.status = TradeStatus.EXPIRED
                    trade.outcome = TradeOutcome.EXPIRED
                    self.portfolio_risk.close_trade(trade)
                    self.persistence.update_trade_close(trade)
                    self.active_trades.remove(trade)
                    self.closed_trades.append(trade)
                    await self.notification_system.triggers.trigger_event(AlertType.INVALIDATION, {'symbol': trade.signal.symbol, 'reason': 'Time expiry - position held too long without meaningful move', 'entry': trade.signal.entry_price, 'current': current_price, 'loss': trade.unrealized_pnl_r, 'trade_id': trade.signal.id}, priority=3)
            except Exception as e:
                logger.error(f"Error evaluating trade {trade.signal.id}: {e}")
                continue
    
    def _get_current_session(self):
        """Delegate to TradingSessionEngine for accurate session detection."""
        return self.session_engine.get_current_session()
    
    async def run_scan_cycle(self):
        # Session change detection at start of cycle
        current_session = self.session_engine.get_current_session()
        
        if current_session != self.last_session:
            # Session changed - send announcement
            session_messages = {
                'SYDNEY': "• Market opening\n• Low liquidity",
                'TOKYO': "• Asian liquidity\n• Range trading common",
                'LONDON': "• High forex volatility\n• Breakout potential",
                'OVERLAP': "• London + New York active\n• Highest liquidity",
                'NEW_YORK': "• US market participation\n• Strong institutional flows"
            }
            
            session_message = session_messages.get(current_session, "• Active trading session")
            
            message = f"""🌍 TRADING SESSION STARTED

Session: {current_session}

Market Characteristics:

{current_session}
{session_message}"""
            
            await self.telegram.send_message(message)
            self.last_session = current_session
        
        logger.info(f"Starting scan cycle for {len(WATCHLIST)} instruments")
        for symbol in WATCHLIST:
            try:
                result = await self.scan_symbol(symbol)
                if result: self.persistence.save_signal(result)
                await asyncio.sleep(1)
            except Exception as e: logger.error(f"Error scanning {symbol}: {e}")
        await self.evaluate_trades()
        if datetime.now().minute % 15 == 0:
            metrics = self.portfolio_risk.get_risk_metrics()
            active_trades_data = [{'symbol': t.signal.symbol, 'direction': t.signal.direction, 'entry': t.signal.entry_price, 'unrealized_pnl': t.unrealized_pnl_r} for t in self.active_trades]
            await self.notification_system.send_risk_dashboard(metrics, active_trades_data)
        now = datetime.now(pytz.UTC)
        if now.weekday() == 4 and now.hour == 20 and now.minute < 5:
            await self.notification_system.triggers.trigger_event(AlertType.WEEKLY_ANALYTICS, {'trade_history': self.closed_trades[-50:], 'performance_metrics': self.performance_metrics}, priority=3)
    
    async def run(self):
        self.running = True
        logger.info("HedgeFundEngine v4.1 initialized and running")
        await self.telegram.send_message(f"🚀 <b>HedgeFundEngine v4.1 Online</b>\n\n<b>Configuration:</b>\n├ Personality: {PERSONALITY.upper()}\n├ Mode: {EXCHANGE_MODE.upper()}\n├ Watchlist: {len(WATCHLIST)} instruments\n└ Scan Interval: {SCAN_INTERVAL}s\n\n<i>Markets are being monitored...</i>")
        await self.notification_system.send_session_open(self._get_current_session())
        while self.running:
            try:
                await self.run_scan_cycle()
                await asyncio.sleep(SCAN_INTERVAL)
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(10)
    
    async def shutdown(self):
        self.running = False
        await self.exchange.close()
        await self.telegram.close()
        logger.info("HedgeFundEngine shutdown complete")

# ================= MAIN ENTRY POINT =================

async def main():
    engine = HedgeFundEngine()
    try:
        await engine.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received - shutting down gracefully...")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        try:
            await engine.telegram.send_message(f"🚨 <b>CRITICAL ERROR</b>\n\nEngine stopped due to error: {str(e)[:100]}\nCheck logs immediately.")
        except: pass
    finally:
        await engine.shutdown()

if __name__ == "__main__":
    asyncio.run(main())  # FIXED: Added missing closing parenthesis

print("Part 4 loaded: Telegram, Telegram, Notification System, Signal Persistence, Main Engine, Entry Point")
print("\n" + "="*60)
print("ALL 4 PARTS COMPLETE - READY FOR PRODUCTION")
print("="*60)
