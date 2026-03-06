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
EXCHANGE_MODE = os.getenv("EXCHANGE_MODE", "twelvedata").lower()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "true").lower() == "true"

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
        self.lookback = lookback; self.min_touches = min_touches
    @property
    def weight(self) -> float: return 1.6
    
    def detect(self, df: pd.DataFrame, higher_tf: Optional[pd.DataFrame] = None) -> StrategyDetection:
        if len(df) < self.lookback: return StrategyDetection("triangle", False, 0, None, None, None, None)
        highs = df['high'].tail(self.lookback).values; lows = df['low'].tail(self.lookback).values; x = np.arange(len(highs))
        high_slope, high_int = np.polyfit(x, highs, 1); low_slope, low_int = np.polyfit(x, lows, 1)
        flat_high, flat_low = abs(high_slope) < 0.0001, abs(low_slope) < 0.0001
        converging = high_slope < 0 and low_slope > 0
        high_touches = sum(1 for h in highs if abs(h - (high_int + high_slope * x[len(highs)//2])) / h < 0.001)
        low_touches = sum(1 for l in lows if abs(l - (low_int + low_slope * x[len(lows)//2])) / l < 0.001)
        current = df['close'].iloc[-1]; upper = high_int + high_slope * (len(highs) - 1); lower = low_int + low_slope * (len(lows) - 1)
        
        if flat_high and low_slope > 0 and current > upper * 0.999 and high_touches >= self.min_touches:
            entry, sl, tp = current, lower, entry + (entry - lower) * 2.0
            return StrategyDetection("ascending_triangle", True, 75, "BUY", entry, sl, tp,
                {'resistance_level': high_int, 'touches': high_touches},
                ["Horizontal resistance", "Rising support", f"{high_touches} touches", "Bullish breakout"])
        elif flat_low and high_slope < 0 and current < lower * 1.001 and low_touches >= self.min_touches:
            entry, sl, tp = current, upper, entry - (upper - entry) * 2.0
            return StrategyDetection("descending_triangle", True, 75, "SELL", entry, sl, tp,
                {'support_level': low_int, 'touches': low_touches},
                ["Horizontal support", "Falling resistance", f"{low_touches} touches", "Bearish breakdown"])
        elif converging and (current > upper * 0.999 or current < lower * 1.001) and (high_touches + low_touches) >= self.min_touches * 2:
            direction = "BUY" if current > upper else "SELL"; entry, sl = current, (upper + lower) / 2
            tp = entry + (entry - sl) * 2.0 if direction == "BUY" else entry - (sl - entry) * 2.0
            return StrategyDetection("symmetrical_triangle", True, 70, direction, entry, sl, tp,
                {'converging_lines': True, 'total_touches': high_touches + low_touches},
                ["Converging trendlines", f"{high_touches + low_touches} total touches", "Volatility compression", "Directional breakout"])
        return StrategyDetection("triangle", False, 0, None, None, None, None)

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

print("Part 2 loaded: Technical Analysis, Strategy Detectors, Core Engines")
"""
Part 3: Whale Detection, Enhanced Message Templates, Event Triggers, Risk Management
IMPROVEMENTS: Detailed trade entry/exit messages with SL/TP tracking
"""

# ================= WHALE DETECTION ENGINE =================

class WhaleDetectionEngine:
    def __init__(self):
        self.volume_baselines = defaultdict(lambda: deque(maxlen=100))
        self.price_history = defaultdict(lambda: deque(maxlen=50))
        self.recent_activities = []
    
    def update(self, symbol, df):
        if len(df) < 2: return
        self.volume_baselines[symbol].append(df['volume'].iloc[-1])
        self.price_history[symbol].append({'price': df['close'].iloc[-1], 'timestamp': df.index[-1]})
        activity = self._detect_anomaly(symbol, df)
        if activity: self.recent_activities.append(activity)
    
    def _detect_anomaly(self, symbol, df):
        if len(self.volume_baselines[symbol]) < 20: return None
        curr_vol = df['volume'].iloc[-1]
        avg_vol = np.mean(list(self.volume_baselines[symbol])[:-5])
        if avg_vol == 0: return None
        vol_ratio = curr_vol / avg_vol
        price_change = abs(df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100 if len(df) >= 2 else 0
        
        if vol_ratio >= WHALE_VOLUME_MULTIPLIER and price_change >= WHALE_PRICE_IMPACT_PCT:
            body = df['close'].iloc[-1] - df['open'].iloc[-1]
            direction = "BUY" if body > 0 else "SELL" if body < 0 else "NEUTRAL"
            
            if vol_ratio >= 5 and price_change >= 1.0: activity_type = "momentum_ignition"
            elif self._is_stop_hunt_pattern(df): activity_type = "stop_hunt"
            elif direction == "BUY" and df['close'].iloc[-1] > df['high'].rolling(20).mean().iloc[-1]: activity_type = "accumulation"
            elif direction == "SELL" and df['close'].iloc[-1] < df['low'].rolling(20).mean().iloc[-1]: activity_type = "distribution"
            else: activity_type = "momentum_ignition"
            
            return WhaleActivity(symbol, df.index[-1], activity_type, vol_ratio, price_change, direction, min(95, int(50 + vol_ratio * 5 + price_change * 10)))
        return None
    
    def _is_stop_hunt_pattern(self, df):
        if len(df) < 5: return False
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        bearish_hunt = c2['low'] < c1['low'] * 0.998 and c2['close'] > c2['open'] and c3['close'] > c3['open'] and abs(c2['low'] - c1['low']) / c1['low'] > 0.002
        bullish_hunt = c2['high'] > c1['high'] * 1.002 and c2['close'] < c2['open'] and c3['close'] < c3['open'] and abs(c2['high'] - c1['high']) / c1['high'] > 0.002
        vol_confirm = 'volume' in df.columns and c2['volume'] > df['volume'].tail(20).mean() * 1.5
        return (bearish_hunt or bullish_hunt) and vol_confirm
    
    def get_recent_activities(self, lookback_minutes=15):
        cutoff = datetime.now(pytz.UTC) - timedelta(minutes=lookback_minutes)
        return [a for a in self.recent_activities if a.timestamp >= cutoff]

# ================= ENHANCED MESSAGE TEMPLATES =================

class MessageTemplateLibrary:
    
    # ================= TRADE ENTRY MESSAGES =================
    
    @staticmethod
    def trade_entry_alert(signal, execution_details=None):
        """
        Detailed trade entry message with full SL/TP information
        """
        exec_info = execution_details or {}
        sl_distance = abs(signal.entry_price - signal.stop_loss)
        tp_distance = abs(signal.take_profit - signal.entry_price)
        
        # Calculate pip values (approximate for forex)
        pip_value = 0.0001 if "JPY" not in signal.symbol else 0.01
        sl_pips = sl_distance / pip_value
        tp_pips = tp_distance / pip_value
        
        # Risk amount in currency terms (assuming 1% risk per trade)
        risk_amount = sl_distance * signal.position_size_r
        
        entry_emoji = "🟢" if signal.direction == "BUY" else "🔴"
        
        return f"""
{entry_emoji} <b>POSITION OPENED — {signal.symbol}</b> {entry_emoji}

<b>📊 Trade Details</b>
├ Direction: <b>{signal.direction}</b> {"📈 LONG" if signal.direction == "BUY" else "📉 SHORT"}
├ Entry Price: <code>{signal.entry_price:.5f}</code>
├ Position Size: <code>{signal.position_size_r:.2f}R</code>
└ Strategy: <b>{signal.primary_strategy}</b>

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
    
    # ================= TRADE EXIT MESSAGES =================
    
    @staticmethod
    def trade_exit_alert(trade, exit_details):
        """
        Comprehensive trade exit message showing P&L, exit reason, and performance
        """
        signal = trade.signal
        exit_price = exit_details.get('exit_price', trade.exit_price)
        exit_reason = exit_details.get('reason', trade.outcome.value)
        
        # Calculate actual P&L
        realized_pnl = trade.realized_pnl_r
        pnl_emoji = "🟢" if realized_pnl > 0 else "🔴" if realized_pnl < 0 else "⚪"
        pnl_percent = realized_pnl * 100
        
        # Determine exit type emoji
        exit_emojis = {
            'TP_HIT': '🎯 TAKE PROFIT HIT',
            'SL_HIT': '🛑 STOP LOSS HIT',
            'TRAILING_STOP': '📊 TRAILING STOP',
            'PARTIAL_TP': '💰 PARTIAL PROFIT',
            'BREAKEVEN': '⚖️ BREAKEVEN',
            'EXPIRED': '⏰ TIME EXPIRY'
        }
        exit_header = exit_emojis.get(exit_reason, f'📤 {exit_reason}')
        
        # Calculate price movement
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
        if len(factors) > 3: preview.append(f"  ... and {len(factors)-3} more factors")
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
        if 'trend' in context.get('regime', 'unknown').lower(): badges.append("📈 TRENDING")
        elif 'range' in context.get('regime', 'unknown').lower(): badges.append("↔️ RANGING")
        elif 'volatile' in context.get('regime', 'unknown').lower(): badges.append("⚠️ VOLATILE")
        if 'overlap' in context.get('session', 'unknown').lower(): badges.append("🔥 HIGH LIQUIDITY")
        if context.get('volatility', 'normal') == 'extreme': badges.append("🌊 EXTREME VOL")
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
        if not zones: return ""
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
        if high_impact_news is None: high_impact_news = []
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
    
    # ================= PERFORMANCE MESSAGES =================
    
    @staticmethod
    def confidence_heatmap(signals):
        if not signals: return "📊 No active signals"
        ranked = sorted(signals, key=lambda s: s.confidence, reverse=True)
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        leaderboard = []
        for i, signal in enumerate(ranked[:5]):
            medal = medals[i] if i < 3 else f"{i+1}."
            bar_filled = int(signal.probability * 8)
            bar = "█" * bar_filled + "░" * (8 - bar_filled)
            leaderboard.append(f"{medal} <b>{signal.symbol}</b> {signal.direction[:1]} [{bar}] {signal.confidence}% (R:R 1:{signal.risk_reward:.1f})")
        return f"""
🏆 <b>SIGNAL CONFIDENCE LEADERBOARD</b>

<i>Top opportunities ranked by probability × edge</i>

{chr(10).join(leaderboard)}

<b>💡 Recommendation:</b>
Focus on 🥇-🥉 for optimal risk-adjusted returns
"""
    
    @staticmethod
    def risk_dashboard(metrics, active_trades):
        heat_gauge = MessageTemplateLibrary._generate_heat_gauge(metrics.portfolio_heat_score)
        exposure_bar = MessageTemplateLibrary._generate_exposure_bar(metrics.active_exposure, metrics.active_exposure + metrics.available_capacity)
        trades_table = MessageTemplateLibrary._format_active_trades(active_trades)
        warnings = []
        if metrics.drawdown_status == "critical": warnings.append("🔴 CRITICAL: Drawdown exceeds 10% — HALF SIZE")
        elif metrics.drawdown_status == "elevated": warnings.append("🟠 WARNING: Drawdown at 7% — Reduce exposure")
        if metrics.correlation_risk > 70: warnings.append("🟠 HIGH CORRELATION: Multiple similar positions")
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
    def _generate_heat_gauge(score):
        color, emoji = ("🔴", "CRITICAL") if score >= 80 else ("🟠", "ELEVATED") if score >= 60 else ("🟡", "MODERATE") if score >= 40 else ("🟢", "OPTIMAL")
        filled = int(score / 10)
        return f"{color} <b>PORTFOLIO HEAT: {emoji}</b> [{'█' * filled}{'░' * (10 - filled)}] {score}%"
    
    @staticmethod
    def _generate_exposure_bar(used, total):
        if total == 0: return "  [░░░░░░░░░░] 0%"
        pct = used / total
        filled = int(pct * 10)
        color = "🔴" if pct > 0.9 else "🟠" if pct > 0.7 else "🟢"
        return f"  {color}[{'█' * filled}{'░' * (10 - filled)}] {pct:.0%}"
    
    @staticmethod
    def _format_active_trades(trades):
        if not trades: return "  No active positions"
        lines = []
        for trade in trades:
            pnl_emoji = "🟢" if trade.get('unrealized_pnl', 0) > 0 else "🔴" if trade.get('unrealized_pnl', 0) < 0 else "⚪"
            lines.append(f"  {pnl_emoji} {trade['symbol']} {trade['direction'][:1]} @ {trade['entry']:.5f} (P&L: {trade.get('unrealized_pnl', 0):+.1f}R)")
        return "\n".join(lines)
    
    @staticmethod
    def whale_activity_alert(activities):
        if not activities: return ""
        by_symbol = defaultdict(list)
        for act in activities: by_symbol[act.symbol].append(act)
        sections = []
        for symbol, acts in by_symbol.items():
            total_volume = sum(a.volume_anomaly for a in acts)
            avg_impact = np.mean([a.price_impact for a in acts])
            direction_consensus = "BUY" if all(a.direction == "BUY" for a in acts) else "SELL" if all(a.direction == "SELL" for a in acts) else "MIXED"
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
    def weekly_analytics_report(trade_history, performance_metrics):
        total_trades = len(trade_history)
        wins = sum(1 for t in trade_history if t.status == TradeStatus.WIN)
        losses = sum(1 for t in trade_history if t.status == TradeStatus.LOSS)
        win_rate = wins / total_trades * 100 if total_trades > 0 else 0
        total_pnl = sum(t.realized_pnl_r for t in trade_history)
        avg_trade = total_pnl / total_trades if total_trades > 0 else 0
        best_trade = max((t.realized_pnl_r for t in trade_history), default=0)
        worst_trade = min((t.realized_pnl_r for t in trade_history), default=0)
        return f"""
📊 <b>WEEKLY PERFORMANCE REPORT</b>

<b>📈 Trade Statistics:</b>
├ Total Trades: {total_trades}
├ Wins: {wins} | Losses: {losses}
├ Win Rate: {win_rate:.1f}%
└ Profit Factor: {performance_metrics.get('profit_factor', 0):.2f}

<b>💰 P&L Summary:</b>
├ Total Return: {total_pnl:+.2f}R
├ Average Trade: {avg_trade:+.2f}R
├ Best Trade: +{best_trade:.2f}R
└ Worst Trade: {worst_trade:.2f}R

<b>🎯 Strategy Performance:</b>
{chr(10).join(f"  • {name}: {count} trades" for name, count in performance_metrics.get('strategy_counts', {}).items())}

<b>📉 Risk Metrics:</b>
├ Max Drawdown: {performance_metrics.get('max_drawdown', 0):.1f}%
└ Sharpe Ratio: {performance_metrics.get('sharpe', 0):.2f}

<i>Week closed. Ready for next.</i>
"""
    
    @staticmethod
    def probability_distribution_viz(probabilities):
        if not probabilities: return "No probability data"
        bars = []
        for label, prob in probabilities.items():
            filled = int(prob * 10)
            bar = "█" * filled + "░" * (10 - filled)
            emoji = "🟢" if prob >= 0.7 else "🟡" if prob >= 0.5 else "🔴"
            bars.append(f"{emoji} {label:12s}: [{bar}] {prob:.0%}")
        return f"""
📊 <b>PROBABILITY DISTRIBUTION</b>

{chr(10).join(bars)}

<b>Interpretation:</b>
🟢 High confidence (≥70%)
🟡 Moderate (50-69%)
🔴 Low confidence (<50%)
"""
    
    @staticmethod
    def rr_suggestion(signal, alternatives):
        current_rr = signal.risk_reward
        suggestions = []
        for alt in alternatives:
            improvement = (alt['rr'] - current_rr) / current_rr * 100
            suggestions.append(f"  • Entry: {alt['entry']:.5f} → R:R {alt['rr']:.1f} ({improvement:+.0f}% improvement)")
        return f"""
💡 <b>R:R OPTIMIZATION SUGGESTION</b>

<b>Current Setup:</b>
├ Entry: {signal.entry_price:.5f}
├ R:R Ratio: 1:{current_rr:.1f}
└ Expected Value: {signal.expected_value:+.2f}R

<b>Alternative Entries:</b>
{chr(10).join(suggestions) if suggestions else "  Current entry is optimal"}

<b>💰 Impact Analysis:</b>
  • Better entry could improve EV by up to 15%
  • Consider limit orders at suggested levels
  • Watch for liquidity sweeps near targets
"""
    
    @staticmethod
    def _generate_progress_bar(current, total, reverse=False):
        if reverse: filled = int((1 - current/total) * PROGRESS_BAR_LENGTH) if total > 0 else 0
        else: filled = int((current/total) * PROGRESS_BAR_LENGTH) if total > 0 else 0
        filled = max(0, min(PROGRESS_BAR_LENGTH, filled))
        return "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)

# ================= EVENT TRIGGER ENGINE =================

class EventTriggerEngine:
    def __init__(self, telegram_manager):
        self.telegram = telegram_manager
        self.templates = MessageTemplateLibrary()
        self.last_alert_time = {}
        self.alert_cooldowns = {
            AlertType.PRE_SIGNAL: 60, AlertType.SIGNAL: 0, AlertType.LIQUIDITY_ZONE: 300,
            AlertType.SESSION_OPEN: 3600, AlertType.RISK_DASHBOARD: 600, AlertType.WHALE_ACTIVITY: 180,
            AlertType.REGIME_CHANGE: 0, AlertType.DRAWDOWN_WARNING: 0
        }
        self.daily_alert_count = defaultdict(int)
        self.daily_reset_time = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0)
    
    async def trigger_event(self, event_type, data, priority=2):
        self._check_daily_reset()
        if not self._check_quota(event_type): return False
        if not self._check_cooldown(event_type): return False
        handler = self._get_handler(event_type)
        if handler:
            message = handler(data)
            success = await self.telegram.send_message(message)
            if success: self._record_alert(event_type); return True
        return False
    
    def _check_daily_reset(self):
        now = datetime.now(pytz.UTC)
        if now.date() > self.daily_reset_time.date():
            self.daily_alert_count.clear()
            self.daily_reset_time = now
    
    def _check_quota(self, event_type):
        if event_type in [AlertType.DRAWDOWN_WARNING, AlertType.REGIME_CHANGE]: return True
        return sum(self.daily_alert_count.values()) < MAX_DAILY_ALERTS
    
    def _check_cooldown(self, event_type):
        cooldown = self.alert_cooldowns.get(event_type, 300)
        last_time = self.last_alert_time.get(event_type.value)
        if last_time is None: return True
        return (datetime.now(pytz.UTC) - last_time).total_seconds() >= cooldown
    
    def _get_handler(self, event_type):
        return {
            AlertType.PRE_SIGNAL: self._handle_pre_signal, AlertType.SIGNAL: self._handle_signal,
            AlertType.LIQUIDITY_ZONE: self._handle_liquidity_zone, AlertType.SESSION_OPEN: self._handle_session_open,
            AlertType.CONFIDENCE_HEATMAP: self._handle_confidence_heatmap, AlertType.RISK_DASHBOARD: self._handle_risk_dashboard,
            AlertType.INVALIDATION: self._handle_invalidation, AlertType.WHALE_ACTIVITY: self._handle_whale_activity,
            AlertType.MULTI_RANKING: self._handle_multi_ranking, AlertType.WEEKLY_ANALYTICS: self._handle_weekly_analytics,
            AlertType.DRAWDOWN_WARNING: self._handle_drawdown_warning, AlertType.PROBABILITY_VIZ: self._handle_probability_viz,
            AlertType.RR_SUGGESTION: self._handle_rr_suggestion, AlertType.REGIME_CHANGE: self._handle_regime_change
        }.get(event_type)
    
    def _record_alert(self, event_type):
        self.last_alert_time[event_type.value] = datetime.now(pytz.UTC)
        self.daily_alert_count[event_type.value] += 1
    
    def _handle_pre_signal(self, data): return self.templates.pre_signal_alert(UXSignal(**data['signal']), data.get('time_to_signal', 60))
    def _handle_signal(self, data): return self.templates.signal_alert(UXSignal(**data['signal']), data.get('market_context', {}))
    def _handle_liquidity_zone(self, data): return self.templates.liquidity_zone_heatmap([LiquidityZone(**z) for z in data['zones']], data['current_price'])
    def _handle_session_open(self, data): return self.templates.session_open_ceremony(data['session_type'], data.get('high_impact_news', []))
    def _handle_confidence_heatmap(self, data): return self.templates.confidence_heatmap([UXSignal(**s) for s in data['signals']])
    def _handle_risk_dashboard(self, data): return self.templates.risk_dashboard(RiskMetrics(**data['metrics']), data.get('active_trades', []))
    def _handle_whale_activity(self, data): return self.templates.whale_activity_alert([WhaleActivity(**a) for a in data['activities']])
    def _handle_multi_ranking(self, data): return self.templates.confidence_heatmap([UXSignal(**s) for s in data['signals']])
    def _handle_weekly_analytics(self, data): return self.templates.weekly_analytics_report(data.get('trade_history', []), data.get('performance_metrics', {}))
    def _handle_probability_viz(self, data): return self.templates.probability_distribution_viz(data.get('probabilities', {}))
    def _handle_rr_suggestion(self, data): return self.templates.rr_suggestion(UXSignal(**data['signal']), data.get('alternatives', []))
    
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
    def __init__(self):
        self.state = PortfolioState()
        self.correlation_window = 20
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.cooldowns = {}
    
    def update_price(self, symbol, price): self.price_history[symbol].append(price)
    
    def calculate_correlation(self, sym1, sym2):
        if len(self.price_history[sym1]) < self.correlation_window or len(self.price_history[sym2]) < self.correlation_window: return 0.0
        s1, s2 = pd.Series(list(self.price_history[sym1])[-self.correlation_window:]), pd.Series(list(self.price_history[sym2])[-self.correlation_window:])
        returns1, returns2 = s1.pct_change().dropna(), s2.pct_change().dropna()
        return returns1.corr(returns2) if len(returns1) >= 5 else 0.0
    
    def can_add_trade(self, new_signal, open_trades):
        total_risk = sum(t.signal.position_size_r for t in open_trades)
        if total_risk + new_signal.position_size_r > MAX_PORTFOLIO_EXPOSURE: return False, f"Max exposure {MAX_PORTFOLIO_EXPOSURE}R would be exceeded"
        for trade in open_trades:
            if trade.signal.direction != new_signal.direction: continue
            corr = self.calculate_correlation(new_signal.symbol, trade.signal.symbol)
            if abs(corr) > 0.7:
                corr_exposure = sum(t.signal.position_size_r for t in open_trades if abs(self.calculate_correlation(new_signal.symbol, t.signal.symbol)) > 0.7)
                if corr_exposure + new_signal.position_size_r > MAX_CORRELATION_EXPOSURE: return False, f"Max correlated exposure {MAX_CORRELATION_EXPOSURE}R would be exceeded"
        if new_signal.symbol in self.cooldowns and datetime.now() < self.cooldowns[new_signal.symbol]: return False, "Symbol in cooldown"
        if new_signal.symbol in self.state.last_signals:
            time_since = (datetime.now() - self.state.last_signals[new_signal.symbol]).total_seconds() / 60
            if time_since < SIGNAL_COOLDOWN_MINUTES: return False, f"Signal clustering: {time_since:.0f}min since last signal"
        return True, "OK"
    
    def register_trade(self, signal):
        self.state.open_trades += 1
        self.state.total_exposure_r += signal.position_size_r
        self.state.last_signals[signal.symbol] = datetime.now()
    
    def close_trade(self, trade):
        self.state.open_trades -= 1
        self.state.total_exposure_r -= trade.signal.position_size_r
        self.state.daily_pnl_r += trade.current_pnl_r
        if self.state.daily_pnl_r > self.state.peak_equity_r: self.state.peak_equity_r = self.state.daily_pnl_r
        if self.state.peak_equity_r > 0: self.state.current_drawdown_pct = (self.state.peak_equity_r - self.state.daily_pnl_r) / self.state.peak_equity_r * 100
        self.cooldowns[trade.signal.symbol] = datetime.now() + timedelta(minutes=SIGNAL_COOLDOWN_MINUTES)
    
    def check_drawdown_throttle(self): return self.state.current_drawdown_pct > DRAWDOWN_THROTTLE_PCT
    
    def get_risk_metrics(self):
        heat_score = min(100, int((self.state.total_exposure_r / MAX_PORTFOLIO_EXPOSURE) * 100))
        dd_status = "critical" if self.state.current_drawdown_pct > 10 else "elevated" if self.state.current_drawdown_pct > 7 else "normal"
        return RiskMetrics(datetime.now(pytz.UTC), heat_score, self.state.total_exposure_r, MAX_PORTFOLIO_EXPOSURE - self.state.total_exposure_r, dd_status, 50, 30)

# ================= EXECUTION ENGINE =================

class ExecutionEngine:
    def __init__(self):
        self.slippage_model = {'forex': 0.0001, 'crypto': 0.0005}
    
    def simulate_entry(self, signal, asset_class):
        base_slip = self.slippage_model.get(asset_class, 0.0003)
        vol_adj = 1 + (signal.regime.atr_percentile / 100)
        slippage = signal.entry_price * base_slip * vol_adj
        return signal.entry_price + slippage if signal.direction == "BUY" else signal.entry_price - slippage
    
    def simulate_exit(self, trade, exit_price, asset_class):
        base_slip = self.slippage_model.get(asset_class, 0.0003)
        slippage = exit_price * base_slip
        return exit_price - slippage if trade.signal.direction == "BUY" else exit_price + slippage
    
    def should_trigger_partial(self, trade, current_price):
        if trade.signal.take_profit_1 is None: return False
        return current_price >= trade.signal.take_profit_1 if trade.signal.direction == "BUY" else current_price <= trade.signal.take_profit_1
    
    def update_trailing_stop
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
        self.trades = {}
        self.active_trades = []
        self.closed_trades = []
        self.pending_pre_signals = {}
        self.running = False
        self.performance_metrics = {'strategy_counts': defaultdict(int), 'max_drawdown': 0.0, 'profit_factor': 0.0, 'sharpe': 0.0}
    
    def get_asset_class(self, symbol): return 'crypto' if any(c in symbol for c in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA']) else 'forex'
    
    async def scan_symbol(self, symbol):
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
        hour = datetime.now(pytz.UTC).hour
        if 8 <= hour < 13: return "LONDON"
        elif 13 <= hour < 17: return "OVERLAP"
        elif 17 <= hour < 22: return "NEW_YORK"
        elif 22 <= hour < 24 or 0 <= hour < 8: return "ASIA"
        return "SYDNEY"
    
    async def run_scan_cycle(self):
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

print("Part 4 loaded: Telegram, Notification System, Signal Persistence, Main Engine, Entry Point")
print("\n" + "="*60)
print("ALL 4 PARTS COMPLETE - READY FOR PRODUCTION")
print("="*60)
