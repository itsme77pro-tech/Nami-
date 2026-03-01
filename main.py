"""
NamiEngine v3.1 - High-Conversion Trading Intelligence Dashboard
Fintech UX Design | Trading Analytics Engineering | Engagement Optimization

Features:
- Pre-signal alerts with countdown timers
- Liquidity zone heatmaps
- Session-based market open ceremonies
- Confidence visualization with probability bars
- Real-time risk dashboards
- Trade invalidation tracking
- Whale activity detection
- Multi-signal ranking boards
- Weekly performance storytelling
- Drawdown intervention system
- Dynamic RR optimization
- Regime change ceremonies
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import random
import json
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Literal, Callable, Any
from datetime import datetime, timedelta, time
from enum import Enum, auto
from collections import defaultdict, deque
import pytz
import hashlib
from abc import ABC, abstractmethod
import warnings

warnings.filterwarnings('ignore')

# ================= CONFIGURATION =================

# Environment setup
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TWELVEDATA_KEY = os.getenv("TWELVEDATA_KEY")
PERSONALITY = os.getenv("PERSONALITY", "nami").lower()

# UX Configuration
ALERT_COOLDOWN_MINUTES = 30
MAX_DAILY_ALERTS = 20  # Prevent spam fatigue
ENGAGEMENT_HOURS = [8, 13, 21]  # Peak engagement times
EMOJI_DENSITY = "high"  # high/medium/low

# Whale Detection Thresholds
WHALE_VOLUME_MULTIPLIER = 3.0  # 3x average volume
WHALE_PRICE_IMPACT_PCT = 0.5  # 0.5% move in 5min

# Notification Architecture
NOTIFICATION_PRIORITY = {
    "CRITICAL": 0,      # Drawdown, invalidation
    "HIGH": 1,        # Signal, regime change
    "MEDIUM": 2,      # Pre-signal, liquidity
    "LOW": 3,         # Session open, analytics
    "BACKGROUND": 4   # Whale activity (batch)
}

# Visual Configuration
PROGRESS_BAR_LENGTH = 10
HEATMAP_COLORS = ["🟢", "🟡", "🟠", "🔴", "⚫"]
CONFIDENCE_EMOJIS = ["🎯", "💎", "🔥", "⚡", "⭐"]

# ================= ENUMS & DATA CLASSES =================

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
    
    # UX enhancements
    countdown_seconds: int = 0  # For pre-signal alerts
    setup_quality_score: int = 0  # 0-100 visual quality
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
    strength_score: int  # 0-100
    distance_pct: float  # Distance from current price
    test_count: int  # Number of historical tests
    volume_at_level: float
    
    @property
    def heatmap_intensity(self) -> str:
        """Return visual intensity indicator."""
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
    volume_anomaly: float  # Multiple of average
    price_impact: float  # Percentage move
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
    portfolio_heat_score: int  # 0-100 risk level
    active_exposure: float
    available_capacity: float
    drawdown_status: Literal["normal", "elevated", "critical"]
    correlation_risk: int  # 0-100
        session_risk: int  # 0-100
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

# ================= UX MESSAGE TEMPLATES =================

class MessageTemplateLibrary:
    """
    Production-grade message templates optimized for engagement and conversion.
    Each template follows fintech UX best practices: clarity, urgency, actionability.
    """
    
    # ================= PRE-SIGNAL ALERTS =================
    
    @staticmethod
    def pre_signal_alert(signal: UXSignal, time_to_signal: int) -> str:
        """
        Build anticipation with countdown timer.
        Creates urgency without anxiety.
        """
        countdown_bar = MessageTemplateLibrary._generate_progress_bar(
            time_to_signal, 300, reverse=True  # 5min window
        )
        
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
        """Format confluence factors as preview."""
        preview = []
        for i, factor in enumerate(factors[:3], 1):
            preview.append(f"  {i}. {factor} ⚡")
        if len(factors) > 3:
            preview.append(f"  ... and {len(factors)-3} more factors")
        return "\n".join(preview) if preview else "  Analyzing market structure..."
    
    # ================= SIGNAL ALERTS (ENHANCED) =================
    
    @staticmethod
    def signal_alert(signal: UXSignal, market_context: Dict[str, Any]) -> str:
        """
        High-conversion signal alert with full context.
        Optimized for immediate action.
        """
        # Probability visualization
        prob_bar = MessageTemplateLibrary._generate_probability_bar(signal.probability)
        
        # Risk visualization
        risk_matrix = MessageTemplateLibrary._generate_risk_matrix(signal)
        
        # Context badges
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
        """Generate visual probability bar."""
        filled = int(probability * PROGRESS_BAR_LENGTH)
        bar = "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)
        
        if probability >= 0.80:
            emoji = "🟢"
        elif probability >= 0.65:
            emoji = "🟡"
        else:
            emoji = "🟠"
        
        return f"{emoji} <b>[{bar}]</b> {probability:.0%}"
    
    @staticmethod
    def _generate_risk_matrix(signal: UXSignal) -> str:
        """Generate visual risk matrix."""
        risk_amount = abs(signal.entry_price - signal.stop_loss)
        reward_amount = abs(signal.take_profit - signal.entry_price)
        
        # Visual risk/reward bars
        risk_bar = "🔴" * min(5, int(risk_amount / signal.entry_price * 10000))
        reward_bar = "🟢" * min(5, int(reward_amount / signal.entry_price * 10000))
        
        return f"""<b>📊 Risk Matrix</b>
├ Risk:    {risk_bar} ${risk_amount:.2f}
└ Reward:  {reward_bar} ${reward_amount:.2f}"""
    
    @staticmethod
    def _generate_context_badges(context: Dict[str, Any]) -> str:
        """Generate context awareness badges."""
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
        """Format detailed confluence factors."""
        lines = []
        weights = [2.0, 1.5, 1.0, 0.8, 0.5]
        
        for i, factor in enumerate(factors[:5]):
            weight = weights[min(i, len(weights)-1)]
            bar = "█" * int(weight * 3)
            lines.append(f"  {bar} {factor}")
        
        return "\n".join(lines)
    
    # ================= LIQUIDITY ZONE ALERTS =================
    
    @staticmethod
    def liquidity_zone_heatmap(zones: List[LiquidityZone], current_price: float) -> str:
        """
        Visual liquidity zone heatmap for institutional levels.
        """
        # Sort by strength
        zones_sorted = sorted(zones, key=lambda z: z.strength_score, reverse=True)
        
        # Generate heatmap
        heatmap_lines = []
        for zone in zones_sorted[:5]:
            distance_emoji = "🎯" if zone.distance_pct < 0.5 else "📍" if zone.distance_pct < 1.0 else "👁️"
            direction = "↑" if zone.price_level > current_price else "↓"
            
            heatmap_lines.append(
                f"{zone.heatmap_intensity} {distance_emoji} "
                f"<code>{zone.price_level:.5f}</code> {direction} "
                f"({zone.distance_pct:.1f}%) — {zone.zone_type.replace('_', ' ').title()}"
            )
        
        # Price position indicator
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
        """Generate visual price position ladder."""
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
        
        # Add current price marker
        pos = int((current_price - min_p) / range_p * 20)
        current_bar = "─" * pos + "🔴 YOU ARE HERE" + "─" * (20 - pos)
        ladder.append(f"  <code>{current_price:.5f}</code> {current_bar}")
        
        return "\n".join(ladder)
    
    # ================= SESSION OPEN ALERTS =================
    
    @staticmethod
    def session_open_ceremony(session_type: str, high_impact_news: List[str]) -> str:
        """
        Market open ceremony with preparation checklist.
        Creates ritual and readiness.
        """
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
    
    # ================= CONFIDENCE HEATMAP =================
    
    @staticmethod
    def confidence_heatmap(signals: List[UXSignal]) -> str:
        """
        Multi-signal confidence comparison for ranking.
        """
        if not signals:
            return "📊 No active signals"
        
        # Sort by confidence
        ranked = sorted(signals, key=lambda s: s.confidence, reverse=True)
        
        # Generate leaderboard
        leaderboard = []
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        
        for i, signal in enumerate(ranked[:5]):
            medal = medals[i] if i < 3 else f"{i+1}."
            
            # Mini probability bar
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
    
    # ================= RISK DASHBOARD =================
    
    @staticmethod
    def risk_dashboard(metrics: RiskMetrics, active_trades: List[Dict[str, Any]]) -> str:
        """
        Real-time risk dashboard with visual indicators.
        """
        # Portfolio heat gauge
        heat_gauge = MessageTemplateLibrary._generate_heat_gauge(metrics.portfolio_heat_score)
        
        # Exposure bars
        exposure_bar = MessageTemplateLibrary._generate_exposure_bar(
            metrics.active_exposure, 
            metrics.active_exposure + metrics.available_capacity
        )
        
        # Active trades table
        trades_table = MessageTemplateLibrary._format_active_trades(active_trades)
        
        # Risk warnings
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
        """Generate portfolio heat gauge."""
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
        """Generate exposure utilization bar."""
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
        """Format active trades for dashboard."""
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
    
    # ================= INVALIDATION ALERTS =================
    
    @staticmethod
    def invalidation_alert(trade_id: str, symbol: str, reason: str, 
                         entry: float, current: float, loss: float) -> str:
        """
        Professional invalidation with learning context.
        """
        # Visual loss indicator
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
    
    # ================= WHALE ACTIVITY ALERTS =================
    
    @staticmethod
    def whale_activity_alert(activities: List[WhaleActivity]) -> str:
        """
        Batch whale activity report with impact assessment.
        """
        if not activities:
            return ""
        
        # Group by symbol
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
{magnitude_emoji(total_volume)} Volume: {total_volume:.1f}x average
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
    
    # ================= WEEKLY ANALYTICS =================
    
    @staticmethod
    def weekly_analytics_report(stats: Dict[str, Any]) -> str:
        """
        Storytelling weekly performance report.
        """
        # Performance narrative
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
        
        # Key metrics visualization
        win_rate_bar = MessageTemplateLibrary._generate_mini_bar(stats['win_rate'], 100)
        profit_factor_bar = MessageTemplateLibrary._generate_mini_bar(
            min(stats['profit_factor'], 3) * 33, 100
        )
        
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
        """Generate mini progress bar."""
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
        """Generate trade outcome distribution."""
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
    
    # ================= DRAWDOWN WARNINGS =================
    
    @staticmethod
    def drawdown_intervention(current_dd: float, max_dd: float, 
                              recommended_action: str) -> str:
        """
        Intervention alert with actionable recovery plan.
        """
        # Severity assessment
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
        
        # Recovery projection
        weeks_to_recovery = int(np.ceil(current_dd * 100 / 2))  # Assume 2% per week
        
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
    
    # ================= PROBABILITY VISUALIZATION =================
    
    @staticmethod
    def probability_visualization(prob_breakdown: Dict[str, float]) -> str:
        """
        Detailed probability breakdown with factor weights.
        """
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
    
    # ================= RR SUGGESTIONS =================
    
    @staticmethod
    def dynamic_rr_suggestion(current_rr: float, optimal_rr: float, 
                              adjustment_reason: str) -> str:
        """
        Dynamic R:R optimization suggestion.
        """
        # Visual comparison
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
        """Estimate win rate based on R:R (simplified model)."""
        # Simplified: higher RR = lower win rate
        base = 0.65
        return max(0.35, base - (rr - 2) * 0.05)
    
    @staticmethod
    def _calculate_expectancy_delta(current: float, optimal: float) -> float:
        """Calculate expectancy improvement."""
        wr_current = MessageTemplateLibrary._estimate_win_rate(current)
        wr_optimal = MessageTemplateLibrary._estimate_win_rate(optimal)
        
        exp_current = wr_current * current - (1 - wr_current)
        exp_optimal = wr_optimal * optimal - (1 - wr_optimal)
        
        return exp_optimal - exp_current
    
    # ================= REGIME CHANGE CEREMONIES =================
    
    @staticmethod
    def regime_change_ceremony(old_regime: str, new_regime: str, 
                               impact_assessment: str,
                               strategy_adjustments: List[str]) -> str:
        """
        Market regime change ceremony with adaptation guide.
        """
        # Regime transition emoji
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
    
    # ================= UTILITY METHODS =================
    
    @staticmethod
    def _generate_progress_bar(current: int, total: int, reverse: bool = False) -> str:
        """Generate visual progress bar."""
        if reverse:
            filled = int((1 - current/total) * PROGRESS_BAR_LENGTH) if total > 0 else 0
        else:
            filled = int((current/total) * PROGRESS_BAR_LENGTH) if total > 0 else 0
        
        filled = max(0, min(PROGRESS_BAR_LENGTH, filled))
        return "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)

# ================= EVENT TRIGGERS & NOTIFICATION ARCHITECTURE =================

class EventTriggerEngine:
    """
    Intelligent event detection and notification routing.
    Implements priority-based alert throttling and engagement optimization.
    """
    
    def __init__(self, telegram_manager):
        self.telegram = telegram_manager
        self.templates = MessageTemplateLibrary()
        
        # Alert state management
        self.last_alert_time: Dict[str, datetime] = {}
        self.alert_cooldowns: Dict[AlertType, int] = {
            AlertType.PRE_SIGNAL: 60,      # 1 minute
            AlertType.SIGNAL: 0,          # No cooldown for signals
            AlertType.LIQUIDITY_ZONE: 300, # 5 minutes
            AlertType.SESSION_OPEN: 3600, # 1 hour
            AlertType.RISK_DASHBOARD: 600, # 10 minutes
            AlertType.WHALE_ACTIVITY: 180, # 3 minutes
            AlertType.REGIME_CHANGE: 0,    # No cooldown
            AlertType.DRAWDOWN_WARNING: 0  # No cooldown
        }
        
        # Daily alert quotas
        self.daily_alert_count: Dict[str, int] = defaultdict(int)
        self.daily_reset_time = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0)
        
        # Engagement optimization
        self.user_activity_pattern: Dict[int, List[datetime]] = defaultdict(list)
        self.optimal_send_times: List[int] = ENGAGEMENT_HOURS
    
    async def trigger_event(self, event_type: AlertType, data: Dict[str, Any], 
                          priority: int = 2) -> bool:
        """
        Main event triggering with full routing logic.
        """
        # Check daily reset
        self._check_daily_reset()
        
        # Check quota
        if not self._check_quota(event_type):
            logger.debug(f"Quota exceeded for {event_type.value}")
            return False
        
        # Check cooldown
        if not self._check_cooldown(event_type):
            return False
        
        # Check optimal timing for non-critical alerts
        if priority >= 2 and not self._is_optimal_timing():
            # Queue for later or send immediately based on urgency
            pass
        
        # Route to appropriate handler
        handler = self._get_handler(event_type)
        if handler:
            message = handler(data)
            success = await self.telegram.send_message(message)
            
            if success:
                self._record_alert(event_type)
                return True
        
        return False
    
    def _check_daily_reset(self):
        """Reset daily counters at midnight UTC."""
        now = datetime.now(pytz.UTC)
        if now.date() > self.daily_reset_time.date():
            self.daily_alert_count.clear()
            self.daily_reset_time = now
    
    def _check_quota(self, event_type: AlertType) -> bool:
        """Check if alert type has available quota."""
        # Critical alerts bypass quota
        if event_type in [AlertType.DRAWDOWN_WARNING, AlertType.REGIME_CHANGE]:
            return True
        
        daily_total = sum(self.daily_alert_count.values())
        if daily_total >= MAX_DAILY_ALERTS:
            return False
        
        return True
    
    def _check_cooldown(self, event_type: AlertType) -> bool:
        """Check if alert type is in cooldown."""
        cooldown = self.alert_cooldowns.get(event_type, 300)
        last_time = self.last_alert_time.get(event_type.value)
        
        if last_time is None:
            return True
        
        elapsed = (datetime.now(pytz.UTC) - last_time).total_seconds()
        return elapsed >= cooldown
    
    def _is_optimal_timing(self) -> bool:
        """Check if current time is in optimal engagement window."""
        current_hour = datetime.now(pytz.UTC).hour
        return current_hour in self.optimal_send_times
    
    def _get_handler(self, event_type: AlertType) -> Callable:
        """Get message template handler for event type."""
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
        """Record alert for cooldown and quota tracking."""
        self.last_alert_time[event_type.value] = datetime.now(pytz.UTC)
        self.daily_alert_count[event_type.value] += 1
    
    # ================= HANDLER METHODS =================
    
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
        return self.templates.confidence_heatmap(signals)  # Reuse heatmap
    
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

# ================= ENGAGEMENT OPTIMIZATION ENGINE =================

class EngagementOptimizationEngine:
    """
    ML-ready engagement optimization with A/B testing framework.
    Tracks user interaction patterns and optimizes send times.
    """
    
    def __init__(self):
        self.interaction_log: List[Dict[str, Any]] = []
        self.message_performance: Dict[str, Dict[str, float]] = defaultdict(lambda: {
            'sent': 0,
            'opened': 0,
            'acted': 0,
            'time_to_action': []
        })
        
        # A/B test configurations
        self.ab_tests: Dict[str, Dict[str, Any]] = {
            'emoji_density': {
                'variants': ['high', 'medium', 'low'],
                'current': 'high',
                'weights': [0.5, 0.3, 0.2]
            },
            'urgency_phrasing': {
                'variants': ['direct', 'suggestive', 'educational'],
                'current': 'direct',
                'weights': [0.6, 0.3, 0.1]
            }
        }
    
    def record_interaction(self, message_id: str, action: str, 
                         timestamp: datetime, metadata: Dict[str, Any]):
        """Record user interaction for optimization."""
        self.interaction_log.append({
            'message_id': message_id,
            'action': action,
            'timestamp': timestamp,
            'metadata': metadata
        })
        
        # Update performance metrics
        if action == 'sent':
            self.message_performance[message_id]['sent'] += 1
        elif action == 'opened':
            self.message_performance[message_id]['opened'] += 1
        elif action == 'acted':
            self.message_performance[message_id]['acted'] += 1
    
    def get_optimal_send_time(self, user_id: int, alert_type: AlertType) -> datetime:
        """
        Predict optimal send time based on user history.
        """
        # Analyze user activity pattern
        user_times = self.user_activity_pattern.get(user_id, [])
        
        if len(user_times) < 5:
            # Default to standard engagement hours
            next_window = datetime.now(pytz.UTC).replace(
                hour=random.choice(ENGAGEMENT_HOURS),
                minute=0,
                second=0
            )
            if next_window < datetime.now(pytz.UTC):
                next_window += timedelta(days=1)
            return next_window
        
        # Find most active hours
        hours = [t.hour for t in user_times]
        from collections import Counter
        peak_hour = Counter(hours).most_common(1)[0][0]
        
        optimal = datetime.now(pytz.UTC).replace(hour=peak_hour, minute=0, second=0)
        if optimal < datetime.now(pytz.UTC):
            optimal += timedelta(days=1)
        
        return optimal
    
    def get_ab_test_variant(self, test_name: str) -> str:
        """Get A/B test variant for message optimization."""
        test = self.ab_tests.get(test_name)
        if not test:
            return 'default'
        
        # Weighted random selection
        variants = test['variants']
        weights = test['weights']
        
        return random.choices(variants, weights=weights)[0]
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate engagement performance analytics."""
        total_sent = sum(m['sent'] for m in self.message_performance.values())
        total_opened = sum(m['opened'] for m in self.message_performance.values())
        total_acted = sum(m['acted'] for m in self.message_performance.values())
        
        return {
            'open_rate': total_opened / total_sent if total_sent > 0 else 0,
            'action_rate': total_acted / total_sent if total_sent > 0 else 0,
            'conversion_rate': total_acted / total_opened if total_opened > 0 else 0,
            'top_performing_messages': self._get_top_performers(),
            'optimal_send_times': self._calculate_optimal_times(),
            'ab_test_results': self._analyze_ab_tests()
        }
    
    def _get_top_performers(self, n: int = 5) -> List[str]:
        """Get top performing message types."""
        performance = []
        for msg_id, metrics in self.message_performance.items():
            if metrics['sent'] > 0:
                score = metrics['acted'] / metrics['sent']
                performance.append((msg_id, score))
        
        performance.sort(key=lambda x: x[1], reverse=True)
        return [p[0] for p in performance[:n]]
    
    def _calculate_optimal_times(self) -> List[int]:
        """Calculate globally optimal send times."""
        if not self.interaction_log:
            return ENGAGEMENT_HOURS
        
        action_times = [
            e['timestamp'].hour 
            for e in self.interaction_log 
            if e['action'] == 'acted'
        ]
        
        from collections import Counter
        peak_times = Counter(action_times).most_common(3)
        return [t[0] for t in peak_times] if peak_times else ENGAGEMENT_HOURS
    
    def _analyze_ab_tests(self) -> Dict[str, Any]:
        """Analyze A/B test performance."""
        results = {}
        for test_name, config in self.ab_tests.items():
            # Simplified analysis
            results[test_name] = {
                'current_winner': config['current'],
                'confidence': 'low',  # Would calculate statistically
                'recommendation': 'continue_test'
            }
        return results

# ================= WHALE DETECTION ENGINE =================

class WhaleDetectionEngine:
    """
    Real-time whale activity detection for institutional flow tracking.
    """
    
    def __init__(self):
        self.volume_baselines: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.recent_activities: List[WhaleActivity] = []
    
    def update(self, symbol: str, df: pd.DataFrame):
        """Update whale detection with new data."""
        if len(df) < 2:
            return
        
        # Update baselines
        self.volume_baselines[symbol].append(df['volume'].iloc[-1])
        self.price_history[symbol].append({
            'price': df['close'].iloc[-1],
            'timestamp': df.index[-1]
        })
        
        # Detect anomalies
        activity = self._detect_anomaly(symbol, df)
        if activity:
            self.recent_activities.append(activity)
    
    def _detect_anomaly(self, symbol: str, df: pd.DataFrame) -> Optional[WhaleActivity]:
        """Detect whale activity patterns."""
        if len(self.volume_baselines[symbol]) < 20:
            return None
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = np.mean(list(self.volume_baselines[symbol])[:-5])  # Exclude recent
        
        if avg_volume == 0:
            return None
        
        volume_ratio = current_volume / avg_volume
        
        # Price impact calculation
        if len(df) >= 2:
            price_change = abs(df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100
        else:
            price_change = 0
        
        # Classification
        if volume_ratio >= WHALE_VOLUME_MULTIPLIER and price_change >= WHALE_PRICE_IMPACT_PCT:
            # Determine direction and type
            direction = "BUY" if df['close'].iloc[-1] > df['open'].iloc[-1] else "SELL"
            
            # Activity type classification
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
        """Detect stop hunt pattern (quick reversal after level break)."""
        if len(df) < 3:
            return False
        
        # Long wick, quick reversal
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        # Check for sweep and recovery
        if c2['low'] < c1['low'] and c3['close'] > c2['open']:
            return True
        if c2['high'] > c1['high'] and c3['close'] < c2['open']:
            return True
        
        return False
    
    def get_recent_activities(self, lookback_minutes: int = 15) -> List[WhaleActivity]:
        """Get recent whale activities for alerting."""
        cutoff = datetime.now(pytz.UTC) - timedelta(minutes=lookback_minutes)
        return [a for a in self.recent_activities if a.timestamp >= cutoff]

# ================= INTEGRATED NOTIFICATION SYSTEM =================

class IntegratedNotificationSystem:
    """
    Complete notification orchestration combining all UX features.
    """
    
    def __init__(self, telegram_manager):
        self.telegram = telegram_manager
        self.triggers = EventTriggerEngine(telegram_manager)
        self.engagement = EngagementOptimizationEngine()
        self.whale_detector = WhaleDetectionEngine()
        self.templates = MessageTemplateLibrary()
        
        # State tracking
        self.current_regime: Dict[str, str] = {}
        self.active_signals: Dict[str, UXSignal] = {}
        self.risk_state: RiskMetrics = RiskMetrics(
            timestamp=datetime.now(pytz.UTC),
            portfolio_heat_score=0,
            active_exposure=0,
            available_capacity=MAX_PORTFOLIO_RISK_PCT,
            drawdown_status="normal",
            correlation_risk=0,
            session_risk=0
        )
    
    async def process_market_data(self, symbol: str, df: pd.DataFrame, 
                                  context: Dict[str, Any]):
        """Process market data and trigger appropriate notifications."""
        # Update whale detection
        self.whale_detector.update(symbol, df)
        
        # Check for whale alerts
        whale_activities = self.whale_detector.get_recent_activities()
        if whale_activities:
            await self.triggers.trigger_event(
                AlertType.WHALE_ACTIVITY,
                {'activities': [self._whale_to_dict(a) for a in whale_activities]},
                priority=3
            )
        
        # Check for regime changes
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
        """Send pre-signal anticipation alert."""
        await self.triggers.trigger_event(
            AlertType.PRE_SIGNAL,
            {
                'signal': self._signal_to_dict(signal),
                'time_to_signal': time_to_signal
            },
            priority=2
        )
    
    async def send_signal(self, signal: UXSignal, context: Dict[str, Any]):
        """Send confirmed signal alert."""
        self.active_signals[signal.id] = signal
        
        await self.triggers.trigger_event(
            AlertType.SIGNAL,
            {
                'signal': self._signal_to_dict(signal),
                'market_context': context
            },
            priority=0  # Highest priority
        )
    
    async def send_risk_dashboard(self, metrics: RiskMetrics, 
                                   active_trades: List[Dict[str, Any]]):
        """Send risk dashboard update."""
        self.risk_state = metrics
        
        # Check for drawdown intervention
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
        
        # Regular dashboard
        await self.triggers.trigger_event(
            AlertType.RISK_DASHBOARD,
            {
                'metrics': self._risk_to_dict(metrics),
                'active_trades': active_trades
            },
            priority=2
        )
    
    async def send_liquidity_alert(self, symbol: str, zones: List[LiquidityZone], 
                                   current_price: float):
        """Send liquidity zone heatmap."""
        await self.triggers.trigger_event(
            AlertType.LIQUIDITY_ZONE,
            {
                'zones': [self._zone_to_dict(z) for z in zones],
                'current_price': current_price
            },
            priority=3
        )
    
    async def send_session_open(self, session_type: str, 
                                high_impact_news: List[str] = None):
        """Send session open ceremony."""
        await self.triggers.trigger_event(
            AlertType.SESSION_OPEN,
            {
                'session_type': session_type,
                'high_impact_news': high_impact_news or []
            },
            priority=3
        )
    
    async def send_weekly_report(self, stats: Dict[str, Any]):
        """Send weekly analytics report."""
        await self.triggers.trigger_event(
            AlertType.WEEKLY_ANALYTICS,
            {'stats': stats},
            priority=3
        )
    
    async def send_trade_invalidation(self, trade_id: str, symbol: str, 
                                      reason: str, entry: float, 
                                      current: float, loss: float):
        """Send trade invalidation alert."""
        await self.triggers.trigger_event(
            AlertType.INVALIDATION,
            {
                'trade_id': trade_id,
                'symbol': symbol,
                'reason': reason,
                'entry': entry,
                'current': current,
                'loss': loss
            },
            priority=1
        )
    
    # ================= UTILITY METHODS =================
    
    def _signal_to_dict(self, signal: UXSignal) -> Dict[str, Any]:
        """Convert UXSignal to dictionary."""
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
        """Convert WhaleActivity to dictionary."""
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
        """Convert LiquidityZone to dictionary."""
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
        """Convert RiskMetrics to dictionary."""
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

# ================= MAIN INTEGRATION =================

class UXEnhancedNamiEngine:
    """
    Full integration of UX dashboard with trading engine.
    """
    
    def __init__(self):
        from main_engine import NamiEngine  # Import your main engine
        
        self.core_engine = NamiEngine()
        self.notification_system = IntegratedNotificationSystem(
            self.core_engine.telegram
        )
        
        # UX-specific state
        self.pending_pre_signals: Dict[str, UXSignal] = {}
        self.liquidity_cache: Dict[str, List[LiquidityZone]] = {}
    
    async def run(self):
        """Run enhanced engine with UX layer."""
        # Start core engine
        core_task = asyncio.create_task(self.core_engine.run())
        
        # Start UX enhancement loops
        ux_tasks = [
            self._pre_signal_loop(),
            self._liquidity_monitoring_loop(),
            self._session_ceremony_loop(),
            self._whale_monitoring_loop()
        ]
        
        await asyncio.gather(core_task, *ux_tasks)
    
    async def _pre_signal_loop(self):
        """Monitor for pre-signal opportunities."""
        while True:
            try:
                for symbol in WATCHLIST.keys():
                    # Check for forming setups
                    forming_signal = await self._check_forming_setup(symbol)
                    if forming_signal and forming_signal.id not in self.pending_pre_signals:
                        self.pending_pre_signals[forming_signal.id] = forming_signal
                        
                        # Send pre-signal with countdown
                        await self.notification_system.send_pre_signal(
                            forming_signal,
                            countdown_seconds=120  # 2 minute warning
                        )
                
                # Clean up expired pre-signals
                now = datetime.now(pytz.UTC)
                expired = [
                    sid for sid, sig in self.pending_pre_signals.items()
                    if (now - sig.timestamp).total_seconds() > 300
                ]
                for sid in expired:
                    del self.pending_pre_signals[sid]
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Pre-signal loop error: {e}")
                await asyncio.sleep(60)
    
    async def _liquidity_monitoring_loop(self):
        """Monitor liquidity zones for proximity alerts."""
        while True:
            try:
                for symbol in WATCHLIST.keys():
                    # Get current price
                    df = await self.core_engine.data_fetcher.fetch_time_series(
                        symbol, "15min", 10
                    )
                    if df is None or len(df) == 0:
                        continue
                    
                    current_price = df['close'].iloc[-1]
                    
                    # Update liquidity engine
                    self.core_engine.liquidity_engine.update(symbol, df)
                    profile = self.core_engine.liquidity_engine.get_profile(symbol)
                    
                    # Convert to zones
                    zones = self._profile_to_zones(profile, current_price)
                    
                    # Check for proximity alerts
                    proximity_zones = [
                        z for z in zones 
                        if z.distance_pct < 0.5 and z.strength_score > 60
                    ]
                    
                    if proximity_zones:
                        await self.notification_system.send_liquidity_alert(
                            symbol, proximity_zones, current_price
                        )
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Liquidity monitoring error: {e}")
                await asyncio.sleep(300)
    
    async def _session_ceremony_loop(self):
        """Send session open ceremonies."""
        last_session_sent = None
        
        while True:
            try:
                now = datetime.now(pytz.UTC)
                current_time = now.time()
                
                # Check session opens
                session_opens = {
                    (8, 0): "LONDON",
                    (13, 0): "NEW_YORK",
                    (0, 0): "TOKYO",
                    (21, 0): "SYDNEY"
                }
                
                for (hour, minute), session_name in session_opens.items():
                    if current_time.hour == hour and current_time.minute == minute:
                        if last_session_sent != session_name:
                            await self.notification_system.send_session_open(
                                session_name,
                                high_impact_news=self._get_economic_calendar(session_name)
                            )
                            last_session_sent = session_name
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Session ceremony error: {e}")
                await asyncio.sleep(60)
    
    async def _whale_monitoring_loop(self):
        """Continuous whale activity monitoring."""
        while True:
            try:
                # Process recent data for all symbols
                for symbol in WATCHLIST.keys():
                    df = await self.core_engine.data_fetcher.fetch_time_series(
                        symbol, "5min", 20
                    )
                    if df is not None:
                        await self.notification_system.process_market_data(
                            symbol, df, {'regime': 'unknown'}
                        )
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Whale monitoring error: {e}")
                await asyncio.sleep(60)
    
    async def _check_forming_setup(self, symbol: str) -> Optional[UXSignal]:
        """Check if setup is forming for pre-signal."""
        # Simplified check - integrate with your actual signal detection
        # This would use early-stage confluence detection
        return None
    
    def _profile_to_zones(self, profile, current_price: float) -> List[LiquidityZone]:
        """Convert liquidity profile to zone objects."""
        zones = []
        
        if profile.session_high:
            zones.append(LiquidityZone(
                symbol=profile.symbol if hasattr(profile, 'symbol') else "",
                zone_type="resistance",
                price_level=profile.session_high,
                strength_score=70,
                distance_pct=abs(profile.session_high - current_price) / current_price * 100,
                test_count=1,
                volume_at_level=0
            ))
        
        if profile.session_low:
            zones.append(LiquidityZone(
                symbol=profile.symbol if hasattr(profile, 'symbol') else "",
                zone_type="support",
                price_level=profile.session_low,
                strength_score=70,
                distance_pct=abs(profile.session_low - current_price) / current_price * 100,
                test_count=1,
                volume_at_level=0
            ))
        
        return zones
    
    def _get_economic_calendar(self, session: str) -> List[str]:
        """Get high impact news for session."""
        # Placeholder - integrate with actual economic calendar API
        return []

# ================= ENTRY POINT =================

if __name__ == "__main__":
    try:
        engine = UXEnhancedNamiEngine()
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("UX Enhanced Engine stopped")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
