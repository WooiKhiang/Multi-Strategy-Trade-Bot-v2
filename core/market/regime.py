"""
Market regime detector for Mark 3.1.
Determines current market environment.
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np

from core.utils.registry import ComponentRegistry

logger = logging.getLogger(__name__)

class RegimeDetector:
    """
    Detects market regime using multiple signals.
    
    Regimes:
    - BULL: Strong uptrend, low volatility
    - NEUTRAL: Mixed conditions
    - BEAR: Downtrend, high volatility
    - CRASH: Extreme conditions, halt trading
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        
        # Get dependencies
        registry = ComponentRegistry()
        self.fetcher = registry.get('data', 'fetcher')()
        
        # Benchmark symbols (stable, not rotating)
        self.benchmarks = ['SPY', 'QQQ', 'IWM', 'XLF', 'XLK', 'XLE', 'TLT']
        
        # Thresholds
        self.trend_threshold = 0.02  # 2% for trend detection
        self.volatility_threshold = 0.25  # 25% VIX equivalent
        self.breadth_threshold = 0.4  # 40% of stocks above 50-day MA
    
    def detect_regime(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        Detect current market regime.
        
        Returns:
            Dict with regime, score, and components
        """
        # Gather signals
        spy_trend = self._get_spy_trend()
        volatility = self._get_volatility()
        breadth = self._get_market_breadth()
        
        # Calculate component scores
        scores = {}
        
        # Trend score (-2 to +2)
        if spy_trend > self.trend_threshold:
            scores['trend'] = 2 if spy_trend > self.trend_threshold * 2 else 1
        elif spy_trend < -self.trend_threshold:
            scores['trend'] = -2 if spy_trend < -self.trend_threshold * 2 else -1
        else:
            scores['trend'] = 0
        
        # Volatility score (-2 to +2) - inverted (low vol = bullish)
        if volatility < 15:
            scores['volatility'] = 2
        elif volatility < 20:
            scores['volatility'] = 1
        elif volatility > 30:
            scores['volatility'] = -2
        elif volatility > 25:
            scores['volatility'] = -1
        else:
            scores['volatility'] = 0
        
        # Breadth score (-2 to +2)
        if breadth > 0.6:
            scores['breadth'] = 2
        elif breadth > 0.5:
            scores['breadth'] = 1
        elif breadth < 0.3:
            scores['breadth'] = -2
        elif breadth < 0.4:
            scores['breadth'] = -1
        else:
            scores['breadth'] = 0
        
        # Calculate total score
        total_score = sum(scores.values())
        
        # Determine regime
        if total_score >= 4:
            regime = "BULL"
            description = "Strong bull market"
        elif total_score >= 1:
            regime = "NEUTRAL_BULL"
            description = "Mildly bullish"
        elif total_score >= -1:
            regime = "NEUTRAL"
            description = "Mixed conditions"
        elif total_score >= -3:
            regime = "NEUTRAL_BEAR"
            description = "Mildly bearish"
        elif total_score >= -5:
            regime = "BEAR"
            description = "Bear market"
        else:
            regime = "CRASH"
            description = "Extreme conditions - trading halted"
        
        # Get multiplier for position sizing
        multiplier = self._get_regime_multiplier(regime)
        
        result = {
            'regime': regime,
            'score': total_score,
            'multiplier': multiplier,
            'description': description,
            'components': {
                'spy_trend': spy_trend,
                'volatility': volatility,
                'breadth': breadth,
                'scores': scores
            },
            'timestamp': datetime.utcnow()
        }
        
        logger.info(f"Market regime: {regime} (score={total_score}, multiplier={multiplier})")
        return result
    
    def _get_spy_trend(self) -> float:
        """Get SPY trend (20-day SMA slope)."""
        try:
            # Fetch SPY daily bars
            df = self.fetcher.get_bars('SPY', period=30, timeframe='Day')
            
            if df is None or len(df) < 20:
                logger.warning("Insufficient SPY data for trend calculation")
                return 0.0
            
            # Calculate 20-day SMA
            sma_20 = df['close'].rolling(20).mean()
            
            # Calculate slope (current vs 5 days ago)
            if len(sma_20) > 5:
                current_sma = sma_20.iloc[-1]
                prev_sma = sma_20.iloc[-5]
                
                if prev_sma > 0:
                    trend_pct = (current_sma - prev_sma) / prev_sma
                    return float(trend_pct)
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating SPY trend: {e}")
            return 0.0
    
    def _get_volatility(self) -> float:
        """Get market volatility (VIX or proxy)."""
        try:
            # Try to fetch VIX directly
            vix_df = self.fetcher.get_bars('VIX', period=5, timeframe='Day')
            
            if vix_df is not None and len(vix_df) > 0:
                return float(vix_df['close'].iloc[-1])
            
            # Fallback: Calculate from SPY options or just use SPY volatility
            spy_df = self.fetcher.get_bars('SPY', period=30, timeframe='Day')
            
            if spy_df is not None and len(spy_df) > 20:
                # Calculate annualized volatility
                returns = spy_df['close'].pct_change().dropna()
                vol = returns.std() * np.sqrt(252) * 100  # Annualized %
                return float(vol)
            
            return 20.0  # Default neutral
            
        except Exception as e:
            logger.error(f"Error calculating volatility: {e}")
            return 20.0
    
    def _get_market_breadth(self) -> float:
        """Calculate market breadth from benchmark symbols."""
        try:
            above_50ma = 0
            total = 0
            
            for symbol in self.benchmarks:
                df = self.fetcher.get_bars(symbol, period=60, timeframe='Day')
                
                if df is not None and len(df) > 50:
                    sma_50 = df['close'].rolling(50).mean()
                    current_price = df['close'].iloc[-1]
                    current_sma = sma_50.iloc[-1]
                    
                    if current_price > current_sma:
                        above_50ma += 1
                    
                    total += 1
            
            if total > 0:
                return above_50ma / total
            
            return 0.5  # Default neutral
            
        except Exception as e:
            logger.error(f"Error calculating breadth: {e}")
            return 0.5
    
    def _get_regime_multiplier(self, regime: str) -> float:
        """Get position sizing multiplier for regime."""
        multipliers = {
            'BULL': 1.2,
            'NEUTRAL_BULL': 1.1,
            'NEUTRAL': 1.0,
            'NEUTRAL_BEAR': 0.7,
            'BEAR': 0.4,
            'CRASH': 0.0
        }
        return multipliers.get(regime, 1.0)
    
    def should_trade(self, regime: str) -> bool:
        """Determine if trading should be allowed in this regime."""
        return regime != 'CRASH'
    
    def get_recommended_strategies(self, regime: str) -> List[str]:
        """Get list of strategies recommended for this regime."""
        recommendations = {
            'BULL': ['rsi_meanrev', 'momentum', 'hybrid'],
            'NEUTRAL_BULL': ['rsi_meanrev', 'momentum', 'hybrid'],
            'NEUTRAL': ['rsi_meanrev', 'hybrid'],
            'NEUTRAL_BEAR': ['rsi_meanrev'],
            'BEAR': ['hybrid'],
            'CRASH': []
        }
        return recommendations.get(regime, [])