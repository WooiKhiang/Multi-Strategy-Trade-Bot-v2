"""
Confidence scoring engine for signals.
Ranks potential trades by quality.
"""

import logging
import math
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfidenceScorer:
    """
    Calculates confidence scores for trading signals.
    
    Factors:
    - Signal strength (from strategy)
    - Distance to stop loss
    - Volume trend
    - Recent volatility
    - Market regime alignment
    """
    
    def __init__(self):
        # Weights for different factors (sum to 1.0)
        self.weights = {
            'signal_strength': 0.35,     # How strong the signal is
            'risk_reward': 0.25,          # Distance to target vs stop
            'volume_trend': 0.15,          # Volume increasing
            'volatility_alignment': 0.15,   # Volatility appropriate for strategy
            'market_regime': 0.10           # Aligns with current regime
        }
    
    def calculate(self, ticker: str, strategy: str,
                  signal_data: Dict[str, Any],
                  market_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate confidence score (0-100).
        
        Args:
            ticker: Symbol
            strategy: Strategy name
            signal_data: Strategy-specific signal details
            market_data: Current market conditions
        
        Returns:
            Dict with score and component breakdown
        """
        scores = {}
        
        # 1. Signal strength (0-100)
        scores['signal_strength'] = self._score_signal_strength(signal_data)
        
        # 2. Risk/reward ratio (0-100)
        scores['risk_reward'] = self._score_risk_reward(signal_data)
        
        # 3. Volume trend (0-100)
        scores['volume_trend'] = self._score_volume_trend(market_data)
        
        # 4. Volatility alignment (0-100)
        scores['volatility_alignment'] = self._score_volatility(
            strategy, signal_data, market_data
        )
        
        # 5. Market regime (0-100)
        scores['market_regime'] = self._score_market_regime(
            strategy, market_data.get('regime', 'NEUTRAL')
        )
        
        # Calculate weighted total
        total = 0
        breakdown = {}
        for factor, score in scores.items():
            weight = self.weights.get(factor, 0)
            contribution = score * weight
            total += contribution
            breakdown[factor] = {
                'score': score,
                'weight': weight,
                'contribution': contribution
            }
        
        # Round to nearest integer
        final_score = round(total)
        
        return {
            'score': final_score,
            'breakdown': breakdown,
            'factors': scores,
            'ticker': ticker,
            'strategy': strategy,
            'timestamp': datetime.utcnow()
        }
    
    def _score_signal_strength(self, signal_data: Dict) -> float:
        """Score based on how strong the signal is."""
        # Different logic per strategy
        strategy = signal_data.get('strategy', '')
        
        if 'rsi' in strategy.lower():
            # RSI mean reversion: closer to threshold = stronger
            rsi = signal_data.get('rsi', 50)
            threshold = signal_data.get('threshold', 25)
            
            if rsi < threshold:
                # Oversold: lower RSI = stronger
                strength = (threshold - rsi) / threshold * 100
                return min(100, max(0, strength))
            
        elif 'momentum' in strategy.lower():
            # Momentum: further above Bollinger = stronger
            price = signal_data.get('price', 0)
            upper = signal_data.get('upper_band', 0)
            
            if upper > 0 and price > upper:
                strength = (price - upper) / upper * 100
                return min(100, max(0, strength * 2))  # Scale up
        
        # Default
        return signal_data.get('strength', 50)
    
    def _score_risk_reward(self, signal_data: Dict) -> float:
        """Score based on risk/reward ratio."""
        entry = signal_data.get('entry_price', 0)
        target = signal_data.get('target_price', 0)
        stop = signal_data.get('stop_price', 0)
        
        if not all([entry, target, stop]) or entry == 0:
            return 50  # Neutral
        
        reward = abs(target - entry)
        risk = abs(entry - stop)
        
        if risk == 0:
            return 0
        
        ratio = reward / risk
        
        # Ideal ratio depends on strategy
        if ratio >= 2.0:
            return 100
        elif ratio >= 1.5:
            return 75
        elif ratio >= 1.0:
            return 50
        else:
            return max(0, ratio * 50)
    
    def _score_volume_trend(self, market_data: Dict) -> float:
        """Score based on volume trend."""
        current_vol = market_data.get('current_volume', 0)
        avg_vol = market_data.get('avg_volume', 1)  # Avoid division by zero
        
        if avg_vol == 0:
            return 50
        
        ratio = current_vol / avg_vol
        
        if ratio >= 1.5:
            return 100
        elif ratio >= 1.2:
            return 80
        elif ratio >= 1.0:
            return 60
        elif ratio >= 0.8:
            return 40
        else:
            return 20
    
    def _score_volatility(self, strategy: str, signal_data: Dict,
                          market_data: Dict) -> float:
        """Score based on volatility alignment."""
        atr_pct = market_data.get('atr_pct', 0.02)  # Default 2%
        
        # Different strategies prefer different volatility
        if 'momentum' in strategy.lower():
            # Momentum likes volatility
            if atr_pct >= 0.03:
                return 90
            elif atr_pct >= 0.02:
                return 70
            else:
                return 40
        else:
            # Mean reversion likes moderate volatility
            if 0.01 <= atr_pct <= 0.03:
                return 80
            elif atr_pct < 0.01:
                return 60
            else:
                return 30
    
    def _score_market_regime(self, strategy: str, regime: str) -> float:
        """Score based on market regime alignment."""
        # Matrix of strategy x regime compatibility
        compatibility = {
            'rsi': {
                'BULL': 90,
                'NEUTRAL': 80,
                'BEAR': 60,
                'CRASH': 0
            },
            'momentum': {
                'BULL': 100,
                'NEUTRAL': 70,
                'BEAR': 30,
                'CRASH': 0
            },
            'hybrid': {
                'BULL': 80,
                'NEUTRAL': 90,
                'BEAR': 70,
                'CRASH': 0
            }
        }
        
        # Find which strategy key to use
        strategy_key = 'rsi'
        if 'momentum' in strategy.lower():
            strategy_key = 'momentum'
        elif 'hybrid' in strategy.lower():
            strategy_key = 'hybrid'
        
        return compatibility.get(strategy_key, {}).get(regime, 50)
    
    def rank_signals(self, signals: List[Dict], max_signals: int = 3) -> List[Dict]:
        """
        Rank multiple signals by confidence score.
        
        Args:
            signals: List of signal dicts with 'confidence' field
            max_signals: Maximum number to return
        
        Returns:
            Ranked list sorted by confidence descending
        """
        # Sort by score descending
        ranked = sorted(signals, key=lambda x: x.get('confidence', 0), reverse=True)
        
        # Take top N
        return ranked[:max_signals]