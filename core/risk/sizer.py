"""
Position sizing engine for Mark 3.1.
Calculates safe position sizes based on capital and risk.
"""

import logging
import math
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class PositionSizer:
    """
    Determines how many shares to buy.
    
    Strategies:
    - Fixed fractional (% of capital)
    - Volatility-adjusted (ATR-based)
    - Confidence-weighted
    """
    
    def __init__(self, total_capital: float = 10000, 
                 max_per_trade: float = 2000,
                 risk_per_trade_pct: float = 0.01):  # 1% risk per trade
        self.total_capital = total_capital
        self.max_per_trade = max_per_trade
        self.risk_per_trade_pct = risk_per_trade_pct
    
    def calculate_shares(self, price: float, confidence_score: float = 50,
                         atr: Optional[float] = None,
                         available_capital: Optional[float] = None) -> Dict[str, Any]:
        """
        Calculate safe position size.
        
        Returns:
            Dict with quantity, notional_value, and reasoning
        """
        if available_capital is None:
            available_capital = self.total_capital
        
        # Base allocation: fixed fractional
        base_allocation = min(self.max_per_trade, available_capital * 0.2)
        
        # Adjust by confidence (0-100 scale)
        confidence_multiplier = confidence_score / 100.0
        confidence_allocation = base_allocation * confidence_multiplier
        
        # Volatility adjustment if ATR provided
        if atr and price > 0:
            atr_pct = atr / price
            if atr_pct > 0.05:  # High volatility
                volatility_multiplier = 0.5
            elif atr_pct < 0.01:  # Low volatility
                volatility_multiplier = 1.2
            else:
                volatility_multiplier = 1.0
        else:
            volatility_multiplier = 1.0
        
        # Final allocation
        final_allocation = confidence_allocation * volatility_multiplier
        
        # Cap at max per trade
        final_allocation = min(final_allocation, self.max_per_trade)
        
        # Calculate shares
        if price > 0:
            shares = math.floor(final_allocation / price)
            actual_value = shares * price
        else:
            shares = 0
            actual_value = 0
        
        return {
            'shares': shares,
            'notional_value': actual_value,
            'base_allocation': base_allocation,
            'confidence_multiplier': confidence_multiplier,
            'volatility_multiplier': volatility_multiplier,
            'final_allocation': final_allocation,
            'reason': f"${actual_value:.2f} ({shares} shares @ ${price:.2f})"
        }
    
    def calculate_risk_amount(self, entry_price: float, stop_loss: float,
                              shares: int) -> float:
        """Calculate dollar risk for a position."""
        if shares <= 0:
            return 0.0
        return abs(entry_price - stop_loss) * shares
    
    def validate_risk(self, entry_price: float, stop_loss: float,
                      shares: int, total_capital: float) -> Tuple[bool, str]:
        """
        Validate that position risk is within limits.
        """
        risk_amount = self.calculate_risk_amount(entry_price, stop_loss, shares)
        risk_pct = risk_amount / total_capital
        
        if risk_pct > self.risk_per_trade_pct * 2:  # Max 2% risk per trade
            return False, f"Risk too high: {risk_pct:.2%} > {self.risk_per_trade_pct*2:.2%}"
        
        if risk_amount > total_capital * 0.05:  # Max 5% of capital at risk
            return False, f"Risk amount ${risk_amount:.2f} > 5% of capital"
        
        return True, f"Risk {risk_pct:.2%} (${risk_amount:.2f})"