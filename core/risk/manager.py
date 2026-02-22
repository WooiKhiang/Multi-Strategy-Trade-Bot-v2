"""
Central risk manager for Mark 3.1.
Coordinates ignore list, limits, and position sizing.
"""

import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

from core.utils.registry import ComponentRegistry
from config.settings import settings

logger = logging.getLogger(__name__)

class RiskManager:
    """
    Central risk management authority.
    
    Responsibilities:
    - Check ignore list
    - Enforce daily limits
    - Calculate position sizes
    - Track allocated capital
    """
    
    def __init__(self):
        registry = ComponentRegistry()
        self.ignore = registry.get('risk', 'ignore')()
        self.limits = registry.get('risk', 'limits')()
        self.sizer = registry.get('risk', 'sizer')(
            total_capital=settings.TOTAL_CAPITAL,
            max_per_trade=settings.MAX_PER_TRADE
        )
        
        self.total_capital = settings.TOTAL_CAPITAL
        self.max_per_trade = settings.MAX_PER_TRADE
        self.max_concurrent = settings.MAX_CONCURRENT_TRADES
        
        logger.info("RiskManager initialized")
    
    def can_trade_symbol(self, ticker: str, strategy: str = "ALL") -> Tuple[bool, str]:
        """
        Check if symbol can be traded this cycle.
        
        Checks:
        1. Ignore list
        2. Daily limits
        3. Already in position (no averaging down)
        """
        # Check ignore list
        ignored, info = self.ignore.is_ignored(ticker)
        if ignored:
            if info['scope'] in [strategy, "ALL"]:
                return False, f"IGNORED:{info['reason']}"
        
        # Check daily limits
        limits_ok, reason = self.limits.can_trade()
        if not limits_ok:
            return False, reason
        
        # Check if already in position (would be handled by capital allocation)
        # This will be enhanced when we have position tracking
        
        return True, "OK"
    
    def get_available_capital(self) -> float:
        """Calculate available capital based on open positions."""
        # This will be implemented when we have position tracking
        # For now, return total capital
        return self.total_capital
    
    def approve_trade(self, ticker: str, price: float, 
                      confidence: float = 50,
                      atr: Optional[float] = None,
                      strategy: str = "ALL") -> Dict[str, Any]:
        """
        Review and approve/deny a trade.
        
        Returns:
            Dict with approval status and position details
        """
        # Check if symbol is tradeable
        can_trade, reason = self.can_trade_symbol(ticker, strategy)
        if not can_trade:
            return {
                'approved': False,
                'reason': reason,
                'ticker': ticker
            }
        
        # Get available capital
        available = self.get_available_capital()
        
        # Calculate position size
        sizing = self.sizer.calculate_shares(
            price=price,
            confidence_score=confidence,
            atr=atr,
            available_capital=available
        )
        
        if sizing['shares'] == 0:
            return {
                'approved': False,
                'reason': "Position size zero",
                'ticker': ticker,
                'sizing': sizing
            }
        
        return {
            'approved': True,
            'ticker': ticker,
            'shares': sizing['shares'],
            'notional_value': sizing['notional_value'],
            'price': price,
            'confidence': confidence,
            'sizing_details': sizing,
            'timestamp': datetime.utcnow()
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get current risk status."""
        limits_summary = self.limits.get_summary()
        active_ignores = self.ignore.get_active_ignores()
        
        return {
            'total_capital': self.total_capital,
            'max_per_trade': self.max_per_trade,
            'max_concurrent': self.max_concurrent,
            'limits': limits_summary,
            'active_ignores': len(active_ignores),
            'ignore_list': active_ignores[:10],  # First 10 for brevity
            'available_capital': self.get_available_capital()
        }