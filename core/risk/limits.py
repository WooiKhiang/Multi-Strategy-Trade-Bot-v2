"""
Daily limits manager for Mark 3.1.
Tracks P&L and enforces daily loss limit.
"""

import sqlite3
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

from core.utils.time_utils import get_utc_midnight

logger = logging.getLogger(__name__)

class LimitsManager:
    """
    Manages daily trading limits.
    
    Tracks:
    - Realized P&L today
    - Unrealized P&L
    - Daily loss limit
    - Optional profit cap
    """
    
    def __init__(self, db_path: str = "data/trade_log.db",
                 daily_loss_limit: float = 500,
                 daily_profit_cap: float = 1000):
        self.db_path = Path(db_path)
        self.daily_loss_limit = daily_loss_limit
        self.daily_profit_cap = daily_profit_cap
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def get_today_pnl(self) -> Tuple[float, float, float]:
        """
        Get today's P&L components.
        
        Returns:
            (realized_pnl, unrealized_pnl, total_pnl)
        """
        midnight = get_utc_midnight()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Realized P&L from today's exits
        cursor.execute("""
            SELECT COALESCE(SUM(entry_price * quantity), 0),
                   COALESCE(SUM(exit_price * quantity), 0)
            FROM trade_history
            WHERE exit_time >= ?
        """, (midnight,))
        
        entry_sum, exit_sum = cursor.fetchone()
        realized = exit_sum - entry_sum
        
        # Unrealized P&L from open positions
        cursor.execute("""
            SELECT COALESCE(SUM(entry_price * quantity), 0),
                   COALESCE(SUM(current_price * quantity), 0)
            FROM positions
            WHERE status IN ('OPEN', 'CLOSING')
        """)
        
        open_entry, open_current = cursor.fetchone()
        unrealized = open_current - open_entry
        
        conn.close()
        
        total = realized + unrealized
        
        return realized, unrealized, total
    
    def is_loss_limit_hit(self) -> bool:
        """Check if daily loss limit has been hit."""
        _, _, total = self.get_today_pnl()
        return total <= -self.daily_loss_limit
    
    def is_profit_cap_hit(self) -> bool:
        """Check if daily profit cap has been hit (optional)."""
        _, _, total = self.get_today_pnl()
        return total >= self.daily_profit_cap
    
    def can_trade(self) -> Tuple[bool, str]:
        """
        Check if new trading is allowed based on limits.
        
        Returns:
            (allowed, reason)
        """
        if self.is_loss_limit_hit():
            return False, f"DAILY_LOSS_LIMIT_HIT (${self.daily_loss_limit})"
        
        if self.is_profit_cap_hit():
            return False, f"DAILY_PROFIT_CAP_HIT (${self.daily_profit_cap})"
        
        return True, "OK"
    
    def get_summary(self) -> dict:
        """Get daily limits summary."""
        realized, unrealized, total = self.get_today_pnl()
        
        return {
            'realized_pnl': realized,
            'unrealized_pnl': unrealized,
            'total_pnl': total,
            'loss_limit': self.daily_loss_limit,
            'profit_cap': self.daily_profit_cap,
            'loss_limit_hit': total <= -self.daily_loss_limit,
            'profit_cap_hit': total >= self.daily_profit_cap,
            'remaining_loss_buffer': self.daily_loss_limit + total if total < 0 else self.daily_loss_limit,
            'remaining_profit_buffer': self.daily_profit_cap - total if total > 0 else self.daily_profit_cap
        }