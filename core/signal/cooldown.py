"""
Cooldown manager for signal processing.
Prevents re-entering same symbol too quickly after exit.
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class CooldownManager:
    """
    Manages signal cooldowns to prevent ping-pong behavior.
    
    After a stop loss, symbol enters cooldown for that strategy.
    After a take profit, shorter cooldown or none.
    """
    
    def __init__(self, db_path: str = "data/trade_log.db",
                 default_cooldown_minutes: int = 60):
        self.db_path = Path(db_path)
        self.default_cooldown = timedelta(minutes=default_cooldown_minutes)
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def set_cooldown(self, ticker: str, strategy: str, 
                     reason: str = "STOP_LOSS",
                     minutes: Optional[int] = None) -> bool:
        """
        Set cooldown for a symbol+strategy combination.
        
        Args:
            ticker: Symbol to cool down
            strategy: Strategy name
            reason: Why cooldown is triggered
            minutes: Custom cooldown duration (uses default if None)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Calculate cooldown end time
        if minutes:
            duration = timedelta(minutes=minutes)
        else:
            # Different cooldowns based on reason
            if reason == "STOP_LOSS":
                duration = self.default_cooldown
            elif reason == "TAKE_PROFIT":
                duration = timedelta(minutes=30)  # Shorter after win
            elif reason == "REJECTED":
                duration = timedelta(minutes=15)  # Very short after rejection
            else:
                duration = self.default_cooldown
        
        cooldown_until = datetime.utcnow() + duration
        
        # Signal_ID format: ticker_strategy_YYYYMMDDHH
        signal_id = f"{ticker}_{strategy}_{datetime.utcnow().strftime('%Y%m%d%H')}"
        
        cursor.execute("""
            UPDATE signals
            SET cooldown_until = ?
            WHERE signal_id = ? OR (ticker = ? AND strategy = ?)
        """, (cooldown_until, signal_id, ticker, strategy))
        
        # Also insert a record if none exists
        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO signals
                (signal_id, ticker, strategy, trigger_time, status, cooldown_until)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (f"cooldown_{ticker}_{strategy}_{datetime.utcnow().timestamp()}",
                  ticker, strategy, datetime.utcnow(), "COOLDOWN", cooldown_until))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Cooldown set for {ticker}/{strategy} until {cooldown_until} ({reason})")
        return True
    
    def is_on_cooldown(self, ticker: str, strategy: str) -> Tuple[bool, Optional[datetime]]:
        """
        Check if symbol+strategy is on cooldown.
        
        Returns:
            (is_on_cooldown, cooldown_until)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT cooldown_until FROM signals
            WHERE ticker = ? AND strategy = ?
              AND cooldown_until > datetime('now')
            ORDER BY cooldown_until DESC
            LIMIT 1
        """, (ticker, strategy))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            cooldown_until = row[0]
            if isinstance(cooldown_until, str):
                cooldown_until = datetime.fromisoformat(cooldown_until)
            return True, cooldown_until
        
        return False, None
    
    def clear_cooldown(self, ticker: str, strategy: str) -> bool:
        """Manually clear cooldown."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE signals
            SET cooldown_until = NULL
            WHERE ticker = ? AND strategy = ?
        """, (ticker, strategy))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected:
            logger.info(f"Cleared cooldown for {ticker}/{strategy}")
        return affected > 0
    
    def get_active_cooldowns(self) -> List[Dict]:
        """Get all active cooldowns."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ticker, strategy, cooldown_until
            FROM signals
            WHERE cooldown_until > datetime('now')
            ORDER BY cooldown_until
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'ticker': r[0],
                'strategy': r[1],
                'cooldown_until': r[2]
            }
            for r in rows
        ]