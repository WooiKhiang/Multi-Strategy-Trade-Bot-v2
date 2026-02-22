"""
Ignore list manager with TTL and backoff.
Quarantines problem symbols to prevent resource waste.
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

class IgnoreManager:
    """
    Manages symbol ignore list with exponential backoff.
    
    Backoff levels:
    1: 1 hour
    2: 4 hours  
    3: 1 day
    4: 7 days (then auto-remove from universe)
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        self.backoff_map = {
            1: timedelta(hours=1),
            2: timedelta(hours=4),
            3: timedelta(days=1),
            4: timedelta(days=7)
        }
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def add(self, ticker: str, reason_code: str, scope: str = "ALL",
            auto_manual: str = "AUTO", notes: str = "") -> bool:
        """
        Add symbol to ignore list or increment backoff if already exists.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Check if already in ignore list
        cursor.execute("""
            SELECT retry_count, backoff_level FROM ignore_list
            WHERE ticker = ?
        """, (ticker,))
        
        existing = cursor.fetchone()
        
        if existing:
            # Increment backoff
            retry_count, backoff_level = existing
            new_level = min(backoff_level + 1, 4)
            new_retry = retry_count + 1
            
            # Calculate new TTL
            ttl = datetime.utcnow() + self.backoff_map[new_level]
            
            cursor.execute("""
                UPDATE ignore_list
                SET retry_count = ?, backoff_level = ?, ttl_utc = ?,
                    last_seen_issue = ?, notes = ?
                WHERE ticker = ?
            """, (new_retry, new_level, ttl, reason_code, notes, ticker))
            
            logger.info(f"Incremented ignore for {ticker} to level {new_level}")
        else:
            # New entry
            ttl = datetime.utcnow() + self.backoff_map[1]
            
            cursor.execute("""
                INSERT INTO ignore_list
                (ticker, reason_code, scope, ttl_utc, auto_manual,
                 retry_count, backoff_level, last_seen_issue, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker, reason_code, scope, ttl, auto_manual,
                  1, 1, reason_code, notes))
            
            logger.info(f"Added {ticker} to ignore list: {reason_code}")
        
        conn.commit()
        conn.close()
        return True
    
    def is_ignored(self, ticker: str) -> Tuple[bool, Optional[Dict]]:
        """
        Check if symbol is currently ignored.
        
        Returns:
            (is_ignored, info_dict)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT reason_code, scope, ttl_utc, backoff_level, retry_count
            FROM ignore_list
            WHERE ticker = ? AND ttl_utc > datetime('now')
        """, (ticker,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            reason, scope, ttl_str, level, retry = row
            return True, {
                'reason': reason,
                'scope': scope,
                'ttl': ttl_str,
                'level': level,
                'retry_count': retry
            }
        
        return False, None
    
    def get_active_ignores(self) -> List[Dict]:
        """Get all currently active ignores."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ticker, reason_code, scope, ttl_utc, backoff_level
            FROM ignore_list
            WHERE ttl_utc > datetime('now')
            ORDER BY ttl_utc
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'ticker': r[0],
                'reason': r[1],
                'scope': r[2],
                'ttl': r[3],
                'level': r[4]
            }
            for r in rows
        ]
    
    def cleanup_expired(self) -> int:
        """Remove expired ignores (but keep history for backoff)."""
        # We don't actually delete - we keep for backoff tracking
        # But we could archive very old ones
        return 0
    
    def get_backoff_level(self, ticker: str) -> int:
        """Get current backoff level for symbol."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT backoff_level FROM ignore_list
            WHERE ticker = ?
        """, (ticker,))
        
        row = cursor.fetchone()
        conn.close()
        
        return row[0] if row else 0
    
    def reset(self, ticker: str) -> bool:
        """Manually reset ignore for a symbol."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM ignore_list WHERE ticker = ?", (ticker,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted:
            logger.info(f"Reset ignore for {ticker}")
        return deleted > 0