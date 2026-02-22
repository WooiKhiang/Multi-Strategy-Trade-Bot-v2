"""
SQLite cache layer for price data.
Provides fast access to recent prices without API calls.
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class PriceCache:
    """
    Manages price cache in SQLite.
    
    Used for Stage A validation to avoid unnecessary API calls.
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        self._ensure_cache_table()
    
    def _ensure_cache_table(self):
        """Ensure price_cache table exists."""
        # Table should already exist from init_db.py
        pass
    
    def _get_connection(self):
        """Get database connection."""
        return sqlite3.connect(str(self.db_path))
    
    def update(self, ticker: str, price: float, volume: int = 0, 
               bid: float = None, ask: float = None, source: str = "snapshot"):
        """
        Update cache with latest price data.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.utcnow()
        
        cursor.execute("""
            INSERT OR REPLACE INTO price_cache 
            (ticker, price, volume, bid, ask, timestamp, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (ticker, price, volume, bid, ask, now, source))
        
        conn.commit()
        conn.close()
        logger.debug(f"Cached {ticker}: ${price} from {source}")
    
    def get(self, ticker: str, max_age_seconds: int = 60) -> Optional[Dict[str, Any]]:
        """
        Get cached price if not stale.
        
        Args:
            ticker: Symbol to look up
            max_age_seconds: Maximum age in seconds (default: 60)
        
        Returns:
            Dict with price data or None if stale/missing
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT price, volume, bid, ask, timestamp, source
            FROM price_cache
            WHERE ticker = ?
        """, (ticker,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        price, volume, bid, ask, ts_str, source = row
        
        # Parse timestamp
        if isinstance(ts_str, str):
            ts = datetime.fromisoformat(ts_str)
        else:
            ts = ts_str
        
        # Check age
        age = (datetime.utcnow() - ts).total_seconds()
        if age > max_age_seconds:
            logger.debug(f"Cache stale for {ticker}: {age:.1f}s old")
            return None
        
        return {
            'price': price,
            'volume': volume,
            'bid': bid,
            'ask': ask,
            'timestamp': ts,
            'source': source,
            'age_seconds': age
        }
    
    def get_batch(self, tickers: list, max_age_seconds: int = 60) -> dict:
        """
        Get multiple cached prices at once.
        
        Returns:
            Dict mapping ticker -> data or None
        """
        result = {}
        for ticker in tickers:
            result[ticker] = self.get(ticker, max_age_seconds)
        return result
    
    def clean_stale(self, max_age_minutes: int = 60):
        """
        Remove entries older than max_age_minutes.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        
        cursor.execute("""
            DELETE FROM price_cache
            WHERE timestamp < ?
        """, (cutoff,))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        logger.info(f"Cleaned {deleted} stale cache entries")
        return deleted