"""
Slippage tracker for Mark 3.1.
Records and analyzes execution quality.
"""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import statistics

logger = logging.getLogger(__name__)

class SlippageTracker:
    """
    Tracks order execution quality.
    
    Records:
    - Expected vs actual fill price
    - Slippage percentage
    - Partial fills
    - Execution speed (when we have timestamps)
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def record_execution(self, ticket_id: str, ticker: str,
                         expected_price: float, actual_price: float,
                         expected_quantity: float, actual_quantity: float,
                         order_type: str, side: str) -> Dict[str, Any]:
        """
        Record an execution for slippage analysis.
        """
        # Calculate metrics
        price_slippage = actual_price - expected_price
        price_slippage_pct = (price_slippage / expected_price) * 100 if expected_price else 0
        
        fill_ratio = actual_quantity / expected_quantity if expected_quantity else 0
        partial_fill = fill_ratio < 0.99  # Less than 99% filled
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Insert into execution_quality table (will be created if not exists)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_quality (
                ticket_id TEXT PRIMARY KEY,
                ticker TEXT,
                timestamp DATETIME,
                expected_price REAL,
                actual_price REAL,
                price_slippage REAL,
                price_slippage_pct REAL,
                expected_quantity REAL,
                actual_quantity REAL,
                fill_ratio REAL,
                partial_fill BOOLEAN,
                order_type TEXT,
                side TEXT
            )
        """)
        
        cursor.execute("""
            INSERT OR REPLACE INTO execution_quality
            (ticket_id, ticker, timestamp, expected_price, actual_price,
             price_slippage, price_slippage_pct, expected_quantity,
             actual_quantity, fill_ratio, partial_fill, order_type, side)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticket_id, ticker, datetime.utcnow(),
              expected_price, actual_price,
              price_slippage, price_slippage_pct,
              expected_quantity, actual_quantity,
              fill_ratio, partial_fill, order_type, side))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Recorded execution for {ticker}: slippage {price_slippage_pct:.2f}%")
        
        return {
            'ticket_id': ticket_id,
            'price_slippage_pct': price_slippage_pct,
            'fill_ratio': fill_ratio,
            'partial_fill': partial_fill
        }
    
    def get_slippage_stats(self, ticker: Optional[str] = None,
                           days: int = 30) -> Dict[str, Any]:
        """
        Get slippage statistics for analysis.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        if ticker:
            cursor.execute("""
                SELECT price_slippage_pct, fill_ratio, partial_fill
                FROM execution_quality
                WHERE ticker = ? AND timestamp > ?
            """, (ticker, cutoff))
        else:
            cursor.execute("""
                SELECT price_slippage_pct, fill_ratio, partial_fill
                FROM execution_quality
                WHERE timestamp > ?
            """, (cutoff,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return {'count': 0}
        
        slippages = [r[0] for r in rows]
        fill_ratios = [r[1] for r in rows]
        partials = [r[2] for r in rows]
        
        return {
            'count': len(rows),
            'avg_slippage_pct': statistics.mean(slippages),
            'median_slippage_pct': statistics.median(slippages),
            'max_slippage_pct': max(slippages),
            'min_slippage_pct': min(slippages),
            'avg_fill_ratio': statistics.mean(fill_ratios),
            'partial_fill_rate': sum(partials) / len(partials) * 100,
            'ticker': ticker if ticker else 'ALL'
        }
    
    def simulate_fill(self, expected_price: float, order_type: str = 'LIMIT',
                      side: str = 'BUY', ticker: Optional[str] = None) -> Dict[str, Any]:
        """
        Simulate a realistic fill for paper trading.
        
        For paper mode, we want realistic slippage, not perfect fills.
        """
        import random
        
        # Base slippage depends on order type
        if order_type == 'MARKET':
            # Market orders get more slippage
            if side == 'BUY':
                slippage_pct = random.uniform(0.05, 0.2)  # 0.05% to 0.2%
            else:
                slippage_pct = random.uniform(-0.2, -0.05)  # Sell slippage
        else:
            # Limit orders get less slippage
            if side == 'BUY':
                slippage_pct = random.uniform(0.01, 0.1)
            else:
                slippage_pct = random.uniform(-0.1, -0.01)
        
        # Adjust based on ticker if we have historical data
        if ticker:
            stats = self.get_slippage_stats(ticker, days=7)
            if stats['count'] > 5:
                # Use historical avg with some randomness
                slippage_pct = stats['avg_slippage_pct'] * random.uniform(0.8, 1.2)
        
        # Calculate fill price
        fill_price = expected_price * (1 + slippage_pct/100)
        
        # Simulate partial fills (10% chance)
        partial_fill = random.random() < 0.1
        if partial_fill:
            fill_ratio = random.uniform(0.5, 0.95)
        else:
            fill_ratio = 1.0
        
        return {
            'fill_price': round(fill_price, 2),
            'slippage_pct': slippage_pct,
            'fill_ratio': fill_ratio,
            'partial_fill': partial_fill
        }