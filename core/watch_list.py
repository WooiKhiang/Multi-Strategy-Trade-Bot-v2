"""
Watch List Manager for Mark 3.1 (Tier 2)
Tracks symbols with unusual activity in SQLite.
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class WatchListManager:
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        self._init_table()
    
    def _init_table(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watch_list (
                ticker TEXT PRIMARY KEY,
                first_spotted DATETIME,
                last_active DATETIME,
                spike_count INTEGER DEFAULT 1,
                avg_score REAL,
                sector TEXT,
                status TEXT DEFAULT 'WATCHING'
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_watch_list_last_active ON watch_list(last_active)")
        conn.commit()
        conn.close()
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def add_or_update(self, ticker: str, score: float, sector: str = 'Other'):
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.utcnow()
        
        cursor.execute("SELECT first_spotted, spike_count, avg_score FROM watch_list WHERE ticker = ?", (ticker,))
        existing = cursor.fetchone()
        
        if existing:
            first_spotted, spike_count, old_avg = existing
            new_count = spike_count + 1
            new_avg = (old_avg * spike_count + score) / new_count
            cursor.execute("UPDATE watch_list SET last_active = ?, spike_count = ?, avg_score = ? WHERE ticker = ?",
                          (now, new_count, new_avg, ticker))
        else:
            cursor.execute("INSERT INTO watch_list (ticker, first_spotted, last_active, spike_count, avg_score, sector) VALUES (?, ?, ?, ?, ?, ?)",
                          (ticker, now, now, 1, score, sector))
        
        conn.commit()
        conn.close()
    
    def get_active_watch_list(self, max_age_hours: int = 72) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        cursor.execute("SELECT ticker, first_spotted, last_active, spike_count, avg_score, sector FROM watch_list WHERE last_active > ? ORDER BY avg_score DESC", (cutoff,))
        rows = cursor.fetchall()
        conn.close()
        
        watch_list = []
        for row in rows:
            watch_list.append({
                'ticker': row[0],
                'first_spotted': row[1],
                'last_active': row[2],
                'spike_count': row[3],
                'avg_score': row[4],
                'sector': row[5]
            })
        return watch_list
    
    def prune_old_entries(self, max_age_days: int = 30):
        conn = self._get_connection()
        cursor = conn.cursor()
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        cursor.execute("DELETE FROM watch_list WHERE last_active < ?", (cutoff,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted
    
    def get_top_candidates(self, limit: int = 50) -> List[Dict]:
        watch_list = self.get_active_watch_list(max_age_hours=24)
        watch_list.sort(key=lambda x: x['avg_score'], reverse=True)
        return watch_list[:limit]