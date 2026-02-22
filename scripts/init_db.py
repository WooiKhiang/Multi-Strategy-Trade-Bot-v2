#!/usr/bin/env python3
"""
Initialize SQLite database for Mark 3.1.
Run this once to create all tables.
"""

import os
import sqlite3
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path - using your Windows path
DB_PATH = Path(__file__).parent.parent / "data" / "trade_log.db"

def init_database():
    """Create all tables if they don't exist."""
    
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Create tables in order (dependencies first)
    
    # 1. IGNORE_LIST
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ignore_list (
        ticker TEXT PRIMARY KEY,
        reason_code TEXT,
        scope TEXT,
        ttl_utc DATETIME,
        auto_manual TEXT,
        retry_count INTEGER DEFAULT 0,
        backoff_level INTEGER DEFAULT 1,
        last_seen_issue TEXT,
        first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
        notes TEXT
    )
    """)
    
    # 2. SIGNALS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        signal_id TEXT PRIMARY KEY,
        ticker TEXT,
        strategy TEXT,
        trigger_time DATETIME,
        trigger_price REAL,
        rebound_bottom REAL,
        go_in_price REAL,
        profit_target REAL,
        stop_loss REAL,
        confidence_score REAL,
        status TEXT,
        cooldown_until DATETIME
    )
    """)
    
    # 3. POSITIONS (with partial unique index)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        ticket_id TEXT PRIMARY KEY,
        ticker TEXT,
        entry_time DATETIME,
        entry_price REAL,
        quantity REAL,
        current_price REAL,
        status TEXT,
        strategy TEXT,
        exit_signal TEXT,
        exit_reason TEXT,
        exit_time DATETIME
    )
    """)
    
    # Partial unique index for active positions
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_positions_one_active 
    ON positions(ticker) 
    WHERE status IN ('OPEN', 'CLOSING')
    """)
    
    # Regular indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_ticker ON positions(ticker)")
    
    # 4. TRADE_HISTORY
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS trade_history (
        exit_time DATETIME,
        ticker TEXT,
        strategy TEXT,
        entry_price REAL,
        exit_price REAL,
        quantity REAL,
        pnl_percent REAL,
        win_loss TEXT,
        exit_reason TEXT,
        why_triggered TEXT,
        correct_action TEXT,
        lesson TEXT,
        ticket_id TEXT REFERENCES positions(ticket_id)
    )
    """)
    
    # 5. HEALTH_STATE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS health_state (
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        state TEXT,
        api_calls_cycle INTEGER,
        data_errors_today INTEGER,
        ignore_list_size INTEGER,
        reason TEXT
    )
    """)
    
    # 6. API_BUDGET
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS api_budget (
        cycle_start DATETIME,
        endpoint TEXT,
        calls INTEGER,
        budget_limit INTEGER
    )
    """)
    
    # 7. DATA_QUALITY_LOG
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_quality_log (
        timestamp DATETIME,
        ticker TEXT,
        issue_type TEXT,
        severity TEXT,
        bars_expected INTEGER,
        bars_actual INTEGER,
        action_taken TEXT
    )
    """)
    
    # 8. ERROR_LOG
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS error_log (
        timestamp DATETIME,
        component TEXT,
        error TEXT,
        severity TEXT,
        resolved BOOLEAN DEFAULT 0
    )
    """)
    
    # 9. PRICE_CACHE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price_cache (
        ticker TEXT PRIMARY KEY,
        price REAL,
        volume INTEGER,
        bid REAL,
        ask REAL,
        timestamp DATETIME,
        source TEXT
    )
    """)
    
    # 10. STRATEGY_STATS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS strategy_stats (
        ticker TEXT,
        date DATE,
        volatility_20d REAL,
        volume_avg_20d REAL,
        proximity_score REAL,
        asset_type TEXT,
        PRIMARY KEY (ticker, date)
    )
    """)
    
    # Create performance indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_cache_timestamp ON price_cache(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_stats_date ON strategy_stats(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_error_log_resolved ON error_log(resolved)")
    
    conn.commit()
    
    # Verify tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    logger.info(f"Created {len(tables)} tables:")
    for table in tables:
        logger.info(f"  - {table[0]}")
    
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")

if __name__ == "__main__":
    init_database()