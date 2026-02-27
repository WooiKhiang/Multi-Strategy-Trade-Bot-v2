"""
KIV Manager for Mark 3.1.
Manages Keep-In-View signals and confirmation logic.
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import uuid

logger = logging.getLogger(__name__)

class KIVManager:
    """
    Manages KIV signals lifecycle:
    - Add signals to KIV
    - Check for price confirmation
    - Move to CONFIRMED when conditions met
    - Handle timeouts
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        
        self.kiv_timeout_hours = 4
        self.confirmation_bounce_pct = 1.0
        self.confirmation_timeout_hours = 2
        
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def _generate_signal_id(self, ticker: str, strategy: str) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return f"{ticker}_{strategy}_{timestamp}"
    
    def add_to_kiv(self, ticker: str, strategy: str, 
                   entry_price: float, rebound_bottom: float,
                   go_in_price: float, target_price: float,
                   stop_loss: float, confidence: float,
                   notes: str = "") -> str:
        
        signal_id = self._generate_signal_id(ticker, strategy)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT signal_id FROM kiv_signals 
            WHERE ticker = ? AND strategy = ? AND status = 'KIV'
        """, (ticker, strategy))
        
        existing = cursor.fetchone()
        if existing:
            conn.close()
            return existing[0]
        
        cursor.execute("""
            INSERT INTO kiv_signals
            (signal_id, ticker, strategy, entry_price, rebound_bottom,
             go_in_price, target_price, stop_loss, confidence,
             trigger_time, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (signal_id, ticker, strategy, entry_price, rebound_bottom,
              go_in_price, target_price, stop_loss, confidence,
              datetime.utcnow(), 'KIV', notes))
        
        conn.commit()
        conn.close()
        
        logger.info(f"📌 Added {ticker} to KIV (confidence: {confidence:.1f})")
        return signal_id
    
    def check_confirmations(self, current_prices: Dict[str, float]) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT signal_id, ticker, strategy, rebound_bottom, go_in_price,
                   target_price, stop_loss, confidence, trigger_time
            FROM kiv_signals
            WHERE status = 'KIV'
        """)
        
        signals = cursor.fetchall()
        confirmed = []
        
        for signal in signals:
            (signal_id, ticker, strategy, rebound_bottom, go_in_price,
             target_price, stop_loss, confidence, trigger_time) = signal
            
            if isinstance(trigger_time, str):
                trigger_dt = datetime.fromisoformat(trigger_time)
            else:
                trigger_dt = trigger_time
            
            age_hours = (datetime.utcnow() - trigger_dt).total_seconds() / 3600
            if age_hours > self.kiv_timeout_hours:
                self._expire_signal(signal_id, f"Timeout after {age_hours:.1f}h")
                continue
            
            current_price = current_prices.get(ticker)
            if not current_price:
                continue
            
            self._record_price_check(signal_id, current_price)
            
            if current_price >= rebound_bottom * (1 + self.confirmation_bounce_pct/100):
                cursor.execute("""
                    UPDATE kiv_signals
                    SET status = 'CONFIRMED'
                    WHERE signal_id = ?
                """, (signal_id,))
                
                confirmed.append({
                    'signal_id': signal_id,
                    'ticker': ticker,
                    'strategy': strategy,
                    'entry_price': go_in_price,
                    'target_price': target_price,
                    'stop_loss': stop_loss,
                    'confidence': confidence,
                    'confirmation_price': current_price
                })
                
                logger.info(f"✅ {ticker} CONFIRMED at ${current_price:.2f}")
        
        conn.commit()
        conn.close()
        return confirmed
    
    def _record_price_check(self, signal_id: str, price: float):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO price_checks
            (signal_id, check_time, price)
            VALUES (?, ?, ?)
        """, (signal_id, datetime.utcnow(), price))
        conn.commit()
        conn.close()
    
    def _expire_signal(self, signal_id: str, reason: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE kiv_signals
            SET status = 'EXPIRED', notes = ?
            WHERE signal_id = ?
        """, (reason, signal_id))
        conn.commit()
        conn.close()
        logger.info(f"⏰ Signal {signal_id} expired: {reason}")
    
    def get_confirmed_signals(self, min_confidence: float = 60) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = datetime.utcnow() - timedelta(hours=self.confirmation_timeout_hours)
        
        cursor.execute("""
            UPDATE kiv_signals
            SET status = 'EXPIRED', notes = 'Confirmation timeout'
            WHERE status = 'CONFIRMED' AND trigger_time < ?
        """, (cutoff,))
        
        cursor.execute("""
            SELECT signal_id, ticker, strategy, go_in_price,
                   target_price, stop_loss, confidence
            FROM kiv_signals
            WHERE status = 'CONFIRMED' AND confidence >= ?
            ORDER BY confidence DESC
        """, (min_confidence,))
        
        rows = cursor.fetchall()
        conn.close()
        
        signals = []
        for row in rows:
            signals.append({
                'signal_id': row[0],
                'ticker': row[1],
                'strategy': row[2],
                'go_in_price': row[3],
                'target_price': row[4],
                'stop_loss': row[5],
                'confidence': row[6]
            })
        
        return signals
    
    def mark_executed(self, signal_id: str, ticket_id: str) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE kiv_signals
            SET status = 'EXECUTED', notes = ?
            WHERE signal_id = ?
        """, (f"Executed: {ticket_id}", signal_id))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0
    
    def get_kiv_summary(self) -> Dict[str, int]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT status, COUNT(*) FROM kiv_signals GROUP BY status")
        rows = cursor.fetchall()
        conn.close()
        
        summary = {'KIV': 0, 'CONFIRMED': 0, 'EXPIRED': 0, 'EXECUTED': 0}
        for status, count in rows:
            summary[status] = count
        return summary