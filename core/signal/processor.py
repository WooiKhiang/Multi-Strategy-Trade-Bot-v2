"""
Signal processor for Mark 3.1.
Manages signal lifecycle: KIV → CONFIRMED → EXECUTED.
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import uuid

from core.utils.registry import ComponentRegistry

logger = logging.getLogger(__name__)

class SignalProcessor:
    """
    Processes trading signals through their lifecycle.
    
    Stages:
    - KIV (Keep In View): Initial signal, waiting for confirmation
    - CONFIRMED: Technical confirmation received, ready to buy
    - EXECUTED: Order placed
    - EXPIRED: Signal timed out
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        
        # Get dependencies from registry
        registry = ComponentRegistry()
        self.confidence = registry.get('signal', 'confidence')()
        self.cooldown = registry.get('signal', 'cooldown')()
        
        # Timeouts
        self.kiv_timeout_hours = 4
        self.confirmed_timeout_hours = 2
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def _generate_signal_id(self, ticker: str, strategy: str) -> str:
        """Generate unique signal ID with hourly bucket."""
        hour_bucket = datetime.utcnow().strftime("%Y%m%d%H")
        return f"{ticker}_{strategy}_{hour_bucket}"
    
    def add_to_kiv(self, ticker: str, strategy: str,
                   trigger_price: float,
                   rebound_bottom: float,
                   go_in_price: float,
                   profit_target: float,
                   stop_loss: float,
                   signal_data: Dict[str, Any],
                   market_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add a new signal to KIV (Keep In View).
        """
        # Check cooldown first
        on_cooldown, cooldown_until = self.cooldown.is_on_cooldown(ticker, strategy)
        if on_cooldown:
            logger.info(f"{ticker}/{strategy} on cooldown until {cooldown_until}")
            return {
                'status': 'REJECTED',
                'reason': 'COOLDOWN',
                'cooldown_until': cooldown_until
            }
        
        # Calculate confidence score
        confidence_result = self.confidence.calculate(
            ticker, strategy, signal_data, market_data
        )
        confidence = confidence_result['score']
        
        # Generate signal ID
        signal_id = self._generate_signal_id(ticker, strategy)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Check if signal already exists (prevent duplicates)
        cursor.execute("""
            SELECT status FROM signals
            WHERE signal_id = ? OR (ticker = ? AND strategy = ? AND status IN ('KIV', 'CONFIRMED'))
        """, (signal_id, ticker, strategy))
        
        existing = cursor.fetchone()
        if existing:
            conn.close()
            logger.debug(f"Signal already exists for {ticker}/{strategy}: {existing[0]}")
            return {
                'status': 'EXISTS',
                'signal_status': existing[0]
            }
        
        # Insert new signal
        cursor.execute("""
            INSERT INTO signals
            (signal_id, ticker, strategy, trigger_time, trigger_price,
             rebound_bottom, go_in_price, profit_target, stop_loss,
             confidence_score, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (signal_id, ticker, strategy, datetime.utcnow(),
              trigger_price, rebound_bottom, go_in_price,
              profit_target, stop_loss, confidence, 'KIV'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Added {ticker}/{strategy} to KIV (confidence: {confidence})")
        
        return {
            'status': 'ADDED',
            'signal_id': signal_id,
            'confidence': confidence,
            'breakdown': confidence_result['breakdown']
        }
    
    def check_confirmation(self, ticker: str, strategy: str,
                          current_price: float) -> Dict[str, Any]:
        """
        Check if a KIV signal is confirmed (price bounced from rebound_bottom).
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Find active KIV signal
        cursor.execute("""
            SELECT signal_id, trigger_time, rebound_bottom, go_in_price,
                   profit_target, stop_loss, confidence_score
            FROM signals
            WHERE ticker = ? AND strategy = ? AND status = 'KIV'
            ORDER BY trigger_time DESC
            LIMIT 1
        """, (ticker, strategy))
        
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return {'confirmed': False, 'reason': 'NO_KIV_SIGNAL'}
        
        (signal_id, trigger_time, rebound_bottom, go_in_price,
         profit_target, stop_loss, confidence) = row
        
        # Check if signal expired
        trigger_dt = datetime.fromisoformat(trigger_time) if isinstance(trigger_time, str) else trigger_time
        age_hours = (datetime.utcnow() - trigger_dt).total_seconds() / 3600
        
        if age_hours > self.kiv_timeout_hours:
            # Mark as expired
            cursor.execute("""
                UPDATE signals SET status = 'EXPIRED'
                WHERE signal_id = ?
            """, (signal_id,))
            conn.commit()
            conn.close()
            logger.info(f"KIV signal {signal_id} expired (age: {age_hours:.1f}h)")
            return {'confirmed': False, 'reason': 'EXPIRED'}
        
        # Check for confirmation (price bounced 1% above rebound_bottom)
        if rebound_bottom and rebound_bottom > 0:
            bounce_threshold = rebound_bottom * 1.01  # 1% bounce
            if current_price >= bounce_threshold:
                # Confirmed! Move to CONFIRMED
                cursor.execute("""
                    UPDATE signals SET status = 'CONFIRMED'
                    WHERE signal_id = ?
                """, (signal_id,))
                conn.commit()
                conn.close()
                
                logger.info(f"Signal {signal_id} CONFIRMED at ${current_price}")
                return {
                    'confirmed': True,
                    'signal_id': signal_id,
                    'go_in_price': go_in_price,
                    'profit_target': profit_target,
                    'stop_loss': stop_loss,
                    'confidence': confidence
                }
        
        conn.close()
        return {'confirmed': False, 'reason': 'NOT_CONFIRMED'}
    
    def get_confirmed_signals(self, min_confidence: int = 60) -> List[Dict]:
        """
        Get all CONFIRMED signals ready for execution.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Check for expired confirmed signals
        cutoff = datetime.utcnow() - timedelta(hours=self.confirmed_timeout_hours)
        
        # Expire old confirmed signals
        cursor.execute("""
            UPDATE signals SET status = 'EXPIRED'
            WHERE status = 'CONFIRMED' AND trigger_time < ?
        """, (cutoff,))
        
        expired = cursor.rowcount
        if expired:
            logger.info(f"Expired {expired} old CONFIRMED signals")
        
        # Get active confirmed signals with sufficient confidence
        cursor.execute("""
            SELECT signal_id, ticker, strategy, go_in_price,
                   profit_target, stop_loss, confidence_score
            FROM signals
            WHERE status = 'CONFIRMED' AND confidence_score >= ?
            ORDER BY confidence_score DESC
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
                'profit_target': row[4],
                'stop_loss': row[5],
                'confidence': row[6]
            })
        
        return signals
    
    def mark_executed(self, signal_id: str, ticket_id: str) -> bool:
        """
        Mark a signal as executed.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE signals SET status = 'EXECUTED'
            WHERE signal_id = ?
        """, (signal_id,))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected:
            logger.info(f"Signal {signal_id} marked EXECUTED (ticket: {ticket_id})")
        return affected > 0
    
    def reject_signal(self, signal_id: str, reason: str) -> bool:
        """
        Reject a signal (e.g., risk manager declined).
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE signals SET status = 'REJECTED'
            WHERE signal_id = ?
        """, (signal_id,))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected:
            logger.info(f"Signal {signal_id} REJECTED: {reason}")
        return affected > 0
    
    def get_signal_status(self, signal_id: str) -> Optional[Dict]:
        """
        Get current status of a signal.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ticker, strategy, status, confidence_score,
                   trigger_time, cooldown_until
            FROM signals
            WHERE signal_id = ?
        """, (signal_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'signal_id': signal_id,
                'ticker': row[0],
                'strategy': row[1],
                'status': row[2],
                'confidence': row[3],
                'trigger_time': row[4],
                'cooldown_until': row[5]
            }
        return None
    
    def cleanup_expired(self) -> Dict[str, int]:
        """
        Clean up expired signals.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.utcnow()
        kiv_cutoff = now - timedelta(hours=self.kiv_timeout_hours)
        conf_cutoff = now - timedelta(hours=self.confirmed_timeout_hours)
        
        # Expire old KIV
        cursor.execute("""
            UPDATE signals SET status = 'EXPIRED'
            WHERE status = 'KIV' AND trigger_time < ?
        """, (kiv_cutoff,))
        expired_kiv = cursor.rowcount
        
        # Expire old CONFIRMED
        cursor.execute("""
            UPDATE signals SET status = 'EXPIRED'
            WHERE status = 'CONFIRMED' AND trigger_time < ?
        """, (conf_cutoff,))
        expired_conf = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        return {
            'expired_kiv': expired_kiv,
            'expired_confirmed': expired_conf
        }