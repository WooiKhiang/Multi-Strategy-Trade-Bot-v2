"""
Exit monitor for Mark 3.1.
Monitors open positions and triggers exits based on conditions.
"""

import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import sqlite3
from pathlib import Path

from core.utils.registry import ComponentRegistry
from core.utils.time_utils import minutes_until_market_close, is_market_hours

logger = logging.getLogger(__name__)

class ExitMonitor:
    """
    Monitors open positions and triggers exits.
    
    Exit conditions:
    - Stop loss (real-time price check)
    - Take profit (price target)
    - Strategy exit (bar completion)
    - Pre-close forced exit (avoid overnight)
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        
        # Get dependencies
        registry = ComponentRegistry()
        self.executor = registry.get('execution', 'executor')()
        self.session = registry.get('data', 'session')()
        
        # Exit thresholds
        self.pre_close_warning_minutes = 15
        self.force_close_minutes = 5
        
        logger.info("ExitMonitor initialized")
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def check_stop_losses(self) -> List[Dict]:
        """
        Check all open positions for stop loss hits.
        Uses current price (real-time check).
        """
        triggered = []
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Get all open positions
        cursor.execute("""
            SELECT ticket_id, ticker, entry_price, quantity, stop_loss, strategy
            FROM positions
            WHERE status = 'OPEN'
        """)
        
        positions = cursor.fetchall()
        conn.close()
        
        for pos in positions:
            ticket_id, ticker, entry_price, quantity, stop_loss, strategy = pos
            
            # Get current price from cache/fetcher
            current_price = self._get_current_price(ticker)
            
            if not current_price:
                logger.warning(f"Cannot get price for {ticker} stop check")
                continue
            
            # Calculate loss percentage
            loss_pct = (current_price - entry_price) / entry_price
            
            # Check if stop loss hit (long only)
            if loss_pct <= -stop_loss:
                logger.info(f"STOP LOSS HIT: {ticker} at ${current_price:.2f} ({loss_pct:.2%})")
                
                # Execute market exit
                exit_result = self.executor.execute_exit(
                    ticker=ticker,
                    quantity=quantity,
                    order_type='MARKET',
                    reason='STOP_LOSS'
                )
                
                if exit_result['status'] == 'FILLED':
                    # Update position in database
                    self._mark_position_closed(
                        ticket_id=ticket_id,
                        exit_price=exit_result['fill_price'],
                        exit_reason='STOP_LOSS'
                    )
                    
                    triggered.append({
                        'ticket_id': ticket_id,
                        'ticker': ticker,
                        'exit_price': exit_result['fill_price'],
                        'reason': 'STOP_LOSS',
                        'pnl_pct': loss_pct
                    })
        
        return triggered
    
    def check_strategy_exits(self) -> List[Dict]:
        """
        Check positions against strategy exit conditions.
        Uses completed bars only (no intra-bar exits).
        """
        # This will be called by main.py after bar completion
        # Actual strategy logic lives in strategy files
        # Here we just execute exits that strategies have signaled
        
        triggered = []
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Get positions marked for exit by strategies
        cursor.execute("""
            SELECT ticket_id, ticker, quantity, exit_signal
            FROM positions
            WHERE status = 'CLOSING'
        """)
        
        closing = cursor.fetchall()
        
        for pos in closing:
            ticket_id, ticker, quantity, exit_signal = pos
            
            # Execute exit
            exit_result = self.executor.execute_exit(
                ticker=ticker,
                quantity=quantity,
                order_type='LIMIT',  # Try limit first for strategy exits
                reason=exit_signal or 'STRATEGY'
            )
            
            if exit_result['status'] == 'FILLED':
                self._mark_position_closed(
                    ticket_id=ticket_id,
                    exit_price=exit_result['fill_price'],
                    exit_reason=exit_signal or 'STRATEGY'
                )
                
                triggered.append({
                    'ticket_id': ticket_id,
                    'ticker': ticker,
                    'exit_price': exit_result['fill_price'],
                    'reason': exit_signal or 'STRATEGY'
                })
            elif exit_result['status'] == 'PENDING':
                logger.info(f"Exit order pending for {ticker}")
        
        conn.close()
        return triggered
    
    def check_pre_close(self) -> List[Dict]:
        """
        Check if we're near market close and force exits if needed.
        """
        now = datetime.utcnow()
        mins_to_close = minutes_until_market_close(now, self.session.calendar)
        
        forced = []
        
        if mins_to_close <= 0:
            # Market already closed
            return forced
        
        if mins_to_close <= self.force_close_minutes:
            # FORCE EXIT: market sell all positions
            logger.warning(f"FORCE CLOSE: {mins_to_close:.1f} minutes to close")
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT ticket_id, ticker, quantity
                FROM positions
                WHERE status = 'OPEN'
            """)
            
            positions = cursor.fetchall()
            conn.close()
            
            for pos in positions:
                ticket_id, ticker, quantity = pos
                
                exit_result = self.executor.execute_exit(
                    ticker=ticker,
                    quantity=quantity,
                    order_type='MARKET',
                    reason='FORCE_CLOSE'
                )
                
                if exit_result['status'] == 'FILLED':
                    self._mark_position_closed(
                        ticket_id=ticket_id,
                        exit_price=exit_result['fill_price'],
                        exit_reason='FORCE_CLOSE'
                    )
                    
                    forced.append({
                        'ticket_id': ticket_id,
                        'ticker': ticker,
                        'exit_price': exit_result['fill_price'],
                        'reason': 'FORCE_CLOSE'
                    })
        
        elif mins_to_close <= self.pre_close_warning_minutes:
            # WARNING: tighten exits, no new entries
            logger.info(f"Pre-close warning: {mins_to_close:.1f} minutes remaining")
            # Signal to main.py to disable new entries
        
        return forced
    
    def signal_exit(self, ticket_id: str, ticker: str, 
                   exit_signal: str) -> bool:
        """
        Mark a position for exit (called by strategies).
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE positions
            SET status = 'CLOSING', exit_signal = ?
            WHERE ticket_id = ? AND status = 'OPEN'
        """, (exit_signal, ticket_id))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected:
            logger.info(f"Position {ticker} marked for exit: {exit_signal}")
        
        return affected > 0
    
    def _get_current_price(self, ticker: str) -> Optional[float]:
        """Get current price from cache/fetcher."""
        try:
            # Try cache first
            cached = self.executor.slippage._get_cached_price(ticker)
            if cached:
                return cached
            
            # Fall back to snapshot
            price_data = self.executor._get_current_price(ticker)
            return price_data.get('price') if price_data else None
            
        except Exception as e:
            logger.error(f"Error getting price for {ticker}: {e}")
            return None
    
    def _mark_position_closed(self, ticket_id: str, exit_price: float,
                              exit_reason: str) -> bool:
        """Mark position as closed and move to history."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Get position details
        cursor.execute("""
            SELECT ticker, strategy, entry_price, quantity, entry_time
            FROM positions
            WHERE ticket_id = ?
        """, (ticket_id,))
        
        pos = cursor.fetchone()
        if not pos:
            conn.close()
            return False
        
        ticker, strategy, entry_price, quantity, entry_time = pos
        
        # Calculate P&L
        pnl_dollar = (exit_price - entry_price) * quantity
        pnl_percent = (exit_price - entry_price) / entry_price
        win_loss = 'WIN' if pnl_dollar > 0 else 'LOSS'
        
        # Insert into trade_history
        cursor.execute("""
            INSERT INTO trade_history
            (exit_time, ticker, strategy, entry_price, exit_price,
             quantity, pnl_percent, win_loss, exit_reason, ticket_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow(), ticker, strategy, entry_price,
              exit_price, quantity, pnl_percent, win_loss,
              exit_reason, ticket_id))
        
        # Delete from positions (or mark closed)
        cursor.execute("""
            DELETE FROM positions WHERE ticket_id = ?
        """, (ticket_id,))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Position {ticker} closed: {pnl_percent:.2%} ({exit_reason})")
        return True
    
    def get_open_positions(self) -> List[Dict]:
        """Get all open positions."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ticket_id, ticker, entry_price, quantity, strategy,
                   stop_loss, current_price
            FROM positions
            WHERE status = 'OPEN'
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        positions = []
        for row in rows:
            positions.append({
                'ticket_id': row[0],
                'ticker': row[1],
                'entry_price': row[2],
                'quantity': row[3],
                'strategy': row[4],
                'stop_loss': row[5],
                'current_price': row[6] or row[2]  # Use entry if no current
            })
        
        return positions