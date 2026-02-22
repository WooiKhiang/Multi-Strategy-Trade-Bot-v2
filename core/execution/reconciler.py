"""
Position reconciler for Mark 3.1.
Ensures local SQLite matches Alpaca's records.
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from core.utils.registry import ComponentRegistry
from core.utils.time_utils import now_utc

logger = logging.getLogger(__name__)

class Reconciler:
    """
    Reconcilies local positions with Alpaca.
    
    Reconciliation ladder:
    1. Detect mismatches
    2. Auto-reconcile safe differences
    3. Escalate to RED state if critical
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        
        # Get dependencies
        registry = ComponentRegistry()
        self.executor = registry.get('execution', 'executor')()
        
        # Tolerance thresholds
        self.price_tolerance_pct = 2.0  # 2% price difference allowed
        self.quantity_tolerance = 0      # Must match exactly
        
        logger.info("Reconciler initialized")
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def reconcile_all(self) -> Dict[str, Any]:
        """
        Run full reconciliation between local DB and Alpaca.
        
        Returns:
            Dict with reconciliation results and recommended action
        """
        logger.info("Starting reconciliation with Alpaca")
        
        # Get positions from both sources
        local_positions = self._get_local_positions()
        alpaca_positions = self._get_alpaca_positions()
        
        results = {
            'matched': [],
            'mismatch_price': [],
            'mismatch_quantity': [],
            'missing_in_alpaca': [],
            'missing_in_local': [],
            'timestamp': now_utc()
        }
        
        # Check all local positions against Alpaca
        for local in local_positions:
            ticker = local['ticker']
            alpaca = alpaca_positions.get(ticker)
            
            if not alpaca:
                # Position in local but not in Alpaca
                results['missing_in_alpaca'].append(local)
                continue
            
            # Compare quantities
            if abs(local['quantity'] - alpaca['quantity']) > self.quantity_tolerance:
                results['mismatch_quantity'].append({
                    'ticker': ticker,
                    'local_qty': local['quantity'],
                    'alpaca_qty': alpaca['quantity'],
                    'local_id': local['ticket_id']
                })
                continue
            
            # Compare prices (within tolerance)
            price_diff_pct = abs(local['entry_price'] - alpaca['avg_entry_price']) / local['entry_price'] * 100
            if price_diff_pct > self.price_tolerance_pct:
                results['mismatch_price'].append({
                    'ticker': ticker,
                    'local_price': local['entry_price'],
                    'alpaca_price': alpaca['avg_entry_price'],
                    'diff_pct': price_diff_pct,
                    'local_id': local['ticket_id']
                })
            else:
                # Perfect match or price within tolerance
                results['matched'].append(ticker)
        
        # Check for positions in Alpaca not in local
        for ticker, alpaca in alpaca_positions.items():
            if not any(l['ticker'] == ticker for l in local_positions):
                results['missing_in_local'].append({
                    'ticker': ticker,
                    'alpaca_qty': alpaca['quantity'],
                    'alpaca_price': alpaca['avg_entry_price']
                })
        
        # Determine overall status
        status, message = self._determine_status(results)
        
        # Auto-reconcile safe differences
        if status in ['WARNING', 'DEGRADED']:
            self._auto_reconcile(results)
        
        # Log results
        self._log_reconciliation(results, status, message)
        
        return {
            'status': status,
            'message': message,
            'results': results
        }
    
    def _get_local_positions(self) -> List[Dict]:
        """Get all open positions from local database."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ticket_id, ticker, entry_price, quantity, strategy
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
                'strategy': row[4]
            })
        
        return positions
    
    def _get_alpaca_positions(self) -> Dict[str, Dict]:
        """Get all positions from Alpaca."""
        try:
            positions = self.executor.client.get_all_positions()
            
            result = {}
            for pos in positions:
                result[pos.symbol] = {
                    'quantity': float(pos.qty),
                    'avg_entry_price': float(pos.avg_entry_price),
                    'current_price': float(pos.current_price),
                    'unrealized_pl': float(pos.unrealized_pl)
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get Alpaca positions: {e}")
            return {}
    
    def _determine_status(self, results: Dict) -> Tuple[str, str]:
        """
        Determine system status based on reconciliation results.
        
        Returns:
            (status, message)
        """
        if results['mismatch_quantity']:
            return 'CRITICAL', f"Quantity mismatch for {len(results['mismatch_quantity'])} symbols"
        
        if results['missing_in_alpaca']:
            return 'CRITICAL', f"Positions in local but not in Alpaca: {len(results['missing_in_alpaca'])}"
        
        if results['missing_in_local']:
            return 'WARNING', f"Positions in Alpaca but not in local: {len(results['missing_in_local'])}"
        
        if results['mismatch_price']:
            return 'DEGRADED', f"Price mismatch for {len(results['mismatch_price'])} symbols (within tolerance)"
        
        return 'OK', f"All positions reconciled: {len(results['matched'])} matches"
    
    def _auto_reconcile(self, results: Dict) -> None:
        """
        Automatically reconcile safe differences.
        
        Safe to auto-reconcile:
        - Price mismatches within tolerance (update local)
        - Positions in Alpaca but not local (add to local)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Fix price mismatches (update local prices)
        for item in results['mismatch_price']:
            cursor.execute("""
                UPDATE positions
                SET entry_price = ?
                WHERE ticket_id = ?
            """, (item['alpaca_price'], item['local_id']))
            
            logger.info(f"Auto-reconciled price for {item['ticker']}: ${item['alpaca_price']:.2f}")
        
        # Add missing positions from Alpaca
        for item in results['missing_in_local']:
            # Generate a ticket ID for the reconciled position
            ticket_id = f"RCL-{item['ticker']}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            
            cursor.execute("""
                INSERT INTO positions
                (ticket_id, ticker, entry_price, quantity, status, entry_time, strategy)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ticket_id, item['ticker'], item['alpaca_price'],
                  item['alpaca_qty'], 'OPEN', datetime.utcnow(), 'RECONCILED'))
            
            logger.info(f"Added missing position from Alpaca: {item['ticker']}")
        
        conn.commit()
        conn.close()
        
        # Note: Quantity mismatches are NOT auto-reconciled
        # Those require manual intervention (CRITICAL state)
    
    def _log_reconciliation(self, results: Dict, status: str, message: str) -> None:
        """Log reconciliation results to database."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO health_state
            (timestamp, state, reason)
            VALUES (?, ?, ?)
        """, (datetime.utcnow(), status, message))
        
        conn.commit()
        conn.close()
        
        # Log summary
        logger.info(f"Reconciliation complete: {status} - {message}")
        logger.info(f"  Matched: {len(results['matched'])}")
        logger.info(f"  Price mismatches: {len(results['mismatch_price'])}")
        logger.info(f"  Quantity mismatches: {len(results['mismatch_quantity'])}")
        logger.info(f"  Missing in Alpaca: {len(results['missing_in_alpaca'])}")
        logger.info(f"  Missing in local: {len(results['missing_in_local'])}")
    
    def quick_check(self) -> Tuple[bool, str]:
        """
        Quick reconciliation check for cycle start.
        Returns (is_ok, message)
        """
        local = len(self._get_local_positions())
        alpaca = len(self._get_alpaca_positions())
        
        if local != alpaca:
            return False, f"Position count mismatch: local={local}, alpaca={alpaca}"
        
        return True, "OK"