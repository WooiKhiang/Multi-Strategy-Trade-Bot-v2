"""
Market sentinel for Mark 3.1.
Monitors system health and provides kill switch functionality.
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
import threading
import time

from core.utils.registry import ComponentRegistry
from core.utils.time_utils import now_utc

logger = logging.getLogger(__name__)

class Sentinel:
    """
    System health monitor and kill switch.
    
    Monitors:
    - API usage and rate limits
    - Data quality issues
    - Reconciliation status
    - Manual kill switch
    - Market crash conditions
    """
    
    def __init__(self, db_path: str = "data/trade_log.db"):
        self.db_path = Path(db_path)
        
        # Get dependencies
        registry = ComponentRegistry()
        self.regime = registry.get('market', 'regime')()
        self.reconciler = registry.get('execution', 'reconciler')()
        
        # Health thresholds
        self.max_api_calls_per_min = 180
        self.max_data_errors_per_day = 10
        self.max_consecutive_failures = 3
        
        # State tracking
        self.consecutive_failures = 0
        self.api_calls_this_minute = 0
        self.minute_start = now_utc()
        
        # Kill switch (can be set manually)
        self.kill_switch_engaged = False
        self.kill_reason = ""
        
        logger.info("Sentinel initialized")
    
    def check_health(self) -> Dict[str, Any]:
        """
        Comprehensive health check.
        
        Returns:
            Dict with health status and recommendations
        """
        checks = {
            'api_usage': self._check_api_usage(),
            'data_quality': self._check_data_quality(),
            'reconciliation': self._check_reconciliation(),
            'market_conditions': self._check_market_conditions(),
            'kill_switch': self._check_kill_switch(),
            'consecutive_failures': self.consecutive_failures
        }
        
        # Determine overall health state
        state, reason = self._determine_health_state(checks)
        
        # Log health state
        self._log_health_state(state, reason)
        
        return {
            'state': state,
            'reason': reason,
            'checks': checks,
            'timestamp': now_utc()
        }
    
    def _check_api_usage(self) -> Dict[str, Any]:
        """Check API usage against limits."""
        # Reset counter if minute has passed
        now = now_utc()
        if (now - self.minute_start).seconds > 60:
            self.api_calls_this_minute = 0
            self.minute_start = now
        
        usage_pct = (self.api_calls_this_minute / self.max_api_calls_per_min) * 100
        
        status = 'OK'
        if usage_pct > 90:
            status = 'CRITICAL'
        elif usage_pct > 75:
            status = 'WARNING'
        
        return {
            'calls': self.api_calls_this_minute,
            'limit': self.max_api_calls_per_min,
            'usage_pct': usage_pct,
            'status': status
        }
    
    def _check_data_quality(self) -> Dict[str, Any]:
        """Check data quality issues from error log."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Count errors today
        today = now_utc().date()
        cursor.execute("""
            SELECT COUNT(*) FROM error_log
            WHERE date(timestamp) = date(?)
            AND severity IN ('ERROR', 'CRITICAL')
        """, (today,))
        
        errors_today = cursor.fetchone()[0]
        
        # Get recent issues
        cursor.execute("""
            SELECT error, severity, timestamp
            FROM error_log
            ORDER BY timestamp DESC
            LIMIT 5
        """)
        
        recent = cursor.fetchall()
        conn.close()
        
        status = 'OK'
        if errors_today > self.max_data_errors_per_day:
            status = 'CRITICAL'
        elif errors_today > self.max_data_errors_per_day * 0.7:
            status = 'WARNING'
        
        return {
            'errors_today': errors_today,
            'limit': self.max_data_errors_per_day,
            'recent_issues': [
                {'error': r[0], 'severity': r[1], 'time': r[2]}
                for r in recent
            ],
            'status': status
        }
    
    def _check_reconciliation(self) -> Dict[str, Any]:
        """Quick reconciliation check."""
        try:
            ok, message = self.reconciler.quick_check()
            
            if ok:
                return {
                    'status': 'OK',
                    'message': message
                }
            else:
                self.consecutive_failures += 1
                return {
                    'status': 'CRITICAL',
                    'message': message,
                    'consecutive': self.consecutive_failures
                }
                
        except Exception as e:
            self.consecutive_failures += 1
            return {
                'status': 'ERROR',
                'message': str(e),
                'consecutive': self.consecutive_failures
            }
    
    def _check_market_conditions(self) -> Dict[str, Any]:
        """Check market conditions for crash detection."""
        regime_data = self.regime.detect_regime()
        
        status = 'OK'
        if regime_data['regime'] == 'CRASH':
            status = 'CRITICAL'
        elif regime_data['regime'] == 'BEAR':
            status = 'WARNING'
        
        return {
            'regime': regime_data['regime'],
            'score': regime_data['score'],
            'multiplier': regime_data['multiplier'],
            'status': status
        }
    
    def _check_kill_switch(self) -> Dict[str, Any]:
        """Check if kill switch is engaged."""
        if self.kill_switch_engaged:
            return {
                'status': 'CRITICAL',
                'engaged': True,
                'reason': self.kill_reason
            }
        
        return {
            'status': 'OK',
            'engaged': False
        }
    
    def _determine_health_state(self, checks: Dict) -> tuple:
        """
        Determine overall health state from checks.
        
        Returns:
            (state, reason)
        """
        # Check for any CRITICAL
        critical = []
        for name, check in checks.items():
            if isinstance(check, dict) and check.get('status') == 'CRITICAL':
                critical.append(name)
        
        if critical:
            return 'RED', f"Critical issues: {', '.join(critical)}"
        
        # Check for consecutive failures
        if checks['consecutive_failures'] >= self.max_consecutive_failures:
            return 'RED', f"{checks['consecutive_failures']} consecutive failures"
        
        # Check for WARNING
        warnings = []
        for name, check in checks.items():
            if isinstance(check, dict) and check.get('status') == 'WARNING':
                warnings.append(name)
        
        if warnings:
            return 'YELLOW', f"Warnings: {', '.join(warnings)}"
        
        # Also YELLOW if near limits but not critical
        api = checks['api_usage']
        if api['usage_pct'] > 70:
            return 'YELLOW', f"High API usage: {api['usage_pct']:.1f}%"
        
        data = checks['data_quality']
        if data['errors_today'] > self.max_data_errors_per_day * 0.5:
            return 'YELLOW', f"Elevated errors: {data['errors_today']}"
        
        return 'GREEN', "All systems nominal"
    
    def _log_health_state(self, state: str, reason: str) -> None:
        """Log health state to database."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO health_state
            (timestamp, state, reason)
            VALUES (?, ?, ?)
        """, (now_utc(), state, reason))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Health state: {state} - {reason}")
    
    def _get_connection(self):
        return sqlite3.connect(str(self.db_path))
    
    def record_api_call(self, count: int = 1) -> None:
        """Record API calls for rate limiting."""
        self.api_calls_this_minute += count
    
    def engage_kill_switch(self, reason: str = "Manual override") -> None:
        """Manually engage kill switch."""
        self.kill_switch_engaged = True
        self.kill_reason = reason
        logger.critical(f"KILL SWITCH ENGAGED: {reason}")
    
    def release_kill_switch(self) -> None:
        """Manually release kill switch."""
        self.kill_switch_engaged = False
        self.kill_reason = ""
        logger.info("Kill switch released")
    
    def should_trade(self) -> tuple:
        """
        Determine if trading should proceed.
        
        Returns:
            (should_trade, reason)
        """
        health = self.check_health()
        
        # Never trade if kill switch engaged
        if self.kill_switch_engaged:
            return False, f"Kill switch: {self.kill_reason}"
        
        # RED state = no trading
        if health['state'] == 'RED':
            return False, f"System RED: {health['reason']}"
        
        # YELLOW state = limited trading (handled by caller)
        if health['state'] == 'YELLOW':
            return True, f"Limited trading: {health['reason']}"
        
        # GREEN state = full trading
        return True, "Normal operations"
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get human-readable status summary."""
        health = self.check_health()
        
        return {
            'state': health['state'],
            'reason': health['reason'],
            'api_usage': f"{health['checks']['api_usage']['calls']}/{health['checks']['api_usage']['limit']}",
            'errors_today': health['checks']['data_quality']['errors_today'],
            'market_regime': health['checks']['market_conditions']['regime'],
            'kill_switch': self.kill_switch_engaged,
            'consecutive_failures': health['checks']['consecutive_failures'],
            'timestamp': health['timestamp']
        }