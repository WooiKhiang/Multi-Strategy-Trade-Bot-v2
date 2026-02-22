"""
Two-stage data validator for Mark 3.1.
Stage A: Ultra-cheap (cache/snapshot)
Stage B: Full bar validation (candidates only)
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
from datetime import datetime

from core.utils.registry import ComponentRegistry

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Validates market data quality.
    
    Stage A: Quick checks on current price (no bars)
    Stage B: Full bar validation for candidates
    """
    
    def __init__(self):
        registry = ComponentRegistry()
        self.cache = registry.get('data', 'cache')()
        self.session = registry.get('data', 'session')()
    
    def stage_a_validate(self, ticker: str, 
                         price_data: Optional[Dict] = None) -> Tuple[bool, str, Dict]:
        """
        Ultra-cheap validation using current price only.
        
        Returns:
            (is_valid, reason, data)
        """
        # Get price data if not provided
        if price_data is None:
            price_data = self.cache.get(ticker, max_age_seconds=60)
        
        if not price_data:
            return False, "NO_PRICE_DATA", {}
        
        # Basic checks
        checks = []
        
        # Price must exist and be positive
        if price_data.get('price', 0) <= 0:
            checks.append("INVALID_PRICE")
        
        # Volume can be zero, but flag it
        if price_data.get('volume', 0) == 0:
            checks.append("ZERO_VOLUME")
        
        # Check timestamp freshness
        age = price_data.get('age_seconds', 0)
        if age > 300:  # 5 minutes
            checks.append(f"STALE_DATA_{age:.0f}s")
        
        # Spread check if available (for entries only)
        if 'bid' in price_data and 'ask' in price_data:
            bid = price_data['bid']
            ask = price_data['ask']
            if bid and ask and ask > bid:
                spread_pct = (ask - bid) / bid * 100
                price_data['spread_pct'] = spread_pct
                
                # Context-aware spread threshold
                price = price_data['price']
                if price < 20:
                    threshold = 3.0
                elif price < 50:
                    threshold = 2.0
                else:
                    threshold = 1.0
                
                if spread_pct > threshold:
                    checks.append(f"WIDE_SPREAD_{spread_pct:.1f}%")
        
        is_valid = len(checks) == 0 or checks == ["ZERO_VOLUME"]  # Zero volume is warning, not fatal
        
        return is_valid, ",".join(checks) if checks else "OK", price_data
    
    def stage_b_validate(self, ticker: str, bars_df: pd.DataFrame,
                         min_bars: int = 20) -> Tuple[bool, str, Dict]:
        """
        Full bar validation for candidates.
        
        Checks:
        - Sufficient bars
        - No NaN values
        - No gaps during trading hours
        """
        issues = []
        
        # Check bar count
        if len(bars_df) < min_bars:
            issues.append(f"INSUFFICIENT_BARS:{len(bars_df)}<{min_bars}")
        
        # Check for NaN values
        if bars_df['close'].isna().any():
            issues.append("NAN_CLOSE")
        
        if bars_df['volume'].isna().any():
            issues.append("NAN_VOLUME")
        
        # Check for gaps during trading hours
        if len(bars_df) > 1:
            time_diffs = bars_df.index.to_series().diff()
            expected_diff = pd.Timedelta(minutes=5)
            
            # Find gaps larger than 1.5x expected
            gaps = time_diffs[time_diffs > expected_diff * 1.5]
            
            if not gaps.empty:
                # Filter gaps that occurred during trading hours
                trading_gaps = []
                for gap_time in gaps.index:
                    if self.session.is_market_open(gap_time):
                        trading_gaps.append(gap_time)
                
                if trading_gaps:
                    issues.append(f"GAPS:{len(trading_gaps)}")
        
        # Check for duplicate timestamps
        if bars_df.index.duplicated().any():
            issues.append("DUPLICATE_TIMESTAMPS")
        
        is_valid = len(issues) == 0
        
        return is_valid, ",".join(issues) if issues else "OK", {
            'bar_count': len(bars_df),
            'issues': issues
        }
    
    def should_skip_symbol(self, ticker: str) -> Tuple[bool, str]:
        """
        Quick check if symbol should be skipped this cycle.
        Checks ignore list via risk manager.
        """
        # This will be implemented when we build risk.ignore
        # For now, return False
        return False, ""
    
    def get_severity(self, validation_result: str) -> str:
        """Map validation result to severity level."""
        if validation_result == "OK":
            return "INFO"
        
        if any(x in validation_result for x in ["NAN", "INSUFFICIENT_BARS"]):
            return "CRITICAL"
        
        if any(x in validation_result for x in ["GAPS", "DUPLICATE"]):
            return "ERROR"
        
        return "WARNING"