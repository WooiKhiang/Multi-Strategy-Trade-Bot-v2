#!/usr/bin/env python3
"""
Main orchestrator for Mark 3.1.
Runs every 5 minutes during market hours.
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.utils.lock import CrossPlatformLock
from core.utils.time_utils import is_market_hours, minutes_until_market_close
from core.utils.registry import ComponentRegistry
from config.settings import settings

# Set up logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradingBot:
    """
    Main trading bot orchestrator.
    Runs 5-minute cycles during market hours.
    """
    
    def __init__(self):
        self.registry = ComponentRegistry()
        self.lock = CrossPlatformLock()
        
        # Initialize components
        self._init_components()
        
        logger.info("Trading bot initialized")
    
    def _init_components(self):
        """Initialize all components."""
        # Data
        self.cache = self.registry.get('data', 'cache')()
        self.session = self.registry.get('data', 'session')()
        self.fetcher = self.registry.get('data', 'fetcher')()
        self.validator = self.registry.get('data', 'validator')()
        
        # Risk
        self.ignore = self.registry.get('risk', 'ignore')()
        self.limits = self.registry.get('risk', 'limits')()
        self.sizer = self.registry.get('risk', 'sizer')()
        self.risk_manager = self.registry.get('risk', 'manager')()
        
        # Signal
        self.cooldown = self.registry.get('signal', 'cooldown')()
        self.confidence = self.registry.get('signal', 'confidence')()
        self.processor = self.registry.get('signal', 'processor')()
        
        # Execution
        self.slippage = self.registry.get('execution', 'slippage')()
        self.executor = self.registry.get('execution', 'executor')()
        self.monitor = self.registry.get('execution', 'monitor')()
        self.reconciler = self.registry.get('execution', 'reconciler')()
        
        # Market
        self.regime = self.registry.get('market', 'regime')()
        self.breadth = self.registry.get('market', 'breadth')()
        self.sentinel = self.registry.get('market', 'sentinel')()
    
    def run_cycle(self):
        """
        Execute one complete trading cycle.
        Follows priority pyramid from spec.
        """
        cycle_start = datetime.utcnow()
        logger.info("=" * 50)
        logger.info(f"Starting cycle at {cycle_start}")
        
        # Check if we should trade
        should_trade, trade_reason = self.sentinel.should_trade()
        if not should_trade:
            logger.warning(f"Trading halted: {trade_reason}")
            return
        
        # Get health state
        health = self.sentinel.check_health()
        logger.info(f"Health state: {health['state']} - {health['reason']}")
        
        # PRIORITY 1: EXITS (always run)
        logger.info("PRIORITY 1: Checking exits")
        self._check_exits()
        
        # PRIORITY 2: RECONCILIATION (always run)
        logger.info("PRIORITY 2: Reconciliation")
        recon_result = self.reconciler.reconcile_all()
        if recon_result['status'] in ['CRITICAL', 'RED']:
            logger.error(f"Reconciliation failed: {recon_result['message']}")
            return
        
        # If health is RED, stop here
        if health['state'] == 'RED':
            logger.warning("System RED - stopping after exits/reconciliation")
            return
        
        # PRIORITY 3: TIER 1 SCAN (if budget allows)
        if health['state'] in ['GREEN', 'YELLOW']:
            logger.info("PRIORITY 3: Scanning TIER 1 symbols")
            self._scan_tier1()
        
        # PRIORITY 4: TIER 2 SCAN (if health allows)
        if health['state'] == 'GREEN':
            logger.info("PRIORITY 4: Scanning TIER 2 symbols")
            self._scan_tier2()
        
        # PRIORITY 5: NEW ENTRIES
        if health['state'] in ['GREEN', 'YELLOW']:
            logger.info("PRIORITY 5: Processing new entries")
            self._process_entries(health['state'])
        
        # Check pre-close window
        mins_to_close = minutes_until_market_close()
        if mins_to_close < 15:
            logger.info(f"Pre-close window: {mins_to_close:.1f} minutes")
        
        cycle_end = datetime.utcnow()
        duration = (cycle_end - cycle_start).total_seconds()
        logger.info(f"Cycle completed in {duration:.2f} seconds")
        logger.info("=" * 50)
    
    def _check_exits(self):
        """Check all exit conditions."""
        # Stop losses (real-time)
        stop_losses = self.monitor.check_stop_losses()
        for exit in stop_losses:
            logger.info(f"Stop loss executed: {exit}")
        
        # Strategy exits (bar completion)
        strategy_exits = self.monitor.check_strategy_exits()
        for exit in strategy_exits:
            logger.info(f"Strategy exit executed: {exit}")
        
        # Pre-close forced exits
        forced = self.monitor.check_pre_close()
        for exit in forced:
            logger.info(f"Force close executed: {exit}")
    
    def _scan_tier1(self):
        """Scan TIER 1 symbols (high priority)."""
        # TODO: Load TIER 1 symbols from universe
        # For now, use a small test list
        tier1_symbols = ['SPY', 'QQQ', 'AAPL', 'MSFT', 'GOOGL']
        
        for symbol in tier1_symbols:
            # Check ignore list
            ignored, info = self.ignore.is_ignored(symbol)
            if ignored:
                logger.debug(f"Skipping {symbol}: {info}")
                continue
            
            # Stage A validation
            price_data = self.fetcher.get_current_price(symbol)
            valid, reason, data = self.validator.stage_a_validate(symbol, price_data)
            
            if not valid:
                logger.debug(f"{symbol} failed Stage A: {reason}")
                # Add to ignore if persistent
                if 'STALE' in reason or 'INVALID' in reason:
                    self.ignore.add(symbol, reason, scope='ALL')
                continue
            
            # For now, just log
            logger.info(f"TIER 1 scan: {symbol} passed Stage A")
    
    def _scan_tier2(self):
        """Scan TIER 2 symbols (lower priority)."""
        # TODO: Load TIER 2 symbols from universe
        # For now, use a small test list
        tier2_symbols = ['NVDA', 'AMD', 'TSLA', 'META', 'NFLX']
        
        for symbol in tier2_symbols:
            # Quick check only
            price_data = self.fetcher.get_current_price(symbol)
            if price_data:
                logger.debug(f"TIER 2 update: {symbol} @ ${price_data['price']:.2f}")
    
    def _process_entries(self, health_state: str):
        """Process new entries from CONFIRMED signals."""
        # Get confirmed signals with min confidence
        min_confidence = 60 if health_state == 'GREEN' else 70
        signals = self.processor.get_confirmed_signals(min_confidence=min_confidence)
        
        if not signals:
            logger.debug("No confirmed signals")
            return
        
        logger.info(f"Found {len(signals)} confirmed signals")
        
        # Sort by confidence
        signals.sort(key=lambda x: x['confidence'], reverse=True)
        
        # Take top N based on health state
        max_entries = 3 if health_state == 'GREEN' else 1
        candidates = signals[:max_entries]
        
        for signal in candidates:
            # Check if we can trade this symbol
            can_trade, reason = self.risk_manager.can_trade_symbol(
                signal['ticker'], signal['strategy']
            )
            
            if not can_trade:
                logger.info(f"Rejecting {signal['ticker']}: {reason}")
                self.processor.reject_signal(signal['signal_id'], reason)
                continue
            
            # Approve trade
            approval = self.risk_manager.approve_trade(
                ticker=signal['ticker'],
                price=signal['go_in_price'],
                confidence=signal['confidence'],
                strategy=signal['strategy']
            )
            
            if approval['approved']:
                # Execute entry
                result = self.executor.execute_entry(
                    ticker=signal['ticker'],
                    strategy=signal['strategy'],
                    price=signal['go_in_price'],
                    quantity=approval['shares']
                )
                
                if result['status'] == 'FILLED':
                    # Mark signal as executed
                    self.processor.mark_executed(signal['signal_id'], result['ticket_id'])
                    logger.info(f"Entry executed: {signal['ticker']} {approval['shares']} shares @ ${result['fill_price']:.2f}")
                else:
                    logger.warning(f"Entry failed for {signal['ticker']}: {result.get('error', 'Unknown')}")
            else:
                logger.info(f"Trade not approved: {approval['reason']}")

def main():
    """Main entry point."""
    bot = TradingBot()
    
    # Check market hours
    if not is_market_hours():
        logger.info("Outside market hours - exiting")
        return
    
    # Acquire lock
    if not bot.lock.acquire(timeout=30):
        logger.error("Could not acquire lock - another instance may be running")
        return
    
    try:
        # Run one cycle
        bot.run_cycle()
    finally:
        bot.lock.release()

if __name__ == "__main__":
    main()