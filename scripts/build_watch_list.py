#!/usr/bin/env python3
"""
Active Watch List Builder (Tier 2)
Runs daily to identify symbols with unusual activity from master universe.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data.fetcher import DataFetcher
from core.utils.sheets import SheetsInterface
from core.watch_list import WatchListManager
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WatchListBuilder:
    
    def __init__(self):
        self.fetcher = DataFetcher()
        self.sheets = SheetsInterface()
        self.watch_manager = WatchListManager()
        
        self.volume_spike_threshold = 1.5
        self.breakout_threshold = 0.02
        self.momentum_threshold = 0.01
        
    def load_master_universe(self) -> List[str]:
        data = self.sheets.read_config("MASTER_UNIVERSE", "A:G")
        if not data or len(data) < 2:
            logger.error("No data in MASTER_UNIVERSE tab")
            return []
        
        symbols = []
        for row in data[1:]:
            if len(row) >= 1:
                symbols.append(row[0])
        logger.info(f"Loaded {len(symbols)} symbols from master universe")
        return symbols
    
    def detect_unusual_activity(self, symbol: str) -> Dict[str, Any]:
        try:
            bars = self.fetcher.get_bars(symbol, period=25, timeframe='5Min')
            if bars is None or len(bars) < 20:
                return None
            
            recent = bars.tail(20)
            current = recent.iloc[-1]
            prev = recent.iloc[-2]
            
            avg_volume = recent['volume'].mean()
            volume_ratio = current['volume'] / avg_volume if avg_volume > 0 else 1
            
            recent_high = recent['high'].max()
            recent_low = recent['low'].min()
            price_range = recent_high - recent_low
            current_price = current['close']
            
            score = 0
            signals = []
            
            if volume_ratio > self.volume_spike_threshold:
                score += min(50, volume_ratio * 20)
                signals.append(f"volume_{volume_ratio:.1f}x")
            
            if price_range > 0:
                breakout_pct = (current_price - recent_high) / recent_high
                if breakout_pct > self.breakout_threshold:
                    score += 30
                    signals.append(f"breakout_{breakout_pct:.1%}")
            
            price_change = (current_price - prev['close']) / prev['close']
            if abs(price_change) > self.momentum_threshold:
                score += 20
                signals.append(f"momentum_{price_change:.1%}")
            
            rel_volume = current['volume'] / prev['volume'] if prev['volume'] > 0 else 1
            if rel_volume > 2:
                score += 10
                signals.append(f"rel_vol_{rel_volume:.1f}x")
            
            if score > 0:
                return {
                    'symbol': symbol,
                    'score': score,
                    'signals': signals,
                    'volume_ratio': volume_ratio,
                    'current_price': current_price,
                    'avg_volume': avg_volume
                }
            return None
        except Exception as e:
            logger.debug(f"Error scanning {symbol}: {e}")
            return None
    
    def scan_master_universe(self, symbols: List[str]) -> List[Dict]:
        active_symbols = []
        total = len(symbols)
        logger.info(f"Scanning {total} symbols for unusual activity...")
        
        for i, symbol in enumerate(symbols):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{total}")
            
            activity = self.detect_unusual_activity(symbol)
            if activity:
                active_symbols.append(activity)
                self.watch_manager.add_or_update(
                    symbol, 
                    activity['score'],
                    sector='Unknown'
                )
        
        active_symbols.sort(key=lambda x: x['score'], reverse=True)
        logger.info(f"Found {len(active_symbols)} symbols with unusual activity")
        return active_symbols
    
    def update_watch_list_sheet(self, active_symbols: List[Dict]):
        headers = [[
            'Ticker', 'First_Spotted', 'Last_Active', 
            'Spike_Count', 'Avg_Score', 'Status'
        ]]
        
        watch_list = self.watch_manager.get_active_watch_list(max_age_hours=72)
        rows = []
        for item in watch_list[:200]:
            rows.append([
                item['ticker'],
                item['first_spotted'][:10] if item['first_spotted'] else '',
                item['last_active'][:10] if item['last_active'] else '',
                str(item['spike_count']),
                f"{item['avg_score']:.1f}",
                item.get('status', 'WATCHING')
            ])
        
        self.sheets.clear_range("WATCH_LIST", "A2:Z")
        self.sheets.write_data("WATCH_LIST", headers + rows, "A1")
        logger.info(f"Updated WATCH_LIST tab with {len(rows)} symbols")
    
    def build(self):
        logger.info("=" * 50)
        logger.info("Building Tier 2: Active Watch List...")
        
        symbols = self.load_master_universe()
        if not symbols:
            logger.error("No master universe available")
            return
        
        active = self.scan_master_universe(symbols)
        self.update_watch_list_sheet(active)
        
        logger.info(f"Watch list build complete: {len(active)} active symbols")
        logger.info("=" * 50)

if __name__ == "__main__":
    builder = WatchListBuilder()
    builder.build()