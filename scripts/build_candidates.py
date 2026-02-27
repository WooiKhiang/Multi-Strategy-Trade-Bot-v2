#!/usr/bin/env python3
"""
Today's Candidates Builder (Tier 3)
Runs every 5 minutes to select top candidates from watch list.
Updates UNIVERSE tab with today's ACTIVE symbols.
Manages KIV signals and confirmations.
"""

import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data.fetcher import DataFetcher
from core.utils.sheets import SheetsInterface
from core.watch_list import WatchListManager
from core.kiv_manager import KIVManager
from core.risk.ignore import IgnoreManager
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CandidateBuilder:
    
    def __init__(self):
        self.fetcher = DataFetcher()
        self.sheets = SheetsInterface()
        self.watch_manager = WatchListManager()
        self.kiv_manager = KIVManager()
        self.ignore = IgnoreManager()
        
        self.max_candidates = 50
        self.min_price = 5
        self.max_price = 100
        
    def load_master_universe_details(self) -> Dict[str, Dict]:
        data = self.sheets.read_config("MASTER_UNIVERSE", "A:G")
        if not data or len(data) < 2:
            return {}
        
        details = {}
        for row in data[1:]:
            if len(row) >= 5:
                try:
                    details[row[0]] = {
                        'sector': row[4],
                        'price': float(row[1]) if row[1] else 0
                    }
                except (ValueError, IndexError):
                    continue
        return details
    
    def select_candidates(self) -> List[Dict]:
        candidates = self.watch_manager.get_top_candidates(limit=200)
        if not candidates:
            logger.info("No candidates in watch list")
            return []
        
        logger.info(f"Evaluating {len(candidates)} top candidates")
        qualified = []
        master_details = self.load_master_universe_details()
        
        for candidate in candidates:
            ticker = candidate['ticker']
            
            ignored, _ = self.ignore.is_ignored(ticker)
            if ignored:
                continue
            
            price_data = self.fetcher.get_current_price(ticker)
            if not price_data:
                continue
            
            price = price_data.get('price', 0)
            
            if price < self.min_price or price > self.max_price:
                continue
            
            sector = master_details.get(ticker, {}).get('sector', 'Unknown')
            
            qualified.append({
                'ticker': ticker,
                'price': price,
                'score': candidate['avg_score'],
                'spike_count': candidate['spike_count'],
                'sector': sector,
                'first_spotted': candidate['first_spotted']
            })
        
        qualified.sort(key=lambda x: x['score'], reverse=True)
        top_candidates = qualified[:self.max_candidates]
        logger.info(f"Selected {len(top_candidates)} candidates for today")
        return top_candidates
    
    def check_and_add_to_kiv(self, candidates: List[Dict]):
        for candidate in candidates:
            ticker = candidate['ticker']
            
            bars = self.fetcher.get_bars(ticker, period=30, timeframe='5Min')
            if bars is None or len(bars) < 20:
                continue
            
            recent_low = bars['low'].tail(10).min()
            current_price = bars['close'].iloc[-1]
            confidence = min(100, candidate['score'] * 1.5)
            
            rebound_bottom = recent_low
            go_in_price = max(current_price, recent_low * 1.005)
            target_price = current_price * 1.025
            stop_loss = recent_low * 0.99
            
            self.kiv_manager.add_to_kiv(
                ticker=ticker,
                strategy='VOLUME_SPIKE',
                entry_price=current_price,
                rebound_bottom=rebound_bottom,
                go_in_price=go_in_price,
                target_price=target_price,
                stop_loss=stop_loss,
                confidence=confidence,
                notes=f"Score: {candidate['score']:.1f}, Spikes: {candidate['spike_count']}"
            )
    
    def check_kiv_confirmations(self):
        summary = self.kiv_manager.get_kiv_summary()
        if summary.get('KIV', 0) == 0:
            return
        
        # In production, fetch prices in batch
        current_prices = {}
        confirmed = self.kiv_manager.check_confirmations(current_prices)
        
        if confirmed:
            logger.info(f"✅ Found {len(confirmed)} newly confirmed signals")
    
    def _update_confirmed_in_sheets(self, confirmed_signals: List[Dict]):
        try:
            headers = [[
                'Signal_ID', 'Ticker', 'Strategy', 'Entry', 'Rebound',
                'Go_In', 'Target', 'Stop', 'Confidence', 'Added'
            ]]
            rows = []
            for sig in confirmed_signals:
                rows.append([
                    sig['signal_id'],
                    sig['ticker'],
                    sig['strategy'],
                    f"${sig['entry_price']:.2f}" if 'entry_price' in sig else '',
                    '',
                    f"${sig['go_in_price']:.2f}" if 'go_in_price' in sig else '',
                    f"${sig['target_price']:.2f}" if 'target_price' in sig else '',
                    f"${sig['stop_loss']:.2f}" if 'stop_loss' in sig else '',
                    f"{sig['confidence']:.1f}",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                ])
            
            self.sheets.clear_range("KIV", "A2:Z")
            self.sheets.write_data("KIV", headers + rows, "A1")
        except Exception as e:
            logger.error(f"Failed to update KIV sheet: {e}")
    
    def update_universe_tab(self, candidates: List[Dict]):
        now_utc = datetime.utcnow()
        today_str = now_utc.strftime("%Y-%m-%d")
        now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")
        
        headers = [[
            'Ticker', 'Price', 'Volume_20min_avg', 'Volume_Current',
            'Volatility_20min', 'Tier', 'Status', 'Added_Date',
            'Last_Active', 'Active_Days', 'Last_Updated_UTC', 'Notes'
        ]]
        
        rows = []
        candidate_tickers = {c['ticker'] for c in candidates}
        
        existing = self.sheets.read_config("UNIVERSE", "A:L")
        existing_map = {}
        if existing and len(existing) > 1:
            for row in existing[1:]:
                if len(row) >= 8:
                    existing_map[row[0]] = {
                        'added_date': row[7],
                        'notes': row[11] if len(row) > 11 else ''
                    }
        
        for candidate in candidates:
            ticker = candidate['ticker']
            added_date = existing_map.get(ticker, {}).get('added_date', today_str)
            
            rows.append([
                ticker,
                f"{candidate['price']:.2f}",
                "0",
                "0",
                "0.0",
                'CANDIDATE',
                'ACTIVE',
                added_date,
                today_str,
                "1",
                now_str,
                f"Score: {candidate['score']:.1f}, Spikes: {candidate['spike_count']}"
            ])
        
        watch_list = self.watch_manager.get_active_watch_list(max_age_hours=24)
        for item in watch_list[:100]:
            if item['ticker'] not in candidate_tickers:
                added_date = existing_map.get(item['ticker'], {}).get('added_date', today_str)
                rows.append([
                    item['ticker'],
                    "0",
                    "0",
                    "0",
                    "0.0",
                    'WATCH',
                    'WATCHING',
                    added_date,
                    "",
                    "0",
                    now_str,
                    f"Score: {item['avg_score']:.1f}"
                ])
        
        self.sheets.clear_range("UNIVERSE", "A2:Z")
        self.sheets.write_data("UNIVERSE", headers + rows, "A1")
        logger.info(f"Updated UNIVERSE tab with {len(rows)} symbols")
    
    def save_universe_json(self, candidates: List[Dict]):
        symbols = [c['ticker'] for c in candidates]
        universe_data = {
            'symbols': symbols,
            'count': len(symbols),
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'watch_list_candidates'
        }
        json_path = Path(__file__).parent.parent / "data" / "universe.json"
        with open(json_path, 'w') as f:
            json.dump(universe_data, f, indent=2)
        logger.info(f"Saved {len(symbols)} candidates to universe.json")
    
    def build(self):
        logger.info("=" * 50)
        logger.info("Building Tier 3: Today's Candidates...")
        
        candidates = self.select_candidates()
        self.check_and_add_to_kiv(candidates)
        self.check_kiv_confirmations()
        self.update_universe_tab(candidates)
        self.save_universe_json(candidates)
        
        kiv_summary = self.kiv_manager.get_kiv_summary()
        logger.info(f"KIV Status: {kiv_summary}")
        logger.info(f"Candidate build complete: {len(candidates)} active symbols")
        logger.info("=" * 50)

if __name__ == "__main__":
    builder = CandidateBuilder()
    builder.build()