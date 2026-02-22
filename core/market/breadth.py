"""
Market breadth calculator for Mark 3.1.
Tracks advance/decline, new highs/lows, and sector rotation.
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd

from core.utils.registry import ComponentRegistry

logger = logging.getLogger(__name__)

class BreadthCalculator:
    """
    Calculates market breadth indicators.
    
    Tracks:
    - % of stocks above moving averages
    - Advance/decline ratio
    - New highs vs new lows
    - Sector performance
    """
    
    def __init__(self):
        # Get dependencies
        registry = ComponentRegistry()
        self.fetcher = registry.get('data', 'fetcher')()
        
        # Benchmark symbols by sector
        self.sectors = {
            'Technology': ['AAPL', 'MSFT', 'NVDA', 'CRM', 'ADBE'],
            'Financial': ['JPM', 'BAC', 'WFC', 'C', 'GS'],
            'Healthcare': ['JNJ', 'UNH', 'PFE', 'MRK', 'ABT'],
            'Consumer': ['AMZN', 'TSLA', 'HD', 'MCD', 'NKE'],
            'Energy': ['XOM', 'CVX', 'COP', 'SLB', 'EOG'],
            'Industrials': ['CAT', 'BA', 'HON', 'UPS', 'LMT'],
            'Utilities': ['NEE', 'DUK', 'SO', 'D', 'AEP'],
            'Real Estate': ['PLD', 'AMT', 'CCI', 'EQIX', 'PSA']
        }
        
        # Flatten for overall breadth
        self.all_benchmarks = []
        for sector_symbols in self.sectors.values():
            self.all_benchmarks.extend(sector_symbols)
    
    def calculate_breadth(self) -> Dict[str, Any]:
        """
        Calculate comprehensive market breadth.
        """
        results = {
            'overall': self._calculate_overall_breadth(),
            'by_sector': {},
            'advance_decline': self._calculate_advance_decline(),
            'new_highs_lows': self._calculate_new_highs_lows(),
            'timestamp': datetime.utcnow()
        }
        
        # Calculate sector breadths
        for sector, symbols in self.sectors.items():
            results['by_sector'][sector] = self._calculate_sector_breadth(symbols)
        
        # Determine overall condition
        results['condition'] = self._determine_condition(results)
        
        logger.info(f"Market breadth: {results['overall']['above_50ma']:.1%} above 50MA")
        return results
    
    def _calculate_overall_breadth(self) -> Dict[str, float]:
        """Calculate overall market breadth metrics."""
        above_20ma = 0
        above_50ma = 0
        above_200ma = 0
        total = 0
        
        for symbol in self.all_benchmarks:
            df = self.fetcher.get_bars(symbol, period=250, timeframe='Day')
            
            if df is not None and len(df) > 200:
                current = df['close'].iloc[-1]
                
                # Calculate moving averages
                sma_20 = df['close'].rolling(20).mean().iloc[-1]
                sma_50 = df['close'].rolling(50).mean().iloc[-1]
                sma_200 = df['close'].rolling(200).mean().iloc[-1]
                
                if current > sma_20:
                    above_20ma += 1
                if current > sma_50:
                    above_50ma += 1
                if current > sma_200:
                    above_200ma += 1
                
                total += 1
        
        if total == 0:
            return {
                'above_20ma': 0.5,
                'above_50ma': 0.5,
                'above_200ma': 0.5
            }
        
        return {
            'above_20ma': above_20ma / total,
            'above_50ma': above_50ma / total,
            'above_200ma': above_200ma / total
        }
    
    def _calculate_sector_breadth(self, symbols: List[str]) -> Dict[str, float]:
        """Calculate breadth for a specific sector."""
        above_50ma = 0
        total = 0
        
        for symbol in symbols:
            df = self.fetcher.get_bars(symbol, period=60, timeframe='Day')
            
            if df is not None and len(df) > 50:
                sma_50 = df['close'].rolling(50).mean().iloc[-1]
                current = df['close'].iloc[-1]
                
                if current > sma_50:
                    above_50ma += 1
                total += 1
        
        if total == 0:
            return {'above_50ma': 0.5}
        
        return {'above_50ma': above_50ma / total}
    
    def _calculate_advance_decline(self) -> Dict[str, int]:
        """
        Calculate advance/decline for the day.
        Simplified: compares current price to yesterday's close.
        """
        advances = 0
        declines = 0
        unchanged = 0
        
        for symbol in self.all_benchmarks:
            df = self.fetcher.get_bars(symbol, period=2, timeframe='Day')
            
            if df is not None and len(df) >= 2:
                yesterday = df['close'].iloc[-2]
                today = df['close'].iloc[-1]
                
                if today > yesterday:
                    advances += 1
                elif today < yesterday:
                    declines += 1
                else:
                    unchanged += 1
        
        return {
            'advances': advances,
            'declines': declines,
            'unchanged': unchanged,
            'ratio': advances / (declines + 1)  # Avoid division by zero
        }
    
    def _calculate_new_highs_lows(self) -> Dict[str, int]:
        """
        Calculate stocks at new highs/lows (52-week).
        """
        new_highs = 0
        new_lows = 0
        
        for symbol in self.all_benchmarks:
            df = self.fetcher.get_bars(symbol, period=365, timeframe='Day')
            
            if df is not None and len(df) > 250:
                year_high = df['high'].rolling(252).max().iloc[-1]
                year_low = df['low'].rolling(252).min().iloc[-1]
                current = df['close'].iloc[-1]
                
                if current >= year_high * 0.99:  # Within 1% of high
                    new_highs += 1
                elif current <= year_low * 1.01:  # Within 1% of low
                    new_lows += 1
        
        return {
            'new_highs': new_highs,
            'new_lows': new_lows,
            'ratio': new_highs / (new_lows + 1)
        }
    
    def _determine_condition(self, results: Dict) -> str:
        """
        Determine overall market condition from breadth.
        """
        overall = results['overall']
        ad = results['advance_decline']
        nh = results['new_highs_lows']
        
        # Strong bull: broad participation
        if (overall['above_50ma'] > 0.7 and 
            ad['ratio'] > 1.5 and 
            nh['ratio'] > 2):
            return 'STRONG_BULL'
        
        # Bull: decent participation
        if (overall['above_50ma'] > 0.6 and 
            ad['ratio'] > 1.2):
            return 'BULL'
        
        # Neutral: mixed
        if (0.4 <= overall['above_50ma'] <= 0.6):
            return 'NEUTRAL'
        
        # Bear: weak participation
        if (overall['above_50ma'] < 0.4 and 
            ad['ratio'] < 0.8):
            return 'BEAR'
        
        # Strong bear: capitulation
        if (overall['above_50ma'] < 0.3 and 
            nh['ratio'] < 0.5):
            return 'STRONG_BEAR'
        
        return 'MIXED'
    
    def get_leading_sectors(self) -> List[str]:
        """Get top performing sectors."""
        sector_scores = {}
        
        for sector, symbols in self.sectors.items():
            breadth = self._calculate_sector_breadth(symbols)
            sector_scores[sector] = breadth['above_50ma']
        
        # Sort by breadth descending
        sorted_sectors = sorted(sector_scores.items(), 
                               key=lambda x: x[1], 
                               reverse=True)
        
        return [sector for sector, score in sorted_sectors if score > 0.5]
    
    def get_lagging_sectors(self) -> List[str]:
        """Get worst performing sectors."""
        sector_scores = {}
        
        for sector, symbols in self.sectors.items():
            breadth = self._calculate_sector_breadth(symbols)
            sector_scores[sector] = breadth['above_50ma']
        
        # Sort by breadth ascending
        sorted_sectors = sorted(sector_scores.items(), 
                               key=lambda x: x[1])
        
        return [sector for sector, score in sorted_sectors if score < 0.3]