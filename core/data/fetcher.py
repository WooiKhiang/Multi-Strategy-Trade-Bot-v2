"""
Data fetcher for Alpaca API.
Implements tiered fetching: cache → snapshot → bars.
"""

import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import pandas as pd

from alpaca.data import StockHistoricalDataClient, StockLatestDataClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame

from core.utils.registry import ComponentRegistry
from config.settings import settings

logger = logging.getLogger(__name__)

class DataFetcher:
    """
    Fetches market data from Alpaca with tiered strategy.
    
    Tier 1: Cache (zero API cost)
    Tier 2: Snapshot (1 API call)
    Tier 3: Bars (1 API call per symbol)
    """
    
    def __init__(self):
        # Initialize Alpaca clients
        self.historical_client = StockHistoricalDataClient(
            api_key=settings.ALPACA_DATA_KEY,
            secret_key=settings.ALPACA_SECRET_KEY
        )
        self.latest_client = StockLatestDataClient(
            api_key=settings.ALPACA_DATA_KEY,
            secret_key=settings.ALPACA_SECRET_KEY
        )
        
        # Get cache from registry
        registry = ComponentRegistry()
        self.cache = registry.get('data', 'cache')()
        
        logger.info("DataFetcher initialized")
    
    def get_current_price(self, ticker: str, max_cache_age: int = 60) -> Optional[Dict[str, Any]]:
        """
        Get current price using tiered approach.
        
        Args:
            ticker: Symbol to fetch
            max_cache_age: Max cache age in seconds
        
        Returns:
            Dict with price data or None
        """
        # Tier 1: Check cache
        cached = self.cache.get(ticker, max_age_seconds=max_cache_age)
        if cached:
            logger.debug(f"Cache hit for {ticker}")
            return cached
        
        # Tier 2: Try snapshot
        try:
            snapshot = self._fetch_snapshot(ticker)
            if snapshot:
                self.cache.update(ticker, snapshot['price'], 
                                  volume=snapshot.get('volume', 0),
                                  bid=snapshot.get('bid'),
                                  ask=snapshot.get('ask'),
                                  source='snapshot')
                return snapshot
        except Exception as e:
            logger.warning(f"Snapshot failed for {ticker}: {e}")
        
        # Tier 3: Try last trade
        try:
            last_trade = self._fetch_last_trade(ticker)
            if last_trade:
                self.cache.update(ticker, last_trade['price'],
                                  volume=last_trade.get('volume', 0),
                                  source='last_trade')
                return last_trade
        except Exception as e:
            logger.warning(f"Last trade failed for {ticker}: {e}")
        
        logger.error(f"All price sources failed for {ticker}")
        return None
    
    def _fetch_snapshot(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch snapshot from Alpaca."""
        try:
            request = StockSnapshotRequest(symbol=ticker)
            snapshot = self.latest_client.get_stock_snapshot(request)
            
            if ticker in snapshot:
                s = snapshot[ticker]
                return {
                    'price': float(s.latest_trade.price) if s.latest_trade else None,
                    'bid': float(s.latest_ask.price) if s.latest_ask else None,
                    'ask': float(s.latest_bid.price) if s.latest_bid else None,
                    'volume': float(s.latest_trade.size) if s.latest_trade else 0,
                    'source': 'snapshot',
                    'timestamp': datetime.utcnow()
                }
        except Exception as e:
            logger.debug(f"Snapshot error for {ticker}: {e}")
        return None
    
    def _fetch_last_trade(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch last trade from Alpaca."""
        try:
            request = StockLatestTradeRequest(symbol=ticker)
            trade_data = self.latest_client.get_stock_latest_trade(request)
            
            if ticker in trade_data:
                trade = trade_data[ticker]
                return {
                    'price': float(trade.price),
                    'volume': float(trade.size),
                    'source': 'last_trade',
                    'timestamp': datetime.utcnow()
                }
        except Exception as e:
            logger.debug(f"Last trade error for {ticker}: {e}")
        return None
    
    def get_bars(self, ticker: str, period: int = 20, 
                 timeframe: TimeFrame = TimeFrame.Minute,
                 timeframe_minutes: int = 5) -> Optional[pd.DataFrame]:
        """
        Fetch historical bars for a symbol.
        
        This is Stage B/C validation - heavier API call.
        """
        try:
            # Calculate start time
            end = datetime.utcnow()
            start = end - timedelta(days=period * timeframe_minutes * 2)  # Buffer
            
            request = StockBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=timeframe,
                start=start,
                end=end,
                feed=settings.ALPACA_FEED  # 'iex' for free tier
            )
            
            bars = self.historical_client.get_stock_bars(request)
            
            if ticker in bars.data:
                df = bars.data[ticker].df
                logger.debug(f"Fetched {len(df)} bars for {ticker}")
                return df
            
        except Exception as e:
            logger.error(f"Bar fetch failed for {ticker}: {e}")
        
        return None
    
    def get_bars_batch(self, tickers: List[str], period: int = 20) -> Dict[str, pd.DataFrame]:
        """Fetch bars for multiple symbols (use sparingly)."""
        result = {}
        for ticker in tickers:
            df = self.get_bars(ticker, period)
            if df is not None:
                result[ticker] = df
        return result