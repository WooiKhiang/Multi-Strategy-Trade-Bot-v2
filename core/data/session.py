"""
Market session awareness for Mark 3.1.
Handles trading hours, holidays, early closes.
"""

import logging
from datetime import datetime, date, time, timedelta
from typing import Optional, Tuple, Dict, List
import pytz

logger = logging.getLogger(__name__)

# Timezone definitions
NY_TZ = pytz.timezone('America/New_York')
UTC_TZ = pytz.UTC

class MarketSession:
    """
    Market session manager with calendar support.
    
    Determines if market is open, expected bar counts, etc.
    """
    
    def __init__(self, calendar: Optional[Dict] = None):
        """
        Initialize with optional calendar.
        
        Calendar format:
        {
            'holiday_dates': [date(2026, 1, 1), ...],
            'early_close_dates': [date(2026, 7, 3), ...],
            'early_close_time': time(13, 0)  # 1:00 PM ET
        }
        """
        self.calendar = calendar or {}
        self.early_close_time = self.calendar.get('early_close_time', time(13, 0))
    
    def is_trading_day(self, dt_utc: Optional[datetime] = None) -> bool:
        """Check if given day is a trading day."""
        if dt_utc is None:
            dt_utc = datetime.now(UTC_TZ)
        
        dt_ny = dt_utc.astimezone(NY_TZ)
        date_ny = dt_ny.date()
        
        # Weekend check
        if dt_ny.weekday() >= 5:
            return False
        
        # Holiday check
        if date_ny in self.calendar.get('holiday_dates', []):
            return False
        
        return True
    
    def is_market_open(self, dt_utc: Optional[datetime] = None) -> bool:
        """Check if market is currently open."""
        if dt_utc is None:
            dt_utc = datetime.now(UTC_TZ)
        
        if not self.is_trading_day(dt_utc):
            return False
        
        dt_ny = dt_utc.astimezone(NY_TZ)
        date_ny = dt_ny.date()
        
        # Check if early close
        if date_ny in self.calendar.get('early_close_dates', []):
            close_time = datetime.combine(date_ny, self.early_close_time)
            close_time = NY_TZ.localize(close_time)
            return dt_ny <= close_time
        
        # Regular hours
        open_time = datetime.combine(date_ny, time(9, 30))
        close_time = datetime.combine(date_ny, time(16, 0))
        
        open_time = NY_TZ.localize(open_time)
        close_time = NY_TZ.localize(close_time)
        
        return open_time <= dt_ny <= close_time
    
    def get_session_bounds(self, dt_utc: Optional[datetime] = None) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Get market open and close times for the day in UTC.
        
        Returns:
            (open_utc, close_utc) or (None, None) if not a trading day
        """
        if dt_utc is None:
            dt_utc = datetime.now(UTC_TZ)
        
        if not self.is_trading_day(dt_utc):
            return None, None
        
        dt_ny = dt_utc.astimezone(NY_TZ)
        date_ny = dt_ny.date()
        
        # Check if early close
        if date_ny in self.calendar.get('early_close_dates', []):
            open_ny = datetime.combine(date_ny, time(9, 30))
            close_ny = datetime.combine(date_ny, self.early_close_time)
        else:
            open_ny = datetime.combine(date_ny, time(9, 30))
            close_ny = datetime.combine(date_ny, time(16, 0))
        
        # Localize and convert to UTC
        open_ny = NY_TZ.localize(open_ny)
        close_ny = NY_TZ.localize(close_ny)
        
        return open_ny.astimezone(UTC_TZ), close_ny.astimezone(UTC_TZ)
    
    def expected_bars_between(self, start_utc: datetime, end_utc: datetime, 
                              bar_minutes: int = 5) -> int:
        """
        Calculate expected number of bars between two times,
        accounting for market sessions.
        """
        if start_utc >= end_utc:
            return 0
        
        current = start_utc
        bars = 0
        
        while current < end_utc:
            if self.is_market_open(current):
                bars += 1
            current += timedelta(minutes=bar_minutes)
        
        return bars
    
    def minutes_until_close(self, dt_utc: Optional[datetime] = None) -> float:
        """Minutes until market close (negative if already closed)."""
        if dt_utc is None:
            dt_utc = datetime.now(UTC_TZ)
        
        _, close_utc = self.get_session_bounds(dt_utc)
        
        if close_utc is None:
            return 0.0
        
        return (close_utc - dt_utc).total_seconds() / 60.0
    
    def is_pre_close_window(self, dt_utc: Optional[datetime] = None, 
                            window_minutes: int = 15) -> bool:
        """Check if we're within N minutes of market close."""
        mins = self.minutes_until_close(dt_utc)
        return 0 < mins <= window_minutes

# Default NYSE calendar for 2026
DEFAULT_CALENDAR_2026 = {
    'holiday_dates': [
        date(2026, 1, 1),   # New Year's
        date(2026, 1, 19),  # MLK Day
        date(2026, 2, 16),  # Presidents' Day
        date(2026, 4, 3),   # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 7, 3),   # Independence Day (observed)
        date(2026, 9, 7),   # Labor Day
        date(2026, 11, 26), # Thanksgiving
        date(2026, 12, 25), # Christmas
    ],
    'early_close_dates': [
        date(2026, 7, 3),   # Day before Independence Day
        date(2026, 11, 27), # Day after Thanksgiving
        date(2026, 12, 24), # Christmas Eve
    ],
    'early_close_time': time(13, 0)  # 1:00 PM ET
}