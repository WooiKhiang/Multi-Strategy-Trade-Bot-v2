"""
Time utilities for Mark 3.1.
Handles timezone conversions, market hours, DST.
"""

import pytz
from datetime import datetime, time, date, timedelta
import logging
from typing import Optional, Tuple, Dict, List

logger = logging.getLogger(__name__)

# Timezone definitions
NY_TZ = pytz.timezone('America/New_York')
UTC_TZ = pytz.UTC

# Default calendar for 2026 (will be updated annually)
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
    ]
}

def now_utc() -> datetime:
    """Get current UTC datetime with timezone."""
    return datetime.now(UTC_TZ)

def utc_to_ny(dt_utc: datetime) -> datetime:
    """Convert UTC to New York time."""
    return dt_utc.astimezone(NY_TZ)

def ny_to_utc(dt_ny: datetime) -> datetime:
    """Convert New York time to UTC."""
    if dt_ny.tzinfo is None:
        dt_ny = NY_TZ.localize(dt_ny)
    return dt_ny.astimezone(UTC_TZ)

def get_utc_midnight() -> datetime:
    """Get UTC midnight for today (start of day)."""
    return now_utc().replace(hour=0, minute=0, second=0, microsecond=0)

def is_market_hours(
    dt_utc: Optional[datetime] = None,
    calendar: Optional[Dict] = None
) -> bool:
    """
    Check if market is open.
    
    Args:
        dt_utc: Time to check (default: now)
        calendar: Optional calendar dict with holiday/early close dates
    
    Returns:
        True if market is open
    """
    if dt_utc is None:
        dt_utc = now_utc()
    
    dt_ny = utc_to_ny(dt_utc)
    
    # Layer 1: Weekend check
    if dt_ny.weekday() >= 5:  # Saturday=5, Sunday=6
        logger.debug(f"Weekend: {dt_ny.date()}, market closed")
        return False
    
    # Layer 2: Calendar check (if available)
    if calendar:
        date_ny = dt_ny.date()
        
        # Check holidays
        if date_ny in calendar.get('holiday_dates', []):
            logger.debug(f"Holiday: {date_ny}, market closed")
            return False
        
        # Check early closes
        if date_ny in calendar.get('early_close_dates', []):
            # Early close at 1:00 PM ET
            early_close = dt_ny.replace(hour=13, minute=0, second=0)
            is_open = dt_ny <= early_close
            logger.debug(f"Early close day: {date_ny}, open until 13:00 ET")
            return is_open
    
    # Layer 3: Regular hours
    market_open = dt_ny.replace(hour=9, minute=30, second=0)
    market_close = dt_ny.replace(hour=16, minute=0, second=0)
    
    is_open = market_open <= dt_ny <= market_close
    logger.debug(f"Regular hours: {is_open} at {dt_ny.time()}")
    return is_open

def get_market_hours_bounds(
    dt_utc: Optional[datetime] = None,
    calendar: Optional[Dict] = None
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get market open and close times for the day.
    
    Returns:
        Tuple of (open_utc, close_utc) or (None, None) if market closed all day
    """
    if dt_utc is None:
        dt_utc = now_utc()
    
    dt_ny = utc_to_ny(dt_utc)
    date_ny = dt_ny.date()
    
    # Check if full holiday
    if calendar and date_ny in calendar.get('holiday_dates', []):
        return None, None
    
    # Check if early close
    if calendar and date_ny in calendar.get('early_close_dates', []):
        open_ny = datetime.combine(date_ny, time(9, 30))
        close_ny = datetime.combine(date_ny, time(13, 0))
    else:
        # Regular day
        open_ny = datetime.combine(date_ny, time(9, 30))
        close_ny = datetime.combine(date_ny, time(16, 0))
    
    # Convert to UTC
    open_utc = ny_to_utc(open_ny)
    close_utc = ny_to_utc(close_ny)
    
    return open_utc, close_utc

def minutes_until_market_close(
    dt_utc: Optional[datetime] = None,
    calendar: Optional[Dict] = None
) -> float:
    """Get minutes until market close (negative if after close)."""
    if dt_utc is None:
        dt_utc = now_utc()
    
    _, close_utc = get_market_hours_bounds(dt_utc, calendar)
    
    if close_utc is None:
        return 0.0  # Market closed all day
    
    return (close_utc - dt_utc).total_seconds() / 60.0

def format_utc_for_sheets(dt_utc: Optional[datetime] = None) -> str:
    """Format UTC time for Google Sheets storage."""
    if dt_utc is None:
        dt_utc = now_utc()
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

def parse_sheets_time(time_str: str) -> datetime:
    """Parse time from Google Sheets (assumes UTC)."""
    # Remove ' UTC' if present
    clean_str = time_str.replace(' UTC', '')
    dt = datetime.strptime(clean_str, "%Y-%m-%d %H:%M:%S")
    return UTC_TZ.localize(dt)