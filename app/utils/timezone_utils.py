"""
IST-First Timezone Utilities
All timestamps are stored and processed in IST timezone - no UTC conversions needed!
"""

from datetime import datetime, timedelta
import pytz
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)

# Timezone constants - IST ONLY!
IST = pytz.timezone('Asia/Kolkata')

# Market hours in IST
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30


class TimezoneUtils:
    """IST-first timezone utilities - everything is in IST!"""
    
    @staticmethod
    def get_ist_now() -> datetime:
        """Get current time in IST timezone (naive for database storage)"""
        return datetime.now(IST).replace(tzinfo=None)
    
    @staticmethod
    def to_ist(dt: Union[datetime, str, None]) -> Optional[datetime]:
        """
        Convert any datetime to IST timezone (naive for database storage)
        
        Args:
            dt: datetime object, ISO string, or None
            
        Returns:
            naive datetime in IST timezone or None if input is invalid
        """
        if dt is None:
            return None
            
        try:
            # Handle string input
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00') if 'Z' in dt else dt)
                
            # Handle datetime object
            if isinstance(dt, datetime):
                if dt.tzinfo is None:
                    # Already naive - assume it's IST
                    return dt
                else:
                    # Convert to IST and make naive
                    return dt.astimezone(IST).replace(tzinfo=None)
            
            return None
            
        except Exception as e:
            logger.error(f"Error converting datetime to IST: {dt}, error: {e}")
            return None
    
    @staticmethod
    def to_naive_ist(dt: Union[datetime, str, None]) -> Optional[datetime]:
        """
        Convert datetime to naive IST for database storage (main method!)
        
        Args:
            dt: datetime object, ISO string, or None
            
        Returns:
            naive datetime in IST timezone for database storage
        """
        return TimezoneUtils.to_ist(dt)
    
    @staticmethod
    def ist_date_range(date: Union[datetime, str]) -> tuple[datetime, datetime]:
        """
        Get start and end of day in IST for a given date (naive datetimes)
        
        Args:
            date: date string (YYYY-MM-DD) or datetime object
            
        Returns:
            tuple of (start_of_day_ist, end_of_day_ist) as naive datetimes
        """
        if isinstance(date, str):
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        else:
            date_obj = date
        
        # Create start and end of day in IST (naive)
        start_of_day = datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0, 0)
        end_of_day = datetime(date_obj.year, date_obj.month, date_obj.day, 23, 59, 59, 999999)
        
        return start_of_day, end_of_day
    
    @staticmethod
    def ist_market_hours(date: Union[datetime, str]) -> tuple[datetime, datetime]:
        """
        Get market hours (9:15 AM - 3:30 PM) in IST for a given date (naive datetimes)
        
        Args:
            date: date string (YYYY-MM-DD) or datetime object
            
        Returns:
            tuple of (market_open_ist, market_close_ist) as naive datetimes
        """
        if isinstance(date, str):
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        else:
            date_obj = date
        
        # Create market hours in IST (naive)
        market_open = datetime(date_obj.year, date_obj.month, date_obj.day, MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE, 0, 0)
        market_close = datetime(date_obj.year, date_obj.month, date_obj.day, MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE, 0, 0)
        
        return market_open, market_close
    
    @staticmethod
    def is_market_hours(dt: Optional[datetime] = None) -> bool:
        """
        Check if given time (or current time) is within market hours
        
        Args:
            dt: naive datetime to check (assumes IST) or None for current time
            
        Returns:
            True if within market hours, False otherwise
        """
        if dt is None:
            dt = TimezoneUtils.get_ist_now()
        else:
            dt = TimezoneUtils.to_ist(dt)
        
        if dt is None:
            return False
        
        # Check if weekday (Monday=0, Sunday=6)
        if dt.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check time range (all naive IST datetimes)
        market_open, market_close = TimezoneUtils.ist_market_hours(dt.date())
        return market_open <= dt <= market_close
    
    @staticmethod
    def is_today_ist(dt: Union[datetime, str, None]) -> bool:
        """
        Check if given datetime is today in IST timezone
        
        Args:
            dt: naive datetime to check (assumes IST)
            
        Returns:
            True if the date is today in IST, False otherwise
        """
        if dt is None:
            return False
        
        dt_ist = TimezoneUtils.to_ist(dt)
        if dt_ist is None:
            return False
        
        today_ist = TimezoneUtils.get_ist_now().date()
        return dt_ist.date() == today_ist
    
    @staticmethod
    def format_for_api(dt: Optional[datetime]) -> Optional[str]:
        """
        Format datetime for API response (ISO format, assumes IST)
        
        Args:
            dt: naive datetime to format (assumes IST)
            
        Returns:
            ISO formatted string or None
        """
        if dt is None:
            return None
        
        return dt.isoformat()
    
    @staticmethod
    def unix_timestamp_to_ist(timestamp: Union[int, float]) -> datetime:
        """
        Convert Unix timestamp to IST datetime (naive)
        
        Args:
            timestamp: Unix timestamp (seconds since epoch)
            
        Returns:
            naive datetime in IST timezone
        """
        utc_dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        ist_dt = utc_dt.astimezone(IST)
        return ist_dt.replace(tzinfo=None)
    
    @staticmethod
    def ist_to_unix_timestamp(dt: datetime) -> int:
        """
        Convert IST datetime to Unix timestamp
        
        Args:
            dt: naive datetime in IST
            
        Returns:
            Unix timestamp (seconds since epoch)
        """
        ist_dt = IST.localize(dt)
        return int(ist_dt.timestamp())


# Convenience functions for backward compatibility
def get_ist_now() -> datetime:
    """Get current time in IST (naive)"""
    return TimezoneUtils.get_ist_now()

def to_ist(dt: Union[datetime, str, None]) -> Optional[datetime]:
    """Convert datetime to IST (naive)"""
    return TimezoneUtils.to_ist(dt)

def to_naive_ist(dt: Union[datetime, str, None]) -> Optional[datetime]:
    """Convert datetime to naive IST for database storage"""
    return TimezoneUtils.to_naive_ist(dt)

def is_market_hours(dt: Optional[datetime] = None) -> bool:
    """Check if within market hours"""
    return TimezoneUtils.is_market_hours(dt)

def is_today_ist(dt: Union[datetime, str, None]) -> bool:
    """Check if date is today in IST"""
    return TimezoneUtils.is_today_ist(dt)

