"""
Session State Model for Database-Backed Session Monitoring
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field


class SessionStatus(str, Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE" 
    COMPLETED = "COMPLETED"


class SymbolData(BaseModel):
    high: Optional[float] = None
    low: Optional[float] = None
    tick_count: int = 0
    first_tick_time: Optional[datetime] = None
    last_tick_time: Optional[datetime] = None
    

class SessionState(BaseModel):
    """Database model for session state tracking"""
    
    # Session Definition
    trading_date: str = Field(..., description="Trading date in YYYY-MM-DD format")
    session_name: str = Field(..., description="Session name (e.g., 'Morning Opening')")
    start_time: str = Field(..., description="Session start time in HH:MM format")
    end_time: str = Field(..., description="Session end time in HH:MM format")
    
    # Session Status
    status: SessionStatus = SessionStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Session Data
    symbols_data: Dict[str, SymbolData] = Field(default_factory=dict)
    
    # Breakout Tracking
    breakouts_checked: bool = False
    signals_generated: List[str] = Field(default_factory=list)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class SessionStateService:
    """Service for managing session states in database"""
    
    def __init__(self):
        self.collection = None
    
    def _get_collection(self):
        """Lazy initialization of collection"""
        if self.collection is None:
            from ..core.database import get_collection
            self.collection = get_collection("session_states")
        return self.collection
    
    async def initialize_daily_sessions(self, trading_date: str) -> None:
        """Initialize all 4 predefined sessions for a trading day"""
        sessions = [
            {"name": "Morning Opening", "start": "09:30", "end": "09:35"},
            {"name": "Mid Morning", "start": "09:45", "end": "09:55"},
            {"name": "Pre Lunch", "start": "10:30", "end": "10:45"},
            {"name": "Lunch Break", "start": "11:50", "end": "12:20"}
        ]
        
        for session in sessions:
            session_state = SessionState(
                trading_date=trading_date,
                session_name=session["name"],
                start_time=session["start"],
                end_time=session["end"]
            )
            
            # Upsert - don't overwrite existing sessions
            await self._get_collection().update_one(
                {
                    "trading_date": trading_date,
                    "session_name": session["name"]
                },
                {"$setOnInsert": session_state.dict()},
                upsert=True
            )
    
    async def get_sessions_by_status(self, trading_date: str, statuses: List[str]) -> List[Dict]:
        """Get sessions by status for monitoring"""
        cursor = self._get_collection().find({
            "trading_date": trading_date,
            "status": {"$in": statuses}
        })
        
        return await cursor.to_list(length=None)
    
    async def get_sessions_for_breakout_check(self, trading_date: str) -> List[Dict]:
        """Get completed sessions that haven't been checked for breakouts"""
        cursor = self._get_collection().find({
            "trading_date": trading_date,
            "status": "COMPLETED",
            "breakouts_checked": False
        })
        
        return await cursor.to_list(length=None)
    
    async def update_session_status(self, session_id: str, status: str, additional_data: Dict = None) -> None:
        """Update session status atomically"""
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if additional_data:
            update_data.update(additional_data)
        
        await self._get_collection().update_one(
            {"_id": session_id},
            {"$set": update_data}
        )
    
    async def update_session_data(self, session_id: str, symbols_data: Dict) -> None:
        """Update session symbol data"""
        await self._get_collection().update_one(
            {"_id": session_id},
            {
                "$set": {
                    "symbols_data": symbols_data,
                    "updated_at": datetime.utcnow()
                }
            }
        )
    
    async def mark_breakouts_checked(self, session_id: str, signal_ids: List[str] = None) -> None:
        """Mark session as checked for breakouts"""
        update_data = {
            "breakouts_checked": True,
            "updated_at": datetime.utcnow()
        }
        
        if signal_ids:
            update_data["signals_generated"] = signal_ids
            
        await self._get_collection().update_one(
            {"_id": session_id},
            {"$set": update_data}
        )
    
    async def get_session_by_name(self, trading_date: str, session_name: str) -> Optional[Dict]:
        """Get specific session by name and date"""
        return await self._get_collection().find_one({
            "trading_date": trading_date,
            "session_name": session_name
        })
    
    async def calculate_session_data(self, session_doc: Dict, end_time: datetime) -> Dict[str, SymbolData]:
        """Calculate final session data from tick data"""
        from ..core.database import get_collection
        from ..utils.timezone_utils import TimezoneUtils
        
        # Parse session times
        trading_date = session_doc["trading_date"]
        start_time_str = session_doc["start_time"]
        end_time_str = session_doc["end_time"]
        
        # Create datetime objects
        date_obj = datetime.strptime(trading_date, "%Y-%m-%d").date()
        start_dt = datetime.combine(date_obj, datetime.strptime(start_time_str, "%H:%M").time())
        end_dt = datetime.combine(date_obj, datetime.strptime(end_time_str, "%H:%M").time())
        
        # Symbols to monitor - use centralized symbols config
        from ..core.symbols import SymbolsConfig
        symbols = [SymbolsConfig.NIFTY_INDEX.symbol, SymbolsConfig.NIFTY_FUTURES.symbol]
        symbols_data = {}
        
        tick_collection = get_collection("tick_data")
        
        for symbol in symbols:
            # Query tick data for this session
            cursor = tick_collection.find({
                "symbol": symbol,
                "received_at": {
                    "$gte": start_dt,
                    "$lt": end_dt
                }
            }).sort("received_at", 1)
            
            ticks = await cursor.to_list(length=None)
            
            if ticks:
                prices = [tick["price"] for tick in ticks]
                symbols_data[symbol] = SymbolData(
                    high=max(prices),
                    low=min(prices),
                    tick_count=len(ticks),
                    first_tick_time=ticks[0]["received_at"],
                    last_tick_time=ticks[-1]["received_at"]
                ).dict()
            else:
                symbols_data[symbol] = SymbolData().dict()
        
        return symbols_data


# Global service instance
session_state_service = SessionStateService()