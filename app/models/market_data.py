from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
from bson import ObjectId
from .user import PyObjectId
from ..utils.timezone_utils import TimezoneUtils


class MarketDataModel(BaseModel):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    
    # Core market data fields
    tk: str = Field(..., description="Token/Symbol identifier")
    symbol: str = Field(..., description="Human readable symbol name")
    exchange: str = Field(default="NSE", description="Exchange (NSE, BSE, etc.)")
    
    # Price data
    ltpc: float = Field(..., description="Last traded price")
    ch: float = Field(..., description="Change in price")
    chp: float = Field(..., description="Change percentage")
    high: Optional[float] = Field(None, description="High price of the day")
    low: Optional[float] = Field(None, description="Low price of the day")
    open: Optional[float] = Field(None, description="Open price")
    close: Optional[float] = Field(None, description="Previous close price")
    
    # Volume and liquidity
    volume: Optional[int] = Field(None, description="Volume traded")
    bid: Optional[float] = Field(None, description="Best bid price")
    ask: Optional[float] = Field(None, description="Best ask price")
    bid_qty: Optional[int] = Field(None, description="Best bid quantity")
    ask_qty: Optional[int] = Field(None, description="Best ask quantity")
    
    # Additional Angel One specific fields
    exchange_type: Optional[int] = Field(None, description="Angel One exchange type (1=NSE, 2=NFO, etc.)")
    token: Optional[str] = Field(None, description="Angel One token identifier")
    instrument_type: Optional[str] = Field(None, description="Instrument type (EQ, CE, PE, etc.)")
    expiry: Optional[str] = Field(None, description="Expiry date for derivatives")
    strike_price: Optional[float] = Field(None, description="Strike price for options")
    
    # Market depth (if available)
    depth: Optional[Dict[str, Any]] = Field(None, description="Market depth data")
    
    # Metadata
    received_at: datetime = Field(default_factory=TimezoneUtils.get_ist_now, description="Timestamp when data was received (IST)")
    source: str = Field(default="angel_one_websocket", description="Data source")
    processed: bool = Field(default=False, description="Whether data has been processed")
    
    # Raw data storage
    raw_data: Optional[Dict[str, Any]] = Field(None, description="Original raw data from Angel One")
    
    # Data quality indicators
    data_quality: Optional[str] = Field(default="good", description="Data quality indicator")
    is_realtime: bool = Field(default=True, description="Whether this is real-time data")
    
    @validator('symbol', pre=True, always=True)
    def set_symbol_from_token(cls, v, values):
        """Set symbol name if not provided"""
        if v:
            return v
        # If symbol is not provided, try to get it from token mapping
        token = values.get('tk', '')
        return cls._get_symbol_from_token(token)
    
    @staticmethod
    def _get_symbol_from_token(token: str) -> str:
        """Map token to symbol name - NIFTY INDEX AND FUTURES ONLY"""
        token_map = {
            # Primary NIFTY tokens only
            "99926000": "NIFTY",           # NIFTY Index
            "64103": "NIFTY28AUG25FUT",   # NIFTY Futures
        }
        return token_map.get(token, f"TOKEN_{token}")
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        json_schema_extra = {
            "example": {
                "tk": "99926000",
                "symbol": "NIFTY",
                "exchange": "NSE",
                "ltpc": 22450.75,
                "ch": 125.50,
                "chp": 0.56,
                "high": 22500.00,
                "low": 22300.00,
                "open": 22325.25,
                "close": 22450.75,
                "volume": 125000000,
                "bid": 22450.00,
                "ask": 22451.00,
                "bid_qty": 100,
                "ask_qty": 150,
                "exchange_type": 1,
                "token": "99926000",
                "instrument_type": "INDEX",
                "source": "angel_one_websocket",
                "processed": False,
                "is_realtime": True
            }
        }


class MarketDataResponse(BaseModel):
    id: str = Field(alias="_id")
    tk: str
    symbol: str
    exchange: str
    ltpc: float
    ch: float
    chp: float
    high: Optional[float]
    low: Optional[float]
    open: Optional[float]
    close: Optional[float]
    volume: Optional[int]
    bid: Optional[float]
    ask: Optional[float]
    bid_qty: Optional[int]
    ask_qty: Optional[int]
    exchange_type: Optional[int]
    token: Optional[str]
    instrument_type: Optional[str]
    expiry: Optional[str]
    strike_price: Optional[float]
    depth: Optional[Dict[str, Any]]
    received_at: datetime
    source: str
    processed: bool
    data_quality: Optional[str]
    is_realtime: bool
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "id": "507f1f77bcf86cd799439013",
                "tk": "99926000",
                "symbol": "NIFTY",
                "exchange": "NSE",
                "ltpc": 22450.75,
                "ch": 125.50,
                "chp": 0.56,
                "high": 22500.00,
                "low": 22300.00,
                "open": 22325.25,
                "close": 22450.75,
                "volume": 125000000,
                "bid": 22450.00,
                "ask": 22451.00,
                "bid_qty": 100,
                "ask_qty": 150,
                "exchange_type": 1,
                "token": "99926000",
                "instrument_type": "INDEX",
                "received_at": "2024-01-15T10:30:00Z",
                "source": "angel_one_websocket",
                "processed": False,
                "data_quality": "good",
                "is_realtime": True
            }
        }


class MarketDataBatch(BaseModel):
    """Model for batch market data operations"""
    data: List[MarketDataModel] = Field(..., description="List of market data entries")
    batch_id: Optional[str] = Field(None, description="Unique batch identifier")
    created_at: datetime = Field(default_factory=TimezoneUtils.get_ist_now)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class MarketDataFilter(BaseModel):
    """Model for filtering market data queries"""
    symbols: Optional[List[str]] = Field(None, description="Filter by symbols")
    exchanges: Optional[List[str]] = Field(None, description="Filter by exchanges")
    start_time: Optional[datetime] = Field(None, description="Start time for data range")
    end_time: Optional[datetime] = Field(None, description="End time for data range")
    source: Optional[str] = Field(None, description="Filter by data source")
    processed: Optional[bool] = Field(None, description="Filter by processed status")
    limit: Optional[int] = Field(default=100, description="Limit number of results")
    
    class Config:
        populate_by_name = True 