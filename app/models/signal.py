from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from bson import ObjectId
from .user import PyObjectId
from enum import Enum
from ..utils.timezone_utils import TimezoneUtils


class SignalType(str, Enum):
    BUY_CALL = "BUY_CALL"
    BUY_PUT = "BUY_PUT" 
    SELL_CALL = "SELL_CALL"
    SELL_PUT = "SELL_PUT"
    BUY = "BUY"
    SELL = "SELL"


class SignalStrength(str, Enum):
    WEAK = "WEAK"
    MODERATE = "MODERATE"
    STRONG = "STRONG"
    VERY_STRONG = "VERY_STRONG"


class SignalModel(BaseModel):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    user_id: Optional[PyObjectId] = Field(default=None)  # Made optional for system-generated signals
    symbol: str = Field(..., min_length=1, max_length=20)
    option_type: Optional[str] = Field(default=None, pattern="^(PE|CE)$")  # PE or CE
    signal_type: str = Field(...)  # Now supports BUY_CALL, BUY_PUT, etc.
    entry_price: float = Field(..., gt=0)
    target_price: Optional[float] = Field(default=None, gt=0)  # Deprecated - use target_1
    target_1: Optional[float] = Field(default=None, gt=0)  # First target
    target_2: Optional[float] = Field(default=None, gt=0)  # Second target
    stop_loss: Optional[float] = Field(default=None, gt=0)
    quantity: int = Field(default=1, gt=0)
    confidence: int = Field(..., ge=0, le=100)
    status: str = Field(default="ACTIVE", pattern="^(ACTIVE|COMPLETED|CANCELLED|EXPIRED)$")
    notes: Optional[str] = Field(default=None, max_length=1000)
    
    # Advanced signal fields
    session_name: Optional[str] = Field(default=None)
    breakout_type: Optional[str] = Field(default=None)  # "HIGH_BREAKOUT", "LOW_BREAKOUT", "DIVERGENT"
    nifty_price: Optional[float] = Field(default=None)
    future_price: Optional[float] = Field(default=None)
    future_symbol: Optional[str] = Field(default=None)
    session_high: Optional[float] = Field(default=None)
    session_low: Optional[float] = Field(default=None)
    vwap_nifty: Optional[float] = Field(default=None)
    vwap_future: Optional[float] = Field(default=None)
    volume_confirmation: Optional[bool] = Field(default=None)
    technical_data: Optional[Dict[str, Any]] = Field(default=None)
    
    # Enhanced Nifty Futures fields
    future_session_high: Optional[float] = Field(default=None, description="Session high for the futures contract")
    future_session_low: Optional[float] = Field(default=None, description="Session low for the futures contract")
    future_open: Optional[float] = Field(default=None, description="Day's open price for futures")
    future_close: Optional[float] = Field(default=None, description="Previous day's close for futures")
    future_volume: Optional[int] = Field(default=None, description="Current volume for futures")
    future_change: Optional[float] = Field(default=None, description="Change in futures price")
    future_change_percent: Optional[float] = Field(default=None, description="Change percentage in futures")
    
    # Breakout details for both index and futures
    nifty_breakout_amount: Optional[float] = Field(default=None, description="Amount by which Nifty broke the level")
    future_breakout_amount: Optional[float] = Field(default=None, description="Amount by which futures broke the level")
    nifty_breaks_high: Optional[bool] = Field(default=None, description="Whether Nifty broke session high")
    nifty_breaks_low: Optional[bool] = Field(default=None, description="Whether Nifty broke session low")
    future_breaks_high: Optional[bool] = Field(default=None, description="Whether futures broke session high")
    future_breaks_low: Optional[bool] = Field(default=None, description="Whether futures broke session low")
    
    # Additional market data
    market_sentiment: Optional[str] = Field(default=None, description="Overall market sentiment")
    volatility_index: Optional[float] = Field(default=None, description="VIX or volatility measure")
    correlation_score: Optional[float] = Field(default=None, description="Correlation between index and futures movement")
    
    # Timestamps
    signal_time: Optional[datetime] = Field(default=None)  # When signal was generated
    entry_time: Optional[datetime] = Field(default=None)   # When position was taken
    exit_time: Optional[datetime] = Field(default=None)    # When position was closed
    created_at: datetime = Field(default_factory=TimezoneUtils.get_ist_now)
    updated_at: datetime = Field(default_factory=TimezoneUtils.get_ist_now)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        json_schema_extra = {
            "example": {
                "user_id": "507f1f77bcf86cd799439011",
                "symbol": "NIFTY",
                "option_type": "CE",
                "signal_type": "BUY_CALL",
                "entry_price": 22450.0,
                "target_1": 25.50,
                "target_2": 51.00,
                "stop_loss": 22355.0,
                "quantity": 1,
                "confidence": 85,
                "status": "ACTIVE",
                "session_name": "Morning Opening",
                "nifty_price": 22450.0,
                "future_price": 22465.5,
                "future_symbol": "NIFTY_FUT1",
                "session_high": 22425.0,
                "session_low": 22380.0,
                "future_session_high": 22440.0,
                "future_session_low": 22395.0,
                "nifty_breaks_high": True,
                "future_breaks_high": True,
                "nifty_breakout_amount": 25.0,
                "future_breakout_amount": 25.5,
                "vwap_nifty": 22410.0,
                "vwap_future": 22420.0,
                "future_change": 85.5,
                "future_change_percent": 0.38,
                "market_sentiment": "BULLISH",
                "correlation_score": 0.95,
                "notes": "Bullish breakout - Both NIFTY and Futures crossed session high"
            }
        }


class SignalInDB(SignalModel):
    pass


class SignalResponse(BaseModel):
    id: str = Field(alias="_id")
    user_id: str
    symbol: str
    option_type: str
    signal_type: str
    entry_price: float
    target_price: float
    stop_loss: float
    quantity: int
    confidence: int
    status: str
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "id": "507f1f77bcf86cd799439012",
                "user_id": "507f1f77bcf86cd799439011",
                "symbol": "NIFTY",
                "option_type": "CE",
                "signal_type": "BUY",
                "entry_price": 22450.0,
                "target_price": 22550.0,
                "stop_loss": 22350.0,
                "quantity": 1,
                "confidence": 85,
                "status": "ACTIVE",
                "notes": "Strong bullish momentum with RSI oversold",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z"
            }
        } 