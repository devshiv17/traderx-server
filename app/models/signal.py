from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from bson import ObjectId
from .user import PyObjectId
from enum import Enum


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
    target_price: Optional[float] = Field(default=None, gt=0)
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
    
    # Timestamps
    signal_time: Optional[datetime] = Field(default=None)  # When signal was generated
    entry_time: Optional[datetime] = Field(default=None)   # When position was taken
    exit_time: Optional[datetime] = Field(default=None)    # When position was closed
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        json_schema_extra = {
            "example": {
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
                "notes": "Strong bullish momentum with RSI oversold"
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