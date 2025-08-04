from fastapi import APIRouter
from .auth import router as auth_router
from .feed import router as feed_router
from .market_data import router as market_data_router
from .signals import router as signals_router

api_router = APIRouter()

# Include auth routes
api_router.include_router(auth_router, prefix="/auth", tags=["authentication"])

# Include feed routes
api_router.include_router(feed_router, tags=["market feed"]) 

# Include market data routes
api_router.include_router(market_data_router, tags=["market data"])

# Include signals routes
api_router.include_router(signals_router, tags=["trading signals"]) 