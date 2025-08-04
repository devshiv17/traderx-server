from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import JSONResponse
from typing import Dict, Any, List
import logging
from ...services.angel_one_service import angel_one_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/feed", status_code=status.HTTP_200_OK)
async def receive_market_data(request: Request):
    """
    Receive market data feed from Angel One.
    
    This endpoint accepts POST requests with market data in JSON format.
    The data is processed, stored, and analyzed for potential trading signals.
    """
    try:
        # Get the raw JSON data
        data = await request.json()
        
        # Validate that we have some data
        if not data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No data received"
            )
        
        # Process the market data
        result = await angel_one_service.process_market_data(data)
        
        return JSONResponse(
            content=result,
            status_code=status.HTTP_200_OK
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing feed data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while processing feed data"
        )


@router.get("/feed/latest", status_code=status.HTTP_200_OK)
async def get_latest_market_data(symbol: str = None, limit: int = 100):
    """
    Get latest market data from the database.
    
    - **symbol**: Optional symbol filter (e.g., "NIFTY", "BANKNIFTY")
    - **limit**: Number of records to return (default: 100, max: 1000)
    """
    try:
        # Validate limit
        if limit > 1000:
            limit = 1000
        
        data = await angel_one_service.get_latest_market_data(symbol=symbol, limit=limit)
        
        return {
            "status": "success",
            "data": data,
            "count": len(data),
            "symbol": symbol,
            "limit": limit
        }
        
    except Exception as e:
        logger.error(f"Error fetching latest market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching market data"
        )


@router.get("/feed/summary", status_code=status.HTTP_200_OK)
async def get_market_summary():
    """
    Get market summary with latest data for all symbols.
    """
    try:
        data = await angel_one_service.get_market_summary()
        
        return {
            "status": "success",
            "data": data,
            "count": len(data),
            "timestamp": "latest"
        }
        
    except Exception as e:
        logger.error(f"Error fetching market summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching market summary"
        )


@router.get("/feed/health", status_code=status.HTTP_200_OK)
async def feed_health_check():
    """
    Health check for the feed service.
    """
    try:
        # Check if we can access the service
        summary = await angel_one_service.get_market_summary()
        
        return {
            "status": "healthy",
            "service": "angel_one_feed",
            "data_points": len(summary),
            "timestamp": "current"
        }
        
    except Exception as e:
        logger.error(f"Feed health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "angel_one_feed",
            "error": str(e),
            "timestamp": "current"
        } 


@router.get("/angel-one/health", status_code=status.HTTP_200_OK)
async def angel_one_health_check():
    """
    Health check for Angel One service.
    """
    try:
        health_status = await angel_one_service.get_service_health()
        return health_status
        
    except Exception as e:
        logger.error(f"Angel One health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "angel_one",
            "error": str(e),
            "timestamp": "current"
        }


@router.get("/angel-one/websocket-status", status_code=status.HTTP_200_OK)
async def websocket_status_check():
    """
    Check WebSocket connection status.
    """
    try:
        ws_status = await angel_one_service.get_websocket_status()
        return ws_status
        
    except Exception as e:
        logger.error(f"WebSocket status check failed: {e}")
        return {
            "status": "error",
            "service": "websocket",
            "error": str(e),
            "timestamp": "current"
        } 