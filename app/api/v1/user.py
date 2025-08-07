from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

router = APIRouter()

# In-memory storage for watchlist (in a real app, this would be in a database)
user_watchlists = {}

@router.get("/user/watchlist")
async def get_user_watchlist(user_id: str = Query("default", description="User ID")):
    """
    Get user's watchlist
    
    Returns the list of symbols in the user's watchlist.
    """
    try:
        watchlist = user_watchlists.get(user_id, [])
        
        return JSONResponse(
            content={
                "user_id": user_id,
                "watchlist": watchlist,
                "count": len(watchlist),
                "timestamp": datetime.now(IST).isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error retrieving watchlist for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve watchlist"
        )


@router.post("/user/watchlist/add")
async def add_to_watchlist(
    symbol: str = Query(..., description="Symbol to add to watchlist"),
    user_id: str = Query("default", description="User ID")
):
    """
    Add symbol to user's watchlist
    
    Adds a symbol to the user's watchlist if it's not already present.
    """
    try:
        if user_id not in user_watchlists:
            user_watchlists[user_id] = []
        
        if symbol not in user_watchlists[user_id]:
            user_watchlists[user_id].append(symbol)
            message = f"Symbol {symbol} added to watchlist"
        else:
            message = f"Symbol {symbol} already in watchlist"
        
        return JSONResponse(
            content={
                "message": message,
                "user_id": user_id,
                "symbol": symbol,
                "watchlist": user_watchlists[user_id],
                "count": len(user_watchlists[user_id])
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error adding {symbol} to watchlist for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add symbol to watchlist"
        )


@router.delete("/user/watchlist/remove")
async def remove_from_watchlist(
    symbol: str = Query(..., description="Symbol to remove from watchlist"),
    user_id: str = Query("default", description="User ID")
):
    """
    Remove symbol from user's watchlist
    
    Removes a symbol from the user's watchlist if it exists.
    """
    try:
        if user_id in user_watchlists and symbol in user_watchlists[user_id]:
            user_watchlists[user_id].remove(symbol)
            message = f"Symbol {symbol} removed from watchlist"
        else:
            message = f"Symbol {symbol} not found in watchlist"
        
        watchlist = user_watchlists.get(user_id, [])
        
        return JSONResponse(
            content={
                "message": message,
                "user_id": user_id,
                "symbol": symbol,
                "watchlist": watchlist,
                "count": len(watchlist)
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error removing {symbol} from watchlist for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to remove symbol from watchlist"
        )


@router.put("/user/watchlist")
async def update_watchlist(
    symbols: List[str],
    user_id: str = Query("default", description="User ID")
):
    """
    Update user's entire watchlist
    
    Replaces the user's watchlist with the provided list of symbols.
    """
    try:
        user_watchlists[user_id] = symbols
        
        return JSONResponse(
            content={
                "message": "Watchlist updated successfully",
                "user_id": user_id,
                "watchlist": symbols,
                "count": len(symbols),
                "timestamp": datetime.now(IST).isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error updating watchlist for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update watchlist"
        )