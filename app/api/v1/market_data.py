from fastapi import APIRouter, HTTPException, status, Query, Depends
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional
import logging
import asyncio
from datetime import datetime, timedelta
import pytz

from ...services.market_data_service import market_data_service
from ...models.market_data import MarketDataModel, MarketDataFilter, MarketDataResponse
from ...core.database import get_collection
from ...core.symbols import SymbolsConfig
from ...utils.timezone_utils import TimezoneUtils

logger = logging.getLogger(__name__)

# IST timezone (for backward compatibility)
IST = pytz.timezone('Asia/Kolkata')

router = APIRouter()
@router.post("/market-data", status_code=status.HTTP_201_CREATED)
async def store_market_data(data: MarketDataModel):
    """
    Store a single market data entry from Angel One WebSocket
    
    This endpoint accepts market data in the standardized format and stores it in the database.
    """
    try:
        # Store the market data
        data_id = await market_data_service.store_market_data(data)
        
        return JSONResponse(
            content={
                "message": "Market data stored successfully",
                "data_id": data_id,
                "symbol": data.symbol,
                "ltpc": data.ltpc
            },
            status_code=status.HTTP_201_CREATED
        )
        
    except Exception as e:
        logger.error(f"Error storing market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store market data"
        )
@router.post("/market-data/batch", status_code=status.HTTP_201_CREATED)
async def store_batch_market_data(data_list: List[MarketDataModel]):
    """
    Store multiple market data entries in batch
    
    This endpoint accepts a list of market data entries and stores them efficiently.
    """
    try:
        # Store the batch market data
        data_ids = await market_data_service.store_batch_market_data(data_list)
        
        return JSONResponse(
            content={
                "message": f"Batch market data stored successfully",
                "count": len(data_ids),
                "data_ids": data_ids
            },
            status_code=status.HTTP_201_CREATED
        )
        
    except Exception as e:
        logger.error(f"Error storing batch market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store batch market data"
        )
@router.get("/market-data/latest", response_model=List[MarketDataResponse])
async def get_latest_market_data(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    limit: int = Query(100, description="Maximum number of records to return", ge=1, le=1000)
):
    """
    Get latest market data entries
    
    Returns the most recent market data entries, optionally filtered by symbol.
    """
    try:
        data = await market_data_service.get_latest_market_data(symbol=symbol, limit=limit)
        return data
        
    except Exception as e:
        logger.error(f"Error retrieving latest market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve latest market data"
        )
@router.get("/market-data/summary")
async def get_market_summary(
    symbols: Optional[str] = Query(None, description="Comma-separated list of symbols")
):
    """
    Get market summary for specified symbols
    
    Returns a summary of the latest market data for each symbol.
    """
    try:
        # Parse symbols from query parameter
        symbol_list = None
        if symbols:
            symbol_list = [s.strip() for s in symbols.split(",")]
        
        summary = await market_data_service.get_market_summary(symbols=symbol_list)
        return summary
        
    except Exception as e:
        logger.error(f"Error retrieving market summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve market summary"
        )
@router.get("/market-data/filter", response_model=List[MarketDataResponse])
async def get_filtered_market_data(
    symbols: Optional[str] = Query(None, description="Comma-separated list of symbols"),
    exchanges: Optional[str] = Query(None, description="Comma-separated list of exchanges"),
    source: Optional[str] = Query(None, description="Data source filter"),
    processed: Optional[bool] = Query(None, description="Filter by processed status"),
    start_time: Optional[datetime] = Query(None, description="Start time for data range"),
    end_time: Optional[datetime] = Query(None, description="End time for data range"),
    limit: int = Query(100, description="Maximum number of records", ge=1, le=1000)
):
    """
    Get filtered market data
    
    Returns market data entries based on specified filter criteria.
    """
    try:
        # Build filter object
        filter_params = MarketDataFilter(
            symbols=[s.strip() for s in symbols.split(",")] if symbols else None,
            exchanges=[e.strip() for e in exchanges.split(",")] if exchanges else None,
            source=source,
            processed=processed,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        data = await market_data_service.get_market_data_by_filter(filter_params)
        return data
        
    except Exception as e:
        logger.error(f"Error retrieving filtered market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve filtered market data"
        )
@router.post("/market-data/process")
async def mark_data_as_processed(data_ids: List[str]):
    """
    Mark market data entries as processed
    
    Updates the processed status of specified market data entries.
    """
    try:
        success = await market_data_service.mark_as_processed(data_ids)
        
        if success:
            return JSONResponse(
                content={
                    "message": f"Marked {len(data_ids)} entries as processed",
                    "processed_count": len(data_ids)
                },
                status_code=status.HTTP_200_OK
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to mark data as processed"
            )
        
    except Exception as e:
        logger.error(f"Error marking data as processed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to mark data as processed"
        )
@router.get("/market-data/statistics")
async def get_market_data_statistics():
    """
    Get statistics about stored market data
    
    Returns various statistics about the market data collection.
    """
    try:
        stats = await market_data_service.get_data_statistics()
        return stats
        
    except Exception as e:
        logger.error(f"Error retrieving market data statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve market data statistics"
        )
@router.delete("/market-data/cleanup")
async def cleanup_old_market_data(
    days: int = Query(30, description="Number of days to keep data for", ge=1, le=365)
):
    """
    Clean up old market data entries
    
    Removes market data entries older than the specified number of days.
    """
    try:
        deleted_count = await market_data_service.cleanup_old_data(days=days)
        
        return JSONResponse(
            content={
                "message": f"Cleanup completed successfully",
                "deleted_count": deleted_count,
                "days_kept": days
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error cleaning up old market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cleanup old market data"
        )
@router.get("/market-data/health")
async def market_data_health_check():
    """
    Health check for market data service
    
    Returns the health status of the market data service.
    """
    try:
        health_status = await market_data_service.get_health_status()
        return health_status
        
    except Exception as e:
        logger.error(f"Error checking market data health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to check market data health"
        )
@router.get("/angel-one/health")
async def angel_one_health_check():
    """
    Health check for Angel One service
    
    Returns the health status of the Angel One service including connection status,
    session information, and auto-reconnection status.
    """
    try:
        from ...services.angel_one_service import angel_one_service
        health_status = await angel_one_service.get_service_health()
        return health_status
        
    except Exception as e:
        logger.error(f"Error checking Angel One health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to check Angel One health"
        )
@router.post("/angel-one/restart")
async def restart_angel_one_service():
    """
    Manually restart the Angel One service
    
    This endpoint can be used to manually trigger a service restart if needed.
    """
    try:
        from ...services.angel_one_service import angel_one_service
        
        logger.info("🔄 Manual restart of Angel One service requested")
        
        # Stop current service
        await angel_one_service.stop_feed_service()
        
        # Wait a moment
        await asyncio.sleep(2)
        
        # Start service again
        await angel_one_service.start_feed_service()
        
        # Start WebSocket queue processor
        asyncio.create_task(angel_one_service.process_ws_queue())
        
        return JSONResponse(
            content={
                "message": "Angel One service restarted successfully",
                "status": "restarted",
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error restarting Angel One service: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to restart Angel One service"
        )
@router.get("/chart-data/{symbol}")
async def get_chart_data(
    symbol: str,
    timeframe: str = Query("1m", description="Timeframe: 1m, 5m, 15m, 1h"),
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)")
):
    """
    Get chart data with production-grade timezone handling
    
    Uses centralized timezone utilities for consistent datetime operations
    """
    try:
        # Validate timeframe
        valid_timeframes = ["1m", "5m", "15m", "1h"]
        if timeframe not in valid_timeframes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid timeframe. Must be one of: {', '.join(valid_timeframes)}"
            )
        
        # Parse date (default to today in IST)
        if not date:
            date = TimezoneUtils.get_ist_now().strftime("%Y-%m-%d")
        
        # Get IST date range for the requested date
        date_start_ist, date_end_ist = TimezoneUtils.ist_date_range(date)
        
        # Use IST dates directly (database now stores naive IST)
        date_start = date_start_ist
        date_end = date_end_ist
        
        # Query database
        collection = get_collection("tick_data")
        cursor = collection.find({
            'symbol': symbol.upper(),
            'received_at': {
                '$gte': date_start,
                '$lt': date_end
            }
        }).sort('received_at', 1)
        
        # Fetch tick data
        tick_data = []
        async for doc in cursor:
            tick_data.append({
                'price': doc.get('price'),
                'timestamp': doc.get('received_at'),  # Use received_at for accurate timing
                'symbol': doc.get('symbol'),
                'exchange': doc.get('exchange', 'NSE')
            })
        
        if not tick_data:
            return JSONResponse(
                content={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "date": date,
                    "data": [],
                    "count": 0,
                    "data_source": "no_data",
                    "latest_price": None,
                    "real_time": False,
                    "tick_count": 0,
                    "timezone": "IST"
                },
                status_code=status.HTTP_200_OK
            )
        
        # Get market hours in IST
        market_open_ist, market_close_ist = TimezoneUtils.ist_market_hours(date)
        
        # Determine interval in minutes
        interval_map = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
        interval_minutes = interval_map[timeframe]
        
        # Process ticks and group by intervals
        interval_data = {}
        market_hour_ticks = 0
        
        for tick in tick_data:
            # Database timestamp is already naive IST
            tick_time_ist = tick['timestamp']
            if not tick_time_ist:
                continue
            
            # Filter to market hours only
            if not (market_open_ist <= tick_time_ist <= market_close_ist):
                continue
            
            market_hour_ticks += 1
            
            # Round to interval boundary
            interval_start_ist = tick_time_ist.replace(
                minute=(tick_time_ist.minute // interval_minutes) * interval_minutes,
                second=0,
                microsecond=0
            )
            
            # Convert to Unix timestamp for chart
            unix_timestamp = TimezoneUtils.ist_to_unix_timestamp(interval_start_ist)
            
            if unix_timestamp not in interval_data:
                interval_data[unix_timestamp] = {
                    'time': unix_timestamp,
                    'prices': [],
                    'symbol': symbol,
                    'exchange': 'NSE'
                }
            
            interval_data[unix_timestamp]['prices'].append(tick['price'])
        
        # Generate OHLCV candles
        chart_data = []
        for timestamp in sorted(interval_data.keys()):
            data = interval_data[timestamp]
            prices = data['prices']
            if prices:
                chart_data.append({
                    'time': timestamp,
                    'open': prices[0],
                    'high': max(prices),
                    'low': min(prices),
                    'close': prices[-1],
                    'volume': len(prices) * 50,  # Synthetic volume
                    'symbol': symbol,
                    'exchange': 'NSE'
                })
        
        latest_price = chart_data[-1]['close'] if chart_data else None
        
        return JSONResponse(
            content={
                "symbol": symbol,
                "timeframe": timeframe,
                "date": date,
                "data": chart_data,
                "count": len(chart_data),
                "data_source": "tick_data_production",
                "latest_price": latest_price,
                "real_time": True,
                "tick_count": len(tick_data),
                "market_hour_ticks": market_hour_ticks,
                "timezone": "IST",
                "market_hours": f"{market_open_ist.strftime('%H:%M')} - {market_close_ist.strftime('%H:%M')} IST"
            },
            status_code=status.HTTP_200_OK
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving chart data for {symbol}: {e}")
        return JSONResponse(
            content={
                "symbol": symbol,
                "timeframe": timeframe,
                "date": date,
                "data": [],
                "count": 0,
                "data_source": "error",
                "latest_price": None,
                "real_time": False,
                "tick_count": 0,
                "error": str(e),
                "timezone": "IST"
            },
            status_code=status.HTTP_200_OK
        )
@router.get("/tick-data/statistics")
async def get_tick_data_statistics():
    """
    Get real-time tick data statistics - PRODUCTION MONITORING
    
    Returns comprehensive statistics about the tick data collection.
    """
    try:
        from ...services.tick_data_service import tick_data_service
        
        stats = await tick_data_service.get_tick_statistics()
        
        return JSONResponse(
            content={
                "message": "Tick data statistics retrieved successfully",
                "statistics": stats,
                "timestamp": datetime.now(IST).isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error retrieving tick statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve tick statistics"
        )
@router.get("/tick-data/latest/{symbol}")
async def get_latest_ticks(
    symbol: str,
    limit: int = Query(100, description="Number of latest ticks to retrieve", ge=1, le=1000)
):
    """
    Get latest ticks for a symbol - PRODUCTION DEBUGGING
    
    Returns the most recent tick data for debugging and monitoring.
    """
    try:
        from ...services.tick_data_service import tick_data_service
        
        ticks = await tick_data_service.get_latest_ticks(symbol=symbol, limit=limit)
        
        return JSONResponse(
            content={
                "symbol": symbol,
                "ticks": ticks,
                "count": len(ticks),
                "latest_price": ticks[0].get("price") if ticks else None,
                "timestamp": datetime.now(IST).isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error retrieving latest ticks for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve latest ticks"
        )
@router.delete("/tick-data/cleanup")
async def cleanup_old_tick_data(
    days: int = Query(7, description="Number of days to keep tick data", ge=1, le=30)
):
    """
    Clean up old tick data - PRODUCTION MAINTENANCE
    
    Removes tick data older than specified days to manage storage.
    """
    try:
        from ...services.tick_data_service import tick_data_service
        
        deleted_count = await tick_data_service.cleanup_old_ticks(days_to_keep=days)
        
        return JSONResponse(
            content={
                "message": f"Cleaned up {deleted_count} old tick records",
                "deleted_count": deleted_count,
                "days_kept": days,
                "timestamp": datetime.now(IST).isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error cleaning up tick data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cleanup tick data"
        )
@router.get("/market/overview")
async def get_market_overview():
    """
    Get market overview with key indices and summary data
    
    Returns current market status, key indices, and trading summary.
    """
    try:
        from ...services.tick_data_service import tick_data_service
        
        # Get current IST time
        now_ist = datetime.now(IST)
        today_start = datetime(now_ist.year, now_ist.month, now_ist.day, 0, 0, 0, 0, tzinfo=IST)
        
        # Key indices to track - using central symbols configuration
        key_indices = SymbolsConfig.get_symbol_names()
        overview_data = {
            "timestamp": now_ist.isoformat(),
            "market_status": "open" if 9 <= now_ist.hour <= 15 else "closed",
            "indices": [],
            "summary": {
                "total_symbols": 0,
                "active_symbols": 0,
                "last_update": None
            }
        }
        
        # Get latest data for key indices
        for symbol in key_indices:
            try:
                latest_ticks = await tick_data_service.get_latest_ticks(symbol=symbol, limit=1)
                if latest_ticks:
                    tick = latest_ticks[0]
                    overview_data["indices"].append({
                        "symbol": symbol,
                        "price": tick.get("price", 0),
                        "timestamp": tick.get("timestamp", ""),
                        "exchange": tick.get("exchange", "NSE")
                    })
                    if not overview_data["summary"]["last_update"]:
                        overview_data["summary"]["last_update"] = tick.get("timestamp", "")
            except Exception as e:
                logger.warning(f"Error getting data for {symbol}: {e}")
        
        # Get symbol count from tick data
        tick_collection = tick_data_service._get_collection()
        total_symbols = await tick_collection.distinct("symbol", {
            "received_at": {"$gte": today_start}
        })
        overview_data["summary"]["total_symbols"] = len(total_symbols)
        overview_data["summary"]["active_symbols"] = len(overview_data["indices"])
        
        return JSONResponse(
            content=overview_data,
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error retrieving market overview: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve market overview"
        )

@router.get("/available-symbols")
async def get_available_symbols():
    """
    Get list of available symbols for charting - TODAY'S DATA ONLY
    
    Returns the list of symbols that have market data available for today (IST).
    """
    try:
        # Get today's date in IST
        now_ist = datetime.now(IST)
        today_start = datetime(now_ist.year, now_ist.month, now_ist.day, 0, 0, 0, 0, tzinfo=IST)
        today_end = today_start + timedelta(days=1)
        
        # Get symbols from both tick_data and market_data collections for today only
        from ...services.tick_data_service import tick_data_service
        
        # Get symbols from tick_data collection for today only
        tick_collection = tick_data_service._get_collection()
        tick_pipeline = [
            {
                "$match": {
                    "received_at": {
                        "$gte": today_start,
                        "$lt": today_end
                    }
                }
            },
            {"$group": {"_id": "$symbol"}},
            {"$sort": {"_id": 1}}
        ]
        tick_cursor = tick_collection.aggregate(tick_pipeline)
        tick_symbols = [doc["_id"] for doc in await tick_cursor.to_list(length=None) if doc["_id"]]
        
        # Get symbols from market_data collection for today only
        market_collection = get_collection("market_data")
        market_pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": today_start,
                        "$lt": today_end
                    }
                }
            },
            {"$group": {"_id": "$symbol"}},
            {"$sort": {"_id": 1}}
        ]
        market_cursor = market_collection.aggregate(market_pipeline)
        market_symbols = [doc["_id"] for doc in await market_cursor.to_list(length=None) if doc["_id"]]
        
        # Combine and deduplicate symbols
        all_symbols = list(set(tick_symbols + market_symbols))
        
        logger.info(f"📊 Found symbols for today ({today_start.strftime('%Y-%m-%d')}) - Tick data: {len(tick_symbols)}, Market data: {len(market_symbols)}, Total unique: {len(all_symbols)}")
        
        # Return only configured symbols (NIFTY 50 index and current month futures) - using central configuration
        available_symbols = SymbolsConfig.get_api_symbols_list()
        
        return JSONResponse(
            content={
                "symbols": available_symbols,
                "count": len(available_symbols),
                "date": today_start.strftime("%Y-%m-%d"),
                "timezone": "IST",
                "tick_data_symbols": tick_symbols,
                "market_data_symbols": market_symbols,
                "total_unique_symbols": all_symbols
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error retrieving available symbols: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve available symbols"
        )
 