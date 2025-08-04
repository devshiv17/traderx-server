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

logger = logging.getLogger(__name__)

# IST timezone
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
                "timestamp": datetime.utcnow().isoformat()
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
    Get chart data for a specific symbol from real-time tick data - PRODUCTION VERSION
    
    Returns aggregated market data for charting purposes from the tick_data collection.
    Timeframes: 1m, 5m, 15m, 1h
    Data is aggregated from real-time ticks stored in the database.
    """
    try:
        from ...services.tick_data_service import tick_data_service
        
        # Validate timeframe
        valid_timeframes = ["1m", "5m", "15m", "1h"]
        if timeframe not in valid_timeframes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid timeframe. Must be one of: {', '.join(valid_timeframes)}"
            )
        
        # Parse date (default to today)
        if not date:
            date = datetime.now(IST).strftime("%Y-%m-%d")
        
        # Convert to datetime range in IST
        date_start = IST.localize(datetime.strptime(date, "%Y-%m-%d"))
        date_end = date_start + timedelta(days=1)
        
        # Determine interval in minutes
        interval_map = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
        interval_minutes = interval_map[timeframe]
        
        logger.info(f"📊 Getting tick data for {symbol} from {date_start.strftime('%Y-%m-%d %H:%M:%S %Z')} to {date_end.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # 🚀 PRODUCTION: Get real-time tick data
        # Database Schema:
        # - 'timestamp': Actual market timestamp from Angel One (when price was recorded)
        # - 'received_at': When our system received the data (system timestamp)
        # We use 'received_at' for querying as it's more accurate for real-time data
        tick_data = await tick_data_service.get_ticks_for_timerange(
            symbol=symbol,
            start_time=date_start,
            end_time=date_end
        )
        
        chart_data = []
        
        # Use tick_data when available, fallback to market_data
        if tick_data:
            logger.info(f"📈 Found {len(tick_data)} ticks for {symbol}")
            
            # Filter ticks to only include market hours data (9:15 AM to 3:30 PM IST)
            market_start = date_start.replace(hour=9, minute=15, second=0, microsecond=0)
            market_end = date_start.replace(hour=15, minute=30, second=0, microsecond=0)
            
            filtered_ticks = []
            for tick in tick_data:
                tick_time = tick['received_at']
                # Convert UTC timestamp to IST for comparison
                if isinstance(tick_time, datetime):
                    if tick_time.tzinfo is None:
                        # Assume UTC if no timezone info (based on sample data)
                        tick_time_utc = pytz.UTC.localize(tick_time)
                    else:
                        tick_time_utc = tick_time.astimezone(pytz.UTC)
                    tick_time_ist = tick_time_utc.astimezone(IST)
                elif isinstance(tick_time, str):
                    # Parse string and convert to IST
                    if 'Z' in tick_time or '+00:00' in tick_time:
                        tick_time_utc = datetime.fromisoformat(tick_time.replace('Z', '+00:00'))
                        tick_time_ist = pytz.UTC.localize(tick_time_utc).astimezone(IST)
                    else:
                        tick_time_ist = IST.localize(datetime.fromisoformat(tick_time))
                else:
                    continue
                
                if market_start <= tick_time_ist <= market_end:
                    filtered_ticks.append(tick)
            
            logger.info(f"📊 Filtered to {len(filtered_ticks)} ticks within market hours (9:15 AM - 3:30 PM IST)")
            
            # Group filtered ticks into actual time intervals based on data
            interval_data = {}
            
            for tick in filtered_ticks:
                tick_time = tick['received_at']
                # Convert UTC timestamp to IST for processing
                if isinstance(tick_time, datetime):
                    if tick_time.tzinfo is None:
                        # Assume UTC if no timezone info (based on sample data)
                        tick_time_utc = pytz.UTC.localize(tick_time)
                    else:
                        tick_time_utc = tick_time.astimezone(pytz.UTC)
                    tick_time_ist = tick_time_utc.astimezone(IST)
                elif isinstance(tick_time, str):
                    # Parse string and convert to IST
                    if 'Z' in tick_time or '+00:00' in tick_time:
                        tick_time_utc = datetime.fromisoformat(tick_time.replace('Z', '+00:00'))
                        tick_time_ist = pytz.UTC.localize(tick_time_utc).astimezone(IST)
                    else:
                        tick_time_ist = IST.localize(datetime.fromisoformat(tick_time))
                else:
                    continue
                
                # Find the appropriate interval for this tick
                interval_start = tick_time_ist.replace(
                    minute=(tick_time_ist.minute // interval_minutes) * interval_minutes,
                    second=0,
                    microsecond=0
                )
                
                interval_key = interval_start.isoformat()
                
                # Create interval if it doesn't exist
                if interval_key not in interval_data:
                    interval_data[interval_key] = {
                        'timestamp': interval_start,
                        'prices': [],
                        'volume': 0,
                        'first_tick': None,
                        'last_tick': None
                    }
                
                if interval_data[interval_key]['first_tick'] is None:
                    interval_data[interval_key]['first_tick'] = tick_time_ist
                
                interval_data[interval_key]['prices'].append(tick['price'])
                # For indices, use a normalized volume approach for better readability
                # Scale volume based on price movement to make chart more meaningful
                price_change = abs(tick['price'] - interval_data[interval_key]['prices'][0]) if interval_data[interval_key]['prices'] else 0
                volume_contribution = max(50, min(500, int(price_change * 10)))  # Scale based on price movement
                interval_data[interval_key]['volume'] += volume_contribution
                interval_data[interval_key]['last_tick'] = max(
                    interval_data[interval_key]['last_tick'] or tick_time_ist, 
                    tick_time_ist
                )
            
            # Convert intervals to OHLCV format with unique timestamps
            sorted_intervals = sorted(interval_data.keys())
            for i, interval_key in enumerate(sorted_intervals):
                data = interval_data[interval_key]
                prices = data['prices']
                
                # Only create bars for intervals that have data
                if prices:
                    # Convert IST timestamp to UTC Unix timestamp for frontend
                    ist_timestamp = data['timestamp']
                    utc_timestamp = ist_timestamp.astimezone(pytz.UTC)
                    base_timestamp = int(utc_timestamp.timestamp())
                    unique_timestamp = base_timestamp + i  # Add offset to ensure uniqueness
                    
                    chart_data.append({
                        "time": unique_timestamp,
                        "open": prices[0],
                        "high": max(prices),
                        "low": min(prices),
                        "close": prices[-1],
                        "is_live": True,
                        "source": "tick_data",
                        "tick_count": len(prices)
                    })
            
            logger.info(f"✅ Created {len(chart_data)} chart intervals from filtered ticks")
        
        # No fallback to aggregated market data - only use real-time tick data
        if not chart_data:
            logger.warning(f"⚠️ No tick data found for {symbol} on {date} - showing empty chart")
            # Return empty chart data instead of fallback
            chart_data = []
        
        # Get latest price
        latest_price = None
        if chart_data:
            latest_price = chart_data[-1].get("close", 0)
        
        # Always return a valid response, even if chart_data is empty
        return JSONResponse(
            content={
                "symbol": symbol,
                "timeframe": timeframe,
                "date": date,
                "data": chart_data,
                "count": len(chart_data),
                "data_source": "tick_data" if tick_data else "no_data",
                "latest_price": latest_price,
                "real_time": bool(tick_data),
                "tick_count": len(tick_data) if tick_data else 0
            },
            status_code=status.HTTP_200_OK
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving chart data for {symbol}: {e}")
        # Always return a valid empty chart on error (except for explicit HTTPException)
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
                "error": str(e)
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


@router.get("/available-symbols")
async def get_available_symbols():
    """
    Get list of available symbols for charting - TODAY'S DATA ONLY
    
    Returns the list of symbols that have market data available for today (IST).
    """
    try:
        # Get today's date in IST
        today_start = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start + timedelta(days=1)
        
        # Get symbols from both tick_data and market_data collections for today only
        from ...services.tick_data_service import tick_data_service
        
        # Get symbols from tick_data collection for today only
        tick_collection = tick_data_service._get_collection()
        tick_pipeline = [
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
        
        # Filter to only show the indices we want - now includes NIFTY futures
        allowed_symbols = [
            {"symbol": "NIFTY", "name": "NIFTY 50", "exchange": "NFO"},
            {"symbol": "BANKNIFTY", "name": "BANKNIFTY", "exchange": "NFO"},
            {"symbol": "FINNIFTY", "name": "FINNIFTY", "exchange": "NFO"},
            {"symbol": "MIDCPNIFTY", "name": "MIDCPNIFTY", "exchange": "NFO"},
            {"symbol": "SENSEX", "name": "SENSEX", "exchange": "BFO"},
            {"symbol": "BANKEX", "name": "BANKEX", "exchange": "BFO"},
            # NIFTY Futures
            {"symbol": "NIFTY_FUT1", "name": "NIFTY Futures 1", "exchange": "NFO"},
            {"symbol": "NIFTY_FUT2", "name": "NIFTY Futures 2", "exchange": "NFO"},
            {"symbol": "NIFTY_ALT", "name": "NIFTY Alternative", "exchange": "NFO"}
        ]
        
        # Filter to only include symbols that have data today
        available_symbols = [s for s in allowed_symbols if s["symbol"] in all_symbols]
        
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