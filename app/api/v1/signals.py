"""
Advanced Trading Signals API
Handles session-based breakout signals with technical analysis
"""

from fastapi import APIRouter, HTTPException, status, Query, Depends
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime, timedelta

from ...services.signal_detection_service import signal_detection_service
from ...models.signal import SignalModel, SignalType, SignalStrength
from ...utils.timezone_utils import TimezoneUtils
import motor.motor_asyncio

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/signals/test")
async def get_signals_test():
    """
    Test endpoint that returns mock signals data
    """
    mock_signals = [
        {
            "id": "Morning_Opening_BUY_PUT_093353",
            "session_name": "Morning Opening",
            "signal_type": "BUY_PUT",
            "timestamp": "2025-08-07T09:33:53",
            "entry_price": 24628.6,
            "stop_loss": 24676.49,
            "target_1": 24607.15,
            "target_2": 24585.7,
            "confidence": 87,
            "status": "EXPIRED",
            "symbol": "NIFTY",
            "display_text": "BUY PUT signal for Morning Opening session"
        },
        {
            "id": "Mid_Morning_BUY_CALL_095334",
            "session_name": "Mid Morning", 
            "signal_type": "BUY_CALL",
            "timestamp": "2025-08-07T09:53:34",
            "entry_price": 24650.2,
            "stop_loss": 24600.1,
            "target_1": 24680.5,
            "target_2": 24720.8,
            "confidence": 92,
            "status": "EXPIRED",
            "symbol": "NIFTY",
            "display_text": "BUY CALL signal for Mid Morning session"
        }
    ]
    
    return JSONResponse(
        content={
            "signals": mock_signals,
            "count": len(mock_signals),
            "source": "mock_data",
            "timestamp": TimezoneUtils.get_ist_now().isoformat()
        },
        status_code=status.HTTP_200_OK
    )


@router.get("/signals/direct")
async def get_signals_direct():
    """
    Direct access to signals with production-grade timezone handling
    
    Returns TODAY'S signals only using centralized timezone utilities
    """
    import pymongo
    
    try:
        # Use centralized timezone utilities
        current_time_ist = TimezoneUtils.get_ist_now()
        today = current_time_ist.date()
        
        # Direct database access
        mongodb_url = "mongodb+srv://mail2shivap17:syqzpekjQBCc9oee@traders.w2xrjgy.mongodb.net/?retryWrites=true&w=majority&appName=traders"
        client = pymongo.MongoClient(mongodb_url)
        db = client['trading_signals']
        signals_collection = db['signals']
        
        # Get today's signals using timezone utils
        cursor = signals_collection.find().sort('created_at', -1)
        today_signals = []
        
        for signal_doc in cursor:
            created_at = signal_doc.get('created_at')
            if created_at and TimezoneUtils.is_today_ist(created_at):
                # Convert to JSON serializable format with proper timezone handling
                signal_data = {}
                for key, value in signal_doc.items():
                    if key == '_id':
                        signal_data['_id'] = str(value)
                    elif isinstance(value, datetime):
                        signal_data[key] = TimezoneUtils.format_for_api(value)
                    elif value is None and key in ['nifty_price', 'future_price', 'entry_price', 'stop_loss', 'target_1', 'target_2']:
                        # Handle null price values
                        if key in ['nifty_price', 'future_price'] and signal_doc.get('entry_price'):
                            signal_data[key] = signal_doc.get('entry_price')
                        else:
                            signal_data[key] = 0
                    else:
                        signal_data[key] = value
                today_signals.append(signal_data)
        
        client.close()
        
        return JSONResponse(
            content={
                "date": today.isoformat(),
                "signals": today_signals,
                "count": len(today_signals),
                "filtered_by": "today_only_production",
                "current_time_ist": TimezoneUtils.format_for_api(current_time_ist),
                "timezone": "IST",
                "market_status": "open" if TimezoneUtils.is_market_hours() else "closed",
                "source": "direct_mongodb_production",
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Direct signals access failed: {e}")
        
        # Safe fallback using timezone utils
        today = TimezoneUtils.get_ist_now().date()
        
        return JSONResponse(
            content={
                "date": today.isoformat(),
                "error": str(e),
                "signals": [],
                "count": 0,
                "filtered_by": "today_only_production",
                "timezone": "IST",
                "source": "direct_mongodb_failed",
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )


@router.post("/signals/start-monitoring")
async def start_signal_monitoring():
    """
    Start real-time signal monitoring service
    
    Begins monitoring trading sessions and detecting breakout signals
    """
    try:
        await signal_detection_service.start_monitoring()
        
        return JSONResponse(
            content={
                "message": "Signal monitoring started successfully",
                "status": "running",
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error starting signal monitoring: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start signal monitoring"
        )


@router.post("/signals/stop-monitoring")
async def stop_signal_monitoring():
    """
    Stop real-time signal monitoring service
    """
    try:
        await signal_detection_service.stop_monitoring()
        
        return JSONResponse(
            content={
                "message": "Signal monitoring stopped successfully",
                "status": "stopped",
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error stopping signal monitoring: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to stop signal monitoring"
        )


@router.get("/signals/active")
async def get_active_signals():
    """
    Get all currently active trading signals
    
    Returns signals that are currently active and not expired
    """
    try:
        active_signals = await signal_detection_service.get_active_signals()
        
        # Convert datetime objects to ISO strings for JSON serialization
        json_signals = []
        for signal in active_signals:
            json_signal = dict(signal)
            if 'timestamp' in json_signal and json_signal['timestamp']:
                if hasattr(json_signal['timestamp'], 'isoformat'):
                    json_signal['timestamp'] = json_signal['timestamp'].isoformat()
                else:
                    json_signal['timestamp'] = str(json_signal['timestamp'])
            json_signals.append(json_signal)
        
        return JSONResponse(
            content={
                "signals": json_signals,
                "count": len(json_signals),
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting active signals: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve active signals"
        )


@router.get("/signals/history")
async def get_signal_history(
    limit: int = Query(50, description="Maximum number of signals to return", ge=1, le=200),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    signal_type: Optional[str] = Query(None, description="Filter by signal type"),
    session_name: Optional[str] = Query(None, description="Filter by session name"),
    group_by_session: bool = Query(False, description="Group results by session")
):
    """
    Get historical trading signals
    
    Returns past signals with optional filtering
    """
    try:
        # Try to get signals from the service first
        try:
            signals = await signal_detection_service.get_signal_history(limit=limit)
        except Exception as service_error:
            logger.warning(f"Signal detection service failed: {service_error}, trying direct database access")
            
            # Direct MongoDB Atlas access as fallback
            import motor.motor_asyncio
            from ...core.config import settings
            
            client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongodb_url)
            db = client[settings.database_name]
            signals_collection = db['signals']
            
            signals = []
            async for signal_doc in signals_collection.find().sort('created_at', -1).limit(limit):
                signal_data = {
                    'id': signal_doc.get('id', str(signal_doc.get('_id', ''))),
                    'session_name': signal_doc.get('session_name'),
                    'signal_type': signal_doc.get('signal_type'),
                    'reason': signal_doc.get('reason'),
                    'timestamp': signal_doc.get('timestamp'),
                    'nifty_price': signal_doc.get('nifty_price'),
                    'future_price': signal_doc.get('future_price'),
                    'future_symbol': signal_doc.get('future_symbol'),
                    'entry_price': signal_doc.get('entry_price'),
                    'stop_loss': signal_doc.get('stop_loss'),
                    'target_1': signal_doc.get('target_1'),
                    'target_2': signal_doc.get('target_2'),
                    'confidence': signal_doc.get('confidence'),
                    'status': signal_doc.get('status'),
                    'session_high': signal_doc.get('session_high'),
                    'session_low': signal_doc.get('session_low'),
                    'created_at': signal_doc.get('created_at'),
                    'display_text': signal_doc.get('display_text')
                }
                signals.append(signal_data)
            
            client.close()
            logger.info(f"Retrieved {len(signals)} signals from direct MongoDB Atlas access")
        
        # Apply filters
        filtered_signals = signals
        
        if symbol:
            filtered_signals = [s for s in filtered_signals if s.get('symbol') == symbol]
        
        if signal_type:
            filtered_signals = [s for s in filtered_signals if s.get('signal_type') == signal_type]
        
        if session_name:
            filtered_signals = [s for s in filtered_signals if s.get('session_name') == session_name]
        
        # Convert datetime objects to ISO strings for JSON serialization
        json_signals = []
        for signal in filtered_signals:
            json_signal = dict(signal)
            if 'timestamp' in json_signal and json_signal['timestamp']:
                if hasattr(json_signal['timestamp'], 'isoformat'):
                    json_signal['timestamp'] = json_signal['timestamp'].isoformat()
                else:
                    json_signal['timestamp'] = str(json_signal['timestamp'])
            json_signals.append(json_signal)
        
        # Group by session if requested
        if group_by_session:
            sessions_grouped = {}
            for signal in json_signals:
                session_name = signal.get('session_name', 'Unknown Session')
                if session_name not in sessions_grouped:
                    sessions_grouped[session_name] = []
                sessions_grouped[session_name].append(signal)
            
            # Sort signals within each session by timestamp
            for session_name in sessions_grouped:
                sessions_grouped[session_name].sort(key=lambda x: x.get('timestamp', ''))
            
            return JSONResponse(
                content={
                    "signals_by_session": sessions_grouped,
                    "count": len(json_signals),
                    "total_signals": len(signals),
                    "sessions_count": len(sessions_grouped),
                    "filters_applied": {
                        "symbol": symbol,
                        "signal_type": signal_type,
                        "session_name": session_name,
                        "group_by_session": group_by_session
                    },
                    "timestamp": TimezoneUtils.get_ist_now().isoformat()
                },
                status_code=status.HTTP_200_OK
            )
        else:
            return JSONResponse(
                content={
                    "signals": json_signals,
                    "count": len(json_signals),
                    "total_signals": len(signals),
                    "filters_applied": {
                        "symbol": symbol,
                        "signal_type": signal_type,
                        "session_name": session_name,
                        "group_by_session": group_by_session
                    },
                    "timestamp": TimezoneUtils.get_ist_now().isoformat()
                },
                status_code=status.HTTP_200_OK
            )
        
    except Exception as e:
        logger.error(f"Error getting signal history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signal history"
        )


@router.get("/signals/sessions")
async def get_session_status():
    """
    Get current trading session status with production-grade timezone handling
    
    Returns TODAY'S signals only using centralized timezone utilities
    """
    try:
        import pymongo
        from ...core.config import settings
        
        # Use centralized timezone utilities
        current_time_ist = TimezoneUtils.get_ist_now()
        today = current_time_ist.date()
        
        # Use configuration for database access
        client = pymongo.MongoClient(settings.mongodb_url)
        db = client['trading_signals']
        signals_collection = db['signals']
        
        # Get all signals and filter by today's date using timezone utils
        cursor = signals_collection.find().sort('created_at', -1)
        today_signals = []
        
        for signal_doc in cursor:
            created_at = signal_doc.get('created_at')
            if created_at and TimezoneUtils.is_today_ist(created_at):
                # Convert to JSON serializable format with proper timezone handling
                signal_data = {}
                for key, value in signal_doc.items():
                    if key == '_id':
                        signal_data['_id'] = str(value)
                    elif isinstance(value, datetime):
                        signal_data[key] = TimezoneUtils.format_for_api(value)
                    else:
                        signal_data[key] = value
                today_signals.append(signal_data)
        
        client.close()
        
        # Define sessions with proper timezone-aware times
        sessions_data = {
            "Morning Opening": {"name": "Morning Opening", "time": "09:30-09:35", "signals": [], "is_active": False},
            "Mid Morning": {"name": "Mid Morning", "time": "09:45-09:55", "signals": [], "is_active": False},
            "Pre Lunch": {"name": "Pre Lunch", "time": "10:30-10:45", "signals": [], "is_active": False},
            "Lunch Break": {"name": "Lunch Break", "time": "11:50-12:20", "signals": [], "is_active": False}
        }
        
        # Distribute signals to sessions
        for signal in today_signals:
            session_name = signal.get('session_name')
            if session_name in sessions_data:
                sessions_data[session_name]['signals'].append(signal)
        
        # Determine active sessions using timezone utils
        current_hour = current_time_ist.hour
        current_minute = current_time_ist.minute
        current_time_minutes = current_hour * 60 + current_minute
        
        # Session times in minutes from midnight
        session_times = {
            "Morning Opening": (9*60 + 30, 9*60 + 35),
            "Mid Morning": (9*60 + 45, 9*60 + 55),
            "Pre Lunch": (10*60 + 30, 10*60 + 45),
            "Lunch Break": (11*60 + 50, 12*60 + 20)
        }
        
        for session_name, (start_minutes, end_minutes) in session_times.items():
            if start_minutes <= current_time_minutes <= end_minutes:
                sessions_data[session_name]['is_active'] = True
                break
        
        sessions_list = list(sessions_data.values())
        total_signals_today = len(today_signals)
        active_sessions = [s for s in sessions_list if s['is_active']]
        
        return JSONResponse(
            content={
                "date": today.isoformat(),
                "sessions": sessions_list,
                "count": len(sessions_list),
                "total_signals_today": total_signals_today,
                "active_sessions": len(active_sessions),
                "filtered_by": "today_only_production",
                "current_time_ist": TimezoneUtils.format_for_api(current_time_ist),
                "timezone": "IST",
                "market_status": "open" if TimezoneUtils.is_market_hours() else "closed",
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        
        # Safe fallback using timezone utils
        today = TimezoneUtils.get_ist_now().date()
        
        return JSONResponse(
            content={
                "date": today.isoformat(),
                "sessions": [],
                "count": 0,
                "total_signals_today": 0,
                "active_sessions": 0,
                "filtered_by": "today_only_production",
                "timezone": "IST",
                "error": str(e),
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )


@router.get("/signals/technical/{symbol}")
async def get_technical_analysis(symbol: str):
    """
    Get technical analysis data for a symbol
    
    Returns VWAP, price data, volume analysis, and other technical indicators
    """
    try:
        logger.info(f"Getting technical analysis for {symbol}")
        
        # Check if service is available
        if not signal_detection_service:
            return JSONResponse(
                content={
                    "error": "Signal detection service not available",
                    "technical_data": {
                        "symbol": symbol,
                        "current_price": None,
                        "vwap": None,
                        "recent_candles": [],
                        "volume_data": []
                    },
                    "timestamp": TimezoneUtils.get_ist_now().isoformat()
                },
                status_code=status.HTTP_200_OK
            )
        
        technical_data = await signal_detection_service.get_technical_data(symbol)
        logger.info(f"Technical data retrieved: {technical_data}")
        
        return JSONResponse(
            content={
                "technical_data": technical_data,
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting technical analysis for {symbol}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        
        # Return error details in response for debugging
        return JSONResponse(
            content={
                "error": str(e),
                "traceback": traceback.format_exc(),
                "technical_data": {
                    "symbol": symbol,
                    "current_price": None,
                    "vwap": None,
                    "recent_candles": [],
                    "volume_data": []
                },
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )


@router.get("/signals/monitoring-status")
async def get_monitoring_status():
    """
    Get current monitoring service status
    
    Returns whether the signal detection service is currently running
    """
    try:
        import pytz
        import pymongo
        
        IST = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(IST)
        
        # Check if monitoring is active
        is_monitoring = signal_detection_service.monitoring_active
        
        # Check if in market hours
        is_market_hours = (
            current_time.weekday() < 5 and  # Monday-Friday
            current_time.hour >= 9 and current_time.hour < 16  # 9 AM - 4 PM
        )
        
        # Get TODAY'S signals from database directly
        latest_signal_json = None
        all_signals = []
        
        try:
            # Direct database access
            mongodb_url = "mongodb+srv://mail2shivap17:syqzpekjQBCc9oee@traders.w2xrjgy.mongodb.net/?retryWrites=true&w=majority&appName=traders"
            client = pymongo.MongoClient(mongodb_url)
            db = client['trading_signals']
            signals_collection = db['signals']
            
            # Get today's date for filtering
            today_ist = current_time.date()
            
            # Get all signals and filter for today only
            cursor = signals_collection.find().sort('created_at', -1)
            for signal_doc in cursor:
                # Check if signal is from today
                created_at = signal_doc.get('created_at')
                if created_at:
                    if isinstance(created_at, str):
                        signal_dt = datetime.fromisoformat(created_at.replace('Z', ''))
                    else:
                        signal_dt = created_at
                    
                    # Convert to IST if needed
                    if signal_dt.tzinfo is None:
                        signal_dt_ist = IST.localize(signal_dt)
                    else:
                        signal_dt_ist = signal_dt.astimezone(IST)
                    
                    signal_date = signal_dt_ist.date()
                    
                    # Only include today's signals
                    if signal_date == today_ist:
                        signal_data = {}
                        for key, value in signal_doc.items():
                            if key == '_id':
                                signal_data['_id'] = str(value)
                            elif isinstance(value, datetime):
                                signal_data[key] = value.isoformat()
                            elif value is None and key in ['nifty_price', 'future_price', 'entry_price', 'stop_loss', 'target_1', 'target_2']:
                                # Handle null price values - use entry_price as fallback or 0
                                if key in ['nifty_price', 'future_price'] and signal_doc.get('entry_price'):
                                    signal_data[key] = signal_doc.get('entry_price')
                                else:
                                    signal_data[key] = 0
                            else:
                                signal_data[key] = value
                        all_signals.append(signal_data)
            
            # Get latest signal for display
            if all_signals:
                latest = all_signals[0]
                latest_signal_json = {
                    "symbol": latest.get("symbol"),
                    "signal_type": latest.get("signal_type"),
                    "confidence": latest.get("confidence"),
                    "session_name": latest.get("session_name"),
                    "timestamp": latest.get("timestamp")
                }
            
            client.close()
            
        except Exception as db_error:
            logger.warning(f"Direct database access failed: {db_error}")
            # Include error in response for debugging
            all_signals = [{"error": str(db_error), "debug": "database_access_failed"}]
        
        return JSONResponse(
            content={
                "monitoring_active": is_monitoring,
                "market_hours": is_market_hours,
                "current_time_ist": current_time.isoformat(),
                "latest_signal": latest_signal_json,
                "service_status": "running" if is_monitoring else "stopped",
                "all_signals": all_signals,  # Include all signals here
                "signals_count": len(all_signals),
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting monitoring status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve monitoring status"
        )


@router.get("/signals/chart-signals/{symbol}")
async def get_chart_signals(
    symbol: str,
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)"),
    timeframe: str = Query("5m", description="Timeframe for signals display"),
    group_by_session: bool = Query(False, description="Group signals by session for clearer display")
):
    """
    Get signals formatted for chart display
    
    Returns signals with exact timestamps and prices for chart overlay
    """
    try:
        import pytz
        IST = pytz.timezone('Asia/Kolkata')
        
        # Parse date
        if not date:
            target_date = datetime.now(IST).date()
        else:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        # Get signals for the date
        all_signals = await signal_detection_service.get_signal_history(limit=200)
        
        # Filter signals for the symbol and date
        chart_signals = []
        
        for signal in all_signals:
            signal_date = None
            
            # Get signal timestamp
            if signal.get('timestamp'):
                if isinstance(signal['timestamp'], str):
                    signal_dt = datetime.fromisoformat(signal['timestamp'].replace('Z', '+00:00'))
                else:
                    signal_dt = signal['timestamp']
                
                # Convert to IST if needed
                if signal_dt.tzinfo is None:
                    # Treat naive datetime as IST (not UTC)
                    signal_dt_ist = IST.localize(signal_dt)
                else:
                    signal_dt_ist = signal_dt.astimezone(IST)
                signal_date = signal_dt_ist.date()
            
            # Check if signal matches our criteria
            if (signal_date == target_date and 
                (signal.get('symbol') == symbol or 
                 signal.get('future_symbol') == symbol or
                 symbol in ['NIFTY', 'NIFTY_FUT1', 'NIFTY_FUT2'])):  # Include NIFTY-related signals
                
                # Format for chart display
                chart_signal = {
                    'id': signal.get('id'),
                    'time': int(signal_dt_ist.timestamp()),  # Unix timestamp for chart
                    'type': signal.get('signal_type'),
                    'price': signal.get('nifty_price') if symbol == 'NIFTY' else signal.get('future_price'),
                    'confidence': signal.get('confidence', 50),
                    'session_name': signal.get('session_name'),
                    'reason': signal.get('reason', ''),
                    'breakout_type': 'HIGH' if 'high' in signal.get('reason', '').lower() else 'LOW',
                    'vwap': signal.get('vwap_nifty') if symbol == 'NIFTY' else signal.get('vwap_future'),
                    'session_high': signal.get('session_high'),
                    'session_low': signal.get('session_low'),
                    'status': signal.get('status', 'ACTIVE')
                }
                
                chart_signals.append(chart_signal)
        
        # Sort by time
        chart_signals.sort(key=lambda x: x['time'])
        
        # Group by session if requested
        if group_by_session:
            signals_by_session = {}
            for signal in chart_signals:
                session_name = signal.get('session_name', 'Unknown Session')
                if session_name not in signals_by_session:
                    signals_by_session[session_name] = []
                signals_by_session[session_name].append(signal)
            
            # Sort signals within each session by time
            for session_name in signals_by_session:
                signals_by_session[session_name].sort(key=lambda x: x['time'])
            
            return JSONResponse(
                content={
                    "symbol": symbol,
                    "date": target_date.isoformat(),
                    "timeframe": timeframe,
                    "signals_by_session": signals_by_session,
                    "total_signals": len(chart_signals),
                    "sessions_count": len(signals_by_session),
                    "timestamp": TimezoneUtils.get_ist_now().isoformat()
                },
                status_code=status.HTTP_200_OK
            )
        else:
            return JSONResponse(
                content={
                    "symbol": symbol,
                    "date": target_date.isoformat(),
                    "timeframe": timeframe,
                    "signals": chart_signals,
                    "count": len(chart_signals),
                    "timestamp": TimezoneUtils.get_ist_now().isoformat()
                },
                status_code=status.HTTP_200_OK
            )
        
    except Exception as e:
        logger.error(f"Error getting chart signals for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve chart signals for {symbol}"
        )


@router.get("/signals/breakout-details")
async def get_signals_with_breakout_details(
    limit: int = Query(50, description="Maximum number of signals to return", ge=1, le=200),
    session_name: Optional[str] = Query(None, description="Filter by session name"),
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)")
):
    """
    Get signals with detailed breakout information
    
    Shows exact NIFTY and Future breakout conditions instead of just arrows
    """
    try:
        import pytz
        IST = pytz.timezone('Asia/Kolkata')
        
        # Parse date - default to today if not provided
        if not date:
            target_date = datetime.now(IST).date()
        else:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        # Get more signals to filter by date
        all_signals = await signal_detection_service.get_signal_history(limit=limit*4)
        
        # Filter signals by date first
        signals = []
        for signal in all_signals:
            # Skip None signals
            if signal is None:
                continue
                
            signal_date = None
            
            # Get signal timestamp - try both timestamp and created_at fields
            signal_timestamp = signal.get('timestamp') or signal.get('created_at')
            if signal_timestamp:
                if isinstance(signal_timestamp, str):
                    signal_dt = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
                else:
                    signal_dt = signal_timestamp
                
                if signal_dt.tzinfo is None:
                    # Treat naive datetime as IST (not UTC)
                    signal_dt_ist = IST.localize(signal_dt)
                else:
                    signal_dt_ist = signal_dt.astimezone(IST)
                signal_date = signal_dt_ist.date()
            
            # Include only signals from the target date
            if signal_date == target_date:
                signals.append(signal)
        
        # Limit the results
        signals = signals[:limit]
        
        # Filter by session if specified
        if session_name:
            signals = [s for s in signals if s is not None and isinstance(s, dict) and s.get('session_name') == session_name]
        
        # Clean up any None values that might have slipped through
        signals = [s for s in signals if s is not None and isinstance(s, dict)]
        
        # Enhanced signal display with breakout details
        detailed_signals = []
        
        for signal in signals:
            # Skip None or invalid signals
            if signal is None or not isinstance(signal, dict):
                continue
            # Clean up the signal data for JSON serialization
            enhanced_signal = dict(signal)
            
            # Convert all datetime fields to strings
            datetime_fields = ['timestamp', 'created_at', 'updated_at']
            for field in datetime_fields:
                if field in enhanced_signal and enhanced_signal[field]:
                    if hasattr(enhanced_signal[field], 'isoformat'):
                        enhanced_signal[field] = enhanced_signal[field].isoformat()
                    else:
                        enhanced_signal[field] = str(enhanced_signal[field])
            
            # Add breakout visualization data - handle None values safely
            breakout_details = enhanced_signal.get('breakout_details') or {}
            display_text = enhanced_signal.get('display_text', '')
            
            # Safely get breakout status
            nifty_breaks_high = breakout_details.get('nifty_breaks_high', False) if isinstance(breakout_details, dict) else False
            nifty_breaks_low = breakout_details.get('nifty_breaks_low', False) if isinstance(breakout_details, dict) else False
            future_breaks_high = breakout_details.get('future_breaks_high', False) if isinstance(breakout_details, dict) else False
            future_breaks_low = breakout_details.get('future_breaks_low', False) if isinstance(breakout_details, dict) else False
            
            # Create summary for display
            enhanced_signal['breakout_summary'] = {
                'display_text': display_text,
                'nifty_status': 'BROKE HIGH' if nifty_breaks_high else 'BROKE LOW' if nifty_breaks_low else 'HELD',
                'future_status': 'BROKE HIGH' if future_breaks_high else 'BROKE LOW' if future_breaks_low else 'HELD',
                'breakout_type': 'BULLISH' if enhanced_signal.get('signal_type') == 'BUY_CALL' and nifty_breaks_high and future_breaks_high else
                               'BEARISH' if enhanced_signal.get('signal_type') == 'BUY_PUT' and nifty_breaks_low and future_breaks_low else
                               'DIVERGENT',
                'levels': {
                    'nifty_session_high': enhanced_signal.get('session_high'),
                    'nifty_session_low': enhanced_signal.get('session_low'),
                    'future_session_high': enhanced_signal.get('future_session_high'),
                    'future_session_low': enhanced_signal.get('future_session_low'),
                    'nifty_price_at_signal': enhanced_signal.get('nifty_price'),
                    'future_price_at_signal': enhanced_signal.get('future_price')
                }
            }
            
            detailed_signals.append(enhanced_signal)
        
        return JSONResponse(
            content={
                "date": target_date.isoformat(),
                "signals": detailed_signals,
                "count": len(detailed_signals),
                "total_signals": len(signals),
                "filters_applied": {
                    "date": target_date.isoformat(),
                    "session_name": session_name
                },
                "legend": {
                    "breakout_types": {
                        "BULLISH": "Both NIFTY and Future crossed session HIGH",
                        "BEARISH": "Both NIFTY and Future crossed session LOW", 
                        "DIVERGENT": "Only one instrument crossed level"
                    },
                    "signal_logic": {
                        "BUY_CALL": "Generated when both break HIGH OR only one breaks LOW",
                        "BUY_PUT": "Generated when both break LOW OR only one breaks HIGH"
                    }
                },
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting signals with breakout details: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve signals with breakout details: {str(e)}"
        )


@router.post("/signals/debug/manual-check")
async def debug_manual_breakout_check():
    """
    DEBUG: Manually trigger breakout checking for completed sessions
    """
    try:
        from ...services.signal_detection_service import signal_detection_service
        
        current_time = TimezoneUtils.get_ist_now()
        logger.info(f"🔧 DEBUG: Manual breakout check triggered at {current_time.strftime('%H:%M:%S')}")
        
        # Get current prices
        nifty_price_result = await signal_detection_service._get_current_price("NIFTY")
        futures_price_result = await signal_detection_service._get_current_price("NIFTY28AUG25FUT")
        
        debug_info = {
            "current_time": current_time.isoformat(),
            "current_prices": {
                "nifty": nifty_price_result,
                "futures": futures_price_result
            },
            "sessions_status": [],
            "breakout_analysis": []
        }
        
        # Check each completed session
        for session in signal_detection_service.sessions:
            session_status = {
                "name": session.name,
                "is_active": session.is_active,
                "is_completed": session.is_completed,
                "session_data": {}
            }
            
            # Get session data
            if hasattr(session, 'session_data') and session.session_data:
                for symbol, data in session.session_data.items():
                    if isinstance(data, dict):
                        session_status["session_data"][symbol] = {
                            "high": data.get('high'),
                            "low": data.get('low'),
                            "ticks_count": len(data.get('all_ticks', [])),
                            "candles_count": len(data.get('candles', []))
                        }
            
            debug_info["sessions_status"].append(session_status)
            
            # If session should be completed, force check breakouts
            session_date = current_time.date()
            session_start_time = TimezoneUtils.to_ist(
                datetime.combine(session_date, datetime.strptime(session.start_time, "%H:%M").time())
            )
            session_end_time = TimezoneUtils.to_ist(
                datetime.combine(session_date, datetime.strptime(session.end_time, "%H:%M").time())
            )
            
            if current_time > session_end_time:
                logger.info(f"🔧 DEBUG: Manually checking breakouts for {session.name}")
                try:
                    await signal_detection_service._check_breakout_conditions(session, current_time)
                    debug_info["breakout_analysis"].append({
                        "session": session.name,
                        "checked": True,
                        "error": None
                    })
                except Exception as e:
                    debug_info["breakout_analysis"].append({
                        "session": session.name,
                        "checked": False,
                        "error": str(e)
                    })
        
        return JSONResponse(
            content={
                "message": "Manual breakout check completed",
                "debug_info": debug_info,
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error in manual breakout check: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return JSONResponse(
            content={
                "error": str(e),
                "traceback": traceback.format_exc(),
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@router.get("/signals/by-session")
async def get_signals_by_session(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)"),
    limit: int = Query(100, description="Maximum number of signals to return per session", ge=1, le=500)
):
    """
    Get signals grouped by trading session
    
    Returns signals organized by session for clearer analysis
    """
    try:
        import pytz
        IST = pytz.timezone('Asia/Kolkata')
        
        # Parse date
        if not date:
            target_date = datetime.now(IST).date()
        else:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        # Get all signals
        all_signals = await signal_detection_service.get_signal_history(limit=limit*4)  # Get more to filter
        
        # Group signals by session
        sessions = {
            "Morning Opening": [],
            "Mid Morning": [],
            "Pre Lunch": [],
            "Lunch Break": []
        }
        
        for signal in all_signals:
            signal_date = None
            
            # Get signal timestamp - try both timestamp and created_at fields
            signal_timestamp = signal.get('timestamp') or signal.get('created_at')
            if signal_timestamp:
                if isinstance(signal_timestamp, str):
                    signal_dt = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
                else:
                    signal_dt = signal_timestamp
                
                if signal_dt.tzinfo is None:
                    # Treat naive datetime as IST (not UTC)
                    signal_dt_ist = IST.localize(signal_dt)
                else:
                    signal_dt_ist = signal_dt.astimezone(IST)
                signal_date = signal_dt_ist.date()
            
            # Filter by date if specified
            if signal_date == target_date:
                session_name = signal.get('session_name', 'Unknown Session')
                
                # Clean up the signal data for JSON serialization
                clean_signal = dict(signal)
                
                # Convert all datetime fields to strings
                datetime_fields = ['timestamp', 'created_at', 'updated_at']
                for field in datetime_fields:
                    if field in clean_signal and clean_signal[field]:
                        if hasattr(clean_signal[field], 'isoformat'):
                            clean_signal[field] = clean_signal[field].isoformat()
                        else:
                            clean_signal[field] = str(clean_signal[field])
                
                # Add to appropriate session
                if session_name in sessions:
                    sessions[session_name].append(clean_signal)
                else:
                    # Handle unknown sessions
                    if 'Other Sessions' not in sessions:
                        sessions['Other Sessions'] = []
                    sessions['Other Sessions'].append(clean_signal)
        
        # Sort signals within each session by timestamp
        for session_name in sessions:
            sessions[session_name].sort(key=lambda x: x.get('timestamp', ''))
        
        # Calculate summary stats
        total_signals = sum(len(signals) for signals in sessions.values())
        session_summary = {}
        
        for session_name, session_signals in sessions.items():
            if session_signals:  # Only include sessions with signals
                session_summary[session_name] = {
                    'count': len(session_signals),
                    'signal_types': {},
                    'avg_confidence': 0
                }
                
                # Calculate signal type distribution and average confidence
                confidences = []
                for signal in session_signals:
                    signal_type = signal.get('signal_type', 'UNKNOWN')
                    session_summary[session_name]['signal_types'][signal_type] = \
                        session_summary[session_name]['signal_types'].get(signal_type, 0) + 1
                    
                    if signal.get('confidence'):
                        confidences.append(signal['confidence'])
                
                if confidences:
                    session_summary[session_name]['avg_confidence'] = round(sum(confidences) / len(confidences), 2)
        
        return JSONResponse(
            content={
                "date": target_date.isoformat(),
                "sessions": sessions,
                "session_summary": session_summary,
                "total_signals": total_signals,
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting signals by session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signals by session"
        )


@router.get("/signals/today")
async def get_today_signals():
    """
    Get today's signals ONLY - filtered endpoint for current day data
    """
    try:
        import pytz
        import pymongo
        
        IST = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(IST)
        today = current_time.date()
        
        # Direct database access
        mongodb_url = "mongodb+srv://mail2shivap17:syqzpekjQBCc9oee@traders.w2xrjgy.mongodb.net/?retryWrites=true&w=majority&appName=traders"
        client = pymongo.MongoClient(mongodb_url)
        db = client['trading_signals']
        signals_collection = db['signals']
        
        # Get all signals and filter by date
        cursor = signals_collection.find().sort('created_at', -1)
        today_signals = []
        latest_signal_json = None
        
        for signal_doc in cursor:
            # Get the signal date from created_at
            created_at = signal_doc.get('created_at')
            if created_at:
                if isinstance(created_at, str):
                    signal_dt = datetime.fromisoformat(created_at.replace('Z', ''))
                else:
                    signal_dt = created_at
                
                # Convert to IST if needed
                if signal_dt.tzinfo is None:
                    signal_dt_ist = IST.localize(signal_dt)
                else:
                    signal_dt_ist = signal_dt.astimezone(IST)
                
                signal_date = signal_dt_ist.date()
                
                # Include ONLY signals from TODAY
                if signal_date == today:
                    # Convert to JSON serializable format
                    signal_data = {}
                    for key, value in signal_doc.items():
                        if key == '_id':
                            signal_data['_id'] = str(value)
                        elif isinstance(value, datetime):
                            signal_data[key] = value.isoformat()
                        elif value is None and key in ['nifty_price', 'future_price', 'entry_price', 'stop_loss', 'target_1', 'target_2']:
                            # Handle null price values
                            if key in ['nifty_price', 'future_price'] and signal_doc.get('entry_price'):
                                signal_data[key] = signal_doc.get('entry_price')
                            else:
                                signal_data[key] = 0
                        else:
                            signal_data[key] = value
                    today_signals.append(signal_data)
        
        # Get latest signal for display
        if today_signals:
            latest = today_signals[0]
            latest_signal_json = {
                "symbol": latest.get("symbol"),
                "signal_type": latest.get("signal_type"),
                "confidence": latest.get("confidence"),
                "session_name": latest.get("session_name"),
                "timestamp": latest.get("timestamp") or latest.get("created_at")
            }
        
        client.close()
        
        # Check if in market hours
        is_market_hours = (
            current_time.weekday() < 5 and  # Monday-Friday
            current_time.hour >= 9 and current_time.hour < 16  # 9 AM - 4 PM
        )
        
        return JSONResponse(
            content={
                "date": today.isoformat(),
                "signals": today_signals,
                "count": len(today_signals),
                "all_signals": today_signals,  # For compatibility with frontend
                "latest_signal": latest_signal_json,
                "monitoring_active": True,
                "market_hours": is_market_hours,
                "current_time_ist": current_time.isoformat(),
                "service_status": "running",
                "signals_count": len(today_signals),
                "source": "today_only_filtered",
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting today's signals: {e}")
        return JSONResponse(
            content={
                "error": str(e),
                "signals": [],
                "count": 0,
                "all_signals": [],
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )


@router.get("/signals/performance")
async def get_signal_performance():
    """
    Get signal performance analytics
    
    Returns statistics on signal accuracy, profitability, and other metrics
    """
    try:
        signals = await signal_detection_service.get_signal_history(limit=100)
        
        # Calculate performance metrics
        total_signals = len(signals)
        active_signals = len([s for s in signals if s.get('status') == 'ACTIVE'])
        completed_signals = len([s for s in signals if s.get('status') == 'COMPLETED'])
        
        # Signal type distribution
        signal_types = {}
        session_distribution = {}
        confidence_distribution = {'high': 0, 'medium': 0, 'low': 0}
        
        for signal in signals:
            # Signal type
            signal_type = signal.get('signal_type', 'UNKNOWN')
            signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
            
            # Session distribution
            session_name = signal.get('session_name', 'UNKNOWN')
            session_distribution[session_name] = session_distribution.get(session_name, 0) + 1
            
            # Confidence distribution
            confidence = signal.get('confidence', 50)
            if confidence >= 80:
                confidence_distribution['high'] += 1
            elif confidence >= 60:
                confidence_distribution['medium'] += 1
            else:
                confidence_distribution['low'] += 1
        
        # Average confidence
        confidences = [s.get('confidence', 50) for s in signals]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        performance_data = {
            'total_signals': total_signals,
            'active_signals': active_signals,
            'completed_signals': completed_signals,
            'signal_types': signal_types,
            'session_distribution': session_distribution,
            'confidence_distribution': confidence_distribution,
            'average_confidence': round(avg_confidence, 2),
            'last_24h_signals': len([s for s in signals 
                                   if s.get('timestamp') and 
                                   (lambda dt: (TimezoneUtils.get_ist_now() - dt.replace(tzinfo=None)).total_seconds() <= 86400)(
                                       datetime.fromisoformat(
                                           s['timestamp'].replace('Z', '+00:00') if isinstance(s['timestamp'], str) 
                                           else s['timestamp'].isoformat()
                                       )
                                   )])
        }
        
        return JSONResponse(
            content={
                "performance": performance_data,
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting signal performance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signal performance"
        )