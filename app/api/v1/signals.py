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

logger = logging.getLogger(__name__)

router = APIRouter()


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
                "timestamp": datetime.utcnow().isoformat()
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
                "timestamp": datetime.utcnow().isoformat()
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
                "timestamp": datetime.utcnow().isoformat()
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
        signals = await signal_detection_service.get_signal_history(limit=limit)
        
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
                    "timestamp": datetime.utcnow().isoformat()
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
                    "timestamp": datetime.utcnow().isoformat()
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
    Get current trading session status
    
    Returns information about all trading sessions and their current state
    """
    try:
        sessions = await signal_detection_service.get_session_status()
        
        return JSONResponse(
            content={
                "sessions": sessions,
                "count": len(sessions),
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve session status"
        )


@router.get("/signals/technical/{symbol}")
async def get_technical_analysis(symbol: str):
    """
    Get technical analysis data for a symbol
    
    Returns VWAP, price data, volume analysis, and other technical indicators
    """
    try:
        technical_data = await signal_detection_service.get_technical_data(symbol)
        
        return JSONResponse(
            content={
                "technical_data": technical_data,
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting technical analysis for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve technical analysis for {symbol}"
        )


@router.get("/signals/monitoring-status")
async def get_monitoring_status():
    """
    Get current monitoring service status
    
    Returns whether the signal detection service is currently running
    """
    try:
        import pytz
        IST = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(IST)
        
        # Check if monitoring is active
        is_monitoring = signal_detection_service.monitoring_active
        
        # Check if in market hours
        is_market_hours = (
            current_time.weekday() < 5 and  # Monday-Friday
            current_time.hour >= 9 and current_time.hour < 16  # 9 AM - 4 PM
        )
        
        # Get latest signal
        signals = await signal_detection_service.get_signal_history(limit=1)
        latest_signal = signals[0] if signals else None
        
        return JSONResponse(
            content={
                "monitoring_active": is_monitoring,
                "market_hours": is_market_hours,
                "current_time_ist": current_time.isoformat(),
                "latest_signal": latest_signal,
                "service_status": "running" if is_monitoring else "stopped",
                "timestamp": datetime.utcnow().isoformat()
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
                    signal_dt = pytz.UTC.localize(signal_dt)
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
                    "timestamp": datetime.utcnow().isoformat()
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
                    "timestamp": datetime.utcnow().isoformat()
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
    session_name: Optional[str] = Query(None, description="Filter by session name")
):
    """
    Get signals with detailed breakout information
    
    Shows exact NIFTY and Future breakout conditions instead of just arrows
    """
    try:
        signals = await signal_detection_service.get_signal_history(limit=limit)
        
        # Filter by session if specified
        if session_name:
            signals = [s for s in signals if s.get('session_name') == session_name]
        
        # Enhanced signal display with breakout details
        detailed_signals = []
        
        for signal in signals:
            # Clean up the signal data for JSON serialization
            enhanced_signal = dict(signal)
            if 'timestamp' in enhanced_signal and enhanced_signal['timestamp']:
                if hasattr(enhanced_signal['timestamp'], 'isoformat'):
                    enhanced_signal['timestamp'] = enhanced_signal['timestamp'].isoformat()
                else:
                    enhanced_signal['timestamp'] = str(enhanced_signal['timestamp'])
            
            # Add breakout visualization data
            breakout_details = enhanced_signal.get('breakout_details', {})
            display_text = enhanced_signal.get('display_text', '')
            
            # Create summary for display
            enhanced_signal['breakout_summary'] = {
                'display_text': display_text,
                'nifty_status': 'BROKE HIGH' if breakout_details.get('nifty_breaks_high') else 'BROKE LOW' if breakout_details.get('nifty_breaks_low') else 'HELD',
                'future_status': 'BROKE HIGH' if breakout_details.get('future_breaks_high') else 'BROKE LOW' if breakout_details.get('future_breaks_low') else 'HELD',
                'breakout_type': 'BULLISH' if enhanced_signal.get('signal_type') == 'BUY_CALL' and breakout_details.get('nifty_breaks_high') and breakout_details.get('future_breaks_high') else
                               'BEARISH' if enhanced_signal.get('signal_type') == 'BUY_PUT' and breakout_details.get('nifty_breaks_low') and breakout_details.get('future_breaks_low') else
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
                "signals": detailed_signals,
                "count": len(detailed_signals),
                "total_signals": len(signals),
                "filters_applied": {
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
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting signals with breakout details: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signals with breakout details"
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
            
            # Get signal timestamp and convert to IST
            if signal.get('timestamp'):
                if isinstance(signal['timestamp'], str):
                    signal_dt = datetime.fromisoformat(signal['timestamp'].replace('Z', '+00:00'))
                else:
                    signal_dt = signal['timestamp']
                
                if signal_dt.tzinfo is None:
                    signal_dt = pytz.UTC.localize(signal_dt)
                signal_dt_ist = signal_dt.astimezone(IST)
                signal_date = signal_dt_ist.date()
            
            # Filter by date if specified
            if signal_date == target_date:
                session_name = signal.get('session_name', 'Unknown Session')
                
                # Clean up the signal data for JSON serialization
                clean_signal = dict(signal)
                if 'timestamp' in clean_signal and clean_signal['timestamp']:
                    if hasattr(clean_signal['timestamp'], 'isoformat'):
                        clean_signal['timestamp'] = clean_signal['timestamp'].isoformat()
                    else:
                        clean_signal['timestamp'] = str(clean_signal['timestamp'])
                
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
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting signals by session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signals by session"
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
                                   (datetime.utcnow() - datetime.fromisoformat(
                                       s['timestamp'].replace('Z', '+00:00') if isinstance(s['timestamp'], str) 
                                       else s['timestamp'].isoformat()
                                   ).replace(tzinfo=None)).total_seconds() <= 86400])
        }
        
        return JSONResponse(
            content={
                "performance": performance_data,
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting signal performance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signal performance"
        )