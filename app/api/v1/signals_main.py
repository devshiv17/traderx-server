"""
Main Trading Signals API - V2 Database-Backed Service
Uses the improved database-backed signal detection service
"""

from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict
import logging
from datetime import datetime

from ...services.signal_detection_service_v2 import signal_detection_service_v2
from ...utils.timezone_utils import TimezoneUtils

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/signals/start-monitoring")
async def start_signal_monitoring():
    """
    Start V2 database-backed signal monitoring service
    """
    try:
        await signal_detection_service_v2.start_monitoring()
        
        return JSONResponse(
            content={
                "message": "V2 Database-backed signal monitoring started successfully",
                "service_version": "v2",
                "status": "running",
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error starting V2 signal monitoring: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start V2 signal monitoring"
        )


@router.post("/signals/stop-monitoring")
async def stop_signal_monitoring():
    """
    Stop V2 database-backed signal monitoring service
    """
    try:
        await signal_detection_service_v2.stop_monitoring()
        
        return JSONResponse(
            content={
                "message": "V2 Database-backed signal monitoring stopped successfully",
                "service_version": "v2",
                "status": "stopped",
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error stopping V2 signal monitoring: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to stop V2 signal monitoring"
        )


@router.get("/signals/active")
async def get_active_signals():
    """
    Get all currently active trading signals from V2 service
    """
    try:
        active_signals = await signal_detection_service_v2.get_active_signals()
        
        # Convert datetime objects for JSON serialization
        json_signals = []
        for signal in active_signals:
            json_signal = dict(signal)
            for key, value in json_signal.items():
                if isinstance(value, datetime):
                    json_signal[key] = TimezoneUtils.format_for_api(value)
            json_signals.append(json_signal)
        
        return JSONResponse(
            content={
                "signals": json_signals,
                "count": len(json_signals),
                "service_version": "v2",
                "timestamp": TimezoneUtils.get_ist_now().isoformat()
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting V2 active signals: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve active signals"
        )


@router.get("/signals/sessions")
async def get_session_status():
    """
    Get current trading session status from V2 database-backed service
    """
    try:
        sessions = await signal_detection_service_v2.get_session_status()
        
        # Convert datetime objects for JSON serialization
        json_sessions = []
        for session in sessions:
            json_session = dict(session)
            for key, value in json_session.items():
                if isinstance(value, datetime):
                    json_session[key] = TimezoneUtils.format_for_api(value)
                elif isinstance(value, dict):
                    # Handle nested datetime objects in symbols_data
                    for nested_key, nested_value in value.items():
                        if isinstance(nested_value, datetime):
                            json_session[key][nested_key] = TimezoneUtils.format_for_api(nested_value)
            json_sessions.append(json_session)
        
        return JSONResponse(
            content={
                "date": TimezoneUtils.get_ist_now().date().isoformat(),
                "sessions": json_sessions,
                "count": len(json_sessions),
                "service_version": "v2",
                "predefined_sessions": [
                    "Morning Opening (9:30-9:35)",
                    "Mid Morning (9:45-9:55)", 
                    "Pre Lunch (10:30-10:45)",
                    "Lunch Break (11:50-12:20)"
                ],
                "current_time_ist": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now()),
                "market_status": "open" if TimezoneUtils.is_market_hours() else "closed",
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting V2 session status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve session status"
        )


@router.get("/signals/monitoring-status")
async def get_monitoring_status():
    """
    Get V2 monitoring service status
    """
    try:
        current_time = TimezoneUtils.get_ist_now()
        is_market_hours = TimezoneUtils.is_market_hours()
        is_monitoring = signal_detection_service_v2.monitoring_active
        
        # Get active signals count
        active_signals = await signal_detection_service_v2.get_active_signals()
        
        return JSONResponse(
            content={
                "monitoring_active": is_monitoring,
                "market_hours": is_market_hours,
                "service_version": "v2",
                "current_time_ist": TimezoneUtils.format_for_api(current_time),
                "active_signals_count": len(active_signals),
                "service_status": "running" if is_monitoring else "stopped",
                "database_backed": True,
                "session_management": "database_atomic",
                "timestamp": TimezoneUtils.format_for_api(current_time)
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting V2 monitoring status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve monitoring status"
        )


@router.get("/signals/today")
async def get_today_signals():
    """
    Get today's signals from V2 database-backed service
    """
    try:
        # Get active signals (today's signals are typically active)
        signals = await signal_detection_service_v2.get_active_signals()
        
        today = TimezoneUtils.get_ist_now().date()
        today_signals = []
        
        # Filter for today's signals
        for signal in signals:
            signal_date = None
            created_at = signal.get('created_at') or signal.get('timestamp')
            
            if created_at:
                if isinstance(created_at, str):
                    signal_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                else:
                    signal_dt = created_at
                
                signal_dt_ist = TimezoneUtils.to_ist(signal_dt)
                if signal_dt_ist:
                    signal_date = signal_dt_ist.date()
            
            if signal_date == today:
                # Convert datetime objects for JSON
                json_signal = dict(signal)
                for key, value in json_signal.items():
                    if isinstance(value, datetime):
                        json_signal[key] = TimezoneUtils.format_for_api(value)
                today_signals.append(json_signal)
        
        latest_signal = today_signals[0] if today_signals else None
        
        return JSONResponse(
            content={
                "date": today.isoformat(),
                "signals": today_signals,
                "count": len(today_signals),
                "latest_signal": latest_signal,
                "service_version": "v2",
                "database_backed": True,
                "monitoring_active": signal_detection_service_v2.monitoring_active,
                "market_hours": TimezoneUtils.is_market_hours(),
                "current_time_ist": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now()),
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting today's V2 signals: {e}")
        return JSONResponse(
            content={
                "error": str(e),
                "signals": [],
                "count": 0,
                "service_version": "v2",
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )


@router.get("/signals/performance")
async def get_signal_performance():
    """
    Get V2 signal performance analytics
    """
    try:
        # Get active signals for analysis
        signals = await signal_detection_service_v2.get_active_signals()
        
        # Calculate performance metrics
        total_signals = len(signals)
        signal_types = {}
        session_distribution = {}
        confidence_distribution = {'high': 0, 'medium': 0, 'low': 0}
        
        for signal in signals:
            # Signal type distribution
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
            'signal_types': signal_types,
            'session_distribution': session_distribution,
            'confidence_distribution': confidence_distribution,
            'average_confidence': round(avg_confidence, 2),
            'sessions_monitored': [
                "Morning Opening (9:30-9:35)",
                "Mid Morning (9:45-9:55)", 
                "Pre Lunch (10:30-10:45)",
                "Lunch Break (11:50-12:20)"
            ]
        }
        
        return JSONResponse(
            content={
                "performance": performance_data,
                "service_version": "v2",
                "database_backed": True,
                "timestamp": TimezoneUtils.format_for_api(TimezoneUtils.get_ist_now())
            },
            status_code=status.HTTP_200_OK
        )
        
    except Exception as e:
        logger.error(f"Error getting V2 signal performance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve signal performance"
        )