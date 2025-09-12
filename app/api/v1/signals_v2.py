"""
API endpoints for Database-Backed Signal Detection V2
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
import logging

from ...services.signal_detection_service_v2 import signal_detection_service_v2
from ...utils.timezone_utils import TimezoneUtils

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v2", tags=["signals-v2"])


@router.post("/signals/start-monitoring")
async def start_signal_monitoring_v2():
    """Start database-backed signal monitoring (V2)"""
    try:
        logger.info("🚀 Starting database-backed signal monitoring V2...")
        
        # Check if already running
        if signal_detection_service_v2.monitoring_active:
            return JSONResponse({
                "message": "Database-backed signal monitoring is already active",
                "status": "already_running",
                "timestamp": TimezoneUtils.get_ist_now().isoformat(),
                "version": "v2"
            })
        
        # Start the service
        await signal_detection_service_v2.start_monitoring()
        
        return JSONResponse({
            "message": "Database-backed signal monitoring started successfully",
            "status": "running",
            "timestamp": TimezoneUtils.get_ist_now().isoformat(),
            "version": "v2",
            "features": [
                "Database-backed session state",
                "No race conditions",
                "Atomic operations",
                "Persistent state",
                "Real-time breakout detection"
            ]
        })
        
    except Exception as e:
        logger.error(f"Error starting signal monitoring V2: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to start database-backed signal monitoring"
        )


@router.post("/signals/stop-monitoring")
async def stop_signal_monitoring_v2():
    """Stop database-backed signal monitoring (V2)"""
    try:
        await signal_detection_service_v2.stop_monitoring()
        
        return JSONResponse({
            "message": "Database-backed signal monitoring stopped successfully",
            "status": "stopped",
            "timestamp": TimezoneUtils.get_ist_now().isoformat(),
            "version": "v2"
        })
        
    except Exception as e:
        logger.error(f"Error stopping signal monitoring V2: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to stop database-backed signal monitoring"
        )


@router.get("/signals/status")
async def get_monitoring_status_v2():
    """Get database-backed monitoring status (V2)"""
    try:
        current_time = TimezoneUtils.get_ist_now()
        
        # Get session status from database
        sessions = await signal_detection_service_v2.get_session_status()
        
        # Get active signals
        active_signals = await signal_detection_service_v2.get_active_signals()
        
        # Calculate status
        is_monitoring = signal_detection_service_v2.monitoring_active
        market_hours = TimezoneUtils.is_market_hours(current_time)
        
        # Count sessions by status
        session_counts = {"PENDING": 0, "ACTIVE": 0, "COMPLETED": 0}
        for session in sessions:
            session_counts[session['status']] = session_counts.get(session['status'], 0) + 1
        
        return {
            "version": "v2",
            "monitoring_active": is_monitoring,
            "market_hours": market_hours,
            "current_time_ist": current_time.isoformat(),
            "service_status": "running" if is_monitoring else "stopped",
            
            # Session information
            "sessions": {
                "total": len(sessions),
                "by_status": session_counts,
                "predefined_sessions": [
                    "Morning Opening (9:30-9:35)",
                    "Mid Morning (9:45-9:55)", 
                    "Pre Lunch (10:30-10:45)",
                    "Lunch Break (11:50-12:20)"
                ]
            },
            
            # Signals information
            "signals": {
                "active_count": len(active_signals)
            },
            
            "timestamp": TimezoneUtils.get_ist_now().isoformat(),
            "database_backed": True,
            "race_condition_safe": True
        }
        
    except Exception as e:
        logger.error(f"Error getting monitoring status V2: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get monitoring status"
        )


@router.get("/signals/sessions")
async def get_sessions_v2():
    """Get database-backed session status (V2)"""
    try:
        sessions = await signal_detection_service_v2.get_session_status()
        
        return {
            "version": "v2",
            "sessions": sessions,
            "total_count": len(sessions),
            "timestamp": TimezoneUtils.get_ist_now().isoformat(),
            "source": "database"
        }
        
    except Exception as e:
        logger.error(f"Error getting sessions V2: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get session information"
        )


@router.get("/signals/active")
async def get_active_signals_v2():
    """Get active signals from database (V2)"""
    try:
        signals = await signal_detection_service_v2.get_active_signals()
        
        return {
            "version": "v2",
            "signals": signals,
            "count": len(signals),
            "timestamp": TimezoneUtils.get_ist_now().isoformat(),
            "source": "database"
        }
        
    except Exception as e:
        logger.error(f"Error getting active signals V2: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get active signals"
        )


# Initialize database collections and indexes
@router.post("/admin/init-database")
async def init_database():
    """Initialize database collections for V2 (admin endpoint)"""
    try:
        from ...core.database import get_collection
        
        # Create indexes for session_states collection
        session_collection = get_collection("session_states")
        
        # Create compound indexes
        await session_collection.create_index([
            ("trading_date", 1),
            ("status", 1)
        ])
        
        await session_collection.create_index([
            ("trading_date", 1),
            ("session_name", 1)
        ])
        
        await session_collection.create_index([
            ("status", 1),
            ("completed_at", 1)
        ])
        
        # Initialize today's sessions
        from ...models.session_state import session_state_service
        today = TimezoneUtils.get_ist_now().strftime('%Y-%m-%d')
        await session_state_service.initialize_daily_sessions(today)
        
        return {
            "message": "Database initialized successfully for V2",
            "collections": ["session_states"],
            "indexes_created": 3,
            "sessions_initialized": 4,
            "trading_date": today,
            "predefined_sessions": [
                "Morning Opening (9:30-9:35)",
                "Mid Morning (9:45-9:55)", 
                "Pre Lunch (10:30-10:45)",
                "Lunch Break (11:50-12:20)"
            ],
            "timestamp": TimezoneUtils.get_ist_now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to initialize database"
        )