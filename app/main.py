from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import logging
import time
import asyncio

from .core.config import settings
from .core.database import connect_to_mongo, close_mongo_connection
from .api.v1.api import api_router
from .services.angel_one_service import angel_one_service
from .services.signal_detection_service import signal_detection_service
from .ws import router as ws_router
from .monitor_service import start_monitoring_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="Trading Signals API - A production-ready FastAPI backend for trading signals",
    version="1.0.0",
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    openapi_url="/openapi.json" if settings.debug else None,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Exception handlers
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": "Validation error", "errors": exc.errors()},
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )

# Database events
@app.on_event("startup")
async def startup_db_client():
    try:
        logger.info("🚀 Starting Trading Signals API...")
        
        # Step 1: Connect to MongoDB first
        logger.info("📊 Connecting to MongoDB Atlas...")
        await connect_to_mongo()
        logger.info("✅ MongoDB connection established")
        
        # Step 2: Start Angel One service (but don't wait for it to fully complete)
        logger.info("📈 Starting Angel One service...")
        asyncio.create_task(angel_one_service.start_feed_service())
        
        # Step 3: Start WebSocket queue processor
        logger.info("🔄 Starting WebSocket queue processor...")
        asyncio.create_task(angel_one_service.process_ws_queue())
        
        # Step 4: Start signal detection service with safe initialization
        logger.info("🎯 Starting signal detection service...")
        asyncio.create_task(safe_start_signal_detection())
        
        # Step 5: Start monitoring service
        logger.info("📊 Starting monitoring service...")
        asyncio.create_task(start_monitoring_service())
        
        logger.info("✅ All services started successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during startup: {e}")
        # Don't crash the server, just log the error


async def safe_start_signal_detection():
    """Safely start signal detection service with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await signal_detection_service.start_monitoring()
            logger.info("✅ Signal detection service started successfully")
            return
        except Exception as e:
            logger.warning(f"⚠️ Signal detection service start attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)  # Wait 5 seconds before retry
            else:
                logger.error("❌ Signal detection service failed to start after all retries")

@app.on_event("shutdown")
async def shutdown_db_client():
    # Stop signal detection service
    await signal_detection_service.stop_monitoring()
    # Stop Angel One service
    await angel_one_service.stop_feed_service()
    # Close database connection
    await close_mongo_connection()

# Health check endpoint
@app.get("/health", tags=["health"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "service": settings.app_name,
        "version": "1.0.0"
    }

# Simple health check for monitoring
@app.get("/health/simple", tags=["health"])
async def simple_health_check():
    """Simple health check for monitoring service."""
    try:
        # Quick check if Angel One service is running
        from .services.angel_one_service import angel_one_service
        angel_status = {
            "is_connected": angel_one_service.is_connected,
            "is_authenticated": bool(angel_one_service.auth_token),
            "last_data_received": angel_one_service.last_data_received is not None
        }
        
        return {
            "status": "healthy",
            "server": "running",
            "angel_one": angel_status,
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": time.time()
        }

# Root endpoint
@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Welcome to Trading Signals API",
        "version": "1.0.0",
        "docs": "/docs" if settings.debug else "Documentation disabled in production",
        "health": "/health"
    }

# Include API routes
app.include_router(api_router, prefix="/api/v1")
app.include_router(ws_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info"
    ) 