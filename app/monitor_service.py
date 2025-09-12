#!/usr/bin/env python3

import asyncio
import requests
import json
from datetime import datetime
import time
from app.utils.timezone_utils import TimezoneUtils

async def check_service_health():
    """Check the health of both market data and Angel One services"""
    try:
        # Import services directly since we're in the same process
        from app.services.angel_one_service import angel_one_service
        from app.services.market_data_service import market_data_service
        
        print("✅ Checking services directly (no HTTP requests)")
        
        # Check Angel One service directly
        try:
            angel_status = {
                "is_authenticated": bool(angel_one_service.auth_token),
                "is_connected": angel_one_service.is_connected,
                "last_data_received": angel_one_service.last_data_received is not None,
                "status": "healthy" if angel_one_service.is_connected else "disconnected"
            }
        except Exception as e:
            print(f"❌ Angel One service check failed: {e}")
            angel_status = None
        
        # Check market data service directly
        try:
            # Get available symbols directly from the service
            symbols = await market_data_service.get_available_symbols()
            
            # Filter to only show NIFTY index and futures
            allowed_symbols = [
                {"symbol": "NIFTY", "name": "NIFTY 50", "exchange": "NSE"},
                {"symbol": "NIFTY30SEP25FUT", "name": "NIFTY September 2025 Futures", "exchange": "NFO"}
            ]
            
            # Filter to only include symbols that have data
            available_symbols = [s for s in allowed_symbols if s["symbol"] in symbols]
            
            market_status = {
                "status": "healthy",
                "available_symbols": len(available_symbols),
                "symbols": available_symbols,
                "service": "market_data"
            }
        except Exception as e:
            print(f"❌ Market data service check failed: {e}")
            market_status = None
        
        return market_status, angel_status
        
    except Exception as e:
        print(f"❌ Error checking service health: {e}")
        return None, None

def print_health_status(market_status, angel_status):
    """Print formatted health status"""
    print("=" * 80)
    print(f"🔍 Service Health Check - {TimezoneUtils.get_ist_now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 80)
    
    # Market Data Service Status
    print("\n📊 MARKET DATA SERVICE:")
    print("-" * 40)
    if market_status:
        print(f"✅ Status: {market_status.get('status', 'Unknown')}")
        print(f"📈 Available Symbols: {market_status.get('available_symbols', 'N/A')}")
        symbols = market_status.get('symbols', [])
        if symbols:
            symbol_names = [s.get('name', s.get('symbol', '')) for s in symbols]
            print(f"📋 Symbols: {', '.join(symbol_names)}")
    else:
        print("❌ Market Data Service: Unavailable")
    
    # Angel One Service Status
    print("\n🔗 ANGEL ONE SERVICE:")
    print("-" * 40)
    if angel_status:
        print(f"✅ Status: {angel_status.get('status', 'Unknown')}")
        print(f"🔐 Authenticated: {angel_status.get('is_authenticated', 'N/A')}")
        print(f"📡 Connected: {angel_status.get('is_connected', 'N/A')}")
        print(f"📊 Data Received: {angel_status.get('last_data_received', 'N/A')}")
    else:
        print("❌ Angel One Service: Unavailable")
    
    print("\n" + "=" * 80)

async def start_monitoring_service():
    """Async monitoring service that can be started from the main app"""
    print("🚀 Starting Angel One Service Monitor (Background)")
    
    # Wait for server to be ready before starting health checks
    print("⏳ Waiting 15 seconds for server to be ready...")
    await asyncio.sleep(15)
    
    # Try to connect a few times before giving up
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            # Quick test to see if server is responding
            test_response = requests.get("http://localhost:8000/health/simple", timeout=5)
            if test_response.status_code == 200:
                print("✅ Server is ready and responding")
                break
            else:
                print(f"⚠️ Server responded with status {test_response.status_code}")
        except Exception as e:
            print(f"🔄 Retry {retry_count + 1}/{max_retries}: Server not ready yet ({e})")
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(5)  # Wait 5 seconds before retry
            else:
                print("❌ Server not responding after retries, starting monitoring anyway...")
    
    while True:
        try:
            market_status, angel_status = await check_service_health()
            print_health_status(market_status, angel_status)
            
            # Wait 30 seconds before next check
            await asyncio.sleep(30)
            
        except Exception as e:
            print(f"❌ Monitoring error: {e}")
            await asyncio.sleep(30)  # Wait before retrying

async def main_async():
    """Async main monitoring loop for standalone execution"""
    print("🚀 Starting Angel One Service Monitor")
    print("Press Ctrl+C to stop monitoring")
    
    try:
        while True:
            market_status, angel_status = await check_service_health()
            print_health_status(market_status, angel_status)
            
            # Wait 30 seconds before next check
            print("⏳ Waiting 30 seconds for next check...")
            await asyncio.sleep(30)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Monitoring error: {e}")

def main():
    """Main monitoring loop for standalone execution"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 