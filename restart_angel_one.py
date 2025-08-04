#!/usr/bin/env python3

import asyncio
import logging
from app.services.angel_one_service import angel_one_service

async def restart_angel_one_service():
    """Restart the Angel One service to fix data flow issues"""
    try:
        print("🔄 Restarting Angel One service...")
        
        # Stop the current service
        await angel_one_service.stop_feed_service()
        print("✅ Stopped current service")
        
        # Wait a moment
        await asyncio.sleep(2)
        
        # Start the service again
        await angel_one_service.start_feed_service()
        print("✅ Started Angel One service")
        
        # Start the WebSocket queue processor
        asyncio.create_task(angel_one_service.process_ws_queue())
        print("✅ Started WebSocket queue processor")
        
        print("🎯 Angel One service restarted successfully!")
        
    except Exception as e:
        print(f"❌ Error restarting Angel One service: {e}")

if __name__ == "__main__":
    asyncio.run(restart_angel_one_service()) 