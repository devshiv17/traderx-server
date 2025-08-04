#!/usr/bin/env python3
"""
Monitor live data insertion from Angel One WebSocket
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.core.database import connect_to_mongo, close_mongo_connection, get_collection


async def monitor_live_data():
    """Monitor live data insertion"""
    
    print("👀 Monitoring Live Data from Angel One WebSocket")
    print("=" * 60)
    
    try:
        # Connect to database
        await connect_to_mongo()
        collection = get_collection("market_data")
        
        print("🔍 Monitoring for new real-time data...")
        print("📊 Subscribed tokens: NIFTY, FINNIFTY, RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK, HINDUNILVR, ITC, BHARTIARTL, AXISBANK")
        print("⏰ Press Ctrl+C to stop monitoring")
        print()
        
        # Get initial count
        last_count = await collection.count_documents({"source": "angel_one_websocket"})
        print(f"📊 Initial WebSocket data count: {last_count}")
        
        # Monitor for new data
        while True:
            await asyncio.sleep(10)  # Check every 10 seconds
            
            current_count = await collection.count_documents({"source": "angel_one_websocket"})
            if current_count > last_count:
                new_records = current_count - last_count
                print(f"📈 New WebSocket records: +{new_records} (Total: {current_count})")
                
                # Get the latest records
                latest_records = await collection.find(
                    {"source": "angel_one_websocket"}
                ).sort("received_at", -1).limit(new_records).to_list(length=new_records)
                
                for record in reversed(latest_records):  # Show oldest first
                    symbol = record.get("symbol", "Unknown")
                    price = record.get("ltpc", 0)
                    change = record.get("ch", 0)
                    timestamp = record.get("received_at")
                    print(f"    📊 {symbol}: ₹{price:,.2f} ({change:+.2f}) - {timestamp}")
                
                last_count = current_count
            else:
                print(f"⏳ No new WebSocket data... (Total: {current_count})")
            
            # Show summary every minute
            if datetime.utcnow().second < 10:
                print(f"📊 Summary: {current_count} total WebSocket records")
        
    except KeyboardInterrupt:
        print("\n⏹️ Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Monitoring failed: {e}")
    finally:
        await close_mongo_connection()


if __name__ == "__main__":
    print("🚀 Starting Live Data Monitor")
    print("=" * 60)
    
    # Run monitoring
    asyncio.run(monitor_live_data())
    
    print("\n✅ Monitoring completed!") 