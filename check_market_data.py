#!/usr/bin/env python3

import asyncio
from app.core.database import get_collection
from datetime import datetime
import pytz

IST = pytz.timezone('Asia/Kolkata')

async def check_market_data():
    try:
        collection = get_collection('market_data')
        
        # Check total count
        count = await collection.count_documents({})
        print(f"Total market_data records: {count}")
        
        if count > 0:
            # Get latest record
            latest = await collection.find().sort('timestamp', -1).limit(1).to_list(1)
            if latest:
                latest_record = latest[0]
                print(f"Latest record timestamp: {latest_record.get('timestamp')}")
                print(f"Latest record symbol: {latest_record.get('symbol')}")
                print(f"Latest record ltpc: {latest_record.get('ltpc')}")
                
                # Check time difference
                if latest_record.get('timestamp'):
                    latest_time = latest_record['timestamp']
                    if isinstance(latest_time, str):
                        latest_time = datetime.fromisoformat(latest_time.replace('Z', '+00:00'))
                    current_time = datetime.now(IST)
                    time_diff = current_time - latest_time.replace(tzinfo=IST)
                    print(f"Time since latest data: {time_diff}")
        
        # Check data by symbol
        symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]
        for symbol in symbols:
            symbol_count = await collection.count_documents({'symbol': symbol})
            print(f"{symbol} records: {symbol_count}")
            
            if symbol_count > 0:
                latest_symbol = await collection.find({'symbol': symbol}).sort('timestamp', -1).limit(1).to_list(1)
                if latest_symbol:
                    latest_time = latest_symbol[0].get('timestamp')
                    print(f"  Latest {symbol} time: {latest_time}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_market_data()) 