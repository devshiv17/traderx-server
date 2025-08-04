#!/usr/bin/env python3
"""
Script to create unique index on tick_data collection to prevent duplicates
"""

import asyncio
import motor.motor_asyncio
from datetime import datetime

async def create_unique_index():
    """Create unique index on tick_data collection"""
    mongo_uri = "mongodb+srv://mail2shivap17:syqzpekjQBCc9oee@traders.w2xrjgy.mongodb.net/?retryWrites=true&w=majority&appName=traders"
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    db = client.trading_signals
    collection = db.tick_data
    
    try:
        # Create unique compound index on symbol, price, and timestamp (rounded to nearest second)
        # This prevents exact duplicates
        index_name = "unique_symbol_price_timestamp"
        
        # Check if index already exists
        existing_indexes = await collection.list_indexes().to_list(None)
        index_names = [idx['name'] for idx in existing_indexes]
        
        if index_name in index_names:
            print(f"✅ Index {index_name} already exists")
        else:
            # Create unique index
            await collection.create_index(
                [
                    ("symbol", 1),
                    ("price", 1),
                    ("timestamp", 1)
                ],
                unique=True,
                name=index_name
            )
            print(f"✅ Created unique index: {index_name}")
        
        # Create additional index for efficient querying
        query_index_name = "symbol_timestamp"
        if query_index_name not in index_names:
            await collection.create_index(
                [
                    ("symbol", 1),
                    ("timestamp", -1)
                ],
                name=query_index_name
            )
            print(f"✅ Created query index: {query_index_name}")
        
        # Show all indexes
        print("\n📊 Current indexes:")
        all_indexes = await collection.list_indexes().to_list(None)
        for idx in all_indexes:
            print(f"  - {idx['name']}: {idx['key']}")
        
    except Exception as e:
        print(f"❌ Error creating index: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(create_unique_index()) 