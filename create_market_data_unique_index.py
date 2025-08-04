#!/usr/bin/env python3
"""
Create a unique index on (symbol, received_at) for market_data collection
"""
import asyncio
from app.core.database import connect_to_mongo, close_mongo_connection, get_collection

async def create_unique_index():
    await connect_to_mongo()
    try:
        collection = get_collection("market_data")
        print("🔍 Creating unique index on (symbol, received_at)...")
        result = await collection.create_index([
            ("symbol", 1),
            ("received_at", 1)
        ], unique=True, name="symbol_receivedAt_unique")
        print(f"✅ Index created: {result}")
    finally:
        await close_mongo_connection()

if __name__ == "__main__":
    asyncio.run(create_unique_index()) 