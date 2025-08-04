#!/usr/bin/env python3
"""
Cleanup script for market data
Removes records with missing high/low values and other data quality issues
"""

import asyncio
import logging
from datetime import datetime
from bson import ObjectId

from app.core.database import connect_to_mongo, close_mongo_connection
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def cleanup_market_data():
    """Clean up market data by removing problematic records"""
    
    # Connect to database
    await connect_to_mongo()
    
    try:
        # Get the collection
        from app.core.database import get_collection
        collection = get_collection("market_data")
        
        print("🔍 Analyzing market data...")
        
        # Count total records
        total_count = await collection.count_documents({})
        print(f"📊 Total records: {total_count}")
        
        # Find records with missing high/low values
        missing_high_low = await collection.count_documents({
            "$or": [
                {"high": {"$exists": False}},
                {"high": None},
                {"low": {"$exists": False}},
                {"low": None}
            ]
        })
        print(f"❌ Records with missing high/low: {missing_high_low}")
        
        # Find records with zero or missing volume
        zero_volume = await collection.count_documents({
            "$or": [
                {"volume": {"$exists": False}},
                {"volume": None},
                {"volume": 0}
            ]
        })
        print(f"❌ Records with zero/missing volume: {zero_volume}")
        
        # Find records with flat data (high = low = open = close)
        flat_data = await collection.count_documents({
            "$expr": {
                "$and": [
                    {"$eq": ["$high", "$low"]},
                    {"$eq": ["$high", "$open"]},
                    {"$eq": ["$high", "$close"]}
                ]
            }
        })
        print(f"❌ Records with flat data: {flat_data}")
        
        # Find records with missing ltpc
        missing_ltpc = await collection.count_documents({
            "$or": [
                {"ltpc": {"$exists": False}},
                {"ltpc": None},
                {"ltpc": 0}
            ]
        })
        print(f"❌ Records with missing/zero ltpc: {missing_ltpc}")
        
        # Calculate total problematic records
        total_problematic = missing_high_low + zero_volume + flat_data + missing_ltpc
        print(f"\n📊 Total problematic records: {total_problematic}")
        
        if total_problematic > 0:
            # Delete problematic records
            result = await collection.delete_many({
                "$or": [
                    # Missing high/low
                    {
                        "$or": [
                            {"high": {"$exists": False}},
                            {"high": None},
                            {"low": {"$exists": False}},
                            {"low": None}
                        ]
                    },
                    # Zero/missing volume
                    {
                        "$or": [
                            {"volume": {"$exists": False}},
                            {"volume": None},
                            {"volume": 0}
                        ]
                    },
                    # Missing ltpc
                    {
                        "$or": [
                            {"ltpc": {"$exists": False}},
                            {"ltpc": None},
                            {"ltpc": 0}
                        ]
                    }
                ]
            })
            
            print(f"✅ Deleted {result.deleted_count} problematic records")
            
            # Count remaining records
            remaining_count = await collection.count_documents({})
            print(f"📊 Remaining records: {remaining_count}")
            
            # Show remaining records by symbol
            pipeline = [
                {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            
            cursor = collection.aggregate(pipeline)
            symbol_counts = await cursor.to_list(length=None)
            
            print("\n📊 Remaining records by symbol:")
            for item in symbol_counts:
                print(f"  {item['_id']}: {item['count']}")
            
        else:
            print("✅ No problematic records found!")
            
        # Check for duplicates (same symbol and timestamp within 1 second)
        print("\n🔍 Checking for duplicates...")
        
        # Get all records and group by symbol and rounded timestamp
        cursor = collection.find({}).sort([("symbol", 1), ("received_at", 1)])
        all_records = await cursor.to_list(length=None)
        
        duplicates_found = 0
        for record in all_records:
            symbol = record.get("symbol")
            timestamp = record.get("received_at")
            
            if symbol and timestamp:
                # Find records with same symbol and timestamp within 1 second
                duplicate_count = await collection.count_documents({
                    "symbol": symbol,
                    "received_at": {
                        "$gte": timestamp,
                        "$lte": timestamp.replace(second=timestamp.second + 1)
                    }
                })
                
                if duplicate_count > 1:
                    duplicates_found += duplicate_count - 1
                    print(f"  Found {duplicate_count} records for {symbol} at {timestamp}")
        
        if duplicates_found > 0:
            print(f"❌ Total duplicate records: {duplicates_found}")
            
            # Remove duplicates (keep the first one for each symbol-timestamp group)
            pipeline = [
                {"$sort": {"received_at": 1}},
                {"$group": {
                    "_id": {
                        "symbol": "$symbol",
                        "timestamp": {
                            "$dateToString": {
                                "format": "%Y-%m-%d %H:%M:%S",
                                "date": "$received_at"
                            }
                        }
                    },
                    "first_id": {"$first": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {"count": {"$gt": 1}}}
            ]
            
            cursor = collection.aggregate(pipeline)
            duplicate_groups = await cursor.to_list(length=None)
            
            for group in duplicate_groups:
                # Delete all but the first record in each duplicate group
                result = await collection.delete_many({
                    "symbol": group["_id"]["symbol"],
                    "received_at": {
                        "$gte": datetime.strptime(group["_id"]["timestamp"], "%Y-%m-%d %H:%M:%S"),
                        "$lt": datetime.strptime(group["_id"]["timestamp"], "%Y-%m-%d %H:%M:%S").replace(second=datetime.strptime(group["_id"]["timestamp"], "%Y-%m-%d %H:%M:%S").second + 1)
                    },
                    "_id": {"$ne": group["first_id"]}
                })
                print(f"  Deleted {result.deleted_count} duplicates for {group['_id']['symbol']}")
        else:
            print("✅ No duplicates found!")
        
        # Final count
        final_count = await collection.count_documents({})
        print(f"\n🎉 Final record count: {final_count}")
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}")
        raise
    finally:
        await close_mongo_connection()

if __name__ == "__main__":
    asyncio.run(cleanup_market_data()) 