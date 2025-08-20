#!/usr/bin/env python3
"""
Cleanup script to remove old BANKNIFTY, FINNIFTY, SENSEX data
Keep only NIFTY and NIFTY futures data
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database import connect_to_mongo, get_collection
from app.core.symbols import SymbolsConfig

async def cleanup_old_symbols():
    """Remove old symbol data that's no longer needed"""
    try:
        # Connect to database
        await connect_to_mongo()
        print("✅ Connected to database")
        
        # Get valid symbols (NIFTY only)
        valid_symbols = set(SymbolsConfig.get_symbol_names())
        print(f"✅ Valid symbols: {valid_symbols}")
        
        # Collections to clean
        collections_to_clean = ["tick_data", "market_data", "signals"]
        
        total_deleted = 0
        
        for collection_name in collections_to_clean:
            try:
                collection = get_collection(collection_name)
                
                # Count documents with invalid symbols
                invalid_query = {"symbol": {"$nin": list(valid_symbols)}}
                invalid_count = await collection.count_documents(invalid_query)
                
                if invalid_count > 0:
                    print(f"\n📊 Found {invalid_count} invalid records in {collection_name}")
                    
                    # Get sample of what would be deleted
                    sample_docs = await collection.find(invalid_query).limit(5).to_list(length=5)
                    print(f"   Sample symbols to be removed: {[doc.get('symbol', 'Unknown') for doc in sample_docs]}")
                    
                    # Delete invalid symbols
                    result = await collection.delete_many(invalid_query)
                    deleted_count = result.deleted_count
                    total_deleted += deleted_count
                    
                    print(f"✅ Deleted {deleted_count} records from {collection_name}")
                else:
                    print(f"✅ No invalid records in {collection_name}")
                    
            except Exception as e:
                print(f"❌ Error cleaning {collection_name}: {e}")
        
        print(f"\n🎯 CLEANUP SUMMARY:")
        print(f"   Total records deleted: {total_deleted}")
        print(f"   Collections cleaned: {len(collections_to_clean)}")
        print(f"   Remaining valid symbols: {valid_symbols}")
        print(f"✅ Database now contains only NIFTY and NIFTY futures data")
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")

async def show_current_symbols():
    """Show what symbols are currently in the database"""
    try:
        await connect_to_mongo()
        
        collections_to_check = ["tick_data", "market_data"]
        
        print("\n📊 CURRENT SYMBOLS IN DATABASE:")
        print("-" * 50)
        
        for collection_name in collections_to_check:
            try:
                collection = get_collection(collection_name)
                
                # Get unique symbols
                pipeline = [
                    {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                
                symbols_data = await collection.aggregate(pipeline).to_list(length=None)
                
                print(f"\n{collection_name.upper()}:")
                if symbols_data:
                    for item in symbols_data:
                        symbol = item["_id"]
                        count = item["count"]
                        status = "✅ VALID" if symbol in SymbolsConfig.get_symbol_names() else "❌ INVALID"
                        print(f"   {symbol:<20} | {count:>8} records | {status}")
                else:
                    print("   No data found")
                    
            except Exception as e:
                print(f"❌ Error checking {collection_name}: {e}")
        
    except Exception as e:
        print(f"❌ Failed to show symbols: {e}")

async def main():
    print("=" * 60)
    print("NIFTY-ONLY DATABASE CLEANUP")
    print("=" * 60)
    
    # Show current state
    await show_current_symbols()
    
    # Ask for confirmation
    print(f"\n⚠️  This will delete all data for symbols other than: {SymbolsConfig.get_symbol_names()}")
    response = input("\nDo you want to proceed with cleanup? (y/N): ").strip().lower()
    
    if response == 'y':
        print("\n🧹 Starting cleanup...")
        await cleanup_old_symbols()
        
        print("\n📊 Showing updated symbol list...")
        await show_current_symbols()
    else:
        print("❌ Cleanup cancelled")

if __name__ == "__main__":
    asyncio.run(main())