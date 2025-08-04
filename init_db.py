#!/usr/bin/env python3
"""
Database Initialization Script
This script initializes the MongoDB database with proper collections and indexes.
Run this script once to set up the database schema.
"""

import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def init_database():
    """Initialize the database with collections and indexes."""
    try:
        # Connect to MongoDB
        client = AsyncIOMotorClient(settings.mongodb_url)
        db = client[settings.database_name]
        
        logger.info(f"Connected to MongoDB: {settings.database_name}")
        
        # Create collections and indexes
        await create_users_collection(db)
        await create_market_data_collection(db)
        await create_signals_collection(db)
        
        logger.info("Database initialization completed successfully!")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        client.close()


async def create_users_collection(db):
    """Create users collection with indexes."""
    try:
        collection = db.users
        
        # Create indexes
        await collection.create_index("email", unique=True)
        await collection.create_index("is_active")
        await collection.create_index("created_at")
        
        logger.info("Users collection and indexes created")
        
    except Exception as e:
        logger.error(f"Error creating users collection: {e}")
        raise


async def create_market_data_collection(db):
    """Create market_data collection with indexes."""
    try:
        collection = db.market_data
        
        # Create indexes
        await collection.create_index("tk")  # Token/Symbol
        await collection.create_index([("received_at", -1)])  # Descending for latest first
        await collection.create_index("source")
        await collection.create_index("processed")
        await collection.create_index([("tk", 1), ("received_at", -1)])  # Compound index
        
        logger.info("Market data collection and indexes created")
        
    except Exception as e:
        logger.error(f"Error creating market_data collection: {e}")
        raise


async def create_signals_collection(db):
    """Create signals collection with indexes."""
    try:
        collection = db.signals
        
        # Create indexes
        await collection.create_index("user_id")
        await collection.create_index("symbol")
        await collection.create_index("status")
        await collection.create_index([("created_at", -1)])
        await collection.create_index([("user_id", 1), ("status", 1)])  # Compound index
        await collection.create_index([("symbol", 1), ("created_at", -1)])  # Compound index
        
        logger.info("Signals collection and indexes created")
        
    except Exception as e:
        logger.error(f"Error creating signals collection: {e}")
        raise


async def verify_database():
    """Verify that all collections and indexes exist."""
    try:
        client = AsyncIOMotorClient(settings.mongodb_url)
        db = client[settings.database_name]
        
        # List all collections
        collections = await db.list_collection_names()
        logger.info(f"Collections in database: {collections}")
        
        # Check indexes for each collection
        for collection_name in ['users', 'market_data', 'signals']:
            if collection_name in collections:
                collection = db[collection_name]
                indexes = await collection.list_indexes().to_list(length=None)
                logger.info(f"Indexes in {collection_name}: {[idx['name'] for idx in indexes]}")
            else:
                logger.warning(f"Collection {collection_name} not found")
        
        client.close()
        
    except Exception as e:
        logger.error(f"Error verifying database: {e}")


if __name__ == "__main__":
    print("🚀 Initializing Trading Signals Database...")
    print(f"Database: {settings.database_name}")
    print(f"MongoDB URL: {settings.mongodb_url.split('@')[1]}")  # Hide credentials
    
    # Run initialization
    asyncio.run(init_database())
    
    # Verify the setup
    print("\n🔍 Verifying database setup...")
    asyncio.run(verify_database())
    
    print("\n✅ Database initialization completed!")
    print("You can now start the application with: python run.py") 