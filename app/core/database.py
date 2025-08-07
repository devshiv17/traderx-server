from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings
import logging

logger = logging.getLogger(__name__)


class Database:
    client: AsyncIOMotorClient = None
    database = None


async def connect_to_mongo():
    """Create database connection."""
    try:
        # Create MongoDB client with proper configuration
        Database.client = AsyncIOMotorClient(
            settings.mongodb_url,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000
        )
        
        Database.database = Database.client[settings.database_name]
        
        # Test the connection
        await Database.client.admin.command('ping')
        logger.info("✅ Connected to MongoDB Atlas successfully.")
        
        # Initialize database schema and indexes
        await initialize_database()
        
    except Exception as e:
        logger.error(f"❌ Could not connect to MongoDB: {e}")
        logger.info("🔄 Trying alternative connection method...")
        
        # Try alternative connection with different parameters
        try:
            # Use a simpler connection string
            alt_url = "mongodb+srv://mail2shivap17:syqzpekjQBCc9oee@traders.w2xrjgy.mongodb.net/trading_signals?retryWrites=true&w=majority"
            Database.client = AsyncIOMotorClient(
                alt_url,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000
            )
            Database.database = Database.client[settings.database_name]
            
            # Test the connection
            await Database.client.admin.command('ping')
            logger.info("✅ Connected to MongoDB using alternative method.")
            
            # Initialize database schema and indexes
            await initialize_database()
            
        except Exception as e2:
            logger.error(f"❌ Alternative connection also failed: {e2}")
            logger.error("🔧 Please check your MongoDB Atlas configuration:")
            logger.error("   1. Verify your IP is whitelisted in Atlas")
            logger.error("   2. Check your username/password")
            logger.error("   3. Ensure the cluster is running")
            logger.error("   4. Check if the cluster name is correct")
            raise e2


async def initialize_database():
    """Initialize database collections and indexes."""
    try:
        logger.info("🔧 Initializing database schema and indexes...")
        
        # Users collection indexes
        users_collection = get_collection("users")
        await users_collection.create_index("email", unique=True)
        await users_collection.create_index("is_active")
        await users_collection.create_index([("created_at", -1)])
        logger.info("✅ Users collection indexes created")
        
        # Market data collection indexes
        market_data_collection = get_collection("market_data")
        await market_data_collection.create_index("tk")  # Token/Symbol
        await market_data_collection.create_index([("received_at", -1)])  # Descending for latest first
        await market_data_collection.create_index("source")
        await market_data_collection.create_index("processed")
        await market_data_collection.create_index([("tk", 1), ("received_at", -1)])  # Compound index
        logger.info("✅ Market data collection indexes created")
        
        # Signals collection indexes
        signals_collection = get_collection("signals")
        await signals_collection.create_index("user_id")
        await signals_collection.create_index("symbol")
        await signals_collection.create_index("status")
        await signals_collection.create_index([("created_at", -1)])
        await signals_collection.create_index([("user_id", 1), ("status", 1)])  # Compound index
        await signals_collection.create_index([("symbol", 1), ("created_at", -1)])  # Compound index
        
        # Create unique index to prevent duplicate signals within same session and signal type
        # This allows multiple breakouts per session but prevents exact duplicates
        try:
            await signals_collection.create_index([
                ("session_name", 1),
                ("signal_type", 1),
                ("timestamp", 1)
            ], unique=True, background=True)
            logger.info("✅ Signals unique constraint created: session_name + signal_type + timestamp")
        except Exception as e:
            # If index already exists or there are duplicate records, log and continue
            logger.warning(f"⚠️ Could not create unique index for signals: {e}")
            
        # Additional index for better query performance
        await signals_collection.create_index([("session_name", 1), ("status", 1)])
        await signals_collection.create_index("id", unique=True)  # Ensure signal IDs are unique
        
        logger.info("✅ Signals collection indexes created")
        
        logger.info("✅ Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error initializing database: {e}")
        raise e


async def close_mongo_connection():
    """Close database connection."""
    try:
        if Database.client:
            Database.client.close()
            logger.info("🔌 Closed MongoDB connection.")
    except Exception as e:
        logger.error(f"Error closing MongoDB connection: {e}")


def get_database():
    """Get database instance."""
    return Database.database


def get_collection(collection_name: str):
    """Get collection instance."""
    if Database.database is None:
        logger.error(f"❌ Database not connected. Cannot get collection: {collection_name}")
        raise RuntimeError("Database connection not established. Please ensure the application is properly started.")
    return Database.database[collection_name] 