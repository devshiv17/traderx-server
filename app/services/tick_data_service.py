"""
Production-Grade Tick Data Service
Handles real-time market data ingestion, storage, and broadcasting
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pytz
from ..core.database import get_collection
from ..ws import broadcast_market_data, broadcast_price_update

logger = logging.getLogger(__name__)


class TickDataService:
    """Production-grade service for real-time tick data management"""
    
    def __init__(self):
        self.collection = None
        self.logger = logging.getLogger(__name__)
        self.tick_buffer = {}  # Buffer for batch inserts
        self.buffer_size = 10  # Insert after 10 ticks
        self.buffer_timeout = 1.0  # Insert after 1 second
        self.last_flush = datetime.now(pytz.timezone('Asia/Kolkata'))
        
        # Market hours configuration (IST)
        self.market_timezone = pytz.timezone('Asia/Kolkata')
        self.market_open_time = (9, 15)  # 9:15 AM
        self.market_close_time = (15, 30)  # 3:30 PM
        
        # Tick deduplication - improved for indices
        self.last_tick_cache = {}  # symbol -> {price, timestamp}
        self.min_price_change = 0.5  # Minimum price change to store (50 paise for indices)
        self.min_time_interval = 0.5  # Minimum 500ms between ticks for indices
        
    def _get_collection(self):
        """Get the tick data collection"""
        if self.collection is None:
            try:
                from ..core.database import get_collection
                self.collection = get_collection("tick_data")
            except RuntimeError as e:
                # Database not connected, this is expected when running outside FastAPI context
                self.logger.warning(f"Database not connected: {e}")
                raise RuntimeError("Database connection not established. Please ensure the application is properly started.") from e
        return self.collection
    
    def _is_market_hours(self, dt: Optional[datetime] = None) -> bool:
        """Check if current time is within market hours"""
        if dt is None:
            dt = datetime.utcnow()
        
        # Convert to IST
        dt_ist = dt.replace(tzinfo=pytz.UTC).astimezone(self.market_timezone)
        
        # Check if weekday (Monday=0, Sunday=6)
        if dt_ist.weekday() >= 5:
            return False
        
        # Check time range
        current_time = dt_ist.time()
        market_open = dt_ist.replace(
            hour=self.market_open_time[0], 
            minute=self.market_open_time[1], 
            second=0, 
            microsecond=0
        ).time()
        market_close = dt_ist.replace(
            hour=self.market_close_time[0], 
            minute=self.market_close_time[1], 
            second=0, 
            microsecond=0
        ).time()
        
        return market_open <= current_time <= market_close
    
    def _should_store_tick(self, symbol: str, price: float, timestamp: datetime) -> bool:
        """Determine if tick should be stored based on deduplication rules"""
        cache_key = symbol
        
        if cache_key in self.last_tick_cache:
            last_data = self.last_tick_cache[cache_key]
            last_price = last_data['price']
            last_time = last_data['timestamp']
            
            # Check minimum price change
            price_change = abs(price - last_price)
            if price_change < self.min_price_change:
                self.logger.debug(f"Skipping {symbol}: price change {price_change:.2f} < {self.min_price_change}")
                return False
            
            # Check minimum time interval (prevent spam)
            time_diff = (timestamp - last_time).total_seconds()
            if time_diff < self.min_time_interval:
                self.logger.debug(f"Skipping {symbol}: time diff {time_diff:.2f}s < {self.min_time_interval}s")
                return False
            
            # Check if price is exactly the same (likely duplicate)
            if price == last_price:
                self.logger.debug(f"Skipping {symbol}: exact same price {price}")
                return False
        
        # Update cache
        self.last_tick_cache[cache_key] = {
            'price': price,
            'timestamp': timestamp
        }
        
        self.logger.debug(f"✅ Storing tick: {symbol} @ ₹{price:.2f}")
        return True
    
    async def store_tick(self, tick_data: Dict[str, Any]) -> bool:
        """
        Store a single tick in the database with production-grade validation
        
        Args:
            tick_data: Raw tick data from Angel One WebSocket
            
        Returns:
            bool: True if stored successfully
        """
        try:
            # Validate required fields
            if not self._validate_tick_data(tick_data):
                return False
            
            # Check market hours
            now = datetime.now(self.market_timezone)
            if not self._is_market_hours(now):
                self.logger.debug(f"Skipping after-hours tick for {tick_data.get('symbol', 'Unknown')}")
                return False
            
            # Parse tick data
            symbol = tick_data.get('symbol', '').upper()
            price = float(tick_data.get('ltpc', 0))
            
            if not symbol or price <= 0:
                self.logger.warning(f"Invalid tick data: {tick_data}")
                return False
            
            # Check if we should store this tick (deduplication)
            if not self._should_store_tick(symbol, price, now):
                return False
            
            # Prepare tick document
            tick_doc = {
                'symbol': symbol,
                'price': price,
                'timestamp': now,  # Market timestamp
                'token': tick_data.get('token', ''),
                'exchange': tick_data.get('exchange', 'NSE'),
                'high': float(tick_data.get('high', 0)) if tick_data.get('high') else None,
                'low': float(tick_data.get('low', 0)) if tick_data.get('low') else None,
                'volume': int(tick_data.get('volume', 0)) if tick_data.get('volume') else None,
                'change': float(tick_data.get('change', 0)) if tick_data.get('change') else None,
                'change_percent': float(tick_data.get('change_percent', 0)) if tick_data.get('change_percent') else None,
                'source': tick_data.get('source', 'angel_one_websocket'),
                'market_status': 'open',
                'received_at': datetime.now(self.market_timezone)  # When our system received it
            }
            
            # Add to buffer for batch insert
            if symbol not in self.tick_buffer:
                self.tick_buffer[symbol] = []
            
            self.tick_buffer[symbol].append(tick_doc)
            
            # Check if we should flush buffer
            await self._maybe_flush_buffer()
            
            # Broadcast to WebSocket clients immediately
            await self._broadcast_tick(tick_doc)
            
            self.logger.debug(f"📊 Buffered tick: {symbol} @ ₹{price}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing tick: {e}", exc_info=True)
            return False

    async def store_tick_data(self, tick_data: Dict[str, Any]) -> bool:
        """
        Store tick data - wrapper method for compatibility with Angel One service
        
        Args:
            tick_data: Parsed tick data from Angel One service
            
        Returns:
            bool: True if stored successfully
        """
        try:
            # Convert the parsed data format to our internal format
            symbol = tick_data.get('symbol', '').upper()
            price = float(tick_data.get('price', 0))
            timestamp = tick_data.get('timestamp', datetime.now(self.market_timezone))
            
            if not symbol or price <= 0:
                self.logger.warning(f"Invalid tick data: {tick_data}")
                return False
            
            # Check if we should store this tick (deduplication)
            if not self._should_store_tick(symbol, price, timestamp):
                return False
            
            # Prepare tick document
            tick_doc = {
                'symbol': symbol,
                'price': price,
                'timestamp': timestamp,
                'token': tick_data.get('token', ''),
                'exchange': tick_data.get('exchange', 'NSE'),
                'high': float(tick_data.get('high', 0)) if tick_data.get('high') else None,
                'low': float(tick_data.get('low', 0)) if tick_data.get('low') else None,
                'volume': int(tick_data.get('volume', 0)) if tick_data.get('volume') else None,
                'change': float(tick_data.get('change', 0)) if tick_data.get('change') else None,
                'change_percent': float(tick_data.get('change_percent', 0)) if tick_data.get('change_percent') else None,
                'source': tick_data.get('source', 'angel_one_websocket'),
                'market_status': 'open',
                'received_at': datetime.now(self.market_timezone)
            }
            
            # Add to buffer for batch insert
            if symbol not in self.tick_buffer:
                self.tick_buffer[symbol] = []
            
            self.tick_buffer[symbol].append(tick_doc)
            
            # Check if we should flush buffer
            await self._maybe_flush_buffer()
            
            # Broadcast to WebSocket clients immediately
            await self._broadcast_tick(tick_doc)
            
            self.logger.info(f"📊 Stored tick: {symbol} @ ₹{price}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing tick data: {e}", exc_info=True)
            return False
    
    async def store_tick_batch(self, ticks: List[Dict[str, Any]]) -> int:
        """Store multiple ticks efficiently"""
        stored_count = 0
        
        for tick in ticks:
            if await self.store_tick(tick):
                stored_count += 1
        
        # Force flush after batch
        await self._flush_buffer()
        
        return stored_count
    
    async def _maybe_flush_buffer(self) -> None:
        """Flush buffer if conditions are met"""
        now = datetime.now(self.market_timezone)
        
        # Count total buffered ticks
        total_ticks = sum(len(ticks) for ticks in self.tick_buffer.values())
        
        # Check flush conditions
        should_flush = (
            total_ticks >= self.buffer_size or
            (now - self.last_flush).total_seconds() >= self.buffer_timeout
        )
        
        if should_flush:
            await self._flush_buffer()
    
    async def _flush_buffer(self) -> None:
        """Flush all buffered ticks to database"""
        if not self.tick_buffer:
            return
        
        try:
            collection = self._get_collection()
            all_docs = []
            
            # Collect all buffered documents
            for symbol, ticks in self.tick_buffer.items():
                all_docs.extend(ticks)
            
            if all_docs:
                inserted_count = 0
                # Use upsert to handle duplicate key errors gracefully
                for doc in all_docs:
                    try:
                        # Create a filter that matches the unique index
                        filter_doc = {
                            'symbol': doc['symbol'],
                            'price': doc['price'],
                            'timestamp': doc['timestamp']
                        }
                        
                        # Use replace_one with upsert=True to avoid duplicate key errors
                        await collection.replace_one(
                            filter_doc,
                            doc,
                            upsert=True
                        )
                        inserted_count += 1
                        # Log individual successful tick stores
                        self.logger.info(f"📊 Stored tick: {doc['symbol']} @ ₹{doc['price']}")
                    except Exception as tick_error:
                        # Log individual errors but continue processing
                        self.logger.warning(f"⚠️ Error storing tick for {doc['symbol']}: {tick_error}")
                
                self.logger.info(f"💾 Successfully stored {inserted_count} ticks to database")
            
            # Clear buffer
            self.tick_buffer.clear()
            self.last_flush = datetime.now(self.market_timezone)
            
        except Exception as e:
            self.logger.error(f"Error flushing tick buffer: {e}", exc_info=True)
            # Clear buffer anyway to prevent memory issues
            self.tick_buffer.clear()
    
    async def _broadcast_tick(self, tick_doc: Dict[str, Any]) -> None:
        """Broadcast tick to WebSocket clients"""
        try:
            symbol = tick_doc['symbol']
            
            # Prepare broadcast data with correct field names for WebSocket
            broadcast_data = {
                'symbol': symbol,
                'ltpc': tick_doc['price'],  # Last traded price
                'ch': tick_doc.get('change', 0),  # Change
                'chp': tick_doc.get('change_percent', 0),  # Change percent
                'high': tick_doc.get('high', tick_doc['price']),
                'low': tick_doc.get('low', tick_doc['price']),
                'open': tick_doc.get('open', tick_doc['price']),
                'close': tick_doc['price'],  # Current price is the close
                'volume': tick_doc.get('volume', 0),
                'exchange': tick_doc.get('exchange', 'NSE'),
                'received_at': tick_doc['timestamp']
            }
            
            # Broadcast to all clients
            await broadcast_market_data(broadcast_data)
            
            # Broadcast specific price update
            await broadcast_price_update(symbol, broadcast_data)
            
        except Exception as e:
            self.logger.error(f"Error broadcasting tick: {e}")
    
    def _validate_tick_data(self, data: Dict[str, Any]) -> bool:
        """Validate incoming tick data"""
        required_fields = ['symbol', 'ltpc']
        
        for field in required_fields:
            if field not in data or data[field] is None:
                self.logger.warning(f"Missing required field '{field}' in tick data")
                return False
        
        try:
            # Validate numeric fields
            float(data['ltpc'])
            return True
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid numeric data in tick: {data}")
            return False
    
    async def get_latest_ticks(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get latest ticks for a symbol"""
        try:
            collection = self._get_collection()
            
            query = {'symbol': symbol.upper()}
            cursor = collection.find(query).sort('timestamp', -1).limit(limit)
            
            ticks = []
            async for doc in cursor:
                # Convert ObjectId to string
                doc['_id'] = str(doc['_id'])
                ticks.append(doc)
            
            return ticks
            
        except Exception as e:
            self.logger.error(f"Error getting latest ticks: {e}")
            return []
    
    async def get_ticks_for_timerange(
        self, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Get ticks for a specific time range"""
        try:
            collection = self._get_collection()
            
            # FIXED: Proper timezone handling for database queries
            # The database stores UTC timestamps as naive datetime objects
            # Signal detection service passes IST times, so we need proper conversion
            
            ist_tz = pytz.timezone('Asia/Kolkata')
            
            if start_time.tzinfo is None:
                # CRITICAL FIX: Treat naive datetime as IST and convert to UTC properly
                # The issue was in timezone conversion - IST is UTC+5:30, so UTC time is EARLIER
                start_time_ist = ist_tz.localize(start_time)
                start_time_utc = start_time_ist.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                start_time_utc = start_time.astimezone(pytz.UTC).replace(tzinfo=None)
            
            if end_time.tzinfo is None:
                # CRITICAL FIX: Treat naive datetime as IST and convert to UTC properly
                end_time_ist = ist_tz.localize(end_time)
                end_time_utc = end_time_ist.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                end_time_utc = end_time.astimezone(pytz.UTC).replace(tzinfo=None)
            
            # Debug logging to track timezone conversion
            self.logger.debug(f"Timezone conversion for {symbol}:")
            self.logger.debug(f"  Input IST: {start_time} to {end_time}")
            self.logger.debug(f"  Query UTC: {start_time_utc} to {end_time_utc}")
            
            # CRITICAL FIX: MongoDB range queries are not working with $gte/$lte
            # Use a workaround by getting all data and filtering manually
            
            # First get all documents for the symbol and date
            date_start = start_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = date_start.replace(hour=23, minute=59, second=59)
            
            # Get all ticks for the day first
            all_day_cursor = collection.find({'symbol': symbol.upper()}).sort('timestamp', 1)
            
            ticks = []
            async for doc in all_day_cursor:
                doc_time = doc['timestamp']
                # Manual time filtering since MongoDB range queries aren't working
                if start_time_utc <= doc_time <= end_time_utc:
                    doc['_id'] = str(doc['_id'])
                    ticks.append(doc)
            
            self.logger.debug(f"Found {len(ticks)} ticks for {symbol} in range {start_time} to {end_time}")
            return ticks
            
        except Exception as e:
            self.logger.error(f"Error getting ticks for timerange: {e}")
            return []
    
    async def cleanup_old_ticks(self, days_to_keep: int = 7) -> int:
        """Clean up old tick data"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            collection = self._get_collection()
            
            result = await collection.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })
            
            self.logger.info(f"🗑️ Cleaned up {result.deleted_count} old ticks")
            return result.deleted_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old ticks: {e}")
            return 0
    
    async def get_tick_statistics(self) -> Dict[str, Any]:
        """Get tick data statistics"""
        try:
            collection = self._get_collection()
            
            # Total count
            total_count = await collection.count_documents({})
            
            # Count by symbol
            pipeline = [
                {'$group': {'_id': '$symbol', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            
            symbol_stats = []
            async for doc in collection.aggregate(pipeline):
                symbol_stats.append(doc)
            
            # Latest tick
            latest_tick = await collection.find_one({}, sort=[('timestamp', -1)])
            
            return {
                'total_ticks': total_count,
                'symbol_distribution': symbol_stats,
                'latest_tick_time': latest_tick.get('timestamp') if latest_tick else None,
                'buffer_size': sum(len(ticks) for ticks in self.tick_buffer.values()),
                'market_hours': self._is_market_hours()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting tick statistics: {e}")
            return {}


# Global instance
tick_data_service = TickDataService() 