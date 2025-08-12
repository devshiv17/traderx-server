import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from bson import ObjectId
from logzero import logger

from ..core.database import get_collection
from ..models.market_data import MarketDataModel, MarketDataBatch, MarketDataFilter, MarketDataResponse
from ..utils.timezone_utils import TimezoneUtils


class MarketDataService:
    """Service for handling market data operations"""
    
    def __init__(self):
        self.collection = None
        self.logger = logging.getLogger(__name__)
    
    def _get_collection(self):
        """Get the market data collection, initializing if needed"""
        if self.collection is None:
            self.collection = get_collection("market_data")
        return self.collection
    
    def _is_market_hours(self, dt: datetime) -> bool:
        """
        Check if a datetime is within Indian market hours using timezone utils
        """
        return TimezoneUtils.is_market_hours(dt)

    async def store_market_data(self, data: Union[MarketDataModel, Dict[str, Any]]) -> str:
        """
        Store a single market data entry with validation and duplicate prevention
        
        Args:
            data: MarketDataModel instance or dictionary with market data
            
        Returns:
            str: The ID of the inserted document
        """
        try:
            # Convert dict to MarketDataModel if needed
            if isinstance(data, dict):
                market_data = MarketDataModel(**data)
            else:
                market_data = data
            
            # Validate data before storing
            if not self._is_valid_market_data(market_data):
                self.logger.warning(f"⚠️ Skipping invalid market data for {market_data.symbol}: {market_data.raw_data}")
                return None

            # Check for after-hours using timezone utils
            now_ist = TimezoneUtils.get_ist_now()
            if not self._is_market_hours(now_ist):
                self.logger.info(f"⏰ Skipping after-hours data for {market_data.symbol} at {now_ist}")
                return None

            # Check for duplicates (same symbol within last 5 seconds)
            if await self._is_duplicate(market_data):
                self.logger.debug(f"🔄 Skipping duplicate data for {market_data.symbol}")
                return None
            
            # Add metadata with proper timezone handling
            market_data.received_at = now_ist
            market_data.source = "angel_one_websocket"
            market_data.processed = False
            market_data.is_realtime = True
            
            # Convert to dict for MongoDB insertion
            data_dict = market_data.dict(by_alias=True, exclude={'id'})
            
            # Insert into database, handle duplicate key error
            try:
                result = await self._get_collection().insert_one(data_dict)
                self.logger.info(f"📊 Stored market data for {market_data.symbol}: ₹{market_data.ltpc:,.2f}")
                return str(result.inserted_id)
            except Exception as e:
                if 'duplicate key error' in str(e):
                    self.logger.info(f"🛑 Duplicate key error for {market_data.symbol} at {now_utc}")
                    return None
                self.logger.error(f"❌ Error storing market data: {e}")
                raise
        except Exception as e:
            self.logger.error(f"❌ Error in store_market_data: {e}")
            raise
    
    def _is_valid_market_data(self, market_data: MarketDataModel) -> bool:
        """
        Validate market data before storing
        
        Args:
            market_data: MarketDataModel instance to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        # Check for required fields
        if not market_data.tk or market_data.tk == "":
            return False
        
        if not market_data.symbol or market_data.symbol == "TOKEN_":
            return False
        
        if market_data.ltpc <= 0:
            return False
        
        # Check for API errors in raw_data
        if market_data.raw_data:
            if isinstance(market_data.raw_data, dict):
                # Check for error codes
                if market_data.raw_data.get("errorcode"):
                    return False
                
                # Check for failed status
                if market_data.raw_data.get("status") is False:
                    return False
                
                # Check for error messages
                if "error" in market_data.raw_data.get("message", "").lower():
                    return False
        
        return True
    
    async def _is_duplicate(self, market_data: MarketDataModel) -> bool:
        """
        Check if this market data is a duplicate (same symbol within 5 seconds)
        
        Args:
            market_data: MarketDataModel instance to check
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        try:
            # Check for recent data with same symbol and similar price
            cutoff_time_ist = TimezoneUtils.get_ist_now() - timedelta(seconds=5)
            cutoff_time = cutoff_time_ist
            
            query = {
                "symbol": market_data.symbol,
                "received_at": {"$gte": cutoff_time},
                "ltpc": {"$gte": market_data.ltpc * 0.999, "$lte": market_data.ltpc * 1.001}  # 0.1% tolerance
            }
            
            count = await self._get_collection().count_documents(query)
            return count > 0
            
        except Exception as e:
            self.logger.error(f"Error checking for duplicates: {e}")
            return False
    
    async def store_batch_market_data(self, data_list: List[Union[MarketDataModel, Dict[str, Any]]]) -> List[str]:
        """
        Store multiple market data entries in batch
        
        Args:
            data_list: List of MarketDataModel instances or dictionaries
            
        Returns:
            List[str]: List of inserted document IDs
        """
        try:
            documents = []
            now_ist = TimezoneUtils.get_ist_now()
            for data in data_list:
                # Convert dict to MarketDataModel if needed
                if isinstance(data, dict):
                    market_data = MarketDataModel(**data)
                else:
                    market_data = data

                # After-hours filter using timezone utils
                if not self._is_market_hours(now_ist):
                    self.logger.info(f"⏰ Skipping after-hours data for {market_data.symbol} at {now_ist}")
                    continue

                # Validate
                if not self._is_valid_market_data(market_data):
                    self.logger.warning(f"⚠️ Skipping invalid market data for {market_data.symbol}: {market_data.raw_data}")
                    continue

                # Add metadata with proper timezone handling
                market_data.received_at = now_ist
                market_data.source = "angel_one_websocket"
                market_data.processed = False
                market_data.is_realtime = True

                # Convert to dict for MongoDB insertion
                data_dict = market_data.dict(by_alias=True, exclude={'id'})
                documents.append(data_dict)

            # Insert batch into database, handle duplicate key errors
            inserted_ids = []
            if documents:
                try:
                    result = await self._get_collection().insert_many(documents, ordered=False)
                    inserted_ids = [str(id) for id in result.inserted_ids]
                    self.logger.info(f"📊 Stored {len(inserted_ids)} market data entries in batch")
                except Exception as e:
                    if 'duplicate key error' in str(e):
                        self.logger.info(f"🛑 Duplicate key error in batch insert")
                        # Optionally, could retry non-duplicates
                    else:
                        self.logger.error(f"❌ Error storing batch market data: {e}")
                        raise
            return inserted_ids
        except Exception as e:
            self.logger.error(f"❌ Error in batch market data: {e}")
            raise
    
    async def get_latest_market_data(self, symbol: Optional[str] = None, limit: int = 100) -> List[MarketDataResponse]:
        """
        Get latest market data entries
        
        Args:
            symbol: Optional symbol filter
            limit: Maximum number of records to return
            
        Returns:
            List[MarketDataResponse]: List of market data entries
        """
        try:
            # Build query
            query = {}
            if symbol:
                query["symbol"] = symbol
            
            # Get latest data
            cursor = self._get_collection().find(query).sort("received_at", -1).limit(limit)
            documents = await cursor.to_list(length=limit)
            
            # Convert to response models
            result = []
            for doc in documents:
                doc["_id"] = str(doc["_id"])
                result.append(MarketDataResponse(**doc))
            
            self.logger.info(f"📊 Retrieved {len(result)} latest market data entries")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Error retrieving latest market data: {e}")
            raise
    
    async def get_market_data_by_filter(self, filter_params: MarketDataFilter) -> List[MarketDataResponse]:
        """
        Get market data using filter parameters
        
        Args:
            filter_params: MarketDataFilter instance with filter criteria
            
        Returns:
            List[MarketDataResponse]: List of filtered market data entries
        """
        try:
            # Build query from filter
            query = {}
            
            if filter_params.symbols:
                query["symbol"] = {"$in": filter_params.symbols}
            
            if filter_params.exchanges:
                query["exchange"] = {"$in": filter_params.exchanges}
            
            if filter_params.source:
                query["source"] = filter_params.source
            
            if filter_params.processed is not None:
                query["processed"] = filter_params.processed
            
            if filter_params.start_time or filter_params.end_time:
                time_query = {}
                if filter_params.start_time:
                    time_query["$gte"] = filter_params.start_time
                if filter_params.end_time:
                    time_query["$lte"] = filter_params.end_time
                query["received_at"] = time_query
            
            # Execute query
            cursor = self._get_collection().find(query).sort("received_at", -1).limit(filter_params.limit)
            documents = await cursor.to_list(length=filter_params.limit)
            
            # Convert to response models
            result = []
            for doc in documents:
                doc["_id"] = str(doc["_id"])
                result.append(MarketDataResponse(**doc))
            
            self.logger.info(f"📊 Retrieved {len(result)} market data entries with filter")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Error retrieving filtered market data: {e}")
            raise
    
    async def get_market_summary(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get market summary for specified symbols
        
        Args:
            symbols: List of symbols to get summary for
            
        Returns:
            List[Dict]: Market summary data
        """
        try:
            # Build query
            query = {}
            if symbols:
                query["symbol"] = {"$in": symbols}
            
            # Aggregate pipeline to get latest data for each symbol
            pipeline = [
                {"$match": query},
                {"$sort": {"received_at": -1}},
                {"$group": {
                    "_id": "$symbol",
                    "latest_data": {"$first": "$$ROOT"},
                    "count": {"$sum": 1}
                }},
                {"$replaceRoot": {"newRoot": "$latest_data"}},
                {"$sort": {"received_at": -1}}
            ]
            
            cursor = self._get_collection().aggregate(pipeline)
            documents = await cursor.to_list(length=None)
            
            # Convert to summary format
            summary = []
            for doc in documents:
                summary.append({
                    "symbol": doc.get("symbol"),
                    "token": doc.get("tk"),
                    "exchange": doc.get("exchange"),
                    "ltpc": doc.get("ltpc"),
                    "change": doc.get("ch"),
                    "change_percent": doc.get("chp"),
                    "high": doc.get("high"),
                    "low": doc.get("low"),
                    "open": doc.get("open"),
                    "volume": doc.get("volume"),
                    "bid": doc.get("bid"),
                    "ask": doc.get("ask"),
                    "received_at": doc.get("received_at"),
                    "source": doc.get("source")
                })
            
            self.logger.info(f"📊 Generated market summary for {len(summary)} symbols")
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ Error generating market summary: {e}")
            raise
    
    async def mark_as_processed(self, data_ids: List[str]) -> bool:
        """
        Mark market data entries as processed
        
        Args:
            data_ids: List of document IDs to mark as processed
            
        Returns:
            bool: Success status
        """
        try:
            # Convert string IDs to ObjectId
            object_ids = [ObjectId(id) for id in data_ids]
            
            # Update documents
            result = await self._get_collection().update_many(
                {"_id": {"$in": object_ids}},
                {"$set": {"processed": True, "processed_at": TimezoneUtils.get_ist_now()}}
            )
            
            self.logger.info(f"📊 Marked {result.modified_count} market data entries as processed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error marking data as processed: {e}")
            return False
    
    async def cleanup_old_data(self, days: int = 30) -> int:
        """
        Clean up old market data entries
        
        Args:
            days: Number of days to keep data for
            
        Returns:
            int: Number of deleted documents
        """
        try:
            cutoff_date_ist = TimezoneUtils.get_ist_now() - timedelta(days=days)
            cutoff_date = cutoff_date_ist
            
            result = await self._get_collection().delete_many({
                "received_at": {"$lt": cutoff_date}
            })
            
            self.logger.info(f"🗑️ Cleaned up {result.deleted_count} old market data entries")
            return result.deleted_count
            
        except Exception as e:
            self.logger.error(f"❌ Error cleaning up old data: {e}")
            return 0
    
    async def get_data_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about stored market data
        
        Returns:
            Dict: Statistics about the market data collection
        """
        try:
            # Total count
            total_count = await self._get_collection().count_documents({})
            
            # Count by source
            source_stats = await self._get_collection().aggregate([
                {"$group": {"_id": "$source", "count": {"$sum": 1}}}
            ]).to_list(length=None)
            
            # Count by symbol
            symbol_stats = await self._get_collection().aggregate([
                {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]).to_list(length=None)
            
            # Unprocessed count
            unprocessed_count = await self._get_collection().count_documents({"processed": False})
            
            # Latest data timestamp
            latest_doc = await self._get_collection().find_one({}, sort=[("received_at", -1)])
            latest_timestamp = latest_doc.get("received_at") if latest_doc else None
            
            stats = {
                "total_entries": total_count,
                "unprocessed_entries": unprocessed_count,
                "latest_data_timestamp": latest_timestamp,
                "source_distribution": {item["_id"]: item["count"] for item in source_stats},
                "top_symbols": {item["_id"]: item["count"] for item in symbol_stats}
            }
            
            self.logger.info(f"📊 Generated market data statistics")
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ Error generating statistics: {e}")
            raise

    async def get_chart_data(self, symbol: str, timeframe: str, date: str) -> List[Dict[str, Any]]:
        """
        Get chart data for a specific symbol with different timeframes
        
        Args:
            symbol: Symbol to get data for
            timeframe: Timeframe (1m, 5m, 15m, 1h)
            date: Date in YYYY-MM-DD format
            
        Returns:
            List[Dict]: Chart data points
        """
        try:
            # Parse date
            start_date = datetime.strptime(date, "%Y-%m-%d")
            end_date = start_date + timedelta(days=1)
            
            # Build query for the specific date
            query = {
                "symbol": symbol,
                "received_at": {
                    "$gte": start_date,
                    "$lt": end_date
                }
            }
            
            # Get raw data
            cursor = self._get_collection().find(query).sort("received_at", 1)
            raw_data = await cursor.to_list(length=None)
            
            if not raw_data:
                return []
            
            # Aggregate data based on timeframe
            aggregated_data = self._aggregate_chart_data(raw_data, timeframe)
            
            self.logger.info(f"📊 Generated chart data for {symbol} ({timeframe}): {len(aggregated_data)} points")
            return aggregated_data
            
        except Exception as e:
            self.logger.error(f"❌ Error generating chart data for {symbol}: {e}")
            raise

    def _aggregate_chart_data(self, raw_data: List[Dict[str, Any]], timeframe: str) -> List[Dict[str, Any]]:
        """
        Aggregate raw market data into chart data points
        
        Args:
            raw_data: List of raw market data points
            timeframe: Timeframe for aggregation
            
        Returns:
            List[Dict]: Aggregated chart data
        """
        if not raw_data:
            return []
        
        # Define time intervals in minutes
        timeframe_minutes = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "1h": 60
        }
        
        interval_minutes = timeframe_minutes.get(timeframe, 1)
        
        # Group data by time intervals
        grouped_data = {}
        
        for data_point in raw_data:
            timestamp = data_point.get("received_at")
            if not timestamp:
                continue
            
            # Round timestamp to interval
            rounded_time = self._round_timestamp(timestamp, interval_minutes)
            
            if rounded_time not in grouped_data:
                grouped_data[rounded_time] = []
            
            grouped_data[rounded_time].append(data_point)
        
        # Aggregate each group
        chart_data = []
        for timestamp, group in sorted(grouped_data.items()):
            if not group:
                continue
            
            # Calculate OHLCV
            prices = [point.get("ltpc", 0) for point in group if point.get("ltpc")]
            volumes = [point.get("volume", 0) for point in group if point.get("volume")]
            
            if not prices:
                continue
            
            # Convert UTC to IST using timezone utils
            ist_timestamp = TimezoneUtils.to_ist(timestamp)
            
            aggregated_point = {
                "time": TimezoneUtils.format_for_api(ist_timestamp),
                "open": group[0].get("ltpc", 0),
                "high": max(prices),
                "low": min(prices),
                "close": group[-1].get("ltpc", 0),
                "volume": sum(volumes),
                "symbol": group[0].get("symbol"),
                "exchange": group[0].get("exchange")
            }
            
            chart_data.append(aggregated_point)
        
        return chart_data

    def _round_timestamp(self, timestamp: datetime, interval_minutes: int) -> datetime:
        """
        Round timestamp to the nearest interval
        
        Args:
            timestamp: Original timestamp
            interval_minutes: Interval in minutes
            
        Returns:
            datetime: Rounded timestamp
        """
        # Convert to minutes since epoch
        minutes_since_epoch = int(timestamp.timestamp() / 60)
        
        # Round to interval
        rounded_minutes = (minutes_since_epoch // interval_minutes) * interval_minutes
        
        # Convert back to datetime
        return datetime.fromtimestamp(rounded_minutes * 60)

    async def get_available_symbols(self) -> List[str]:
        """
        Get list of available symbols that have market data
        
        Returns:
            List[str]: List of available symbols
        """
        try:
            # Get unique symbols from the database
            pipeline = [
                {"$group": {"_id": "$symbol"}},
                {"$sort": {"_id": 1}}
            ]
            
            cursor = self._get_collection().aggregate(pipeline)
            documents = await cursor.to_list(length=None)
            
            symbols = [doc["_id"] for doc in documents if doc["_id"]]
            
            self.logger.info(f"📊 Found {len(symbols)} available symbols")
            return symbols
            
        except Exception as e:
            self.logger.error(f"❌ Error retrieving available symbols: {e}")
            raise

    async def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the market data service
        
        Returns:
            Dict: Health status information
        """
        try:
            # Check database connection
            total_count = await self._get_collection().count_documents({})
            
            # Get latest data timestamp
            latest_doc = await self._get_collection().find_one({}, sort=[("received_at", -1)])
            latest_timestamp = latest_doc.get("received_at") if latest_doc else None
            
            # Calculate data freshness using timezone utils
            data_freshness = None
            if latest_timestamp:
                current_ist = TimezoneUtils.get_ist_now()
                time_diff = current_ist - latest_timestamp
                data_freshness = {
                    "seconds_ago": int(time_diff.total_seconds()),
                    "minutes_ago": int(time_diff.total_seconds() / 60),
                    "is_recent": time_diff.total_seconds() < 300  # 5 minutes
                }
            
            health_status = {
                "status": "healthy",
                "database_connected": True,
                "total_entries": total_count,
                "latest_data_timestamp": latest_timestamp,
                "data_freshness": data_freshness,
                "service_uptime": "running"
            }
            
            return health_status
            
        except Exception as e:
            self.logger.error(f"❌ Error checking health status: {e}")
            return {
                "status": "unhealthy",
                "database_connected": False,
                "error": str(e)
            }


# Global instance
market_data_service = MarketDataService() 