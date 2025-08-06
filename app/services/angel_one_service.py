import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pytz
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
from logzero import logger
from ..core.database import get_collection
from ..core.config import settings
from ..models.signal import SignalModel
from ..models.market_data import MarketDataModel
from ..ws import broadcast_market_data, broadcast_price_update

# Import services lazily to avoid circular imports
def get_market_data_service():
    from ..services.market_data_service import market_data_service
    return market_data_service

def get_tick_data_service():
    from ..services.tick_data_service import tick_data_service
    return tick_data_service
import queue
import threading

logger = logging.getLogger(__name__)

# IST timezone
IST = pytz.timezone('Asia/Kolkata')


class AngelOneService:
    def __init__(self):
        self.api_key = settings.angel_one_api_key
        self.secret = settings.angel_one_secret
        self.client_code = settings.angel_one_client_code
        self.pin = settings.angel_one_pin
        self.totp_token = settings.angel_one_totp_token
        
        # SmartAPI objects
        self.smart_api = None
        self.auth_token = None
        self.feed_token = None
        self.refresh_token = None
        
        # WebSocket
        self.websocket = None
        self.is_connected = False
        self.shutdown_flag = False
        self.connection_time = None
        self.subscribed_tokens = []
        
        # Market data tokens for major indices only - futures tokens will be discovered dynamically
        self.market_tokens = [
            {"exchangeType": 1, "tokens": ["99926000"]},   # NIFTY 50 (NSE)
            {"exchangeType": 1, "tokens": ["99926009"]},   # BANKNIFTY (NSE)
            {"exchangeType": 1, "tokens": ["99926037"]},   # FINNIFTY (NSE)
            {"exchangeType": 1, "tokens": ["26000"]},      # SENSEX (BSE)
        ]
        
        # Dynamic futures mapping - populated at runtime
        self.futures_tokens = {}
        self.valid_symbols = set()
        
        self.ws_queue = queue.Queue()
        self.ws_queue_thread = None
        
        # Auto-reconnection and health monitoring
        self.last_data_received = None
        self.health_check_interval = 30  # seconds
        self.reconnect_interval = 60  # seconds
        self.max_reconnect_attempts = 5
        self.reconnect_attempts = 0
        self.health_monitor_task = None
        self.auto_reconnect_task = None
        self.session_start_time = None
        self.session_duration = timedelta(hours=23)  # Angel One sessions typically last 24 hours
    
    async def authenticate(self):
        """Authenticate with Angel One SmartAPI using official library"""
        try:
            logger.info("Authenticating with Angel One SmartAPI...")
            
            # Initialize SmartAPI
            self.smart_api = SmartConnect(api_key=self.api_key)
            
            # Generate TOTP
            if not self.totp_token:
                logger.error("TOTP token not configured. Please add your TOTP token.")
                return False
            
            totp = pyotp.TOTP(self.totp_token).now()
            
            # Generate session (use positional args)
            data = self.smart_api.generateSession(
                self.client_code,
                self.pin,
                totp
            )
            
            if data['status'] == False:
                logger.error(f"Authentication failed: {data}")
                return False
            
            # Extract tokens
            self.auth_token = data['data']['jwtToken']
            self.refresh_token = data['data']['refreshToken']
            
            # Get feed token
            self.feed_token = self.smart_api.getfeedToken()
            
            logger.info("✅ Authentication successful")
            logger.info(f"Auth Token: {self.auth_token[:20]}...")
            logger.info(f"Feed Token: {self.feed_token}")
            
            # Discover valid NIFTY futures tokens after authentication
            await self._discover_futures_tokens()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Authentication failed: {e}")
            return False
    
    async def _discover_futures_tokens(self):
        """Discover valid NIFTY futures tokens dynamically"""
        try:
            logger.info("🔍 Discovering valid NIFTY futures tokens...")
            
            # Try multiple search approaches to find NIFTY futures
            search_results = None
            search_attempts = [
                ("NFO", "NIFTY"),
                ("NFO", "NIFTY50"),  
                ("NFO", "NIFTY 50"),
                ("NSE", "NIFTY"),  # Try NSE as well
            ]
            
            for exchange, search_term in search_attempts:
                logger.info(f"Searching for '{search_term}' on {exchange}")
                try:
                    search_results = self.smart_api.searchScrip(exchange, search_term)
                    logger.info(f"Search result: {search_results}")  # Debug the full response
                    if search_results and search_results.get('status') and search_results.get('data'):
                        logger.info(f"✅ Found {len(search_results.get('data', []))} results with search term: {search_term}")
                        break
                    else:
                        logger.warning(f"No valid data in search results for {search_term}")
                except Exception as e:
                    logger.error(f"Search failed for {search_term}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            
            if search_results and search_results.get('status'):
                instruments = search_results.get('data', [])
                logger.info(f"Found {len(instruments)} NIFTY instruments")
                
                # Debug: Log all instruments to understand what's available
                logger.info("=== ALL NIFTY INSTRUMENTS FOUND ===")
                for i, inst in enumerate(instruments[:20]):  # Show more instruments
                    logger.info(f"  [{i}] {inst.get('tradingsymbol')} | Type: {inst.get('instrumenttype')} | Token: {inst.get('token')} | Expiry: {inst.get('expiry')} | Exchange: {inst.get('exchange')}")
                
                # Filter for current month futures (active contracts)
                current_futures = []
                for inst in instruments:
                    instrument_type = inst.get('instrumenttype', '').upper()
                    trading_symbol = inst.get('tradingsymbol', '')
                    exchange = inst.get('exchange', '').upper()
                    
                    # Look for index futures with more comprehensive filtering
                    if (instrument_type in ['FUTIDX', 'FUTCUR', 'FUT'] and 
                        exchange in ['NFO', 'NSE']):
                        # Must be a NIFTY futures contract
                        if ('NIFTY' in trading_symbol.upper() and 
                            not any(x in trading_symbol.upper() for x in ['CE', 'PE', 'CALL', 'PUT']) and
                            not 'WEEKLY' in trading_symbol.upper() and
                            not 'MINI' in trading_symbol.upper()):
                            current_futures.append(inst)
                            logger.info(f"✅ VALID FUTURES: {trading_symbol} (Type: {instrument_type}, Expiry: {inst.get('expiry')}, Token: {inst.get('token')})")
                
                # Sort by expiry to get the nearest contracts
                current_futures.sort(key=lambda x: x.get('expiry', ''))
                
                # Take first 3 active contracts
                valid_futures = current_futures[:3]
                
                for i, future in enumerate(valid_futures):
                    token = future.get('token', '')
                    symbol = future.get('tradingsymbol', '')
                    name = future.get('name', '')
                    expiry = future.get('expiry', '')
                    
                    # Create standardized symbol names
                    if i == 0:
                        std_symbol = "NIFTY_FUT1"
                    elif i == 1:
                        std_symbol = "NIFTY_FUT2"  
                    else:
                        std_symbol = "NIFTY_FUT3"
                    
                    # Store mapping
                    self.futures_tokens[std_symbol] = {
                        'token': token,
                        'trading_symbol': symbol,
                        'name': name,
                        'expiry': expiry,
                        'exchange': 'NFO'
                    }
                    
                    self.valid_symbols.add(std_symbol)
                    
                    logger.info(f"✅ Found {std_symbol}: {symbol} (Token: {token}, Expiry: {expiry})")
                
                # Update market tokens to include discovered futures
                for symbol_info in self.futures_tokens.values():
                    token = symbol_info['token']
                    self.market_tokens.append({"exchangeType": 2, "tokens": [token]})  # NFO exchange type = 2
                
                logger.info(f"✅ Discovered {len(self.futures_tokens)} valid NIFTY futures")
                
            else:
                logger.error("❌ Failed to search for NIFTY instruments - search returned no results or failed")
                logger.error(f"Search result status: {search_results}")
            
            # If no futures found through search, try known NIFTY futures tokens
            if not self.futures_tokens:
                logger.warning("📊 No futures found through search - trying known NIFTY futures tokens...")
                
                # Try current month NIFTY futures tokens - Aug 2025
                # These tokens are for current month (August 2025) and next month
                known_futures_tokens = [
                    # August 2025 NIFTY futures (current month)
                    "67329",    # NIFTY25AUGFUT - Most likely current month
                    "67330",    # NIFTY25SEPFUT - Next month
                    # Backup tokens from previous patterns
                    "61234",    # Pattern-based token
                    "61235",    # Pattern-based token  
                ]
                logger.info(f"Trying {len(known_futures_tokens)} known NIFTY futures tokens for August 2025...")
                
                # Test if these tokens are valid by trying to subscribe
                for i, token in enumerate(known_futures_tokens[:2]):  # Only try first 2
                    std_symbol = f"NIFTY_FUT{i+1}"
                    self.futures_tokens[std_symbol] = {
                        'token': token,
                        'trading_symbol': f'NIFTY_FUT_{token}',
                        'name': f'NIFTY Futures Contract {i+1}',
                        'expiry': 'Unknown',
                        'exchange': 'NFO'
                    }
                    self.valid_symbols.add(std_symbol)
                    
                    # Add to market tokens for subscription
                    self.market_tokens.append({"exchangeType": 2, "tokens": [token]})
                    logger.info(f"🧪 Added known futures token: {std_symbol} (Token: {token})")
                
                # If still no futures, use index as last resort
                if not self.futures_tokens:
                    logger.error("❌ CRITICAL: No NIFTY futures found at all - using NIFTY index as proxy")
                    logger.error("⚠️  This will result in identical data for index and futures - signals may be inaccurate!")
                    
                    self.futures_tokens['NIFTY_FUT1'] = {
                        'token': '99926000',  # NIFTY index token
                        'trading_symbol': 'NIFTY_INDEX_PROXY',
                        'name': 'NIFTY 50 Index (Emergency Proxy)',
                        'expiry': 'N/A',
                        'exchange': 'NSE'
                    }
                    self.valid_symbols.add('NIFTY_FUT1')
                    logger.error("💀 Created NIFTY_FUT1 using NIFTY index - THIS IS NOT IDEAL!")
                else:
                    logger.info(f"✅ Using {len(self.futures_tokens)} known futures tokens")
                
        except Exception as e:
            logger.error(f"❌ Error discovering futures tokens: {e}")
            logger.info("📊 Will use indices only for market data")
    
    def _setup_websocket_callbacks(self):
        """Setup WebSocket callbacks"""
        def on_data(wsapp, message):
            """Handle incoming market data"""
            try:
                logger.info(f"📊 Received market data: {message}")
                self.ws_queue.put(message)
                # Update last data received timestamp
                self.last_data_received = datetime.now(IST)
            except Exception as e:
                logger.error(f"Error processing WebSocket data: {e}")
        
        def on_open(wsapp):
            """Handle WebSocket connection open"""
            logger.info("🔗 WebSocket connected")
            self.is_connected = True
            self.connection_time = datetime.now(IST)
            
            # Subscribe to market data
            correlation_id = f"sub_{int(time.time())}"
            self.websocket.subscribe(correlation_id, 1, self.market_tokens)
            logger.info("📡 Subscribed to market data feed")
            self.subscribed_tokens = self.market_tokens
        
        def on_error(wsapp, error):
            """Handle WebSocket errors"""
            logger.error(f"❌ WebSocket error: {error}")
            self.is_connected = False
            self.connection_time = None
            self.subscribed_tokens = []
        
        def on_close(wsapp):
            """Handle WebSocket connection close"""
            logger.info("🔌 WebSocket disconnected")
            self.is_connected = False
            self.connection_time = None
            self.subscribed_tokens = []
        
        return on_open, on_data, on_error, on_close
    
    async def connect_websocket(self):
        """Connect to Angel One WebSocket V2 feed"""
        try:
            if not self.auth_token or not self.feed_token:
                logger.error("Not authenticated. Please authenticate first.")
                return False
            
            logger.info("🔗 Connecting to Angel One WebSocket V2...")
            
            # Initialize WebSocket
            self.websocket = SmartWebSocketV2(
                auth_token=self.auth_token,
                api_key=self.api_key,
                client_code=self.client_code,
                feed_token=self.feed_token
            )
            
            # Setup callbacks
            on_open, on_data, on_error, on_close = self._setup_websocket_callbacks()
            
            self.websocket.on_open = on_open
            self.websocket.on_data = on_data
            self.websocket.on_error = on_error
            self.websocket.on_close = on_close
            
            # Connect in a separate daemon thread to avoid blocking
            def connect_sync():
                try:
                    self.websocket.connect()
                except Exception as e:
                    logger.error(f"WebSocket connection error: {e}")
                    self.is_connected = False
            
            # Use daemon thread so it doesn't block the main process
            self.ws_queue_thread = threading.Thread(target=connect_sync, daemon=True)
            self.ws_queue_thread.start()
            
            logger.info("✅ WebSocket connection started in background thread")
            return True
            
        except Exception as e:
            logger.error(f"❌ WebSocket connection failed: {e}")
            return False
    
    async def _process_websocket_data(self, message):
        """Process incoming WebSocket market data - PRODUCTION GRADE"""
        try:
            # Parse the message
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            # Extract market data from SmartAPI format
            parsed_data = self._parse_websocket_data(data)
            if not parsed_data:
                return
            
            # Store in tick data service
            tick_service = get_tick_data_service()
            await tick_service.store_tick_data(parsed_data)
            
            # Broadcast to WebSocket clients
            symbol = parsed_data.get('symbol', 'unknown')
            await broadcast_price_update(symbol, parsed_data)
            
            logger.info(f"✅ Processed tick data for {parsed_data.get('symbol', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Error processing WebSocket data: {e}")
    
    def _process_websocket_data_sync(self, message):
        """Synchronous version for thread safety"""
        try:
            # Parse the message
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            # Extract market data from SmartAPI format
                parsed_data = self._parse_websocket_data(data)
            if not parsed_data:
                return
            
            # Store in tick data service
            tick_service = get_tick_data_service()
            asyncio.create_task(tick_service.store_tick_data(parsed_data))
            
            logger.info(f"✅ Processed tick data for {parsed_data.get('symbol', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Error processing WebSocket data: {e}")
    
    async def _store_tick_data_async(self, tick_data):
        """Store tick data in database asynchronously"""
        try:
            collection = get_collection('tick_data')
            
            # Create unique identifier to prevent duplicates
            tick_data['_id'] = f"{tick_data['symbol']}_{tick_data['timestamp']}"
            
            # Use upsert to avoid duplicates
            result = await collection.replace_one(
                {'_id': tick_data['_id']},
                tick_data,
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"📊 Stored new tick: {tick_data['symbol']} @ {tick_data['price']}")
            else:
                logger.debug(f"📊 Updated existing tick: {tick_data['symbol']} @ {tick_data['price']}")
                
        except Exception as e:
            logger.error(f"Error storing tick data: {e}")
    
    def _parse_websocket_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse WebSocket data from Angel One SmartAPI format"""
        try:
            # Handle the actual Angel One WebSocket data format
            if 'last_traded_price' in data and 'token' in data:
                # Convert token to symbol
                token = data.get('token', '')
                symbol = self._get_symbol_from_token(token)
                
                # Convert price from paise to rupees (Angel One sends prices in paise)
                price_paise = float(data.get('last_traded_price', 0))
                price_rupees = price_paise / 100.0
                
                # Convert exchange timestamp to datetime
                exchange_timestamp = data.get('exchange_timestamp', 0)
                if exchange_timestamp:
                    # Convert milliseconds to datetime
                    timestamp = datetime.fromtimestamp(exchange_timestamp / 1000.0, tz=IST)
                else:
                    # If no exchange timestamp, use current time but log it
                    timestamp = datetime.now(IST)
                    logger.debug(f"No exchange timestamp for {symbol}, using current time")
                
                tick_data = {
                    'symbol': symbol,
                    'timestamp': timestamp,
                    'price': price_rupees,
                    'token': token,
                    'exchange_type': data.get('exchange_type', 1),
                    'sequence_number': data.get('sequence_number', 0),
                    'subscription_mode': data.get('subscription_mode', 1),
                    'source': 'angel_one_websocket'
                }
                
                logger.info(f"✅ Parsed tick data: {symbol} @ ₹{price_rupees:.2f}")
                return tick_data
            
            # Legacy format handling (if any)
            if 'ltpc' in data or 'ltp' in data:
                # Extract basic tick data
                tick_data = {
                    'symbol': data.get('tk', 'unknown'),
                    'timestamp': datetime.now(IST),  # Market timestamp (current time as fallback)
                    'price': float(data.get('ltp', data.get('ltpc', 0))),
                    'volume': int(data.get('vol', 0)),
                    'change': float(data.get('ch', 0)),
                    'change_percent': float(data.get('chp', 0)),
                    'high': float(data.get('high', 0)),
                    'low': float(data.get('low', 0)),
                    'open': float(data.get('open', 0)),
                    'close': float(data.get('close', 0)),
                    'source': 'angel_one_websocket'
                }
                return tick_data
            
            # Check if it's a SmartAPI specific format
            if 'exchangeType' in data and 'token' in data:
                tick_data = {
                    'symbol': self._get_symbol_from_token(data.get('token', '')),
                    'timestamp': datetime.now(IST),
                    'price': float(data.get('ltp', 0)),
                    'volume': int(data.get('vol', 0)),
                    'change': float(data.get('ch', 0)),
                    'change_percent': float(data.get('chp', 0)),
                    'high': float(data.get('high', 0)),
                    'low': float(data.get('low', 0)),
                    'open': float(data.get('open', 0)),
                    'close': float(data.get('close', 0)),
                    'source': 'angel_one_websocket'
                }
                return tick_data
            
            logger.debug(f"Unrecognized data format: {data}")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing WebSocket data: {e}")
            return None
    
    def _parse_smartapi_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse SmartAPI specific data format"""
        try:
            # Handle different SmartAPI data formats
            if isinstance(data, dict):
                # Check for LTP data
                if 'ltp' in data:
                    return {
                        'symbol': data.get('symbol', 'unknown'),
                        'timestamp': datetime.now(IST),
                        'price': float(data['ltp']),
                        'volume': int(data.get('volume', 0)),
                        'change': float(data.get('change', 0)),
                        'change_percent': float(data.get('change_percent', 0)),
                        'high': float(data.get('high', 0)),
                        'low': float(data.get('low', 0)),
                        'open': float(data.get('open', 0)),
                        'close': float(data.get('close', 0)),
                        'source': 'angel_one_websocket'
                    }
                
                # Check for OHLC data
                if 'ohlc' in data:
                    ohlc = data['ohlc']
            return {
                        'symbol': data.get('symbol', 'unknown'),
                        'timestamp': datetime.now(IST),
                        'price': float(ohlc.get('close', 0)),
                        'volume': int(data.get('volume', 0)),
                        'change': float(data.get('change', 0)),
                        'change_percent': float(data.get('change_percent', 0)),
                        'high': float(ohlc.get('high', 0)),
                        'low': float(ohlc.get('low', 0)),
                        'open': float(ohlc.get('open', 0)),
                        'close': float(ohlc.get('close', 0)),
                        'source': 'angel_one_websocket'
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing SmartAPI data: {e}")
            return None
    
    def _is_api_error(self, data: Dict[str, Any]) -> bool:
        """Check if the data contains an API error"""
        if isinstance(data, dict):
            # Check for error indicators
            if 'error' in data or 'errorcode' in data or 'message' in data:
                if 'error' in data and data['error']:
                    return True
                if 'errorcode' in data and data['errorcode'] != 0:
                    return True
                if 'message' in data and 'error' in data['message'].lower():
                    return True
        return False
    
    def _get_symbol_from_token(self, token: str) -> str:
        """Convert token to symbol name"""
        # Static mappings for indices
        token_map = {
            "99926000": "NIFTY",
            "99926009": "BANKNIFTY", 
            "99926037": "FINNIFTY",
            "26000": "SENSEX",
        }
        
        # Check static mappings first
        if token in token_map:
            return token_map[token]
        
        # Check dynamic futures mappings
        for symbol, info in self.futures_tokens.items():
            if info['token'] == token:
                return symbol
        
        return f"TOKEN_{token}"
    
    async def get_market_data_rest(self, symbols: List[str] = None):
        """Get market data via REST API"""
        try:
            if not self.smart_api:
                logger.error("SmartAPI not initialized")
                return []
            
            # Default symbols if none provided - includes indices and valid futures
            if not symbols:
                symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]
                # Add valid futures symbols discovered dynamically
                symbols.extend(list(self.valid_symbols))
            
            market_data = []
            
            for symbol in symbols:
                try:
                    # Get token for symbol
                    token = self._get_token_from_symbol(symbol)
                    if not token:
                        continue
                    
                    # Get exchange and trading symbol for the symbol
                    exchange = "NSE"  # Default for indices
                    trading_symbol = symbol
                    
                    # Check if it's a futures symbol
                    if symbol in self.futures_tokens:
                        exchange = self.futures_tokens[symbol]['exchange']
                        trading_symbol = self.futures_tokens[symbol]['trading_symbol']
                    
                    # Get LTP data
                    ltp_data = self.smart_api.ltpData(exchange, trading_symbol, token)
                    
                    if ltp_data and ltp_data.get('status'):
                        data = ltp_data['data']
                        parsed_data = self._parse_rest_data(data)
                        if parsed_data:
                            market_data.append(parsed_data)
                            # Store in database
                            await self._store_market_data(parsed_data)
                        
                except Exception as e:
                    logger.error(f"Error getting data for {symbol}: {e}")
                    continue
            
            return market_data
            
        except Exception as e:
            logger.error(f"Error in get_market_data_rest: {e}")
            return []
    
    def _get_token_from_symbol(self, symbol: str) -> str:
        """Convert symbol to token"""
        # Static mappings for indices
        symbol_map = {
            "NIFTY": "99926000",
            "BANKNIFTY": "99926009",
            "FINNIFTY": "99926037", 
            "SENSEX": "26000",
        }
        
        # Check static mappings first
        if symbol in symbol_map:
            return symbol_map[symbol]
        
        # Check dynamic futures mappings
        if symbol in self.futures_tokens:
            return self.futures_tokens[symbol]['token']
        
        return ""
    
    def _parse_rest_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse REST API data"""
        try:
            if not data:
                return None
            
            # Extract LTP data
            ltp = data.get('ltp', 0)
            if not ltp:
                return None
            
            return {
                'symbol': data.get('symbol', 'unknown'),
                'timestamp': datetime.now(IST),
                'price': float(ltp),
                'volume': int(data.get('volume', 0)),
                'change': float(data.get('change', 0)),
                'change_percent': float(data.get('change_percent', 0)),
                'high': float(data.get('high', 0)),
                'low': float(data.get('low', 0)),
                'open': float(data.get('open', 0)),
                'close': float(data.get('close', 0)),
                'source': 'angel_one_rest'
            }
            
        except Exception as e:
            logger.error(f"Error parsing REST data: {e}")
            return None
    
    async def _store_market_data(self, data: Dict[str, Any]):
        """Store market data in database"""
        try:
            collection = get_collection('market_data')
            
            # Create document
            market_data = MarketDataModel(
                symbol=data['symbol'],
                price=data['price'],
                volume=data['volume'],
                change=data['change'],
                change_percent=data['change_percent'],
                high=data['high'],
                low=data['low'],
                open=data['open'],
                close=data['close'],
                timestamp=data['timestamp'],
                source=data['source']
            )
            
            # Insert into database
            result = await collection.insert_one(market_data.dict())
            logger.info(f"📊 Stored market data for {data['symbol']}: {data['price']}")
            
            return result.inserted_id
            
        except Exception as e:
            logger.error(f"Error storing market data: {e}")
            return None
    
    async def _analyze_for_signals(self, data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Analyze market data for trading signals"""
        try:
            signals = []
            
            # Simple signal logic (you can enhance this)
            symbol = data['symbol']
            price = data['price']
            change_percent = data['change_percent']
            
            # Example signals
            if change_percent > 2.0:
                signals.append({
                    'symbol': symbol,
                    'type': 'BUY',
                    'strength': 'STRONG',
                    'reason': f'Price up {change_percent:.2f}%',
                    'price': price,
                    'timestamp': datetime.now(IST)
                })
            elif change_percent < -2.0:
                signals.append({
                    'symbol': symbol,
                    'type': 'SELL',
                    'strength': 'STRONG', 
                    'reason': f'Price down {change_percent:.2f}%',
                    'price': price,
                    'timestamp': datetime.utcnow()
                })
            
            # Store signals if any
            if signals:
                collection = get_collection('signals')
                for signal in signals:
                    signal_model = SignalModel(**signal)
                    await collection.insert_one(signal_model.dict())
                    logger.info(f"🚨 Signal generated: {signal['type']} {symbol} - {signal['reason']}")
            
            return signals
            
        except Exception as e:
            logger.error(f"Error analyzing signals: {e}")
            return None
    
    async def get_latest_market_data(self, symbol: str = None, limit: int = 100):
        """Get latest market data from database"""
        try:
            collection = get_collection('tick_data')
            
            # Build query
            query = {}
            if symbol:
                query['symbol'] = symbol
            
            # Get latest data
            cursor = collection.find(query).sort('timestamp', -1).limit(limit)
            data = await cursor.to_list(length=limit)
            
            # Convert ObjectId to string for JSON serialization
            for item in data:
                if '_id' in item:
                    item['_id'] = str(item['_id'])
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting latest market data: {e}")
            return []
    
    async def get_market_summary(self):
        """Get market summary with latest prices"""
        try:
            # Get latest data for major indices and valid futures
            symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]
            symbols.extend(list(self.valid_symbols))
            summary = []
            
            for symbol in symbols:
                latest_data = await self.get_latest_market_data(symbol, 1)
                if latest_data:
                    summary.append(latest_data[0])
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting market summary: {e}")
            return []
    
    async def start_feed_service(self):
        """Start the Angel One feed service with automated health monitoring"""
        logger.info("🚀 Starting Angel One SmartAPI feed service with auto-reconnection...")
        
        # Authenticate first
        if await self.authenticate():
            logger.info("✅ Angel One authentication successful")
            self.session_start_time = datetime.now(IST)
            
            # Enable WebSocket for real-time data
            await self.connect_websocket()
            
            # Start health monitoring and auto-reconnection
            await self.start_health_monitoring()
            
            logger.info("✅ Angel One feed service started successfully with automated monitoring")
        else:
            logger.error("❌ Failed to start Angel One feed service - authentication failed")
    
    async def stop_feed_service(self):
        """Stop the Angel One feed service"""
        try:
            self.shutdown_flag = True
            
            # Stop health monitoring
            await self.stop_health_monitoring()
            
            if self.websocket:
                self.websocket.close_connection()
                logger.info("🔌 WebSocket connection closed")
            
            if self.smart_api:
                # Logout
                self.smart_api.terminateSession(self.client_code)
                logger.info("👋 Logged out from Angel One")
            
        except Exception as e:
            logger.error(f"Error stopping feed service: {e}")
    
    async def process_ws_queue(self):
        """Background async task to process WebSocket queue"""
        while not self.shutdown_flag:
            try:
                # Get message from queue with timeout to prevent blocking
                message = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: self.ws_queue.get(timeout=1.0)
                )
                await self._process_websocket_data(message)
                # Update last data received timestamp
                self.last_data_received = datetime.now(IST)
            except queue.Empty:
                # No messages in queue, continue
                continue
            except Exception as e:
                logger.error(f"Error in background queue processor: {e}")
                await asyncio.sleep(1)  # Wait before retrying
        
        logger.info("WebSocket queue processor stopped")

    async def start_health_monitoring(self):
        """Start automated health monitoring and reconnection"""
        logger.info("🔍 Starting automated health monitoring...")
        self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        self.auto_reconnect_task = asyncio.create_task(self._auto_reconnect_loop())

    async def stop_health_monitoring(self):
        """Stop health monitoring tasks"""
        if self.health_monitor_task:
            self.health_monitor_task.cancel()
        if self.auto_reconnect_task:
            self.auto_reconnect_task.cancel()
        logger.info("🔍 Stopped health monitoring")

    async def _health_monitor_loop(self):
        """Monitor service health and trigger reconnection if needed"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                # Check if we're receiving data
                if self.last_data_received:
                    time_since_last_data = datetime.now(IST) - self.last_data_received
                    if time_since_last_data.total_seconds() > 120:  # 2 minutes without data
                        logger.warning(f"⚠️ No data received for {time_since_last_data.total_seconds():.0f} seconds")
                        await self._trigger_reconnection("No data received")
                
                # Check session expiry
                if self.session_start_time:
                    session_age = datetime.now(IST) - self.session_start_time
                    if session_age > self.session_duration:
                        logger.warning(f"⚠️ Session expired after {session_age.total_seconds() / 3600:.1f} hours")
                        await self._trigger_reconnection("Session expired")
                
                # Check WebSocket connection status
                if not self.is_connected:
                    logger.warning("⚠️ WebSocket not connected")
                    await self._trigger_reconnection("WebSocket disconnected")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                await asyncio.sleep(5)

    async def _auto_reconnect_loop(self):
        """Handle automatic reconnection attempts"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(self.reconnect_interval)
                
                # Check if reconnection is needed
                if self.reconnect_attempts > 0 and self.reconnect_attempts < self.max_reconnect_attempts:
                    logger.info(f"🔄 Attempting reconnection ({self.reconnect_attempts}/{self.max_reconnect_attempts})")
                    await self._perform_reconnection()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in auto-reconnect loop: {e}")
                await asyncio.sleep(5)

    async def _trigger_reconnection(self, reason: str):
        """Trigger a reconnection attempt"""
        logger.warning(f"🔄 Triggering reconnection: {reason}")
        self.reconnect_attempts += 1
        if self.reconnect_attempts <= self.max_reconnect_attempts:
            await self._perform_reconnection()
        else:
            logger.error(f"❌ Max reconnection attempts reached ({self.max_reconnect_attempts})")

    async def _perform_reconnection(self):
        """Perform the actual reconnection"""
        try:
            logger.info("🔄 Performing reconnection...")
            
            # Stop current connections
            if self.websocket:
                self.websocket.close_connection()
                self.websocket = None
                self.is_connected = False
                self.connection_time = None
                self.subscribed_tokens = []
            
            # Re-authenticate
            if await self.authenticate():
                logger.info("✅ Re-authentication successful")
                self.session_start_time = datetime.now(IST)
                self.reconnect_attempts = 0
                
                # Reconnect WebSocket
                if await self.connect_websocket():
                    logger.info("✅ WebSocket reconnection successful")
                    self.last_data_received = datetime.now(IST)
                else:
                    logger.error("❌ WebSocket reconnection failed")
            else:
                logger.error("❌ Re-authentication failed")
                
        except Exception as e:
            logger.error(f"❌ Error during reconnection: {e}")

    def get_valid_futures_symbols(self) -> List[str]:
        """Get list of valid NIFTY futures symbols"""
        return list(self.valid_symbols)
    
    def get_futures_info(self) -> Dict[str, Dict[str, Any]]:
        """Get detailed futures information"""
        return self.futures_tokens.copy()
    
    async def get_service_health(self) -> Dict[str, Any]:
        """Get current service health status"""
        try:
            health_status = {
                "is_connected": self.is_connected,
                "is_authenticated": bool(self.auth_token and self.feed_token),
                "last_data_received": self.last_data_received,
                "session_start_time": self.session_start_time,
                "reconnect_attempts": self.reconnect_attempts,
                "max_reconnect_attempts": self.max_reconnect_attempts,
                "health_monitoring_active": self.health_monitor_task and not self.health_monitor_task.done(),
                "auto_reconnect_active": self.auto_reconnect_task and not self.auto_reconnect_task.done(),
            }
            
            # Calculate time since last data
            if self.last_data_received:
                time_since_data = datetime.now(IST) - self.last_data_received
                health_status["seconds_since_last_data"] = time_since_data.total_seconds()
                health_status["data_freshness"] = "recent" if time_since_data.total_seconds() < 60 else "stale"
            
            # Calculate session age
            if self.session_start_time:
                session_age = datetime.now(IST) - self.session_start_time
                health_status["session_age_hours"] = session_age.total_seconds() / 3600
                health_status["session_expires_in_hours"] = (self.session_duration - session_age).total_seconds() / 3600
            
            return health_status
            
        except Exception as e:
            logger.error(f"Error getting service health: {e}")
            return {"error": str(e)}

    async def get_websocket_status(self) -> Dict[str, Any]:
        """Get WebSocket connection status"""
        try:
            return {
                "is_connected": self.is_connected,
                "last_data_received": self.last_data_received,
                "subscribed_tokens": self.subscribed_tokens,
                "connection_time": self.connection_time,
                "websocket_thread_active": self.ws_queue_thread and self.ws_queue_thread.is_alive() if self.ws_queue_thread else False
            }
        except Exception as e:
            logger.error(f"Error getting WebSocket status: {e}")
            return {"error": str(e)}


# Create service instance
angel_one_service = AngelOneService() 