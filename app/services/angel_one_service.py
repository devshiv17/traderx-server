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
from ..core.symbols import SymbolsConfig
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
        
        # Rate-limited WebSocket mode for 2 tokens only (Option 3)
        self.websocket = None
        self.is_connected = False
        self.shutdown_flag = False
        self.connection_time = None
        self.subscribed_tokens = []
        
        # Rate-limited WebSocket configuration
        self.use_polling = False  # Enable WebSocket for real-time data
        self.polling_interval = 120  # Fallback polling if WebSocket fails
        self.websocket_reconnect_delay = 30  # 30-second reconnect delay
        self.max_subscriptions_per_connection = 2  # Limit to 2 tokens only
        self.connection_retry_count = 0
        self.max_connection_retries = 3
        self.last_websocket_error = None
        
        # Rate limiting protection (based on Angel One forum recommendations)
        self.max_retries = 2
        self.base_delay = 60  # Base delay for exponential backoff
        self.api_call_delay = 1.0  # 1 second delay between API calls (Angel One requirement)
        self.rate_limit_detected = False
        self.last_poll_time = None
        self.polling_task = None
        
        # Market data tokens - LIMITED TO 2 TOKENS ONLY for rate limiting
        self.market_tokens = self._get_limited_market_tokens()
        
        # Static futures mapping - using central symbols configuration
        self.futures_tokens = SymbolsConfig.get_futures_tokens_dict()
        self.valid_symbols = set(SymbolsConfig.get_symbol_names())
        
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
            
            # Skip futures discovery - using central symbols configuration
            logger.info(f"✅ Using central symbols configuration: {SymbolsConfig.get_symbol_names()}")
            
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
                    logger.debug(f"Search result: {search_results}")  # Debug the full response
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
                logger.debug(f"Found {len(instruments)} NIFTY instruments")
                
                # Debug: Log all instruments to understand what's available
                logger.debug("=== ALL NIFTY INSTRUMENTS FOUND ===")
                for i, inst in enumerate(instruments[:20]):  # Show more instruments
                    logger.debug(f"  [{i}] {inst.get('tradingsymbol')} | Type: {inst.get('instrumenttype')} | Token: {inst.get('token')} | Expiry: {inst.get('expiry')} | Exchange: {inst.get('exchange')}")
                
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
                # Only NIFTY futures for focused trading strategy
                known_futures_tokens = [
                    "64103",    # NIFTY28AUG25FUT - Only NIFTY futures needed
                ]
                logger.info(f"Trying {len(known_futures_tokens)} known NIFTY futures tokens for August 2025...")
                
                # Only add NIFTY_FUT1 and NIFTY_FUT2 (limit to 2 contracts)
                futures_added = 0
                for i, token in enumerate(known_futures_tokens[:2]):  # ONLY 2 futures contracts
                    std_symbol = f"NIFTY_FUT{i+1}"
                    self.futures_tokens[std_symbol] = {
                        'token': token,
                        'trading_symbol': f'NIFTY25{"AUG" if i == 0 else "SEP"}FUT',
                        'name': f'NIFTY Futures {["August", "September"][i]} 2025',
                        'expiry': f'2025-{8+i:02d}-30',  # Last Thursday estimate
                        'exchange': 'NFO'
                    }
                    self.valid_symbols.add(std_symbol)
                    
                    # Add to market tokens for subscription
                    self.market_tokens.append({"exchangeType": 2, "tokens": [token]})
                    logger.info(f"🧪 Added known futures token: {std_symbol} (Token: {token})")
                    futures_added += 1
                
                # CRITICAL: Always ensure NIFTY_FUT1 exists (required by signal detection)
                if 'NIFTY_FUT1' not in self.futures_tokens:
                    logger.warning("⚠️ NIFTY_FUT1 not found - creating proxy from NIFTY index")
                    self.futures_tokens['NIFTY_FUT1'] = {
                        'token': '99926000',  # NIFTY index token - will be processed with premium
                        'trading_symbol': 'NIFTY_PROXY_FUT1',
                        'name': 'NIFTY 50 Proxy Futures (Auto-generated)',
                        'expiry': '2025-08-30',
                        'exchange': 'NFO'
                    }
                    self.valid_symbols.add('NIFTY_FUT1')
                    logger.warning("🔄 Created NIFTY_FUT1 proxy - will add premium during data processing")
                
                logger.info(f"✅ Added {futures_added} known futures tokens + 1 proxy = {len(self.futures_tokens)} total futures")
                
            else:
                logger.info(f"✅ Using {len(self.futures_tokens)} futures found via search")
                
        except Exception as e:
            logger.error(f"❌ Error discovering futures tokens: {e}")
            logger.info("📊 Will use indices only for market data")
    
    def _setup_websocket_callbacks(self):
        """Setup WebSocket callbacks"""
        def on_data(wsapp, message):
            """Handle incoming market data"""
            try:
                logger.debug(f"📊 Received market data: {message}")
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
    
    def _get_limited_market_tokens(self):
        """Get only 2 tokens for rate-limited WebSocket: NIFTY + NIFTY28AUG25FUT"""
        limited_tokens = []
        
        # 1. NIFTY Index token
        nifty_index = SymbolsConfig.NIFTY_INDEX
        limited_tokens.append({
            "exchangeType": 1,  # NSE
            "tokens": [nifty_index.token]
        })
        
        # 2. NIFTY Futures token  
        nifty_futures = SymbolsConfig.NIFTY_FUTURES
        limited_tokens.append({
            "exchangeType": 2,  # NFO
            "tokens": [nifty_futures.token]
        })
        
        logger.info(f"🎯 Limited WebSocket tokens: NIFTY ({nifty_index.token}) + {nifty_futures.symbol} ({nifty_futures.token})")
        return limited_tokens
    
    async def start_polling(self):
        """Start REST API polling every 2 minutes (instead of WebSocket to avoid rate limits)"""
        try:
            if not self.auth_token:
                logger.error("Not authenticated. Please authenticate first.")
                return False
            
            logger.info(f"🔄 Starting REST API polling every {self.polling_interval} seconds (2 minutes) to avoid rate limits...")
            
            # Start polling task
            self.polling_task = asyncio.create_task(self._polling_loop())
            self.is_connected = True
            self.connection_time = datetime.now(IST)
            
            logger.info("✅ REST API polling started successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Polling start failed: {e}")
            return False
    
    async def _polling_loop(self):
        """Main polling loop that fetches data every 25 seconds"""
        while not self.shutdown_flag:
            try:
                # Check if it's market hours
                now = datetime.now(IST)
                if self._is_market_hours(now):
                    # Fetch live data using REST API
                    await self._fetch_live_data()
                    self.last_poll_time = now
                    self.last_data_received = now
                    
                    logger.debug(f"📊 Data fetched at {now.strftime('%H:%M:%S')} IST")
                else:
                    logger.debug("⏰ Outside market hours, skipping data fetch")
                
                # Wait for next polling interval
                await asyncio.sleep(self.polling_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in polling loop: {e}")
                await asyncio.sleep(5)  # Wait 5 seconds before retrying
        
        logger.info("🛑 Polling loop stopped")
    
    async def _fetch_with_retry(self, api_call):
        """Execute API call with exponential backoff on rate limits"""
        for attempt in range(self.max_retries):
            try:
                # Execute the API call
                result = await asyncio.get_event_loop().run_in_executor(None, api_call)
                
                # If successful, reset rate limit flag
                if self.rate_limit_detected:
                    self.rate_limit_detected = False
                    logger.info("✅ Rate limit recovered, resuming normal operation")
                
                return result
                
            except Exception as e:
                error_msg = str(e)
                
                # Check if it's a rate limiting error
                if "exceeding access rate" in error_msg or "Access denied" in error_msg:
                    self.rate_limit_detected = True
                    delay = self.base_delay * (2 ** attempt)  # Exponential backoff
                    
                    logger.warning(f"⚠️ Rate limit detected (attempt {attempt + 1}/{self.max_retries}). Waiting {delay}s before retry...")
                    
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("❌ Max retries exceeded for rate limit. Skipping this cycle.")
                        return None
                else:
                    # Non-rate-limit error, don't retry
                    logger.error(f"❌ API call failed with non-rate-limit error: {e}")
                    raise e
        
        return None
    
    def _is_market_hours(self, dt: datetime) -> bool:
        """Check if current time is within market hours (9:15 AM - 3:30 PM IST)"""
        if dt.weekday() >= 5:  # Weekend
            return False
        
        market_open = datetime(dt.year, dt.month, dt.day, 9, 15, 0, 0)
        market_close = datetime(dt.year, dt.month, dt.day, 15, 30, 0, 0)
        
        return market_open <= dt <= market_close
    
    async def _fetch_live_data(self):
        """Fetch live market data using REST API - using central symbols configuration"""
        try:
            if not self.smart_api:
                logger.error("SmartAPI not initialized")
                return
            
            # Get NIFTY 50 data using central config with rate limiting protection
            nifty_symbol = SymbolsConfig.NIFTY_INDEX
            nifty_data = await self._fetch_with_retry(
                lambda: self.smart_api.ltpData(nifty_symbol.exchange, nifty_symbol.name, nifty_symbol.token)
            )
            
            if nifty_data and 'data' in nifty_data:
                # Process NIFTY data
                await self._process_rest_data(nifty_symbol.symbol, nifty_data['data'])
                logger.debug(f"✅ Processed {nifty_symbol.symbol} data: ₹{nifty_data['data'].get('ltp', 0):.2f}")
            else:
                logger.warning("⚠️ Failed to fetch NIFTY data")
                return
            
            # Angel One requirement: Add delay between API calls
            logger.debug(f"⏱️ Waiting {self.api_call_delay}s between API calls (Angel One requirement)")
            await asyncio.sleep(self.api_call_delay)
            
            # Fetch futures data using central config
            futures_symbol = SymbolsConfig.NIFTY_FUTURES
            logger.debug(f"🔍 Fetching {futures_symbol.symbol} data...")
            
            futures_info = self.futures_tokens.get(futures_symbol.symbol)
            if futures_info:
                try:
                    token = futures_info['token']
                    trading_symbol = futures_info['trading_symbol']
                    exchange = futures_info.get('exchange', 'NFO')
                    
                    logger.debug(f"📊 Fetching NIFTY Futures: {trading_symbol} (Token: {token})")
                    
                    # Fetch real futures data with rate limiting protection
                    futures_data = await self._fetch_with_retry(
                        lambda t=token, ts=trading_symbol, ex=exchange: self.smart_api.ltpData(ex, ts, t)
                    )
                    
                    if futures_data and futures_data.get('status') and 'data' in futures_data:
                        # Process real futures data
                        await self._process_rest_data(futures_symbol.symbol, futures_data['data'])
                        logger.debug(f"✅ Processed {futures_symbol.symbol} data: ₹{futures_data['data'].get('ltp', 0):.2f}")
                    else:
                        logger.warning(f"⚠️ No valid data for {futures_symbol.symbol} - creating proxy from NIFTY")
                        
                        # FALLBACK: Use NIFTY index as proxy with premium
                        proxy_data = nifty_data['data'].copy()
                        premium = 20.0  # Premium for futures
                        if 'ltp' in proxy_data:
                            proxy_data['ltp'] = float(proxy_data['ltp']) + premium
                        await self._process_rest_data(futures_symbol.symbol, proxy_data)
                        logger.info(f"✅ Created fallback proxy for {futures_symbol.symbol} with ₹{premium} premium")
                        
                except Exception as futures_error:
                    logger.error(f"❌ Error fetching {futures_symbol.symbol} data: {futures_error}")
                    
                    # EMERGENCY FALLBACK: Create proxy data from NIFTY
                    try:
                        logger.warning(f"🔄 Creating emergency proxy for {futures_symbol.symbol}")
                        proxy_data = nifty_data['data'].copy()
                        premium = 25.0
                        if 'ltp' in proxy_data:
                            proxy_data['ltp'] = float(proxy_data['ltp']) + premium
                        await self._process_rest_data(futures_symbol.symbol, proxy_data)
                        logger.info(f"✅ Emergency proxy created for {futures_symbol.symbol} with ₹{premium} premium")
                    except Exception as proxy_error:
                        logger.error(f"❌ Failed to create proxy for {futures_symbol.symbol}: {proxy_error}")
            
            # Log summary - using central config
            symbol_names = SymbolsConfig.get_symbol_names()
            logger.debug(f"📊 Data fetch complete: {len(symbol_names)} symbols processed ({', '.join(symbol_names)})")
            
        except Exception as e:
            logger.error(f"❌ Error fetching live data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
    
    async def _process_rest_data(self, symbol: str, data: Dict[str, Any]):
        """Process REST API data and store as tick data"""
        try:
            # Convert REST API response to tick data format
            tick_data = {
                'symbol': symbol,
                'price': float(data.get('ltp', 0)),
                'timestamp': TimezoneUtils.get_ist_now(),
                'token': data.get('symboltoken', ''),
                'exchange': 'NSE',
                'high': None,  # REST API doesn't provide OHLC
                'low': None,
                'volume': None,
                'change': None,
                'change_percent': None,
                'source': 'angel_one_rest_api'
            }
            
            if tick_data['price'] > 0:
                # Store in tick data service
                tick_service = get_tick_data_service()
                await tick_service.store_tick_data(tick_data)
                
                # Broadcast to WebSocket clients
                await broadcast_price_update(symbol, tick_data)
                
                logger.debug(f"✅ Processed REST data: {symbol} @ ₹{tick_data['price']:.2f}")
            
        except Exception as e:
            logger.error(f"❌ Error processing REST data for {symbol}: {e}")
    
    async def connect_websocket(self):
        """Rate-limited WebSocket connection for 2 tokens only"""
        try:
            if not self.auth_token or not self.feed_token:
                logger.error("❌ Missing authentication tokens for WebSocket")
                return False
            
            # Check if we've exceeded retry limit
            if self.connection_retry_count >= self.max_connection_retries:
                logger.error(f"❌ Max WebSocket retries ({self.max_connection_retries}) exceeded. Falling back to polling.")
                return await self.start_polling()
            
            logger.info(f"🔗 Connecting rate-limited WebSocket (attempt {self.connection_retry_count + 1}/{self.max_connection_retries})...")
            logger.info(f"🎯 Subscribing to only {len(self.market_tokens)} tokens to avoid rate limits")
            
            # Initialize WebSocket with rate limiting
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
            
            # Connect with delay to avoid rate limits
            await asyncio.sleep(2)  # Initial delay
            connection_result = await asyncio.get_event_loop().run_in_executor(
                None, self.websocket.connect
            )
            
            if connection_result:
                logger.info("✅ Rate-limited WebSocket connected successfully")
                self.connection_retry_count = 0  # Reset retry count on success
                self.last_websocket_error = None
                return True
            else:
                raise Exception("WebSocket connection failed")
                
        except Exception as e:
            self.connection_retry_count += 1
            self.last_websocket_error = str(e)
            logger.error(f"❌ WebSocket connection failed (attempt {self.connection_retry_count}): {e}")
            
            if "429" in str(e) or "rate" in str(e).lower():
                logger.warning(f"⚠️ Rate limit detected. Waiting {self.websocket_reconnect_delay}s before retry...")
                await asyncio.sleep(self.websocket_reconnect_delay)
            
            # Fallback to polling if all retries failed
            if self.connection_retry_count >= self.max_connection_retries:
                logger.warning("🔄 Falling back to polling mode after WebSocket failures")
                return await self.start_polling()
            
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
            
            logger.debug(f"✅ Processed tick data for {parsed_data.get('symbol', 'unknown')}")
            
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
            
            logger.debug(f"✅ Processed tick data for {parsed_data.get('symbol', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Error processing WebSocket data: {e}")
    
    async def _store_tick_data_async(self, tick_data):
        """Store tick data in database asynchronously"""
        try:
            collection = get_collection('tick_data')
            
            # Create unique identifier to prevent duplicates using received_at
            tick_data['_id'] = f"{tick_data['symbol']}_{tick_data['received_at']}"
            
            # Use upsert to avoid duplicates
            result = await collection.replace_one(
                {'_id': tick_data['_id']},
                tick_data,
                upsert=True
            )
            
            if result.upserted_id:
                logger.debug(f"📊 Stored new tick: {tick_data['symbol']} @ {tick_data['price']}")
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
                
                logger.debug(f"✅ Parsed tick data: {symbol} @ ₹{price_rupees:.2f}")
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
        """Convert token to symbol name using central configuration"""
        # Get token to symbol mapping from central config
        token_map = SymbolsConfig.get_token_to_symbol_map()
        
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
            
            # Default symbols if none provided - using central configuration
            if not symbols:
                symbols = SymbolsConfig.get_symbol_names()
            
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
        """Convert symbol to token using central configuration"""
        # Get symbol to token mapping from central config
        symbol_map = SymbolsConfig.get_symbol_to_token_map()
        
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
            logger.debug(f"📊 Stored market data for {data['symbol']}: {data['price']}")
            
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
                    'timestamp': TimezoneUtils.get_ist_now()
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
            cursor = collection.find(query).sort('received_at', -1).limit(limit)
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
        """Get market summary with latest prices - using central configuration"""
        try:
            # Get latest data using central symbols configuration
            symbols = SymbolsConfig.get_symbol_names()
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
            
            # Enable rate-limited WebSocket for real-time data (2 tokens only)
            await self.connect_websocket()
            
            # Start health monitoring and auto-reconnection
            await self.start_health_monitoring()
            
            logger.info("✅ Angel One feed service started with rate-limited WebSocket (2 tokens only)")
        else:
            logger.error("❌ Failed to start Angel One feed service - authentication failed")
    
    async def stop_feed_service(self):
        """Stop the Angel One feed service"""
        try:
            self.shutdown_flag = True
            
            # Stop health monitoring
            await self.stop_health_monitoring()
            
            # Stop polling task
            if self.polling_task:
                self.polling_task.cancel()
                try:
                    await self.polling_task
                except asyncio.CancelledError:
                    pass
                logger.info("🛑 REST API polling stopped")
            
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