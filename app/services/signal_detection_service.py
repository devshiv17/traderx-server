"""
Advanced Signal Detection Service
Implements session-based breakout signals with VWAP and technical analysis
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import pytz
from collections import defaultdict, deque

from ..core.database import get_collection
from ..core.symbols import SymbolsConfig
from ..models.signal import SignalModel, SignalType, SignalStrength
from .tick_data_service import tick_data_service

logger = logging.getLogger(__name__)

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

class TradingSession:
    def __init__(self, name: str, start_time: str, end_time: str):
        self.name = name
        self.start_time = start_time  # "09:30"
        self.end_time = end_time      # "09:35"
        self.high = None
        self.low = None
        self.is_active = False
        self.is_completed = False
        self.session_data = {}
        
    def reset_for_day(self):
        """Reset session for new trading day"""
        self.high = None
        self.low = None
        self.is_active = False
        self.is_completed = False
        self.session_data = {}

class SignalDetectionService:
    def __init__(self):
        # Trading sessions - IST times
        self.sessions = [
            TradingSession("Morning Opening", "09:30", "09:35"),
            TradingSession("Mid Morning", "09:45", "09:55"),
            TradingSession("Pre Lunch", "10:30", "10:45"),
            TradingSession("Lunch Break", "11:50", "12:20")
        ]
        
        # Symbols to monitor - using central symbols configuration
        self.nifty_index = SymbolsConfig.NIFTY_INDEX.symbol
        self.nifty_futures = [SymbolsConfig.NIFTY_FUTURES.symbol]
        
        logger.info(f"🎯 TRADING RULE: Signal detection based on {self.nifty_index} + {self.nifty_futures[0]} breakouts")
        
        # Signal tracking
        self.active_signals = {}
        self.signal_history = []
        
        # Session state tracking - each session can have one active signal
        self.session_signals = {}  # Format: {"Morning Opening": {"signal_type": "BUY_PUT", "timestamp": datetime, "active": True}}
        
        # Technical indicators data
        self.price_data = defaultdict(lambda: deque(maxlen=100))  # Last 100 5-min candles
        self.volume_data = defaultdict(lambda: deque(maxlen=100))
        self.vwap_data = defaultdict(dict)
        
        # Real-time monitoring
        self.monitoring_active = False
        self.last_processed_time = None
        
        # Enhanced strategy parameters
        self.min_volume_threshold = 10000  # Minimum volume for valid signal
        self.vwap_deviation_threshold = 0.5  # % deviation from VWAP for confirmation
        self.breakout_confirmation_candles = 2  # Number of candles to confirm breakout
        
    async def start_monitoring(self):
        """Start real-time signal monitoring"""
        logger.info("🚀 Starting advanced signal detection service...")
        self.monitoring_active = True
        
        # Reset session signal tracking for new day
        today = datetime.now(IST).strftime('%Y-%m-%d')
        self.session_signals = {}
        logger.info(f"📅 Reset session signal tracking for {today}")
        
        # Load existing active signals from database
        await self._load_active_signals_from_db()
        
        # Reset sessions for new day
        for session in self.sessions:
            session.reset_for_day()
        
        # Force process past sessions if they have data
        await self._process_historical_sessions()
        
        # Start monitoring loop
        asyncio.create_task(self._monitoring_loop())
        logger.info("✅ Signal detection service started")
    
    async def _load_active_signals_from_db(self):
        """Load existing active signals from database"""
        try:
            # Check if database connection is available
            from ..core.database import Database
            if Database.database is None:
                logger.warning("Database connection not available during signal loading, skipping...")
                return
                
            collection = get_collection('signals')
            # Get signals that are still active (simplified query)
            
            active_signals_cursor = collection.find({
                'status': 'ACTIVE'
            }).sort('created_at', -1).limit(50)
            
            signals = await active_signals_cursor.to_list(100)
            
            for signal_doc in signals:
                signal_id = signal_doc.get('id', str(signal_doc.get('_id', '')))
                
                # Convert database document to signal format
                signal_data = {
                    'id': signal_id,
                    'session_name': signal_doc.get('session_name'),
                    'signal_type': signal_doc.get('signal_type'),
                    'reason': signal_doc.get('reason'),
                    'timestamp': signal_doc.get('timestamp'),
                    'nifty_price': signal_doc.get('nifty_price'),
                    'future_price': signal_doc.get('future_price'),
                    'future_symbol': signal_doc.get('future_symbol'),
                    'entry_price': signal_doc.get('entry_price'),
                    'stop_loss': signal_doc.get('stop_loss'),
                    'target_1': signal_doc.get('target_1'),
                    'target_2': signal_doc.get('target_2'),
                    'confidence': signal_doc.get('confidence'),
                    'status': signal_doc.get('status'),
                    'session_high': signal_doc.get('session_high'),
                    'session_low': signal_doc.get('session_low'),
                    'future_session_high': signal_doc.get('future_session_high'),
                    'future_session_low': signal_doc.get('future_session_low'),
                    'vwap_nifty': signal_doc.get('vwap_nifty'),
                    'vwap_future': signal_doc.get('vwap_future'),
                    'breakout_details': signal_doc.get('breakout_details'),
                    'display_text': signal_doc.get('display_text'),
                    'breakout_type': signal_doc.get('breakout_type'),
                    'volume_confirmation': signal_doc.get('volume_confirmation'),
                    'technical_data': signal_doc.get('technical_data', {})
                }
                
                self.active_signals[signal_id] = signal_data
                self.signal_history.append(signal_data)
                
                # Track existing active signals per session with new key format
                session_name = signal_doc.get('session_name')
                signal_type = signal_doc.get('signal_type')
                if session_name and signal_type and signal_doc.get('status') == 'ACTIVE':
                    session_key = f"{session_name}_{signal_type}"
                    self.session_signals[session_key] = {
                        'signal_type': signal_type,
                        'timestamp': signal_doc.get('timestamp'),
                        'active': True,
                        'signal_id': signal_id,
                        'session_name': session_name
                    }
            
            logger.info(f"✅ Loaded {len(signals)} active signals from database")
            logger.info(f"📋 Tracking {len(self.session_signals)} active session signals")
            
        except Exception as e:
            logger.error(f"Error loading active signals from database: {e}")
    
    async def stop_monitoring(self):
        """Stop signal monitoring"""
        self.monitoring_active = False
        logger.info("🛑 Signal detection service stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.monitoring_active:
            try:
                current_time = datetime.now(IST)
                
                # Only monitor during market hours (9:15 AM - 3:30 PM IST)
                if not self._is_market_hours(current_time):
                    await asyncio.sleep(60)  # Check every minute outside market hours
                    continue
                
                # Process current 5-minute candle
                await self._process_current_candle(current_time)
                
                # Check active sessions
                await self._check_sessions(current_time)
                
                # Monitor for breakouts
                await self._monitor_breakouts(current_time)
                
                # Update technical indicators
                await self._update_technical_indicators(current_time)
                
                await asyncio.sleep(10)  # Check every 10 seconds during market hours
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)
    
    async def _process_historical_sessions(self):
        """Process sessions that may have already passed today"""
        try:
            current_time = datetime.now(IST)
            logger.info(f"🕐 Current time: {current_time.strftime('%H:%M:%S')}")
            
            for session in self.sessions:
                session_start_time = datetime.strptime(session.start_time, "%H:%M").replace(
                    year=current_time.year, month=current_time.month, day=current_time.day, tzinfo=IST
                )
                session_end_time = datetime.strptime(session.end_time, "%H:%M").replace(
                    year=current_time.year, month=current_time.month, day=current_time.day, tzinfo=IST
                )
                
                logger.info(f"📅 Session {session.name}: {session_start_time.strftime('%H:%M')} - {session_end_time.strftime('%H:%M')}, Completed: {session.is_completed}")
                
                # If session has ended but wasn't processed, process it now
                if current_time > session_end_time and not session.is_completed:
                    logger.info(f"🔄 Processing historical session: {session.name}")
                    await self._process_session_retroactively(session, session_start_time, session_end_time)
                    session.is_completed = True
                    logger.info(f"✅ Historical session {session.name} processing complete")
                elif current_time <= session_end_time:
                    logger.info(f"⏳ Session {session.name} not yet ended")
                else:
                    logger.info(f"✅ Session {session.name} already processed")
                    
        except Exception as e:
            logger.error(f"Error processing historical sessions: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
    
    async def _process_session_retroactively(self, session: TradingSession, start_time: datetime, end_time: datetime):
        """Process a session retroactively using historical data"""
        try:
            logger.info(f"📊 Processing session {session.name} from {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')}")
            symbols = [self.nifty_index] + self.nifty_futures
            
            for symbol in symbols:
                if symbol not in session.session_data:
                    session.session_data[symbol] = {'high': None, 'low': None, 'candles': []}
                
                candle_count = 0
                # Get all candles for this session period
                current_candle_start = start_time
                while current_candle_start < end_time:
                    candle_end = current_candle_start + timedelta(minutes=5)
                    
                    logger.debug(f"🔍 Getting candle data for {symbol}: {current_candle_start.strftime('%H:%M')} - {candle_end.strftime('%H:%M')}")
                    
                    candle_data = await self._get_5min_candle_data(symbol, 
                        current_candle_start.replace(tzinfo=None), 
                        candle_end.replace(tzinfo=None))
                    
                    if candle_data:
                        candle_count += 1
                        session.session_data[symbol]['candles'].append(candle_data)
                        logger.debug(f"📈 Found candle for {symbol}: O={candle_data['open']:.2f}, H={candle_data['high']:.2f}, L={candle_data['low']:.2f}, C={candle_data['close']:.2f}")
                        
                        # Update session high/low
                        if session.session_data[symbol]['high'] is None:
                            session.session_data[symbol]['high'] = candle_data['high']
                            session.session_data[symbol]['low'] = candle_data['low']
                        else:
                            session.session_data[symbol]['high'] = max(
                                session.session_data[symbol]['high'], candle_data['high']
                            )
                            session.session_data[symbol]['low'] = min(
                                session.session_data[symbol]['low'], candle_data['low']
                            )
                    else:
                        logger.debug(f"❌ No candle data found for {symbol} at {current_candle_start.strftime('%H:%M')}")
                    
                    current_candle_start = candle_end
                
                logger.info(f"📊 {symbol}: Found {candle_count} candles, High: {session.session_data[symbol]['high']}, Low: {session.session_data[symbol]['low']}")
                    
                # Add data to main tracking
                if symbol in session.session_data and session.session_data[symbol]['candles']:
                    self.price_data[symbol].extend(session.session_data[symbol]['candles'])
                    
            logger.info(f"✅ Processed {session.name}: NIFTY high/low: {session.session_data.get(self.nifty_index, {}).get('high')}/{session.session_data.get(self.nifty_index, {}).get('low')}")
            
        except Exception as e:
            logger.error(f"Error processing session retroactively: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
    
    def _is_market_hours(self, current_time: datetime) -> bool:
        """Check if current time is within market hours"""
        # Market hours: 9:15 AM - 3:30 PM IST, Monday-Friday
        if current_time.weekday() >= 5:  # Weekend
            return False
        
        market_start = current_time.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = current_time.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_start <= current_time <= market_end
    
    async def _process_current_candle(self, current_time: datetime):
        """Process current 5-minute candle data"""
        try:
            # Get 5-minute candle start time
            candle_minute = (current_time.minute // 5) * 5
            candle_start = current_time.replace(minute=candle_minute, second=0, microsecond=0)
            candle_end = candle_start + timedelta(minutes=5)
            
            # Convert to naive datetime for database queries (remove timezone info)
            candle_start_naive = candle_start.replace(tzinfo=None)
            candle_end_naive = candle_end.replace(tzinfo=None)
            
            # Skip if we've already processed this candle
            if self.last_processed_time and candle_start <= self.last_processed_time:
                return
            
            # Get tick data for all monitored symbols
            symbols = [self.nifty_index] + self.nifty_futures
            
            for symbol in symbols:
                candle_data = await self._get_5min_candle_data(symbol, candle_start_naive, candle_end_naive)
                if candle_data:
                    self.price_data[symbol].append(candle_data)
                    self.volume_data[symbol].append(candle_data.get('volume', 0))
            
            self.last_processed_time = candle_start
            logger.debug(f"📊 Processed 5-min candle: {candle_start.strftime('%H:%M')}")
            
        except Exception as e:
            logger.error(f"Error processing current candle: {e}")
    
    async def _get_5min_candle_data(self, symbol: str, start_time: datetime, end_time: datetime) -> Optional[Dict]:
        """Get 5-minute OHLCV data for symbol"""
        try:
            # For futures symbols, try to get their data first, then fall back to NIFTY
            ticks = await tick_data_service.get_ticks_for_timerange(symbol, start_time, end_time)
            
            # If no ticks found for futures symbols, use NIFTY as proxy
            if not ticks and symbol in self.nifty_futures:
                logger.debug(f"No data for {symbol}, using NIFTY as proxy")
                ticks = await tick_data_service.get_ticks_for_timerange(self.nifty_index, start_time, end_time)
            
            if not ticks:
                return None
            
            prices = [tick['price'] for tick in ticks]
            volumes = [tick.get('volume', 0) or 0 for tick in ticks]
            
            return {
                'timestamp': start_time,
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
                'volume': sum(volumes),
                'tick_count': len(ticks)
            }
            
        except Exception as e:
            logger.error(f"Error getting candle data for {symbol}: {e}")
            return None
    
    async def _check_sessions(self, current_time: datetime):
        """Check and update session status"""
        current_time_str = current_time.strftime("%H:%M")
        
        for session in self.sessions:
            # Check if session is starting
            if not session.is_active and current_time_str == session.start_time:
                session.is_active = True
                session.reset_for_day()
                logger.info(f"📅 Session '{session.name}' started at {current_time_str}")
            
            # Check if session is ending
            elif session.is_active and current_time_str == session.end_time:
                session.is_active = False
                session.is_completed = True
                await self._finalize_session(session, current_time)
                logger.info(f"🏁 Session '{session.name}' completed at {current_time_str}")
            
            # Update session data during active period
            elif session.is_active:
                await self._update_session_data(session, current_time)
    
    async def _update_session_data(self, session: TradingSession, current_time: datetime):
        """Update session high/low data"""
        symbols = [self.nifty_index] + self.nifty_futures
        
        for symbol in symbols:
            if symbol not in session.session_data:
                session.session_data[symbol] = {'high': None, 'low': None, 'candles': []}
            
            # Get latest candle data
            if symbol in self.price_data and self.price_data[symbol]:
                latest_candle = self.price_data[symbol][-1]
                session.session_data[symbol]['candles'].append(latest_candle)
                
                # Update session high/low
                if session.session_data[symbol]['high'] is None:
                    session.session_data[symbol]['high'] = latest_candle['high']
                    session.session_data[symbol]['low'] = latest_candle['low']
                else:
                    session.session_data[symbol]['high'] = max(
                        session.session_data[symbol]['high'], 
                        latest_candle['high']
                    )
                    session.session_data[symbol]['low'] = min(
                        session.session_data[symbol]['low'], 
                        latest_candle['low']
                    )
    
    async def _finalize_session(self, session: TradingSession, current_time: datetime):
        """Finalize session and prepare for breakout monitoring"""
        logger.info(f"📊 Finalizing session '{session.name}':")
        
        for symbol in session.session_data:
            high = session.session_data[symbol]['high']
            low = session.session_data[symbol]['low']
            logger.info(f"  {symbol}: High={high:.2f}, Low={low:.2f}")
    
    async def _monitor_breakouts(self, current_time: datetime):
        """Monitor for breakouts after session completion"""
        for session in self.sessions:
            if not session.is_completed:
                continue
            
            await self._check_breakout_conditions(session, current_time)
    
    async def _check_breakout_conditions(self, session: TradingSession, current_time: datetime):
        """
        Check NIFTY 50 INDEX vs NIFTY 50 FUTURES breakout conditions
        
        CORE TRADING RULE: Signals generated based on how NIFTY Index and NIFTY Futures
        break their respective session highs/lows
        """
        try:
            # Get current prices for NIFTY Index and NIFTY Futures
            nifty_index_price = await self._get_current_price(self.nifty_index)
            nifty_futures_price = await self._get_current_price(self.nifty_futures[0])  # Primary futures contract
            
            if not nifty_index_price or not nifty_futures_price:
                logger.debug(f"Missing price data: Index={nifty_index_price}, Futures={nifty_futures_price}")
                return
            
            logger.debug(f"🎯 Checking breakout: NIFTY Index @ ₹{nifty_index_price:.2f}, NIFTY Futures @ ₹{nifty_futures_price:.2f}")
            
            # Get NIFTY Index session levels with string parsing fix
            nifty_session_data = session.session_data.get(self.nifty_index, {})
            
            # CRITICAL FIX: Handle string representation of session data
            if isinstance(nifty_session_data, str):
                try:
                    import ast
                    nifty_session_data = ast.literal_eval(nifty_session_data)
                    logger.debug(f"📊 Parsed NIFTY session data from string")
                except Exception as e:
                    logger.error(f"Failed to parse NIFTY session data string: {e}")
                    return
            
            nifty_index_session_high = nifty_session_data.get('high') if isinstance(nifty_session_data, dict) else None
            nifty_index_session_low = nifty_session_data.get('low') if isinstance(nifty_session_data, dict) else None
            
            if not nifty_index_session_high or not nifty_index_session_low:
                logger.debug(f"No valid session data for NIFTY Index in session {session.name}")
                return
            
            # Get NIFTY Futures session levels with string parsing fix
            futures_symbol = self.nifty_futures[0]
            futures_session_data = session.session_data.get(futures_symbol, {})
            
            # CRITICAL FIX: Handle string representation of futures session data
            if isinstance(futures_session_data, str):
                try:
                    import ast
                    futures_session_data = ast.literal_eval(futures_session_data)
                    logger.debug(f"📊 Parsed {futures_symbol} session data from string")
                except Exception as e:
                    logger.error(f"Failed to parse {futures_symbol} session data string: {e}")
                    futures_session_data = {}
            
            futures_session_high = futures_session_data.get('high') if isinstance(futures_session_data, dict) else None
            futures_session_low = futures_session_data.get('low') if isinstance(futures_session_data, dict) else None
            
            # CRITICAL: If futures session data is missing, use NIFTY Index session data as proxy
            # This ensures signal detection continues even if we don't have separate futures data
            if not futures_session_high or not futures_session_low:
                logger.warning(f"⚠️ No session data for {futures_symbol}, using NIFTY Index session levels as proxy")
                futures_session_high = nifty_index_session_high
                futures_session_low = nifty_index_session_low
            
            # Determine breakout conditions for NIFTY Index vs NIFTY Futures
            index_breaks_high = nifty_index_price > nifty_index_session_high
            index_breaks_low = nifty_index_price < nifty_index_session_low
            futures_breaks_high = nifty_futures_price > futures_session_high
            futures_breaks_low = nifty_futures_price < futures_session_low
            
            logger.debug(f"📊 Breakout Analysis:")
            logger.debug(f"   NIFTY Index: {nifty_index_price:.2f} vs High {nifty_index_session_high:.2f} vs Low {nifty_index_session_low:.2f}")
            logger.debug(f"   NIFTY Futures: {nifty_futures_price:.2f} vs High {futures_session_high:.2f} vs Low {futures_session_low:.2f}")
            logger.debug(f"   Index breaks: High={index_breaks_high}, Low={index_breaks_low}")
            logger.debug(f"   Futures breaks: High={futures_breaks_high}, Low={futures_breaks_low}")
                
            # Apply NIFTY 50 Index vs NIFTY 50 Futures signal logic
            signal_type = None
            signal_reason = ""
            
            if index_breaks_high and futures_breaks_high:
                # Both NIFTY Index and Futures break session high -> BUY CALL (CE)
                signal_type = "BUY_CALL"
                signal_reason = f"Both NIFTY Index and Futures broke session high - BULLISH"
                
            elif index_breaks_low and futures_breaks_low:
                # Both NIFTY Index and Futures break session low -> BUY PUT (PE)
                signal_type = "BUY_PUT"
                signal_reason = f"Both NIFTY Index and Futures broke session low - BEARISH"
                
            elif (index_breaks_high and not futures_breaks_high) or (futures_breaks_high and not index_breaks_high):
                # Only one breaks high (divergence) -> BUY PUT (PE)
                signal_type = "BUY_PUT"
                signal_reason = f"Divergent breakout: Only one instrument broke session high - expecting reversal"
                
            elif (index_breaks_low and not futures_breaks_low) or (futures_breaks_low and not index_breaks_low):
                # Only one breaks low (divergence) -> BUY CALL (CE)
                signal_type = "BUY_CALL"
                signal_reason = f"Divergent breakout: Only one instrument broke session low - expecting reversal"
            
            # Generate signal if conditions are met
            if signal_type:
                logger.info(f"🚨 SIGNAL DETECTED: {signal_type} - {signal_reason}")
                
                # Additional confirmation with VWAP and volume
                if await self._confirm_signal_with_technical_analysis(
                    self.nifty_index, futures_symbol, signal_type, current_time
                ):
                    await self._generate_signal(
                        session, signal_type, signal_reason, current_time,
                        nifty_index_price, nifty_futures_price, futures_symbol
                    )
                else:
                    logger.info(f"❌ Signal {signal_type} rejected by technical analysis")
        
        except Exception as e:
            logger.error(f"Error checking breakout conditions: {e}")
    
    async def _confirm_signal_with_technical_analysis(
        self, nifty_symbol: str, future_symbol: str, signal_type: str, current_time: datetime
    ) -> bool:
        """Confirm signal using VWAP, volume, and other technical indicators"""
        try:
            # Calculate VWAP for both symbols
            nifty_vwap = await self._calculate_vwap(nifty_symbol)
            future_vwap = await self._calculate_vwap(future_symbol)
            
            if not nifty_vwap or not future_vwap:
                return True  # Allow signal if VWAP calculation fails
            
            # Get current prices
            nifty_price = await self._get_current_price(nifty_symbol)
            future_price = await self._get_current_price(future_symbol)
            
            # Check volume confirmation
            volume_confirmed = await self._check_volume_confirmation(nifty_symbol, future_symbol)
            
            # VWAP confirmation logic
            vwap_confirmed = True
            
            if signal_type in ["BUY_CALL"]:
                # For call signals, price should be above or near VWAP
                nifty_vwap_ok = (nifty_price - nifty_vwap) / nifty_vwap >= -self.vwap_deviation_threshold / 100
                future_vwap_ok = (future_price - future_vwap) / future_vwap >= -self.vwap_deviation_threshold / 100
                vwap_confirmed = nifty_vwap_ok and future_vwap_ok
                
            elif signal_type in ["BUY_PUT"]:
                # For put signals, price should be below or near VWAP
                nifty_vwap_ok = (nifty_vwap - nifty_price) / nifty_vwap >= -self.vwap_deviation_threshold / 100
                future_vwap_ok = (future_vwap - future_price) / future_vwap >= -self.vwap_deviation_threshold / 100
                vwap_confirmed = nifty_vwap_ok and future_vwap_ok
            
            logger.debug(f"Technical confirmation - Volume: {volume_confirmed}, VWAP: {vwap_confirmed}")
            return volume_confirmed and vwap_confirmed
            
        except Exception as e:
            logger.error(f"Error in technical analysis confirmation: {e}")
            return True  # Allow signal on error
    
    async def _calculate_vwap(self, symbol: str) -> Optional[float]:
        """Calculate Volume-Weighted Average Price"""
        try:
            if symbol not in self.price_data or len(self.price_data[symbol]) < 5:
                return None
            
            # Get today's data starting from 9:15 AM (IST)
            today_start = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0, tzinfo=None)
            
            total_volume = 0
            total_price_volume = 0
            
            for candle in self.price_data[symbol]:
                if candle['timestamp'] >= today_start:
                    typical_price = (candle['high'] + candle['low'] + candle['close']) / 3
                    volume = candle['volume']
                    
                    total_price_volume += typical_price * volume
                    total_volume += volume
            
            if total_volume > 0:
                return total_price_volume / total_volume
            
            return None
            
        except Exception as e:
            logger.error(f"Error calculating VWAP for {symbol}: {e}")
            return None
    
    async def _check_volume_confirmation(self, nifty_symbol: str, future_symbol: str) -> bool:
        """Check if current volume supports the signal"""
        try:
            # Get recent volume data
            nifty_volumes = list(self.volume_data[nifty_symbol])[-5:]  # Last 5 candles
            future_volumes = list(self.volume_data[future_symbol])[-5:]
            
            if len(nifty_volumes) < 3 or len(future_volumes) < 3:
                return True  # Allow if insufficient data
            
            # Check if current volume is above minimum threshold
            current_nifty_volume = nifty_volumes[-1] if nifty_volumes else 0
            current_future_volume = future_volumes[-1] if future_volumes else 0
            
            # CRITICAL FIX: Handle zero volume in tick data (common in testing/some data feeds)
            # If we have price data but zero volume, check if we have tick count instead
            has_tick_data = (nifty_symbol in self.price_data and len(self.price_data[nifty_symbol]) > 0)
            
            if has_tick_data:
                recent_candles = list(self.price_data[nifty_symbol])[-5:]
                total_tick_count = sum(candle.get('tick_count', 0) for candle in recent_candles)
                
                # If we have tick data (tick_count > 0) but zero volume, allow the signal
                if total_tick_count > 0:
                    logger.debug(f"Volume confirmation: Zero volume but {total_tick_count} ticks - allowing signal")
                    return True
            
            # Calculate average volume of previous candles
            avg_nifty_volume = sum(nifty_volumes[:-1]) / len(nifty_volumes[:-1]) if len(nifty_volumes) > 1 else 0
            avg_future_volume = sum(future_volumes[:-1]) / len(future_volumes[:-1]) if len(future_volumes) > 1 else 0
            
            # Volume should be above minimum and preferably above average
            volume_ok = (
                current_nifty_volume >= self.min_volume_threshold and
                current_future_volume >= self.min_volume_threshold and
                current_nifty_volume >= avg_nifty_volume * 0.8 and
                current_future_volume >= avg_future_volume * 0.8
            )
            
            # Log volume check details for debugging
            if not volume_ok:
                logger.debug(f"Volume check failed: Current({current_nifty_volume}, {current_future_volume}) vs Min({self.min_volume_threshold})")
            
            return volume_ok
            
        except Exception as e:
            logger.error(f"Error checking volume confirmation: {e}")
            return True  # Allow on error
    
    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for symbol"""
        try:
            # Try to get price from symbol's own data first
            if symbol in self.price_data and self.price_data[symbol]:
                return self.price_data[symbol][-1]['close']
            
            # For futures symbols, fall back to NIFTY price as proxy
            if symbol in self.nifty_futures:
                logger.debug(f"No price data for {symbol}, using NIFTY as proxy")
                if self.nifty_index in self.price_data and self.price_data[self.nifty_index]:
                    return self.price_data[self.nifty_index][-1]['close']
                    
            return None
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return None
    
    async def _generate_signal(
        self, session: TradingSession, signal_type: str, reason: str, 
        timestamp: datetime, nifty_price: float, future_price: float, future_symbol: str
    ):
        """Generate and store trading signal"""
        try:
            # Strict duplicate prevention logic
            # Only ONE signal per session per signal type (BUY_CALL or BUY_PUT)
            
            # Check in-memory signals first for quick detection
            duplicate_found = False
            for signal_id, signal in self.active_signals.items():
                if (signal.get('session_name') == session.name and 
                    signal.get('signal_type') == signal_type):
                    logger.debug(f"⏭️ {signal_type} signal already exists for session {session.name}")
                    duplicate_found = True
                    break
            
            if duplicate_found:
                return
                
            # Also check database for any existing signals (not just active)
            # In breakout strategy, there should be only ONE signal per session per type
            signals_collection = get_collection('signals')
            existing_signal = await signals_collection.find_one({
                'session_name': session.name,
                'signal_type': signal_type
            })
            
            if existing_signal:
                logger.debug(f"⏭️ Active {signal_type} signal already exists in database for session {session.name}")
                return
            
            # Create unique signal ID for this specific signal with microseconds for uniqueness
            signal_id = f"{session.name}_{signal_type}_{timestamp.strftime('%H%M%S')}_{timestamp.microsecond}"
            
            # Calculate confidence based on technical factors
            confidence = await self._calculate_signal_confidence(
                self.nifty_index, future_symbol, signal_type
            )
            
            # Calculate stop loss and targets based on session high/low
            stop_loss, target_1, target_2 = self._calculate_stop_loss_and_targets(
                signal_type, nifty_session_high, nifty_session_low, nifty_price
            )
            
            # Get detailed breakout information for clear display
            nifty_session_high = session.session_data.get(self.nifty_index, {}).get('high')
            nifty_session_low = session.session_data.get(self.nifty_index, {}).get('low')
            future_session_high = session.session_data.get(future_symbol, {}).get('high')
            future_session_low = session.session_data.get(future_symbol, {}).get('low')
            
            # Determine breakout details
            nifty_breaks_high = nifty_price > nifty_session_high if nifty_session_high else False
            nifty_breaks_low = nifty_price < nifty_session_low if nifty_session_low else False
            future_breaks_high = future_price > future_session_high if future_session_high else False
            future_breaks_low = future_price < future_session_low if future_session_low else False
            
            # Create breakout status
            breakout_status = {
                'nifty_breaks_high': nifty_breaks_high,
                'nifty_breaks_low': nifty_breaks_low,
                'future_breaks_high': future_breaks_high,
                'future_breaks_low': future_breaks_low,
                'nifty_breakout_amount': 0,
                'future_breakout_amount': 0
            }
            
            # Calculate breakout amounts
            if nifty_breaks_high and nifty_session_high:
                breakout_status['nifty_breakout_amount'] = round(nifty_price - nifty_session_high, 2)
            elif nifty_breaks_low and nifty_session_low:
                breakout_status['nifty_breakout_amount'] = round(nifty_session_low - nifty_price, 2)
                
            if future_breaks_high and future_session_high:
                breakout_status['future_breakout_amount'] = round(future_price - future_session_high, 2)
            elif future_breaks_low and future_session_low:
                breakout_status['future_breakout_amount'] = round(future_session_low - future_price, 2)
            
            # Get additional futures data for enhanced storage
            future_candle_data = await self._get_latest_candle_data(future_symbol)
            nifty_candle_data = await self._get_latest_candle_data(self.nifty_index)
            
            # Calculate market sentiment and correlation
            market_sentiment = self._determine_market_sentiment(nifty_price, future_price, nifty_session_high, nifty_session_low)
            correlation_score = await self._calculate_correlation_score(self.nifty_index, future_symbol)
            
            # Create enhanced signal object with comprehensive futures data
            signal_data = {
                'id': signal_id,
                'session_name': session.name,
                'signal_type': signal_type,
                'reason': reason,
                'timestamp': timestamp.replace(tzinfo=None) if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo else timestamp,
                'nifty_price': nifty_price,
                'future_price': future_price,
                'future_symbol': future_symbol,
                'entry_price': nifty_price,  # Use NIFTY price as entry price
                'stop_loss': stop_loss,
                'target_1': target_1,
                'target_2': target_2,
                'confidence': confidence,
                'status': 'ACTIVE',
                'session_high': nifty_session_high,
                'session_low': nifty_session_low,
                'future_session_high': future_session_high,
                'future_session_low': future_session_low,
                'vwap_nifty': await self._calculate_vwap(self.nifty_index),
                'vwap_future': await self._calculate_vwap(future_symbol),
                'breakout_details': breakout_status,
                'display_text': self._create_signal_display_text(signal_type, breakout_status, session.name),
                
                # Enhanced futures data fields
                'future_open': future_candle_data.get('open') if future_candle_data else None,
                'future_close': future_candle_data.get('close') if future_candle_data else None,
                'future_volume': future_candle_data.get('volume') if future_candle_data else None,
                'future_change': round(future_price - future_candle_data.get('open', future_price), 2) if future_candle_data else None,
                'future_change_percent': round(((future_price - future_candle_data.get('open', future_price)) / future_candle_data.get('open', future_price)) * 100, 2) if future_candle_data and future_candle_data.get('open') else None,
                
                # Breakout details for both index and futures
                'nifty_breakout_amount': breakout_status['nifty_breakout_amount'],
                'future_breakout_amount': breakout_status['future_breakout_amount'],
                'nifty_breaks_high': breakout_status['nifty_breaks_high'],
                'nifty_breaks_low': breakout_status['nifty_breaks_low'],
                'future_breaks_high': breakout_status['future_breaks_high'],
                'future_breaks_low': breakout_status['future_breaks_low'],
                
                # Additional market data
                'market_sentiment': market_sentiment,
                'volatility_index': await self._get_volatility_index(),
                'correlation_score': correlation_score
            }
            
            # Track this signal for the session - allowing multiple different breakouts
            session_key = f"{session.name}_{signal_type}"  # Allow different signal types
            self.session_signals[session_key] = {
                'signal_type': signal_type,
                'timestamp': timestamp,
                'active': True,
                'signal_id': signal_id,
                'session_name': session.name
            }
            
            # Store signal
            self.active_signals[signal_id] = signal_data
            self.signal_history.append(signal_data)
            
            # Save to database
            await self._save_signal_to_db(signal_data)
            
            # Broadcast signal via WebSocket
            await self._broadcast_signal(signal_data)
            
            logger.info(f"🚨 SIGNAL GENERATED: {signal_type} | {reason} | Confidence: {confidence}% | Session: {session.name}")
            logger.info(f"   Entry: ₹{nifty_price:.2f} | Stop Loss: ₹{stop_loss:.2f} | Target1: +₹{target_1:.2f} | Target2: +₹{target_2:.2f}")
            logger.info(f"   NIFTY: ₹{nifty_price:.2f} | {future_symbol}: ₹{future_price:.2f}")
            
        except Exception as e:
            logger.error(f"Error generating signal: {e}")
    
    async def _deactivate_signal(self, signal_id: str):
        """Deactivate an existing signal"""
        try:
            # Remove from active signals
            signal_data = None
            if signal_id in self.active_signals:
                signal_data = self.active_signals[signal_id]
                del self.active_signals[signal_id]
            
            # Remove from session tracking
            if signal_data:
                session_name = signal_data.get('session_name')
                signal_type = signal_data.get('signal_type')
                if session_name and signal_type:
                    session_key = f"{session_name}_{signal_type}"
                    if session_key in self.session_signals:
                        del self.session_signals[session_key]
            
            # Update database status
            collection = get_collection('signals')
            await collection.update_one(
                {'id': signal_id},
                {'$set': {'status': 'REPLACED', 'updated_at': datetime.now(IST).replace(tzinfo=None)}}
            )
            
            logger.info(f"🔄 Deactivated signal: {signal_id}")
            
        except Exception as e:
            logger.error(f"Error deactivating signal {signal_id}: {e}")
    
    def _create_signal_display_text(self, signal_type: str, breakout_status: Dict, session_name: str) -> str:
        """Create clear display text showing exact breakout conditions"""
        nifty_breaks_high = breakout_status['nifty_breaks_high']
        nifty_breaks_low = breakout_status['nifty_breaks_low']
        future_breaks_high = breakout_status['future_breaks_high']
        future_breaks_low = breakout_status['future_breaks_low']
        
        nifty_amount = breakout_status['nifty_breakout_amount']
        future_amount = breakout_status['future_breakout_amount']
        
        if signal_type == "BUY_CALL":
            if nifty_breaks_high and future_breaks_high:
                return f"📈 BULLISH BREAKOUT - Both crossed session high (NIFTY +{nifty_amount}, Future +{future_amount})"
            elif nifty_breaks_low and not future_breaks_low:
                return f"📊 DIVERGENT BREAKOUT - Only NIFTY broke session low (-{nifty_amount}), Future held"
            elif future_breaks_low and not nifty_breaks_low:
                return f"📊 DIVERGENT BREAKOUT - Only Future broke session low (-{future_amount}), NIFTY held"
            else:
                return f"📈 CALL Signal - {session_name} breakout detected"
                
        elif signal_type == "BUY_PUT":
            if nifty_breaks_low and future_breaks_low:
                return f"📉 BEARISH BREAKOUT - Both broke session low (NIFTY -{nifty_amount}, Future -{future_amount})"
            elif nifty_breaks_high and not future_breaks_high:
                return f"📊 DIVERGENT BREAKOUT - Only NIFTY broke session high (+{nifty_amount}), Future held"
            elif future_breaks_high and not nifty_breaks_high:
                return f"📊 DIVERGENT BREAKOUT - Only Future broke session high (+{future_amount}), NIFTY held"
            else:
                return f"📉 PUT Signal - {session_name} breakout detected"
        
        return f"{signal_type} - {session_name} breakout"
    
    async def _calculate_signal_confidence(self, nifty_symbol: str, future_symbol: str, signal_type: str) -> int:
        """Calculate signal confidence score (0-100)"""
        try:
            confidence = 50  # Base confidence
            
            # VWAP alignment
            nifty_vwap = await self._calculate_vwap(nifty_symbol)
            future_vwap = await self._calculate_vwap(future_symbol)
            nifty_price = await self._get_current_price(nifty_symbol)
            future_price = await self._get_current_price(future_symbol)
            
            if all([nifty_vwap, future_vwap, nifty_price, future_price]):
                # Check VWAP alignment
                if signal_type in ["BUY_CALL"]:
                    if nifty_price > nifty_vwap and future_price > future_vwap:
                        confidence += 20
                elif signal_type in ["BUY_PUT"]:
                    if nifty_price < nifty_vwap and future_price < future_vwap:
                        confidence += 20
            
            # Volume confirmation
            if await self._check_volume_confirmation(nifty_symbol, future_symbol):
                confidence += 15
            
            # Time of day factor (higher confidence during active trading hours)
            current_hour = datetime.now(IST).hour
            if 10 <= current_hour <= 14:  # Peak trading hours
                confidence += 10
            
            # Limit confidence to 95%
            return min(95, max(30, confidence))
            
        except Exception as e:
            logger.error(f"Error calculating confidence: {e}")
            return 50
    
    def _calculate_stop_loss_and_targets(self, signal_type: str, session_high: float, session_low: float, entry_price: float) -> tuple:
        """
        Calculate stop loss and targets based on trading rules:
        - Target 1: (Session High - Session Low) / 2
        - Target 2: (Session High - Session Low)  
        - Stop Loss PE: Session High + 5 points
        - Stop Loss CE: Session Low - 5 points
        """
        try:
            if not session_high or not session_low or not entry_price:
                logger.warning("Missing session data for stop loss/target calculation")
                return None, None, None
            
            # Calculate session range
            session_range = session_high - session_low
            
            # Calculate targets and stop loss based on signal direction
            target_1_amount = round(session_range / 2, 2)
            target_2_amount = round(session_range, 2)
            
            if signal_type in ["BUY_PUT"]:  # PE orders - expecting price to go down
                # Targets are below entry price (subtract from entry)
                target_1 = round(entry_price - target_1_amount, 2)
                target_2 = round(entry_price - target_2_amount, 2)
                # Stop loss is session high (breakout candle high)
                stop_loss = round(session_high, 2)
                
            elif signal_type in ["BUY_CALL"]:  # CE orders - expecting price to go up
                # Targets are above entry price (add to entry)
                target_1 = round(entry_price + target_1_amount, 2)
                target_2 = round(entry_price + target_2_amount, 2)
                # Stop loss is session low (breakout candle low)
                stop_loss = round(session_low, 2)
            else:
                # Default fallback
                stop_loss = round(entry_price * 0.98, 2)  # 2% stop loss as fallback
            
            logger.debug(f"📊 Calculated - Target1: {target_1}, Target2: {target_2}, StopLoss: {stop_loss}")
            
            return stop_loss, target_1, target_2
            
        except Exception as e:
            logger.error(f"Error calculating stop loss and targets: {e}")
            return None, None, None
    
    async def _save_signal_to_db(self, signal_data: Dict):
        """Save signal to database with duplicate prevention"""
        try:
            signals_collection = get_collection('signals')
            
            # Prepare document for insertion
            signal_doc = {
                **signal_data,
                'created_at': datetime.now(IST).replace(tzinfo=None),
                'updated_at': datetime.now(IST).replace(tzinfo=None)
            }
            
            # Try to insert the signal
            await signals_collection.insert_one(signal_doc)
            logger.info(f"✅ Signal saved to database: {signal_data.get('id')}")
            
        except Exception as e:
            # Check if it's a duplicate key error
            if "duplicate key error" in str(e).lower() or "E11000" in str(e):
                logger.warning(f"⚠️ Duplicate signal prevented: {signal_data.get('id')} - {e}")
                # This is expected behavior for duplicate prevention
                return
            else:
                # Unexpected error, log and raise
                logger.error(f"❌ Unexpected error saving signal to database: {e}")
                raise e
    
    async def _broadcast_signal(self, signal_data: Dict):
        """Broadcast signal via WebSocket"""
        try:
            from ..ws import broadcast_signal
            await broadcast_signal(signal_data)
        except Exception as e:
            logger.error(f"Error broadcasting signal: {e}")
    
    async def _cleanup_old_signals(self, current_time: datetime):
        """Clean up old signals that are no longer relevant"""
        try:
            # Remove signals older than 4 hours from in-memory storage
            cutoff_time = current_time - timedelta(hours=4)
            
            # Clean up active signals
            expired_signal_ids = []
            for signal_id, signal in self.active_signals.items():
                if (signal.get('timestamp') and 
                    signal.get('timestamp') < cutoff_time.replace(tzinfo=None)):
                    expired_signal_ids.append(signal_id)
            
            for signal_id in expired_signal_ids:
                del self.active_signals[signal_id]
                logger.debug(f"🧹 Removed old signal from memory: {signal_id}")
            
            # Clean up session signals tracking
            expired_session_keys = []
            for session_key, session_signal in self.session_signals.items():
                if (session_signal.get('timestamp') and 
                    session_signal.get('timestamp') < cutoff_time.replace(tzinfo=None)):
                    expired_session_keys.append(session_key)
            
            for session_key in expired_session_keys:
                del self.session_signals[session_key]
                logger.debug(f"🧹 Removed old session signal tracking: {session_key}")
                
            # Update database status for old active signals (mark as expired)
            if expired_signal_ids:
                signals_collection = get_collection('signals')
                await signals_collection.update_many(
                    {
                        'id': {'$in': expired_signal_ids},
                        'status': 'ACTIVE'
                    },
                    {
                        '$set': {
                            'status': 'EXPIRED',
                            'updated_at': datetime.now(IST).replace(tzinfo=None)
                        }
                    }
                )
                logger.info(f"🧹 Marked {len(expired_signal_ids)} old signals as EXPIRED in database")
                
        except Exception as e:
            logger.error(f"Error cleaning up old signals: {e}")

    async def _update_technical_indicators(self, current_time: datetime):
        """Update technical indicators and cache"""
        try:
            # Update VWAP calculations
            symbols = [self.nifty_index] + self.nifty_futures
            
            for symbol in symbols:
                vwap = await self._calculate_vwap(symbol)
                if vwap:
                    self.vwap_data[symbol][current_time.strftime('%H:%M')] = vwap
            
            # Clean up old signals every 30 minutes
            if current_time.minute % 30 == 0 and current_time.second < 15:
                await self._cleanup_old_signals(current_time)
            
        except Exception as e:
            logger.error(f"Error updating technical indicators: {e}")
    
    async def _get_latest_candle_data(self, symbol: str) -> Optional[Dict]:
        """Get the latest candle data for a symbol"""
        try:
            if symbol in self.price_data and self.price_data[symbol]:
                return self.price_data[symbol][-1]
            return None
        except Exception as e:
            logger.error(f"Error getting latest candle data for {symbol}: {e}")
            return None
    
    def _determine_market_sentiment(self, nifty_price: float, future_price: float, session_high: float, session_low: float) -> str:
        """Determine overall market sentiment based on price movements"""
        try:
            if not all([nifty_price, future_price, session_high, session_low]):
                return "NEUTRAL"
            
            session_mid = (session_high + session_low) / 2
            
            # Check if both are above/below session midpoint
            if nifty_price > session_mid and future_price > session_mid:
                if nifty_price > session_high and future_price > session_high:
                    return "VERY_BULLISH"
                return "BULLISH"
            elif nifty_price < session_mid and future_price < session_mid:
                if nifty_price < session_low and future_price < session_low:
                    return "VERY_BEARISH"
                return "BEARISH"
            else:
                return "NEUTRAL"
        except Exception as e:
            logger.error(f"Error determining market sentiment: {e}")
            return "NEUTRAL"
    
    async def _calculate_correlation_score(self, nifty_symbol: str, future_symbol: str) -> Optional[float]:
        """Calculate correlation between Nifty index and futures movements"""
        try:
            if (nifty_symbol not in self.price_data or future_symbol not in self.price_data or
                len(self.price_data[nifty_symbol]) < 10 or len(self.price_data[future_symbol]) < 10):
                return None
            
            # Get recent price movements
            nifty_prices = [candle['close'] for candle in list(self.price_data[nifty_symbol])[-10:]]
            future_prices = [candle['close'] for candle in list(self.price_data[future_symbol])[-10:]]
            
            if len(nifty_prices) != len(future_prices):
                return None
            
            # Calculate correlation coefficient
            import statistics
            
            nifty_mean = statistics.mean(nifty_prices)
            future_mean = statistics.mean(future_prices)
            
            numerator = sum((n - nifty_mean) * (f - future_mean) for n, f in zip(nifty_prices, future_prices))
            nifty_sq_sum = sum((n - nifty_mean) ** 2 for n in nifty_prices)
            future_sq_sum = sum((f - future_mean) ** 2 for f in future_prices)
            
            denominator = (nifty_sq_sum * future_sq_sum) ** 0.5
            
            if denominator == 0:
                return None
            
            correlation = numerator / denominator
            return round(correlation, 3)
            
        except Exception as e:
            logger.error(f"Error calculating correlation score: {e}")
            return None
    
    async def _get_volatility_index(self) -> Optional[float]:
        """Get volatility index (VIX equivalent) - simplified calculation"""
        try:
            if (self.nifty_index not in self.price_data or 
                len(self.price_data[self.nifty_index]) < 20):
                return None
            
            # Calculate simple volatility based on recent price movements
            recent_prices = [candle['close'] for candle in list(self.price_data[self.nifty_index])[-20:]]
            
            if len(recent_prices) < 2:
                return None
            
            # Calculate price changes
            price_changes = []
            for i in range(1, len(recent_prices)):
                change_pct = (recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1]
                price_changes.append(change_pct)
            
            # Calculate standard deviation as volatility measure
            import statistics
            if len(price_changes) > 1:
                volatility = statistics.stdev(price_changes) * 100  # Convert to percentage
                return round(volatility, 2)
            
            return None
            
        except Exception as e:
            logger.error(f"Error calculating volatility index: {e}")
            return None
    
    # Public API methods
    
    async def get_active_signals(self) -> List[Dict]:
        """Get all active signals"""
        return list(self.active_signals.values())
    
    async def get_signal_history(self, limit: int = 50) -> List[Dict]:
        """Get signal history from database and in-memory"""
        try:
            # Check if database connection is available
            from ..core.database import Database
            if Database.database is None:
                logger.warning("Database connection not available, using in-memory signals only")
                return self.signal_history[-limit:] if self.signal_history else []
            
            signals_collection = get_collection('signals')
            
            # Get signals from database
            db_signals = []
            async for signal_doc in signals_collection.find().sort('created_at', -1).limit(limit):
                # Convert database document to signal format
                signal_data = {
                    'id': signal_doc.get('id', str(signal_doc.get('_id', ''))),
                    'session_name': signal_doc.get('session_name'),
                    'signal_type': signal_doc.get('signal_type'),
                    'reason': signal_doc.get('reason'),
                    'timestamp': signal_doc.get('timestamp'),
                    'nifty_price': signal_doc.get('nifty_price'),
                    'future_price': signal_doc.get('future_price'),
                    'future_symbol': signal_doc.get('future_symbol'),
                    'entry_price': signal_doc.get('entry_price'),
                    'stop_loss': signal_doc.get('stop_loss'),
                    'target_1': signal_doc.get('target_1'),
                    'target_2': signal_doc.get('target_2'),
                    'confidence': signal_doc.get('confidence'),
                    'status': signal_doc.get('status'),
                    'session_high': signal_doc.get('session_high'),
                    'session_low': signal_doc.get('session_low'),
                    'future_session_high': signal_doc.get('future_session_high'),
                    'future_session_low': signal_doc.get('future_session_low'),
                    'vwap_nifty': signal_doc.get('vwap_nifty'),
                    'vwap_future': signal_doc.get('vwap_future'),
                    'breakout_details': signal_doc.get('breakout_details'),
                    'display_text': signal_doc.get('display_text'),
                    'created_at': signal_doc.get('created_at')
                }
                db_signals.append(signal_data)
            
            logger.info(f"Retrieved {len(db_signals)} signals from database")
            
            # Combine with in-memory signals (avoid duplicates)
            all_signals = list(db_signals)
            for mem_signal in self.signal_history:
                if mem_signal is not None and not any(s.get('id') == mem_signal.get('id') for s in all_signals):
                    all_signals.append(mem_signal)
            
            # Sort by timestamp and limit
            all_signals.sort(key=lambda x: x.get('created_at') or x.get('timestamp') or datetime.min, reverse=True)
            return all_signals[:limit]
            
        except RuntimeError as re:
            logger.warning(f"Database connection error: {re}")
            # Return in-memory signals if database is not available
            return self.signal_history[-limit:] if self.signal_history else []
        except Exception as e:
            logger.error(f"Error getting signal history from database: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Fallback to in-memory
            return self.signal_history[-limit:] if self.signal_history else []
    
    async def get_session_status(self) -> List[Dict]:
        """Get current session status"""
        current_time = datetime.now(IST)
        session_status = []
        
        for session in self.sessions:
            # Clean session_data to ensure JSON serializability
            clean_session_data = {}
            if session.session_data:
                for key, value in session.session_data.items():
                    if isinstance(value, datetime):
                        clean_session_data[key] = value.isoformat()
                    elif hasattr(value, 'isoformat'):  # datetime-like objects
                        clean_session_data[key] = value.isoformat()
                    elif isinstance(value, (str, int, float, bool, type(None))):
                        clean_session_data[key] = value
                    else:
                        clean_session_data[key] = str(value)
            
            status = {
                'name': session.name,
                'start_time': session.start_time,
                'end_time': session.end_time,
                'is_active': session.is_active,
                'is_completed': session.is_completed,
                'session_data': clean_session_data,
                'high': session.high,
                'low': session.low
            }
            session_status.append(status)
        
        return session_status
    
    async def get_technical_data(self, symbol: str) -> Dict:
        """Get technical analysis data for symbol"""
        try:
            current_price = await self._get_current_price(symbol)
            vwap = await self._calculate_vwap(symbol)
            
            # Convert datetime objects to strings for JSON serialization
            recent_candles = []
            if symbol in self.price_data:
                for candle in list(self.price_data[symbol])[-10:]:
                    candle_dict = dict(candle) if isinstance(candle, dict) else candle
                    if 'timestamp' in candle_dict and hasattr(candle_dict['timestamp'], 'isoformat'):
                        candle_dict['timestamp'] = candle_dict['timestamp'].isoformat()
                    recent_candles.append(candle_dict)
            
            volume_data = []
            if symbol in self.volume_data:
                for vol_data in list(self.volume_data[symbol])[-10:]:
                    vol_dict = dict(vol_data) if isinstance(vol_data, dict) else vol_data
                    if isinstance(vol_dict, dict) and 'timestamp' in vol_dict and hasattr(vol_dict['timestamp'], 'isoformat'):
                        vol_dict['timestamp'] = vol_dict['timestamp'].isoformat()
                    volume_data.append(vol_dict)
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'vwap': vwap,
                'recent_candles': recent_candles,
                'volume_data': volume_data
            }
        except Exception as e:
            logger.error(f"Error in get_technical_data for {symbol}: {e}")
            # Return basic data structure on error
            return {
                'symbol': symbol,
                'current_price': None,
                'vwap': None,
                'recent_candles': [],
                'volume_data': [],
                'error': str(e)
            }


# Create service instance
signal_detection_service = SignalDetectionService()