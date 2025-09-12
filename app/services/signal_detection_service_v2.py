"""
Database-Backed Signal Detection Service V2
Eliminates race conditions by using database for session state management
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from ..core.database import get_collection
from ..core.symbols import SymbolsConfig
from ..models.signal import SignalModel, SignalType, SignalStrength
from ..models.session_state import session_state_service, SessionStatus
from .tick_data_service import tick_data_service
from ..utils.timezone_utils import TimezoneUtils

logger = logging.getLogger(__name__)


class SignalDetectionServiceV2:
    """Database-backed signal detection service - no race conditions"""
    
    def __init__(self):
        # Symbols to monitor
        self.nifty_index = SymbolsConfig.NIFTY_INDEX.symbol
        self.nifty_futures = [SymbolsConfig.NIFTY_FUTURES.symbol]
        
        logger.info(f"🎯 DB-BACKED TRADING RULE: Signal detection based on {self.nifty_index} + {self.nifty_futures[0]} breakouts")
        
        # Single monitoring task reference
        self.monitoring_active = False
        self.monitoring_task = None
        
        # Service dependencies
        self.session_service = session_state_service
    
    async def start_monitoring(self):
        """Start database-backed real-time monitoring"""
        print("🚀 SIGNAL DETECTION V2: Starting database-backed signal detection service...")
        logger.info("🚀 Starting database-backed signal detection service...")
        
        self.monitoring_active = True
        
        # Initialize today's sessions in database
        today = TimezoneUtils.get_ist_now().strftime('%Y-%m-%d')
        await self.session_service.initialize_daily_sessions(today)
        logger.info(f"📅 Initialized sessions in database for {today}")
        
        # Start single monitoring loop
        logger.info("🔄 Creating single monitoring loop task...")
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        def task_callback(task):
            try:
                result = task.result()
                print(f"🔄 SIGNAL DETECTION V2: Monitoring loop ended: {result}")
                logger.info(f"Monitoring loop ended: {result}")
            except Exception as e:
                print(f"❌ SIGNAL DETECTION V2: Monitoring loop failed: {e}")
                logger.error(f"Monitoring loop failed: {e}")
        
        self.monitoring_task.add_done_callback(task_callback)
        
        # Wait a moment to ensure task starts
        await asyncio.sleep(0.1)
        
        logger.info("✅ Database-backed signal detection service started")
        print("✅ SIGNAL DETECTION V2: Database-backed signal detection service started")
    
    async def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring_active = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            self.monitoring_task = None
        logger.info("🛑 Database-backed signal detection service stopped")
    
    async def _monitoring_loop(self):
        """Single stateless monitoring loop - all state in database"""
        print("🔄 SIGNAL DETECTION V2: Starting database-backed monitoring loop")
        logger.info("🔄 Starting database-backed monitoring loop")
        
        while self.monitoring_active:
            try:
                current_time = TimezoneUtils.get_ist_now()
                current_date = current_time.strftime('%Y-%m-%d')
                
                logger.debug(f"⏰ Monitoring loop tick at {current_time.strftime('%H:%M:%S')}")
                
                # Only monitor during market hours
                if not TimezoneUtils.is_market_hours(current_time):
                    logger.debug(f"📴 Outside market hours, sleeping...")
                    await asyncio.sleep(60)
                    continue
                
                logger.debug(f"🏪 Market hours active, processing sessions...")
                
                # Process pending and active sessions
                sessions_to_process = await self.session_service.get_sessions_by_status(
                    current_date, ["PENDING", "ACTIVE"]
                )
                
                for session_doc in sessions_to_process:
                    await self._process_session_realtime(session_doc, current_time)
                
                # Check completed sessions for breakouts
                sessions_for_breakouts = await self.session_service.get_sessions_for_breakout_check(current_date)
                
                for session_doc in sessions_for_breakouts:
                    await self._check_session_breakouts(session_doc, current_time)
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in database-backed monitoring loop: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                await asyncio.sleep(30)
    
    async def _process_session_realtime(self, session_doc: Dict, current_time: datetime):
        """Process single session using database state - atomic operations"""
        try:
            session_name = session_doc["session_name"]
            start_time_str = session_doc["start_time"]
            end_time_str = session_doc["end_time"]
            current_status = session_doc["status"]
            
            # Convert times to minutes for comparison
            start_minutes = self._time_to_minutes(start_time_str)
            end_minutes = self._time_to_minutes(end_time_str)
            current_minutes = current_time.hour * 60 + current_time.minute
            
            logger.debug(f"📅 Processing session {session_name}: Status={current_status}, Time={current_time.strftime('%H:%M')}")
            
            # State transition: PENDING -> ACTIVE
            if current_status == "PENDING" and current_minutes >= start_minutes:
                logger.info(f"📅 Session '{session_name}' starting at {current_time.strftime('%H:%M')}")
                
                await self.session_service.update_session_status(
                    str(session_doc["_id"]), 
                    "ACTIVE",
                    {"started_at": current_time}
                )
                
            # State transition: ACTIVE -> COMPLETED
            elif current_status == "ACTIVE" and current_minutes > end_minutes:
                logger.info(f"🏁 Session '{session_name}' completing at {current_time.strftime('%H:%M')}")
                
                # Calculate final session data
                symbols_data = await self.session_service.calculate_session_data(session_doc, current_time)
                
                await self.session_service.update_session_status(
                    str(session_doc["_id"]),
                    "COMPLETED", 
                    {
                        "completed_at": current_time,
                        "symbols_data": symbols_data
                    }
                )
                
                # Log session completion
                for symbol, data in symbols_data.items():
                    if data.get('tick_count', 0) > 0:
                        logger.info(f"   📊 {symbol}: High=₹{data.get('high'):.2f}, Low=₹{data.get('low'):.2f}, Ticks={data.get('tick_count')}")
                
        except Exception as e:
            logger.error(f"Error processing session {session_doc.get('session_name')}: {e}")
    
    async def _check_session_breakouts(self, session_doc: Dict, current_time: datetime):
        """Check breakouts for completed sessions - real-time detection"""
        try:
            session_name = session_doc["session_name"]
            symbols_data = session_doc.get("symbols_data", {})
            
            logger.debug(f"🎯 Checking breakouts for completed session: {session_name}")
            
            # Get session levels
            nifty_data = symbols_data.get(self.nifty_index, {})
            session_high = nifty_data.get("high")
            session_low = nifty_data.get("low")
            
            if not session_high or not session_low:
                logger.debug(f"❌ No session data for {session_name} - skipping breakout check")
                await self.session_service.mark_breakouts_checked(str(session_doc["_id"]))
                return
            
            # Get current prices
            nifty_price = await self._get_current_price(self.nifty_index)
            futures_price = await self._get_current_price(self.nifty_futures[0])
            
            if not nifty_price or not futures_price:
                logger.debug(f"❌ No current prices available - skipping breakout check")
                return
            
            logger.info(f"🎯 Breakout check: {session_name} | NIFTY @ ₹{nifty_price:.2f} vs High ₹{session_high:.2f} vs Low ₹{session_low:.2f}")
            
            # Check breakout conditions
            signals_generated = []
            
            # High breakout
            if nifty_price > session_high:
                breakout_amount = round(nifty_price - session_high, 2)
                logger.info(f"🔥 HIGH BREAKOUT DETECTED: {session_name} | ₹{nifty_price:.2f} > ₹{session_high:.2f} (+{breakout_amount})")
                
                signal_id = await self._generate_breakout_signal(
                    session_doc, "HIGH_BREAK", current_time, 
                    nifty_price, futures_price, breakout_amount
                )
                if signal_id:
                    signals_generated.append(signal_id)
            
            # Low breakout
            elif nifty_price < session_low:
                breakout_amount = round(session_low - nifty_price, 2)
                logger.info(f"🔥 LOW BREAKOUT DETECTED: {session_name} | ₹{nifty_price:.2f} < ₹{session_low:.2f} (-{breakout_amount})")
                
                signal_id = await self._generate_breakout_signal(
                    session_doc, "LOW_BREAK", current_time,
                    nifty_price, futures_price, breakout_amount
                )
                if signal_id:
                    signals_generated.append(signal_id)
            
            else:
                logger.debug(f"📊 No breakout: {session_name} | ₹{nifty_price:.2f} within range ₹{session_low:.2f} - ₹{session_high:.2f}")
            
            # Mark as checked (even if no breakouts)
            await self.session_service.mark_breakouts_checked(
                str(session_doc["_id"]), 
                signals_generated
            )
            
        except Exception as e:
            logger.error(f"Error checking breakouts for session {session_doc.get('session_name')}: {e}")
    
    async def _generate_breakout_signal(self, session_doc: Dict, breakout_type: str, 
                                      timestamp: datetime, nifty_price: float, 
                                      futures_price: float, breakout_amount: float) -> Optional[str]:
        """Generate breakout signal and store in database"""
        try:
            session_name = session_doc["session_name"]
            symbols_data = session_doc["symbols_data"]
            
            # Determine signal type based on breakout
            if breakout_type == "HIGH_BREAK":
                signal_type = "BUY_CALL"
                reason = f"High breakout: ₹{nifty_price:.2f} broke {session_name} high by +{breakout_amount}"
            else:  # LOW_BREAK
                signal_type = "BUY_PUT" 
                reason = f"Low breakout: ₹{nifty_price:.2f} broke {session_name} low by -{breakout_amount}"
            
            # Check for duplicate signals
            signals_collection = get_collection('signals')
            existing_signal = await signals_collection.find_one({
                'session_name': session_name,
                'signal_type': signal_type,
                'status': 'ACTIVE'
            })
            
            if existing_signal:
                logger.info(f"⏭️ Signal {signal_type} already exists for session {session_name}")
                return None
            
            # Create signal ID
            signal_id = f"{session_name}_{signal_type}_{timestamp.strftime('%H%M%S')}_{timestamp.microsecond}"
            
            # Get session levels
            nifty_data = symbols_data.get(self.nifty_index, {})
            session_high = nifty_data.get("high")
            session_low = nifty_data.get("low")
            
            # Calculate stop loss and targets
            stop_loss, target_1, target_2 = self._calculate_stop_loss_and_targets(
                signal_type, session_high, session_low, nifty_price
            )
            
            # Create comprehensive signal data
            signal_data = {
                'id': signal_id,
                'session_name': session_name,
                'signal_type': signal_type,
                'reason': reason,
                'timestamp': TimezoneUtils.to_ist(timestamp),
                'nifty_price': nifty_price,
                'future_price': futures_price,
                'future_symbol': self.nifty_futures[0],
                'entry_price': nifty_price,
                'stop_loss': stop_loss,
                'target_1': target_1,
                'target_2': target_2,
                'confidence': 80,  # High confidence for clear breakouts
                'status': 'ACTIVE',
                'session_high': session_high,
                'session_low': session_low,
                'breakout_amount': breakout_amount,
                'breakout_type': breakout_type,
                'display_text': f"🔥 {reason}",
                'created_at': TimezoneUtils.to_ist(TimezoneUtils.get_ist_now()),
                'updated_at': TimezoneUtils.to_ist(TimezoneUtils.get_ist_now())
            }
            
            # Save to database
            await self._save_signal_to_db(signal_data)
            
            # Broadcast via WebSocket
            await self._broadcast_signal(signal_data)
            
            logger.info(f"🚨 BREAKOUT SIGNAL GENERATED: {signal_type} | {reason}")
            logger.info(f"   Entry: ₹{nifty_price:.2f} | Stop: ₹{stop_loss:.2f} | Target1: ₹{target_1:.2f} | Target2: ₹{target_2:.2f}")
            
            return signal_id
            
        except Exception as e:
            logger.error(f"Error generating breakout signal: {e}")
            return None
    
    def _time_to_minutes(self, time_str: str) -> int:
        """Convert HH:MM to total minutes"""
        hour, minute = map(int, time_str.split(':'))
        return hour * 60 + minute
    
    def _calculate_stop_loss_and_targets(self, signal_type: str, session_high: float, 
                                       session_low: float, entry_price: float) -> tuple:
        """Calculate stop loss and targets based on session range"""
        try:
            if not session_high or not session_low or not entry_price:
                return None, None, None
            
            session_range = session_high - session_low
            target_1_amount = round(session_range / 2, 2)
            target_2_amount = round(session_range, 2)
            
            if signal_type == "BUY_PUT":
                target_1 = round(entry_price - target_1_amount, 2)
                target_2 = round(entry_price - target_2_amount, 2)
                stop_loss = round(session_high, 2)
            else:  # BUY_CALL
                target_1 = round(entry_price + target_1_amount, 2)
                target_2 = round(entry_price + target_2_amount, 2)
                stop_loss = round(session_low, 2)
            
            return stop_loss, target_1, target_2
            
        except Exception as e:
            logger.error(f"Error calculating stop loss and targets: {e}")
            return None, None, None
    
    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for symbol"""
        try:
            current_time = TimezoneUtils.get_ist_now()
            start_time = current_time - timedelta(minutes=2)
            
            ticks = await tick_data_service.get_ticks_for_timerange(symbol, start_time, current_time)
            if ticks:
                return ticks[-1]['price']
            
            # Fallback for futures - use NIFTY as proxy
            if symbol in self.nifty_futures:
                nifty_ticks = await tick_data_service.get_ticks_for_timerange(self.nifty_index, start_time, current_time)
                if nifty_ticks:
                    return nifty_ticks[-1]['price']
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return None
    
    async def _save_signal_to_db(self, signal_data: Dict):
        """Save signal to database"""
        try:
            signals_collection = get_collection('signals')
            await signals_collection.insert_one(signal_data)
            logger.debug(f"✅ Signal saved to database: {signal_data.get('id')}")
        except Exception as e:
            logger.error(f"❌ Error saving signal to database: {e}")
    
    async def _broadcast_signal(self, signal_data: Dict):
        """Broadcast signal via WebSocket"""
        try:
            from ..ws import broadcast_signal
            await broadcast_signal(signal_data)
        except Exception as e:
            logger.error(f"Error broadcasting signal: {e}")
    
    # Public API methods
    async def get_active_signals(self) -> List[Dict]:
        """Get active signals from database"""
        try:
            signals_collection = get_collection('signals')
            cursor = signals_collection.find({'status': 'ACTIVE'}).sort('created_at', -1)
            
            signals = []
            async for signal_doc in cursor:
                # Convert ObjectId to string for JSON serialization
                clean_signal = {}
                for key, value in signal_doc.items():
                    if key == '_id':
                        clean_signal['_id'] = str(value)
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        clean_signal[key] = value.isoformat()
                    else:
                        clean_signal[key] = value
                signals.append(clean_signal)
            
            return signals
        except Exception as e:
            logger.error(f"Error getting active signals: {e}")
            return []
    
    async def get_session_status(self) -> List[Dict]:
        """Get current session status from database"""
        try:
            current_date = TimezoneUtils.get_ist_now().strftime('%Y-%m-%d')
            
            sessions = await self.session_service.get_sessions_by_status(
                current_date, ["PENDING", "ACTIVE", "COMPLETED"]
            )
            
            status_list = []
            for session in sessions:
                status_list.append({
                    'name': session['session_name'],
                    'start_time': session['start_time'],
                    'end_time': session['end_time'],
                    'status': session['status'],
                    'symbols_data': session.get('symbols_data', {}),
                    'breakouts_checked': session.get('breakouts_checked', False),
                    'signals_generated': session.get('signals_generated', [])
                })
            
            return status_list
            
        except Exception as e:
            logger.error(f"Error getting session status: {e}")
            return []


# Create service instance
signal_detection_service_v2 = SignalDetectionServiceV2()