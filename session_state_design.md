# Database-Backed Session State Design

## Problem
- Multiple monitoring loops cause race conditions
- Sessions need to be monitored continuously for real-time breakouts
- In-memory state is unreliable with concurrent processes

## Solution: Database-Backed Session State

### Collection: `session_states`
```javascript
{
  _id: ObjectId,
  trading_date: "2025-08-26",           // IST date
  session_name: "Morning Opening",       // Session identifier
  start_time: "09:30",                   // Session start time
  end_time: "09:35",                     // Session end time
  
  // Session Status
  status: "PENDING|ACTIVE|COMPLETED",    // Current status
  started_at: ISODate,                   // When session became active
  completed_at: ISODate,                 // When session completed
  
  // Session Data
  symbols_data: {
    "NIFTY": {
      high: 24802.25,
      low: 24782.35,
      tick_count: 234,
      first_tick_time: ISODate,
      last_tick_time: ISODate
    },
    "NIFTY30SEP25FUT": {
      high: 24815.0,
      low: 24793.0,
      tick_count: 156,
      first_tick_time: ISODate,
      last_tick_time: ISODate
    }
  },
  
  // Breakout Tracking
  breakouts_checked: false,              // Whether breakouts have been checked
  signals_generated: [],                 // Array of signal IDs generated from this session
  
  // Metadata
  created_at: ISODate,
  updated_at: ISODate
}
```

### Indexes:
```javascript
// Compound index for efficient queries
{ trading_date: 1, status: 1 }
{ trading_date: 1, session_name: 1 }
{ status: 1, completed_at: 1 }
```

## Monitoring Logic

### 1. Session Initialization (Daily)
```python
async def initialize_daily_sessions(trading_date: str):
    sessions = [
        {"name": "Morning Opening", "start": "09:30", "end": "09:35"},
        {"name": "Mid Morning", "start": "09:45", "end": "09:55"},
        {"name": "Pre Lunch", "start": "10:30", "end": "10:45"},
        {"name": "Lunch Break", "start": "11:50", "end": "12:20"}
    ]
    
    for session in sessions:
        await upsert_session_state({
            "trading_date": trading_date,
            "session_name": session["name"],
            "start_time": session["start"],
            "end_time": session["end"],
            "status": "PENDING",
            "symbols_data": {},
            "breakouts_checked": False,
            "signals_generated": []
        })
```

### 2. Real-time Session Monitoring
```python
async def monitor_sessions_realtime():
    """Single monitoring loop - stateless, DB-driven"""
    current_time = TimezoneUtils.get_ist_now()
    current_date = current_time.strftime('%Y-%m-%d')
    
    # Get all sessions for today that need monitoring
    active_or_pending = await get_sessions_by_status(current_date, ["PENDING", "ACTIVE"])
    
    for session_doc in active_or_pending:
        await process_session_realtime(session_doc, current_time)
    
    # Check completed sessions for breakouts
    completed_unchecked = await get_sessions_for_breakout_check(current_date)
    for session_doc in completed_unchecked:
        await check_session_breakouts(session_doc, current_time)
```

### 3. Session State Transitions
```python
async def process_session_realtime(session_doc, current_time):
    """Process single session - atomic DB operations"""
    session_name = session_doc["session_name"]
    start_minutes = time_to_minutes(session_doc["start_time"])
    end_minutes = time_to_minutes(session_doc["end_time"])
    current_minutes = current_time.hour * 60 + current_time.minute
    
    # State transitions
    if session_doc["status"] == "PENDING" and current_minutes >= start_minutes:
        # Start session
        await update_session_status(session_doc["_id"], "ACTIVE", {
            "started_at": current_time,
            "updated_at": current_time
        })
        
    elif session_doc["status"] == "ACTIVE" and current_minutes > end_minutes:
        # Complete session - calculate final data
        session_data = await calculate_session_data(session_doc, current_time)
        await update_session_status(session_doc["_id"], "COMPLETED", {
            "completed_at": current_time,
            "symbols_data": session_data,
            "updated_at": current_time
        })
        
    elif session_doc["status"] == "ACTIVE":
        # Update active session data (optional - can be done periodically)
        pass
```

### 4. Breakout Detection
```python
async def check_session_breakouts(session_doc, current_time):
    """Check breakouts for completed sessions"""
    if session_doc["breakouts_checked"]:
        return
        
    # Get current prices
    nifty_price = await get_current_price("NIFTY")
    future_price = await get_current_price("NIFTY30SEP25FUT")
    
    # Get session levels
    nifty_data = session_doc["symbols_data"].get("NIFTY", {})
    session_high = nifty_data.get("high")
    session_low = nifty_data.get("low")
    
    if not session_high or not session_low:
        return
        
    # Check breakout conditions
    signals = []
    if nifty_price > session_high:
        signal = await generate_breakout_signal(
            session_doc, "HIGH_BREAK", current_time, 
            nifty_price, future_price
        )
        if signal:
            signals.append(signal["id"])
    
    elif nifty_price < session_low:
        signal = await generate_breakout_signal(
            session_doc, "LOW_BREAK", current_time,
            nifty_price, future_price
        )
        if signal:
            signals.append(signal["id"])
    
    # Mark as checked
    await update_session_breakout_status(session_doc["_id"], True, signals)
```

## Benefits

1. **Single Monitoring Loop**: No race conditions
2. **Stateless**: Each monitoring cycle queries DB for current state
3. **Atomic Operations**: DB ensures consistency
4. **Persistent State**: Survives service restarts
5. **Historical Tracking**: Complete audit trail
6. **Scalable**: Multiple services can monitor without conflicts
7. **Real-time**: Continuous monitoring without missing breakouts

## Migration Strategy

1. Create `session_states` collection with indexes
2. Initialize today's sessions in DB
3. Replace in-memory session logic with DB queries
4. Test with current monitoring loop
5. Ensure breakout detection works in real-time