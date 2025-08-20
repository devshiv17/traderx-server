# Tick Data Timestamp Fields Guide

## Overview

The tick data collection has two timestamp fields that serve different purposes:

- **`timestamp`**: Market timestamp (when the tick actually occurred in the market)
- **`received_at`**: System timestamp (when our system received and processed the data)

## When to Use Each Field

### Use `timestamp` field for:
- ✅ **Chart data** - Showing historical price movements at market time
- ✅ **Historical analysis** - Analyzing market behavior at specific market times
- ✅ **Market hours validation** - Checking if data falls within official trading hours
- ✅ **Price discovery** - Understanding when prices actually changed in the market

### Use `received_at` field for:
- ✅ **Breakout detection** - Determining when breakouts were detected by your system
- ✅ **Signal generation** - Ensuring signals are generated based on processing time
- ✅ **Real-time monitoring** - Tracking when your system processes new data
- ✅ **System performance** - Measuring data processing latency

## Why `received_at` is Critical for Breakouts

When detecting breakouts, you need to know **when your system processed the data**, not when it happened in the market, because:

1. **Processing Delays**: There can be delays between market time and processing time
2. **Network Latency**: Data feed delays affect when you receive market data
3. **System Processing**: Your system takes time to process and store the data
4. **Signal Timing**: Breakout signals should be based on when you could actually act on the data

## Updated Implementation

### Tick Data Service
```python
# Default behavior (uses received_at for system timing)
ticks = await tick_data_service.get_ticks_for_timerange(
    symbol, start_time, end_time
)

# For market timing only (when needed)
ticks = await tick_data_service.get_ticks_for_timerange(
    symbol, start_time, end_time, use_received_at=False
)
```

### System-Wide Implementation
ALL services now automatically use `received_at` by default for:
- ✅ Signal detection and breakout analysis
- ✅ 5-minute candle generation
- ✅ Chart data API endpoints
- ✅ Market overview statistics
- ✅ Symbol availability checks
- ✅ Tick data cleanup and maintenance
- ✅ Latest tick retrieval

## Data Structure

Each tick document contains both timestamps:

```python
{
    "symbol": "NIFTY",
    "price": 22450.75,
    "timestamp": datetime(2025, 8, 13, 10, 30, 0),      # Market time (IST)
    "received_at": datetime(2025, 8, 13, 10, 30, 2),    # System time (IST)
    "source": "angel_one_websocket",
    "exchange": "NSE"
}
```

## Best Practices

1. **Breakout Detection**: Always use `received_at` for signal generation
2. **Chart Display**: Use `timestamp` for historical chart data
3. **Performance Monitoring**: Compare `timestamp` vs `received_at` to measure latency
4. **Testing**: Use the test script to verify correct timestamp usage

## Migration Impact

- ✅ **System-wide consistency** - All tick_data queries now use received_at by default
- ✅ **Backward compatibility** - Can still access market timing with use_received_at=False
- ✅ **Improved accuracy** - All timing based on actual system processing
- ✅ **Unified behavior** - Charts, signals, and APIs all use same timing logic

## Testing

Run the verification test:
```bash
python3 test_received_at_timing.py
```

This will verify that all tick_data operations use the correct timestamp field and show timing differences between market time and processing time.

## Summary of Changes

### Files Updated:
1. **`tick_data_service.py`** - Default to received_at, all methods updated
2. **`signal_detection_service.py`** - Simplified to use defaults
3. **`api/v1/market_data.py`** - Chart and statistics APIs updated  
4. **`angel_one_service.py`** - Tick storage and retrieval updated

### Key Benefits:
- **Consistent Timing**: All operations use system processing time
- **Accurate Signals**: Breakouts based on when you could actually trade
- **Better Performance**: Reduced confusion about which timestamp to use
- **Future-Proof**: Single source of truth for tick timing