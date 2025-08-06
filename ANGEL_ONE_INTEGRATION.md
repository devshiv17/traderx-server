# Angel One SmartAPI Integration

This document explains the integration with Angel One SmartAPI for real-time market data feeds and trading signals.

## Overview

The integration consists of two main components:
1. **WebSocket Feed Service** - Real-time market data streaming
2. **REST API Client** - Authentication and historical data fetching

## Configuration

### Required Credentials

Update the following in `app/services/angel_one_service.py` and `app/services/angel_one_rest_client.py`:

```python
self.api_key = "bjBWV4Ey"  # Your API Key
self.secret = "7e4849de-0541-4430-b87e-2f3f7ef700ef"  # Your Secret
self.client_code = "S123456"  # Your Client Code
self.pin = "1234"  # Your PIN
```

### Environment Variables

Add to your `.env` file:

```env
# Angel One Configuration
ANGEL_ONE_API_KEY=bjBWV4Ey
ANGEL_ONE_SECRET=7e4849de-0541-4430-b87e-2f3f7ef700ef
ANGEL_ONE_CLIENT_CODE=S123456
ANGEL_ONE_PIN=1234
```

## API Endpoints

### Feed Endpoints

1. **POST `/api/v1/feed`** - Receive market data from Angel One
2. **GET `/api/v1/feed/latest`** - Get latest market data
3. **GET `/api/v1/feed/summary`** - Get market summary
4. **GET `/api/v1/feed/health`** - Health check

### Authentication Flow

1. **Generate Checksum**: HMAC-SHA256 of `{api_key}{timestamp}{client_code}{pin}`
2. **Login Request**: POST to `/rest/auth/angelbroking/user/v1/loginByPassword`
3. **Get Tokens**: JWT token and refresh token for subsequent requests

## WebSocket Integration

### Connection Details

- **URL**: `wss://smartapisocket.angelone.in/websocket`
- **Parameters**: `api_key` and `access_token`
- **Subscription**: JSON message with tokens to subscribe

### Sample WebSocket Message

```json
{
  "action": "subscribe",
  "key": "feed",
  "source": "API",
  "tokens": [
    {"exchangeType": 1, "tokens": ["26009"]},  // NIFTY
    {"exchangeType": 1, "tokens": ["26017"]},  // BANKNIFTY
    {"exchangeType": 1, "tokens": ["26037"]}   // FINNIFTY
  ]
}
```

### Market Data Format

```json
{
  "tk": "26009",
  "ltpc": 22450.75,
  "ch": 125.50,
  "chp": 0.56,
  "high": 22500.00,
  "low": 22300.00,
  "open": 22325.25,
  "close": 22450.75,
  "volume": 125000000,
  "bid": 22450.00,
  "ask": 22451.00,
  "bid_qty": 100,
  "ask_qty": 150
}
```

## Symbol to Token Mapping

| Symbol | Token | Exchange |
|--------|-------|----------|
| NIFTY | 26009 | NSE |
| BANKNIFTY | 26017 | NSE |
| FINNIFTY | 26037 | NSE |
| SENSEX | 1 | BSE |
| RELIANCE | 2885 | NSE |
| TCS | 11536 | NSE |

## Signal Generation Logic

### Criteria

1. **Price Movement**: > 1.5% change triggers signal
2. **Volume Analysis**: Higher volume increases confidence
3. **Volatility Adjustment**: Dynamic target and stop loss based on volatility

### Signal Types

- **BUY CE**: Positive price movement
- **SELL PE**: Negative price movement

### Calculation

```python
# Target and Stop Loss based on volatility
volatility = abs(change_percent) / 100
target_multiplier = 0.02 + (volatility * 0.5)
stop_loss_multiplier = 0.01 + (volatility * 0.3)

# Confidence calculation
volume_factor = min(1.0, volume / 1000000)
price_factor = min(1.0, price / 10000)
confidence = min(95, int((abs(change_percent) * 5) + (volume_factor * 20) + (price_factor * 10)))
```

## REST API Endpoints

### Market Data

```python
# Get LTP data
POST /rest/secure/angelbroking/order/v1/getLtpData
{
  "mode": "FULL",
  "exchangeTokens": [
    {"exchangeType": 1, "tokens": ["26009"]}
  ]
}
```

### Historical Data

```python
# Get candle data
GET /rest/secure/angelbroking/historical/v1/getCandleData
{
  "exchange": "NSE",
  "symboltoken": "26009",
  "interval": "ONE_DAY",
  "fromdate": "2024-01-01 09:15",
  "todate": "2024-01-15 15:30"
}
```

## Error Handling

### Common Errors

1. **Authentication Failed**: Check API credentials
2. **Token Expired**: Refresh token automatically
3. **WebSocket Disconnect**: Auto-reconnect with exponential backoff
4. **Rate Limit**: Implement request throttling

### Logging

All operations are logged with appropriate levels:
- `INFO`: Successful operations
- `WARNING`: Non-critical issues
- `ERROR`: Critical failures

## Testing

### Test Script

Run the test script to verify integration:

```bash
python test_feed.py
```

### Manual Testing

1. **Start the server**:
   ```bash
   python run.py
   ```

2. **Test feed endpoint**:
   ```bash
   curl -X POST http://localhost:8000/api/v1/feed \
     -H "Content-Type: application/json" \
     -d '{"tk":"NIFTY","ltpc":22450.75,"ch":125.50,"chp":0.56}'
   ```

3. **Check latest data**:
   ```bash
   curl http://localhost:8000/api/v1/feed/latest
   ```

## Production Considerations

### Security

1. **Store credentials securely** in environment variables
2. **Use HTTPS** for all API communications
3. **Implement rate limiting** to avoid API restrictions
4. **Log sensitive data carefully**

### Performance

1. **Connection pooling** for REST API calls
2. **WebSocket reconnection** with exponential backoff
3. **Database indexing** for market data queries
4. **Caching** for frequently accessed data

### Monitoring

1. **Health checks** for WebSocket connection
2. **Data quality validation** for incoming feeds
3. **Performance metrics** for API response times
4. **Error alerting** for critical failures

## Troubleshooting

### Common Issues

1. **WebSocket Connection Fails**
   - Check network connectivity
   - Verify API credentials
   - Check if market is open

2. **Authentication Errors**
   - Verify API key and secret
   - Check client code and PIN
   - Ensure timestamp is correct

3. **No Market Data**
   - Verify symbol to token mapping
   - Check if symbols are subscribed
   - Ensure market is open

### Debug Mode

Enable debug logging by setting:

```python
logging.getLogger("app.services.angel_one_service").setLevel(logging.DEBUG)
```

## References

- [Angel One SmartAPI Documentation](https://smartapi.angelbroking.com/docs)
- [WebSocket API Guide](https://smartapi.angelbroking.com/docs/websocket)
- [REST API Reference](https://smartapi.angelbroking.com/docs/rest) 