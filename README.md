# Trading Signals API Backend

A production-ready FastAPI backend for the Trading Signals application with MongoDB Atlas integration and real-time market data from Angel One SmartAPI.

## Features

- **FastAPI**: Modern, fast web framework for building APIs
- **MongoDB Atlas**: Cloud-hosted MongoDB database with optimized indexes
- **Angel One Integration**: Real-time market data via WebSocket and REST API
- **JWT Authentication**: Secure token-based authentication
- **Async/Await**: Full async support for better performance
- **Pydantic**: Data validation and serialization
- **Production Ready**: Logging, error handling, CORS, middleware
- **Duplicate Prevention**: Unique indexes and validation to prevent duplicate data
- **After-Hours Filtering**: Only store data during Indian market hours (09:15–15:30 IST, Mon–Fri)
- **Real-time Data Storage**: Live market data storage with quality validation

## Project Structure

```
backend/
├── app/
│   ├── api/
│   │   ├── v1/
│   │   │   ├── auth.py          # Authentication endpoints
│   │   │   ├── api.py           # API router
│   │   │   ├── market_data.py   # Market data endpoints
│   │   │   └── __init__.py
│   │   ├── deps.py              # Dependency injection
│   │   └── __init__.py
│   ├── core/
│   │   ├── config.py            # Application settings
│   │   ├── database.py          # Database connection and indexes
│   │   ├── security.py          # Security utilities
│   │   └── __init__.py
│   ├── models/
│   │   ├── user.py              # User model
│   │   ├── signal.py            # Signal model
│   │   ├── market_data.py       # Market data model
│   │   └── __init__.py
│   ├── schemas/
│   │   ├── auth.py              # Authentication schemas
│   │   └── __init__.py
│   ├── services/
│   │   ├── user_service.py      # User business logic
│   │   ├── market_data_service.py # Market data operations
│   │   ├── angel_one_service.py # Angel One API integration
│   │   ├── angel_one_rest_client.py # REST API client
│   │   └── __init__.py
│   ├── utils/
│   │   └── __init__.py
│   ├── main.py                  # FastAPI application
│   ├── ws.py                    # WebSocket handler
│   └── __init__.py
├── tests/                       # Test files
├── requirements.txt             # Python dependencies
├── env.example                  # Environment variables template
├── ANGEL_ONE_INTEGRATION.md     # Angel One integration guide
└── README.md                    # This file
```

## Database Schema

### Users Collection
```json
{
  "_id": "ObjectId",
  "email": "string (unique)",
  "name": "string",
  "hashed_password": "string",
  "is_active": "boolean",
  "is_verified": "boolean",
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

### Market Data Collection
```json
{
  "_id": "ObjectId",
  "tk": "string (token)",
  "symbol": "string (unique with received_at)",
  "exchange": "string",
  "ltpc": "float (last traded price)",
  "ch": "float (change)",
  "chp": "float (change percentage)",
  "high": "float",
  "low": "float",
  "open": "float",
  "close": "float",
  "volume": "integer",
  "exchange_type": "integer",
  "token": "string",
  "instrument_type": "string",
  "expiry": "string",
  "strike_price": "float",
  "depth": "object",
  "received_at": "datetime (unique with symbol)",
  "source": "string",
  "processed": "boolean",
  "raw_data": "object",
  "data_quality": "string",
  "is_realtime": "boolean"
}
```

### Signals Collection
```json
{
  "_id": "ObjectId",
  "user_id": "ObjectId (ref: users)",
  "symbol": "string",
  "option_type": "string (PE|CE)",
  "signal_type": "string (BUY|SELL)",
  "entry_price": "float",
  "target_price": "float",
  "stop_loss": "float",
  "quantity": "integer",
  "confidence": "integer (0-100)",
  "status": "string (ACTIVE|COMPLETED|CANCELLED)",
  "notes": "string (optional)",
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

## Database Indexes

### Market Data Indexes
- `_id_`: Primary key
- `tk_1`: Token/Symbol index
- `received_at_-1`: Timestamp index (descending)
- `source_1`: Source index
- `processed_1`: Processed status index
- `tk_1_received_at_-1`: Compound index for efficient queries
- `symbol_receivedAt_unique`: **Unique index on (symbol, received_at)** - Prevents duplicates

### Users Indexes
- `_id_`: Primary key
- `email_1`: Unique email index
- `is_active_1`: Active status index
- `created_at_1`: Creation timestamp index

### Signals Indexes
- `_id_`: Primary key
- `user_id_1`: User reference index
- `symbol_1`: Symbol index
- `status_1`: Status index
- `created_at_-1`: Creation timestamp index (descending)
- `user_id_1_status_1`: Compound index for user signals
- `symbol_1_created_at_-1`: Compound index for symbol history

## Angel One Integration

The backend integrates with Angel One SmartAPI for real-time market data:

### Supported Indices
- **NIFTY 50** (Token: 99926000)
- **BANKNIFTY** (Token: 99926009)
- **FINNIFTY** (Token: 99926037)
- **MIDCPNIFTY** (Token: 99926074)
- **SENSEX** (Token: 99926017)
- **BANKEX** (Token: 99926018)

### Features
- **Real-time WebSocket**: Live price updates during market hours
- **REST API**: Historical data and market information
- **Data Validation**: Quality checks and error handling
- **Duplicate Prevention**: Unique indexes prevent duplicate records
- **After-Hours Filtering**: Only stores data during market hours (09:15–15:30 IST, Mon–Fri)

## Installation

1. **Clone the repository**
   ```bash
   cd backend
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

5. **Initialize database indexes**
   ```bash
   python create_market_data_unique_index.py
   ```

6. **Run the application**
   ```bash
   # Development
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   
   # Production
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## API Endpoints

### Authentication

- `POST /api/v1/auth/register` - Register a new user
- `POST /api/v1/auth/login` - Login and get access token
- `GET /api/v1/auth/me` - Get current user profile (requires authentication)

### Market Data

- `GET /api/v1/market-data/latest` - Get latest market data
- `GET /api/v1/market-data/symbol/{symbol}` - Get data for specific symbol
- `GET /api/v1/market-data/filter` - Filter market data with parameters
- `GET /api/v1/market-data/statistics` - Get data statistics
- `GET /api/v1/market-data/chart/{symbol}` - Get chart data for symbol
- `GET /api/v1/market-data/symbols` - Get available symbols

### Health Check

- `GET /health` - Health check endpoint
- `GET /` - Root endpoint with API information

## WebSocket Endpoints

- `WS /ws` - WebSocket connection for real-time updates

## API Documentation

Once the server is running, you can access:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

## Environment Variables

Create a `.env` file with the following variables:

```env
# MongoDB Configuration
MONGODB_URL=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
DATABASE_NAME=trading_signals

# JWT Configuration
SECRET_KEY=your-secret-key-here-make-it-long-and-secure
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# Application Configuration
APP_NAME=Trading Signals API
DEBUG=False
ENVIRONMENT=production

# CORS Configuration
ALLOWED_ORIGINS=["http://localhost:3000", "http://localhost:5173"]

# Angel One Configuration (if using)
ANGEL_ONE_API_KEY=your_api_key
ANGEL_ONE_CLIENT_ID=your_client_id
ANGEL_ONE_PASSWORD=your_password
ANGEL_ONE_TOTP_KEY=your_totp_key
```

## Data Quality & Validation

### Market Data Validation
- **Required Fields**: Token, symbol, last traded price
- **Data Quality**: Checks for API errors, invalid prices, missing data
- **Duplicate Prevention**: Unique index on (symbol, received_at)
- **After-Hours Filter**: Only stores data during market hours
- **Real-time Flag**: Distinguishes live vs historical data

### Error Handling
- **Duplicate Key Errors**: Gracefully handled and logged
- **API Errors**: Filtered out invalid responses
- **Connection Issues**: Automatic retry and reconnection
- **Data Quality**: Poor quality data is logged and skipped

## Testing

```bash
# Run tests
pytest

# Run tests with coverage
pytest --cov=app

# Run tests with verbose output
pytest -v
```

## Development

### Code Formatting

```bash
# Format code with black
black app/

# Sort imports with isort
isort app/

# Check code style with flake8
flake8 app/
```

### Type Checking

```bash
# Run mypy for type checking
mypy app/
```

## Production Deployment

### Database Migration
Before deploying to production, ensure all indexes are created:

```bash
python create_market_data_unique_index.py
   ```

### Monitoring
- Monitor WebSocket connections and data flow
- Check database performance and index usage
- Monitor API response times and error rates
- Track market data quality and validation results

### Performance Optimization
- Database indexes are optimized for common queries
- Batch operations for efficient data insertion
- Connection pooling for database and API calls
- Async operations for better throughput

## Troubleshooting

### Common Issues

1. **Duplicate Data**: Check if unique index is properly created
2. **No Live Data**: Verify market hours and WebSocket connection
3. **API Errors**: Check Angel One credentials and API limits
4. **Database Issues**: Verify MongoDB connection and indexes

### Logs
Check the `logs/` directory for detailed application logs:
- `app.log`: General application logs
- WebSocket connection logs
- Data validation and storage logs

## License

This project is licensed under the MIT License. 