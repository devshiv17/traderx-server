from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, status
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
from jose import JWTError, jwt
from .core.config import settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.symbol_subscriptions: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total: {len(self.active_connections)}")
        
        # Remove from symbol subscriptions
        for symbol, connections in self.symbol_subscriptions.items():
            if websocket in connections:
                connections.remove(websocket)
                if not connections:
                    del self.symbol_subscriptions[symbol]

    async def subscribe_to_symbol(self, websocket: WebSocket, symbol: str):
        """Subscribe a WebSocket connection to updates for a specific symbol"""
        if symbol not in self.symbol_subscriptions:
            self.symbol_subscriptions[symbol] = []
        
        if websocket not in self.symbol_subscriptions[symbol]:
            self.symbol_subscriptions[symbol].append(websocket)
            logger.info(f"WebSocket subscribed to {symbol}. Total subscribers: {len(self.symbol_subscriptions[symbol])}")

    async def unsubscribe_from_symbol(self, websocket: WebSocket, symbol: str):
        """Unsubscribe a WebSocket connection from updates for a specific symbol"""
        if symbol in self.symbol_subscriptions and websocket in self.symbol_subscriptions[symbol]:
            self.symbol_subscriptions[symbol].remove(websocket)
            if not self.symbol_subscriptions[symbol]:
                del self.symbol_subscriptions[symbol]
            logger.info(f"WebSocket unsubscribed from {symbol}")

    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast message to all connected clients"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error sending message to WebSocket client: {e}")
                disconnected.append(connection)
        
        # Remove disconnected clients
        for connection in disconnected:
            self.disconnect(connection)

    async def broadcast_to_symbol(self, symbol: str, message: Dict[str, Any]):
        """Broadcast message to clients subscribed to a specific symbol"""
        if symbol not in self.symbol_subscriptions:
            return
        
        disconnected = []
        for connection in self.symbol_subscriptions[symbol]:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error sending message to WebSocket client for {symbol}: {e}")
                disconnected.append(connection)
        
        # Remove disconnected clients
        for connection in disconnected:
            await self.unsubscribe_from_symbol(connection, symbol)
            self.disconnect(connection)

manager = ConnectionManager()

# JWT auth helper
async def get_current_user_ws(token: str = None):
    if not token:
        return None
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        email: str = payload.get("sub")
        if email is None:
            return None
        return email
    except JWTError:
        return None

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Get token from query params
    token = websocket.query_params.get("token")
    
    # For development, allow all connections - more permissive
    logger.info(f"WebSocket connection attempt with token: {token[:20] if token else 'None'}...")
    
    if not token:
        logger.warning("WebSocket connection without token - allowing for development")
        user = "anonymous-user"
    elif token == "mock-jwt-token-for-admin":
        logger.warning("WebSocket connection with mock token - allowing for development")
        user = "dev-user"
    else:
        user = await get_current_user_ws(token)
        if not user:
            logger.warning(f"WebSocket JWT validation failed - allowing for development anyway")
            # Instead of closing, allow it for development
            user = "jwt-invalid-user"
    
    await manager.connect(websocket)
    try:
        while True:
            # Receive messages from client
            data = await websocket.receive_json()
            
            # Handle different message types
            message_type = data.get("type")
            
            if message_type == "subscribe":
                symbol = data.get("symbol")
                if symbol:
                    await manager.subscribe_to_symbol(websocket, symbol)
                    await websocket.send_json({
                        "type": "subscription_confirmed",
                        "symbol": symbol,
                        "message": f"Subscribed to {symbol} updates"
                    })
            
            elif message_type == "unsubscribe":
                symbol = data.get("symbol")
                if symbol:
                    await manager.unsubscribe_from_symbol(websocket, symbol)
                    await websocket.send_json({
                        "type": "unsubscription_confirmed",
                        "symbol": symbol,
                        "message": f"Unsubscribed from {symbol} updates"
                    })
            
            elif message_type == "ping":
                await websocket.send_json({"type": "pong"})
            
            else:
                # Keep connection alive
                await websocket.send_json({"type": "heartbeat"})
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

# Helper to broadcast new market data
async def broadcast_market_data(data: Dict[str, Any]):
    """Broadcast market data to all connected clients"""
    message = {
        "type": "market_data",
        "data": data,
        "timestamp": data.get("received_at", "").isoformat() if data.get("received_at") else ""
    }
    await manager.broadcast(message)

# Helper to broadcast price updates for specific symbols
async def broadcast_price_update(symbol: str, data: Dict[str, Any]):
    """Broadcast price update to clients subscribed to a specific symbol"""
    message = {
        "type": "price_update",
        "symbol": symbol,
        "data": {
            "symbol": symbol,
            "ltpc": data.get("ltpc", 0),
            "ch": data.get("ch", 0),
            "chp": data.get("chp", 0),
            "high": data.get("high", 0),
            "low": data.get("low", 0),
            "open": data.get("open", 0),
            "close": data.get("close", 0),
            "volume": data.get("volume", 0),
            "exchange": data.get("exchange", ""),
            "received_at": data.get("received_at", "").isoformat() if data.get("received_at") else ""
        },
        "timestamp": data.get("received_at", "").isoformat() if data.get("received_at") else ""
    }
    await manager.broadcast_to_symbol(symbol, message)

# Helper to broadcast chart data updates
async def broadcast_chart_update(symbol: str, timeframe: str, chart_data: List[Dict[str, Any]]):
    """Broadcast chart data update to clients subscribed to a specific symbol"""
    message = {
        "type": "chart_update",
        "symbol": symbol,
        "timeframe": timeframe,
        "data": chart_data,
        "timestamp": ""
    }
    await manager.broadcast_to_symbol(symbol, message)

# Helper to broadcast trading signals
async def broadcast_signal(signal_data: Dict[str, Any]):
    """Broadcast trading signal to all connected clients"""
    message = {
        "type": "trading_signal",
        "signal": {
            "id": signal_data.get("id"),
            "signal_type": signal_data.get("signal_type"),
            "symbol": signal_data.get("symbol", "NIFTY"),
            "future_symbol": signal_data.get("future_symbol"),
            "session_name": signal_data.get("session_name"),
            "reason": signal_data.get("reason"),
            "confidence": signal_data.get("confidence", 50),
            "nifty_price": signal_data.get("nifty_price"),
            "future_price": signal_data.get("future_price"),
            "session_high": signal_data.get("session_high"),
            "session_low": signal_data.get("session_low"),
            "vwap_nifty": signal_data.get("vwap_nifty"),
            "vwap_future": signal_data.get("vwap_future"),
            "timestamp": signal_data.get("timestamp").isoformat() if signal_data.get("timestamp") else "",
            "status": signal_data.get("status", "ACTIVE")
        },
        "timestamp": signal_data.get("timestamp").isoformat() if signal_data.get("timestamp") else ""
    }
    
    # Broadcast to all clients
    await manager.broadcast(message)
    
    # Also broadcast to specific symbol subscribers
    symbol = signal_data.get("symbol", "NIFTY")
    future_symbol = signal_data.get("future_symbol")
    
    await manager.broadcast_to_symbol(symbol, message)
    if future_symbol and future_symbol != symbol:
        await manager.broadcast_to_symbol(future_symbol, message)

# Helper to broadcast session status updates
async def broadcast_session_update(session_data: Dict[str, Any]):
    """Broadcast trading session status update"""
    message = {
        "type": "session_update",
        "session": session_data,
        "timestamp": ""
    }
    await manager.broadcast(message) 