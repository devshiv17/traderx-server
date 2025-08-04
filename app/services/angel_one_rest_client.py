import aiohttp
import json
import logging
import hashlib
import hmac
import time
from typing import Dict, Any, Optional
from ..core.config import settings

logger = logging.getLogger(__name__)


class AngelOneRestClient:
    def __init__(self):
        self.api_key = settings.angel_one_api_key
        self.secret = settings.angel_one_secret
        self.client_code = settings.angel_one_client_code
        self.pin = settings.angel_one_pin
        self.base_url = "https://apiconnect.angelbroking.com"
        self.access_token = None
        self.refresh_token = None
        
    def _generate_checksum(self, timestamp: str) -> str:
        """Generate checksum for API authentication"""
        message = f"{self.api_key}{timestamp}{self.client_code}{self.pin}"
        checksum = hmac.new(
            self.secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return checksum
    
    async def authenticate(self) -> bool:
        """Authenticate with Angel One SmartAPI"""
        try:
            timestamp = str(int(time.time()))
            checksum = self._generate_checksum(timestamp)
            
            auth_data = {
                "api_key": self.api_key,
                "client_code": self.client_code,
                "pin": self.pin,
                "timestamp": timestamp,
                "checksum": checksum
            }
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/rest/auth/angelbroking/user/v1/loginByPassword",
                    json=auth_data,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get("status") and data["status"]:
                            self.access_token = data["data"]["jwtToken"]
                            self.refresh_token = data["data"]["refreshToken"]
                            logger.info("Angel One authentication successful")
                            return True
                        else:
                            logger.error(f"Authentication failed: {data.get('message', 'Unknown error')}")
                            return False
                    else:
                        logger.error(f"Authentication request failed with status {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    async def get_market_data(self, symbols: list) -> Optional[Dict[str, Any]]:
        """Get market data for given symbols"""
        try:
            if not self.access_token:
                if not await self.authenticate():
                    return None
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB"
            }
            
            # Convert symbols to tokens (you'll need to map your symbols to Angel One tokens)
            tokens = self._symbols_to_tokens(symbols)
            
            data = {
                "mode": "FULL",
                "exchangeTokens": tokens
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/rest/secure/angelbroking/order/v1/getLtpData",
                    json=data,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        logger.error(f"Market data request failed with status {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")
            return None
    
    def _symbols_to_tokens(self, symbols: list) -> list:
        """Convert symbols to Angel One tokens"""
        # This is a mapping of common symbols to Angel One tokens
        # You'll need to get the correct token mapping from Angel One
        symbol_to_token = {
            "NIFTY": "26009",
            "BANKNIFTY": "26017", 
            "FINNIFTY": "26037",
            "SENSEX": "1",
            "RELIANCE": "2885",
            "TCS": "11536",
            "HDFC": "1330",
            "INFY": "1594"
        }
        
        tokens = []
        for symbol in symbols:
            if symbol in symbol_to_token:
                tokens.append({
                    "exchangeType": 1,  # NSE
                    "tokens": [symbol_to_token[symbol]]
                })
        
        return tokens
    
    async def get_historical_data(self, symbol: str, interval: str = "ONE_DAY") -> Optional[Dict[str, Any]]:
        """Get historical data for a symbol"""
        try:
            if not self.access_token:
                if not await self.authenticate():
                    return None
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB"
            }
            
            # Get token for symbol
            tokens = self._symbols_to_tokens([symbol])
            if not tokens:
                logger.error(f"No token found for symbol: {symbol}")
                return None
            
            data = {
                "exchange": "NSE",
                "symboltoken": tokens[0]["tokens"][0],
                "interval": interval,
                "fromdate": "2024-01-01 09:15",
                "todate": "2024-01-15 15:30"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/rest/secure/angelbroking/historical/v1/getCandleData",
                    params=data,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        logger.error(f"Historical data request failed with status {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return None
    
    async def get_user_profile(self) -> Optional[Dict[str, Any]]:
        """Get user profile information"""
        try:
            if not self.access_token:
                if not await self.authenticate():
                    return None
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/rest/secure/angelbroking/user/v1/getProfile",
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        logger.error(f"Profile request failed with status {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error fetching user profile: {e}")
            return None


# Create client instance
angel_one_rest_client = AngelOneRestClient() 