"""
Central Symbols Configuration
This file contains all trading symbols and tokens used throughout the application.
Update symbols/tokens here to reflect changes across the entire system.
"""

from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class TradingSymbol:
    """Represents a trading symbol with all its properties"""
    symbol: str
    token: str
    name: str
    exchange: str
    exchange_type: int
    instrument_type: str = "INDEX"
    expiry: str = None

class SymbolsConfig:
    """Central configuration for all trading symbols"""
    
    # Primary trading symbols - UPDATE HERE TO CHANGE SYSTEM-WIDE
    NIFTY_INDEX = TradingSymbol(
        symbol="NIFTY",
        token="99926000",
        name="NIFTY 50",
        exchange="NSE",
        exchange_type=1,
        instrument_type="INDEX"
    )
    
    NIFTY_FUTURES = TradingSymbol(
        symbol="NIFTY28AUG25FUT",
        token="64103",
        name="NIFTY August 2025 Futures",
        exchange="NFO",
        exchange_type=2,
        instrument_type="FUTIDX",
        expiry="2025-08-28"
    )
    
    # List of all active symbols (for easy iteration)
    @classmethod
    def get_all_symbols(cls) -> List[TradingSymbol]:
        """Get all active trading symbols"""
        return [cls.NIFTY_INDEX, cls.NIFTY_FUTURES]
    
    @classmethod
    def get_symbol_names(cls) -> List[str]:
        """Get list of symbol names"""
        return [symbol.symbol for symbol in cls.get_all_symbols()]
    
    @classmethod
    def get_tokens(cls) -> List[str]:
        """Get list of all tokens"""
        return [symbol.token for symbol in cls.get_all_symbols()]
    
    @classmethod
    def get_symbol_by_name(cls, symbol_name: str) -> TradingSymbol:
        """Get symbol object by name"""
        symbol_map = {
            cls.NIFTY_INDEX.symbol: cls.NIFTY_INDEX,
            cls.NIFTY_FUTURES.symbol: cls.NIFTY_FUTURES
        }
        return symbol_map.get(symbol_name)
    
    @classmethod
    def get_symbol_by_token(cls, token: str) -> TradingSymbol:
        """Get symbol object by token"""
        token_map = {
            cls.NIFTY_INDEX.token: cls.NIFTY_INDEX,
            cls.NIFTY_FUTURES.token: cls.NIFTY_FUTURES
        }
        return token_map.get(token)
    
    @classmethod
    def get_token_to_symbol_map(cls) -> Dict[str, str]:
        """Get token to symbol name mapping"""
        return {
            cls.NIFTY_INDEX.token: cls.NIFTY_INDEX.symbol,
            cls.NIFTY_FUTURES.token: cls.NIFTY_FUTURES.symbol
        }
    
    @classmethod
    def get_symbol_to_token_map(cls) -> Dict[str, str]:
        """Get symbol name to token mapping"""
        return {
            cls.NIFTY_INDEX.symbol: cls.NIFTY_INDEX.token,
            cls.NIFTY_FUTURES.symbol: cls.NIFTY_FUTURES.token
        }
    
    @classmethod
    def get_market_tokens(cls) -> List[Dict[str, Any]]:
        """Get market tokens in Angel One API format"""
        return [
            {
                "exchangeType": cls.NIFTY_INDEX.exchange_type,
                "tokens": [cls.NIFTY_INDEX.token]
            },
            {
                "exchangeType": cls.NIFTY_FUTURES.exchange_type,
                "tokens": [cls.NIFTY_FUTURES.token]
            }
        ]
    
    @classmethod
    def get_futures_tokens_dict(cls) -> Dict[str, Dict[str, Any]]:
        """Get futures tokens in the format expected by Angel One service"""
        return {
            cls.NIFTY_FUTURES.symbol: {
                'token': cls.NIFTY_FUTURES.token,
                'trading_symbol': cls.NIFTY_FUTURES.symbol,
                'name': cls.NIFTY_FUTURES.name,
                'expiry': cls.NIFTY_FUTURES.expiry,
                'exchange': cls.NIFTY_FUTURES.exchange
            }
        }
    
    @classmethod
    def get_api_symbols_list(cls) -> List[Dict[str, str]]:
        """Get symbols list for API responses"""
        return [
            {
                "symbol": cls.NIFTY_INDEX.symbol,
                "name": cls.NIFTY_INDEX.name,
                "exchange": cls.NIFTY_INDEX.exchange
            },
            {
                "symbol": cls.NIFTY_FUTURES.symbol,
                "name": cls.NIFTY_FUTURES.name,
                "exchange": cls.NIFTY_FUTURES.exchange
            }
        ]

# Create instance for easy import
symbols_config = SymbolsConfig()

# Constants for backward compatibility
NIFTY_SYMBOL = SymbolsConfig.NIFTY_INDEX.symbol
NIFTY_TOKEN = SymbolsConfig.NIFTY_INDEX.token
NIFTY_FUTURES_SYMBOL = SymbolsConfig.NIFTY_FUTURES.symbol
NIFTY_FUTURES_TOKEN = SymbolsConfig.NIFTY_FUTURES.token

# Export all for easy access
__all__ = [
    'SymbolsConfig',
    'TradingSymbol', 
    'symbols_config',
    'NIFTY_SYMBOL',
    'NIFTY_TOKEN',
    'NIFTY_FUTURES_SYMBOL',
    'NIFTY_FUTURES_TOKEN'
]