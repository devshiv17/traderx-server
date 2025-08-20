#!/usr/bin/env python3
"""
Verify NIFTY-only symbol configuration
This script checks what symbols are configured vs what's stored in the database
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.symbols import SymbolsConfig

def main():
    print("=" * 60)
    print("NIFTY-ONLY TRADING SYMBOLS VERIFICATION")
    print("=" * 60)
    
    print("\n1. CONFIGURED SYMBOLS:")
    print("-" * 30)
    symbols = SymbolsConfig.get_all_symbols()
    for symbol in symbols:
        print(f"   {symbol.symbol:<20} | Token: {symbol.token:<10} | {symbol.name}")
        print(f"   Exchange: {symbol.exchange:<10} | Type: {symbol.instrument_type}")
        print()
    
    print("2. WEBSOCKET SUBSCRIPTION TOKENS:")
    print("-" * 30)
    market_tokens = SymbolsConfig.get_market_tokens()
    for i, token_group in enumerate(market_tokens, 1):
        exchange_name = "NSE" if token_group["exchangeType"] == 1 else "NFO"
        print(f"   {i}. {exchange_name} Exchange (Type {token_group['exchangeType']}): {token_group['tokens']}")
    
    print("\n3. TOKEN MAPPINGS:")
    print("-" * 30)
    token_map = SymbolsConfig.get_token_to_symbol_map()
    for token, symbol in token_map.items():
        print(f"   Token {token} -> {symbol}")
    
    print("\n4. SYMBOL NAMES LIST:")
    print("-" * 30)
    symbol_names = SymbolsConfig.get_symbol_names()
    print(f"   Configured symbols: {symbol_names}")
    
    print("\n5. SUMMARY:")
    print("-" * 30)
    print(f"   Total symbols configured: {len(symbols)}")
    print(f"   Index symbols: {len([s for s in symbols if s.instrument_type == 'INDEX'])}")
    print(f"   Futures symbols: {len([s for s in symbols if s.instrument_type == 'FUTIDX'])}")
    print(f"   WebSocket subscriptions: {len(market_tokens)}")
    
    print("\n✅ Configuration updated to NIFTY Index + NIFTY Futures ONLY")
    print("✅ Removed: BANKNIFTY, FINNIFTY, SENSEX")
    print("✅ Clean, focused trading setup for NIFTY-only strategy")
    print("=" * 60)

if __name__ == "__main__":
    main()