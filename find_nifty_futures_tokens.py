#!/usr/bin/env python3
"""
Script to find NIFTY futures tokens for Angel One SmartAPI
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add the backend directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.services.angel_one_service import AngelOneService
from logzero import logger

async def find_nifty_futures_tokens():
    """Find NIFTY futures tokens for current and next month"""
    
    # Initialize service
    service = AngelOneService()
    
    # Generate potential futures tokens based on current date
    current_date = datetime.now()
    
    # NIFTY futures typically have tokens in the format:
    # Current month: Usually 26000 series
    # Next month: Usually 26001 series, etc.
    
    # Common NIFTY futures token patterns
    futures_tokens = [
        # Current month contracts (Dec 2024, Jan 2025, etc.)
        {"token": "39", "symbol": "NIFTY25JAN", "description": "NIFTY Jan 2025 Futures"},
        {"token": "40", "symbol": "NIFTY25FEB", "description": "NIFTY Feb 2025 Futures"},
        {"token": "41", "symbol": "NIFTY25MAR", "description": "NIFTY Mar 2025 Futures"},
        
        # Alternative token ranges
        {"token": "26010", "symbol": "NIFTYFUT1", "description": "NIFTY Futures Current Month"},
        {"token": "26011", "symbol": "NIFTYFUT2", "description": "NIFTY Futures Next Month"},
        {"token": "26012", "symbol": "NIFTYFUT3", "description": "NIFTY Futures Month+2"},
        
        # Common patterns from other brokers
        {"token": "2885", "symbol": "NIFTY_FUT", "description": "Generic NIFTY Futures"},
        {"token": "15083", "symbol": "NIFTY_FUT_ALT", "description": "Alternative NIFTY Futures"},
        
        # Try systematic approach
        {"token": "99926001", "symbol": "NIFTYFUT_SYS1", "description": "Systematic NIFTY Futures 1"},
        {"token": "99926002", "symbol": "NIFTYFUT_SYS2", "description": "Systematic NIFTY Futures 2"},
        {"token": "99926003", "symbol": "NIFTYFUT_SYS3", "description": "Systematic NIFTY Futures 3"},
    ]
    
    print("🔍 Searching for NIFTY Futures tokens...")
    print("=" * 60)
    
    # Authenticate
    print("🔐 Authenticating with Angel One...")
    auth_success = await service.authenticate()
    
    if not auth_success:
        print("❌ Authentication failed!")
        return
    
    print("✅ Authentication successful!")
    print()
    
    working_tokens = []
    
    for token_info in futures_tokens:
        token = token_info["token"]
        symbol = token_info["symbol"]
        description = token_info["description"]
        
        print(f"🔍 Testing {description} (Token: {token}, Symbol: {symbol})...")
        
        try:
            # Try to get LTP data for NFO (derivatives) exchange
            for exchange in ["NFO", "NSE"]:
                try:
                    quote_response = service.smart_api.ltpData(exchange, symbol, token)
                    
                    if quote_response and isinstance(quote_response, dict):
                        if quote_response.get('status') == True:
                            data = quote_response.get('data', {})
                            if data:
                                ltp = data.get('ltp', 0)
                                if ltp and ltp > 0:  # Valid price
                                    print(f"   ✅ WORKING! Exchange: {exchange}, Price: ₹{ltp:,.2f}")
                                    working_tokens.append({
                                        "token": token,
                                        "symbol": symbol,
                                        "exchange": exchange,
                                        "description": description,
                                        "price": ltp
                                    })
                                    break
                            else:
                                print(f"   ❌ No data in response ({exchange})")
                        else:
                            error = quote_response.get('message', 'Unknown error')
                            print(f"   ❌ API Error ({exchange}): {error}")
                    else:
                        print(f"   ❌ Invalid response format ({exchange})")
                        
                except Exception as e:
                    print(f"   ❌ Exception ({exchange}): {e}")
                    continue
                
        except Exception as e:
            print(f"   ❌ General Exception: {e}")
        
        print()
    
    # Also try to search for instruments using Angel One's search
    print("🔍 Searching for NIFTY instruments using Angel One search...")
    try:
        # Search for NIFTY instruments
        search_results = service.smart_api.searchScrip("NFO", "NIFTY")
        if search_results and search_results.get('status'):
            instruments = search_results.get('data', [])
            print(f"   Found {len(instruments)} NIFTY instruments in NFO exchange")
            
            # Filter for futures only
            futures_instruments = [
                inst for inst in instruments 
                if 'FUT' in inst.get('tradingsymbol', '') or 
                   inst.get('instrumenttype', '').upper() == 'FUTIDX'
            ]
            
            print(f"   Found {len(futures_instruments)} NIFTY futures:")
            for inst in futures_instruments[:5]:  # Show first 5
                token = inst.get('token', '')
                symbol = inst.get('tradingsymbol', '')
                name = inst.get('name', '')
                print(f"     • {symbol} (Token: {token}) - {name}")
                
                # Add to working tokens
                working_tokens.append({
                    "token": token,
                    "symbol": symbol,
                    "exchange": "NFO",
                    "description": f"NIFTY Futures - {name}",
                    "price": "Unknown"
                })
        
    except Exception as e:
        print(f"   ❌ Search failed: {e}")
    
    print()
    
    # Summary
    print("=" * 60)
    print("📊 NIFTY FUTURES TOKEN SEARCH RESULTS")
    print("=" * 60)
    
    if working_tokens:
        print("✅ Found working NIFTY Futures tokens:")
        for token_info in working_tokens:
            print(f"   • {token_info['description']}")
            print(f"     Token: {token_info['token']}, Exchange: {token_info['exchange']}")
            print(f"     Symbol: {token_info['symbol']}")
            if token_info['price'] != "Unknown":
                print(f"     Price: ₹{token_info['price']:,.2f}")
            print()
        
        # Provide code to add to the service
        print("🔧 TO ADD TO ANGEL ONE SERVICE:")
        print("=" * 40)
        print("Add these tokens to market_tokens in angel_one_service.py:")
        print()
        
        for token_info in working_tokens[:3]:  # Show top 3
            exchange_type = 2 if token_info['exchange'] == 'NFO' else 1
            print(f'    {{"exchangeType": {exchange_type}, "tokens": ["{token_info["token"]}"]}}')  # {token_info['symbol']}
        
        print()
        print("Add to _get_symbol_from_token method:")
        for token_info in working_tokens[:3]:
            print(f'    "{token_info["token"]}": "{token_info["symbol"]}",')
        
        print()
        print("Add to _get_token_from_symbol method:")
        for token_info in working_tokens[:3]:
            print(f'    "{token_info["symbol"]}": "{token_info["token"]}",')
        
    else:
        print("❌ No working NIFTY Futures tokens found")
        print("💡 This could mean:")
        print("   1. NIFTY Futures are not available in your account")
        print("   2. Market is closed")
        print("   3. Different token format is used")
        print("   4. Need to enable F&O trading in your account")

if __name__ == "__main__":
    asyncio.run(find_nifty_futures_tokens())