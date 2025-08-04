#!/usr/bin/env python3
"""
Script to find the correct SENSEX token for Angel One SmartAPI
"""

import asyncio
import sys
import os

# Add the backend directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.services.angel_one_service import AngelOneService
from logzero import logger

async def find_sensex_token():
    """Find the correct SENSEX token"""
    
    # Initialize service
    service = AngelOneService()
    
    # Test different SENSEX tokens
    sensex_tokens = [
        {"token": "1", "exchange": "NSE", "description": "Standard SENSEX"},
        {"token": "1", "exchange": "BSE", "description": "BSE SENSEX"},
        {"token": "26009", "exchange": "NSE", "description": "NSE SENSEX (same as NIFTY)"},
        {"token": "26000", "exchange": "NSE", "description": "Alternative SENSEX"},
        {"token": "26001", "exchange": "NSE", "description": "Another SENSEX variant"},
        {"token": "26002", "exchange": "NSE", "description": "SENSEX variant 2"},
        {"token": "26003", "exchange": "NSE", "description": "SENSEX variant 3"},
        {"token": "26004", "exchange": "NSE", "description": "SENSEX variant 4"},
        {"token": "26005", "exchange": "NSE", "description": "SENSEX variant 5"},
    ]
    
    print("🔍 Searching for correct SENSEX token...")
    print("=" * 60)
    
    # Authenticate
    print("🔐 Authenticating...")
    auth_success = await service.authenticate()
    
    if not auth_success:
        print("❌ Authentication failed!")
        return
    
    print("✅ Authentication successful!")
    print()
    
    working_tokens = []
    
    for token_info in sensex_tokens:
        token = token_info["token"]
        exchange = token_info["exchange"]
        description = token_info["description"]
        
        print(f"🔍 Testing {description} (Token: {token}, Exchange: {exchange})...")
        
        try:
            # Try to get quote data
            quote_response = service.smart_api.ltpData(exchange, token, token)
            
            if quote_response and isinstance(quote_response, dict):
                if quote_response.get('status') == True:
                    data = quote_response.get('data', {})
                    if data:
                        ltp = data.get('ltp', 0)
                        print(f"   ✅ WORKING! Price: ₹{ltp:,.2f}")
                        working_tokens.append({
                            "token": token,
                            "exchange": exchange,
                            "description": description,
                            "price": ltp
                        })
                    else:
                        print(f"   ❌ No data in response")
                else:
                    error = quote_response.get('message', 'Unknown error')
                    print(f"   ❌ API Error: {error}")
            else:
                print(f"   ❌ Invalid response format")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
        
        print()
    
    # Summary
    print("=" * 60)
    print("📊 SENSEX TOKEN SEARCH RESULTS")
    print("=" * 60)
    
    if working_tokens:
        print("✅ Found working SENSEX tokens:")
        for token_info in working_tokens:
            print(f"   • {token_info['description']}")
            print(f"     Token: {token_info['token']}, Exchange: {token_info['exchange']}")
            print(f"     Price: ₹{token_info['price']:,.2f}")
            print()
        
        # Recommend the best one
        best_token = working_tokens[0]
        print(f"🎯 RECOMMENDED: Use token '{best_token['token']}' on {best_token['exchange']}")
        print(f"   Update your _get_token_from_symbol method with:")
        print(f"   'SENSEX': '{best_token['token']}'")
        
    else:
        print("❌ No working SENSEX tokens found")
        print("💡 SENSEX might not be available in your Angel One account")
        print("💡 You can use NIFTY as an alternative (it's already working)")

if __name__ == "__main__":
    asyncio.run(find_sensex_token()) 