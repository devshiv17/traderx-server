#!/bin/bash
set -e

echo "Installing packages one by one to identify problematic ones..."

# Core FastAPI packages
pip install fastapi==0.104.1
pip install uvicorn==0.24.0

# Database packages
pip install motor==3.3.2
pip install pymongo==4.6.0

# Try pydantic - this might be the issue
echo "Installing pydantic..."
pip install --only-binary=pydantic pydantic==2.0.3 || pip install pydantic==1.10.13

# Auth packages
pip install python-jose==3.3.0
pip install passlib==1.7.4

# Other packages
pip install python-multipart==0.0.6
pip install python-dotenv==1.0.0
pip install websockets==12.0
pip install aiohttp==3.9.1

# Angel One packages
pip install pyotp==2.9.0
pip install logzero==1.7.0
pip install websocket-client==1.6.4

echo "Basic installation complete!"

# Try problematic packages one by one
echo "Trying potentially problematic packages..."

# This might be the issue
echo "Trying smartapi-python..."
pip install smartapi-python==1.4.8 || echo "smartapi-python failed"

echo "Trying pycryptodome..."
pip install --only-binary=pycryptodome pycryptodome==3.15.0 || echo "pycryptodome failed"

echo "Build script complete!"