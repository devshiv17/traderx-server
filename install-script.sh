#!/bin/bash
set -e

echo "🚀 Installing packages with dependency bypass..."

# Upgrade pip first
pip install --upgrade pip

# Install packages individually to bypass dependency resolution
pip install --no-deps fastapi==0.115.0
pip install --no-deps uvicorn==0.30.6
pip install --no-deps motor==3.5.1
pip install --no-deps pymongo==4.10.1

# Install latest pydantic that should work
pip install --no-deps pydantic==2.9.2
pip install --no-deps pydantic-settings==2.5.2

# Install remaining packages
pip install --no-deps python-multipart==0.0.12
pip install --no-deps python-dotenv==1.0.1
pip install --no-deps aiohttp==3.10.5

# Install missing dependencies that might be needed
pip install typing-extensions
pip install annotated-types
pip install starlette
pip install click
pip install h11

echo "✅ Installation complete!"