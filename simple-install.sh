#!/bin/bash
set -e

echo "🚀 Simple installation with dependency resolution..."

# Upgrade pip first
pip install --upgrade pip

# Install from requirements file - let pip resolve dependencies
pip install -r requirements-final.txt

echo "✅ Installation complete!"