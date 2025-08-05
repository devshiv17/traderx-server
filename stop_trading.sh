#!/bin/bash
echo "🛑 Stopping Trading Signal System"
echo "=================================="

# Read PID from file if it exists
if [ -f .server_pid ]; then
    SERVER_PID=$(cat .server_pid)
    echo "📝 Found server PID: $SERVER_PID"
    
    # Check if process is running
    if ps -p $SERVER_PID > /dev/null 2>&1; then
        echo "🔄 Stopping server process..."
        kill $SERVER_PID
        sleep 3
        
        # Force kill if still running
        if ps -p $SERVER_PID > /dev/null 2>&1; then
            echo "🔨 Force stopping server..."
            kill -9 $SERVER_PID
        fi
        
        echo "✅ Server stopped successfully"
    else
        echo "⚠️ Server process not found (may already be stopped)"
    fi
    
    # Clean up PID file
    rm -f .server_pid
else
    echo "📝 No PID file found, attempting to kill by port..."
fi

# Kill any remaining processes on port 8000
echo "🧹 Cleaning up any remaining processes on port 8000..."
pkill -f "uvicorn.*app.main.*8000" 2>/dev/null || true

# Verify port is free
sleep 2
if curl -s -m 2 "http://127.0.0.1:8000/api/v1/signals/monitoring-status" > /dev/null 2>&1; then
    echo "❌ Server may still be running on port 8000"
else
    echo "✅ Port 8000 is now free"
fi

echo "🏁 Trading system shutdown complete"