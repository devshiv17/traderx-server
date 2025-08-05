#!/bin/bash
echo "🚀 Starting Trading Signal System (Persistent)"
echo "============================================="

# Activate virtual environment
source venv/bin/activate

# Kill any existing server on port 8000
echo "🧹 Cleaning up any existing servers..."
pkill -f "uvicorn.*app.main.*8000" 2>/dev/null || true
sleep 2

# Start server with better persistence
echo "🔧 Starting FastAPI server on port 8000..."
nohup python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level warning </dev/null >/dev/null 2>signal_server.log &
SERVER_PID=$!

# Save PID to file for later management
echo $SERVER_PID > .server_pid

echo "Server PID: $SERVER_PID (saved to .server_pid)"
echo "⏳ Waiting 30 seconds for full initialization..."
sleep 30

# Test server with multiple attempts
echo "🧪 Testing server connection..."
for i in {1..5}; do
    if curl -s -f -m 10 "http://127.0.0.1:8000/api/v1/signals/monitoring-status" > /dev/null; then
        echo "✅ Server is running and responding!"
        break
    else
        echo "   Attempt $i/5 - Server not ready yet..."
        sleep 5
    fi
done

# Final check
if curl -s -f -m 10 "http://127.0.0.1:8000/api/v1/signals/monitoring-status" > /dev/null; then
    # Start monitoring
    echo "🔧 Starting signal monitoring..."
    MONITORING_RESULT=$(curl -s -X POST "http://127.0.0.1:8000/api/v1/signals/start-monitoring")
    echo "$MONITORING_RESULT" | grep -q "successfully" && echo "✅ Monitoring started successfully" || echo "⚠️ Monitoring may already be active"
    
    # Show current status
    echo -e "\n📊 Current System Status:"
    STATUS=$(curl -s "http://127.0.0.1:8000/api/v1/signals/monitoring-status")
    
    # Parse JSON status
    echo "$STATUS" | grep -q '"monitoring_active":true' && echo "  🟢 Monitoring: ACTIVE" || echo "  🔴 Monitoring: INACTIVE"
    echo "$STATUS" | grep -q '"market_hours":true' && echo "  🟢 Market: OPEN" || echo "  🔴 Market: CLOSED"
    echo "$STATUS" | grep -q '"service_status":"running"' && echo "  🟢 Service: RUNNING" || echo "  ⚪ Service: STOPPED"
    
    echo -e "\n✅ Signal system is now running!"
    echo "📊 Current NIFTY price being tracked"
    echo "🎯 System will detect breakouts of session high/low levels"
    echo ""
    echo "🌐 API Documentation: http://127.0.0.1:8000/docs"
    echo "📊 Monitor Status: http://127.0.0.1:8000/api/v1/signals/monitoring-status"
    echo "🚨 Active Signals: http://127.0.0.1:8000/api/v1/signals/active"
    echo "📈 Session Status: http://127.0.0.1:8000/api/v1/signals/sessions"
    echo ""
    echo "💡 For tomorrow's trading:"
    echo "   - Start this script by 9:25 AM"
    echo "   - System will capture 9:30-9:35 session levels"
    echo "   - Signals will generate when levels break"
    echo ""
    echo "📝 Server PID: $SERVER_PID (saved to .server_pid)"
    echo "📋 Server logs: tail -f signal_server.log"
    echo "🛑 To stop server: ./stop_trading.sh"
    
else
    echo "❌ Server failed to respond after multiple attempts"
    echo "📋 Check logs: tail -20 signal_server.log"
    kill $SERVER_PID 2>/dev/null
    rm -f .server_pid
    exit 1
fi