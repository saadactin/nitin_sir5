#!/bin/bash
# Script to run polling tests with mock API server

echo "=========================================="
echo "POLLING TEST RUNNER"
echo "=========================================="

# Check if mock API is running
if ! curl -s http://localhost:5001/api/health > /dev/null 2>&1; then
    echo "[INFO] Mock API server not running. Starting it..."
    python mock_incremental_api.py &
    MOCK_API_PID=$!
    sleep 3
    echo "[OK] Mock API server started (PID: $MOCK_API_PID)"
else
    echo "[OK] Mock API server is already running"
    MOCK_API_PID=""
fi

# Run tests
echo ""
echo "Running tests..."
echo "=========================================="
python test_polling_incremental_api.py

TEST_EXIT_CODE=$?

# Cleanup
if [ ! -z "$MOCK_API_PID" ]; then
    echo ""
    echo "Stopping mock API server (PID: $MOCK_API_PID)..."
    kill $MOCK_API_PID 2>/dev/null
    echo "[OK] Mock API server stopped"
fi

exit $TEST_EXIT_CODE

