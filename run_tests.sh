#!/bin/bash
# Test Runner Script
# Run all tests to verify project health

echo "=========================================="
echo "Project Health Check Test Suite"
echo "=========================================="
echo ""

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo "Error: Python not found"
    exit 1
fi

# Check if required packages are installed
echo "Checking dependencies..."
python -c "import requests" 2>/dev/null || {
    echo "Installing required packages..."
    pip install requests
}

# Run quick check first
echo ""
echo "Running Quick Health Check..."
echo "----------------------------------------"
python test_quick_check.py

# Run full test suite
echo ""
echo "Running Full Test Suite..."
echo "----------------------------------------"
python test_project_health.py

echo ""
echo "=========================================="
echo "Tests Complete"
echo "=========================================="

