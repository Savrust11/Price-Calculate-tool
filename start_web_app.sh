#!/bin/bash
# Yahoo Auction Scraper - Web Application Launcher
# This script starts the Flask web server

echo "🚀 Starting Yahoo Auction Scraper Web Application..."
echo "📍 Server will be available at: http://localhost:5001"
echo ""
echo "Press Ctrl+C to stop the server"
echo "================================================"
echo ""

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Run the Flask application
python app.py
