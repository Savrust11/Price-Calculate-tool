# Yahoo Auction Scraper - Web Application

A modern web interface for scraping Yahoo Auctions Japan sold listings with real-time progress tracking.

## Features

✨ **Modern Web Interface**
- Drag & drop file upload
- Real-time scraping progress
- Live URL tracking showing which website is currently being scraped
- Beautiful gradient design with smooth animations

🚀 **Easy to Use**
1. Upload your Excel file (.xlsx or .xls)
2. Click "Start Scraping" button  
3. Watch real-time progress as each product is scraped
4. Download the results when complete

📊 **Real-Time Status**
- Progress bar showing completion percentage
- Live feed of currently scraping URLs
- Status messages with timestamps
- Automatic download when finished

## Installation

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Running the Web Application

### Option 1: Using the start script (Recommended)
```bash
chmod +x start_web_app.sh
./start_web_app.sh
```

### Option 2: Direct Python command
```bash
python app.py
```

### Option 3: With virtual environment
```bash
source .venv/bin/activate
python app.py
```

## Accessing the Web Interface

Once the server is running, open your browser and navigate to:
```
http://localhost:5001
```

Or from another device on the same network:
```
http://YOUR_IP_ADDRESS:5001
```

## Usage

1. **Upload File**
   - Click the upload area or drag & drop your Excel file
   - Supported formats: .xlsx, .xls
   - File should contain product information

2. **Start Scraping**
   - Click the "Start Scraping" button
   - The scraper will begin processing each product
   - Watch the real-time progress updates

3. **Monitor Progress**
   - Progress bar shows overall completion
   - Status feed shows current scraping URL
   - Timestamps for each action
   - Live updates as the scraper works

4. **Download Results**
   - When complete, the download button appears automatically
   - Click to download the Excel file with bid results
   - File includes scraped data, market analysis, and bid decisions

## File Structure

```
Yahoo-extraction/
├── app.py                    # Flask web application
├── templates/
│   └── index.html           # Modern web interface
├── uploads/                 # Uploaded Excel files (auto-created)
├── outputs/                 # Generated result files (auto-created)
├── yahoo_auction_scraper.py # Core scraping logic
├── main.py                  # Pipeline orchestration
├── market_analysis.py       # Market price analysis
├── bid_calculator.py        # Bid decision logic
└── excel_output.py          # Excel formatting
```

## Technical Details

- **Backend**: Flask with Server-Sent Events (SSE) for real-time updates
- **Frontend**: Pure HTML/CSS/JavaScript (no frameworks required)
- **Real-time Updates**: Server-Sent Events stream
- **Multi-threading**: Background worker for scraping process
- **File Upload**: Secure file handling with werkzeug

## Troubleshooting

**Port already in use:**
```bash
# Change the port in app.py (last line):
app.run(debug=True, host='0.0.0.0', port=5002)  # Use different port
```

Note: Port 5000 is used by AirPlay Receiver on macOS, so we use port 5001 by default.

**Can't access from other devices:**
- Make sure firewall allows port 5001
- Use `0.0.0.0` as host (already configured)
- Find your IP: `ifconfig` (Mac/Linux) or `ipconfig` (Windows)

**Upload fails:**
- Check file format (.xlsx or .xls only)
- Maximum file size: 16MB
- Ensure uploads/ directory has write permissions

## Command Line Version

The original command-line version is still available:
```bash
python main.py --input your_file.xlsx
```

## License

Proprietary - For authorized use only
