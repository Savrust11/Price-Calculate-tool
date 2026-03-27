"""
Yahoo Auction Scraper - Web Application
========================================
Flask web app with real-time scraping status updates.
"""

import os
import json
import time
import threading
from pathlib import Path
from datetime import datetime
from queue import Queue

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template, request, jsonify, send_file, Response
from flask_cors import CORS
from werkzeug.utils import secure_filename
import pandas as pd

from yahoo_auction_scraper import read_input_products
from main import run_scraper, analyse_market_prices, apply_bid_decisions, write_output_excel

app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', 'uploads')
app.config['OUTPUT_FOLDER'] = os.environ.get('OUTPUT_FOLDER', 'outputs')
app.config['MAX_CONTENT_LENGTH'] = int(os.environ.get('MAX_UPLOAD_MB', 16)) * 1024 * 1024

# Create necessary folders
Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)
Path(app.config['OUTPUT_FOLDER']).mkdir(exist_ok=True)

# Global message queue for Server-Sent Events
message_queues = {}
scraping_status = {
    'running': False,
    'current_url': '',
    'progress': 0,
    'total': 0,
    'status': 'idle',
    'stop_requested': False
}


class ScrapingProgressLogger:
    """Custom logger that sends progress updates to the frontend."""
    
    def __init__(self, queue_id):
        self.queue_id = queue_id
        self.queue = message_queues.get(queue_id)
    
    def log(self, message, url='', progress=None, total=None):
        """Send a log message to the frontend."""
        global scraping_status
        
        data = {
            'message': message,
            'url': url,
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
        
        if progress is not None:
            data['progress'] = progress
            scraping_status['progress'] = progress
        
        if total is not None:
            data['total'] = total
            scraping_status['total'] = total
        
        scraping_status['current_url'] = url
        scraping_status['status'] = message
        
        if self.queue:
            self.queue.put(json.dumps(data))


def scrape_worker(input_files, output_file, queue_id, profit_margin=0.50, marketplace_fee=0.10, consumption_tax=0.10, priority_bids=None, grade_adjustments=None):
    """Background worker that runs the scraping process."""
    global scraping_status
    scraping_status['running'] = True
    scraping_status['stop_requested'] = False
    if priority_bids is None:
        priority_bids = {}
    if isinstance(input_files, str):
        input_files = [input_files]
    
    logger = ScrapingProgressLogger(queue_id)
    
    try:
        # Read and merge products from all files
        logger.log("Reading input file(s)...", progress=0, total=100)
        all_products = []
        for f in input_files:
            df = read_input_products(f)
            all_products.append(df)
            logger.log(f"  ✓ {os.path.basename(f)}: {len(df)} products")
        
        products_df = pd.concat(all_products, ignore_index=True)
        # Normalize Details to stripped strings for consistent matching throughout the pipeline
        if "Details" in products_df.columns:
            products_df["Details"] = (
                products_df["Details"]
                .astype(str)
                .str.replace("\u3000", " ", regex=False)
                .str.strip()
            )
            products_df = products_df[~products_df["Details"].isin(["nan", "None", ""])]
        total_products = len(products_df)
        
        logger.log(f"Found {total_products} products", progress=5, total=100)
        
        # Run scraper with custom progress tracking
        logger.log("Starting scraping process...", progress=10, total=100)
        
        # Modified run to send progress updates
        scraped_df, scraped_products = run_scraper_with_progress(
            products_df, 
            output_file,
            logger,
            total_products
        )
        
        # Check if stopped
        if scraping_status['stop_requested']:
            logger.log("⏸ Stopped by user. Processing collected data...", progress=70, total=100)
        
        # Process whatever data we collected
        if scraped_df is not None and not scraped_df.empty:
            logger.log(f"Analyzing market prices (Profit: {int(profit_margin*100)}%, Fee: {int(marketplace_fee*100)}%, Tax: {int(consumption_tax*100)}%)...", progress=70, total=100)
            market_df = analyse_market_prices(scraped_df)

            # Filter products_df to only include scraped products
            # Normalize Details to stripped strings to match scraped_products (which are str-stripped)
            products_df_filtered = products_df[products_df["Details"].astype(str).str.strip().isin(scraped_products)].copy()
            logger.log(f"Processing {len(products_df_filtered)} scraped products...", progress=80, total=100)
            
            logger.log("Calculating bid decisions...", progress=85, total=100)
            result_df = apply_bid_decisions(
                products_df_filtered,
                market_df,
                profit_margin=profit_margin,
                fees=marketplace_fee,
                tax=consumption_tax,
                priority_bids=priority_bids,
                grade_adjustments=grade_adjustments
            )
            
            logger.log("Generating Excel output...", progress=95, total=100)
            final_output = os.path.join(
                app.config['OUTPUT_FOLDER'],
                f"bid_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            write_output_excel(result_df, final_output)
            
            if scraping_status['stop_requested']:
                logger.log("✓ Partial data saved successfully!", progress=100, total=100)
            else:
                logger.log("✓ Scraping completed successfully!", progress=100, total=100)
            
            logger.log(f"DOWNLOAD_READY:{final_output}", progress=100, total=100)
        else:
            logger.log("⚠ No data collected", progress=100, total=100)
        
    except Exception as e:
        logger.log(f"✗ Error: {str(e)}", progress=0, total=100)
    
    finally:
        scraping_status['running'] = False
        scraping_status['stop_requested'] = False
        time.sleep(2)
        if queue_id in message_queues:
            message_queues[queue_id].put("DONE")


def run_scraper_with_progress(products_df, output_file, logger, total_products):
    """Run scraper with progress updates.
    
    Args:
        products_df: DataFrame of products to scrape (already merged from all files)
    
    Returns:
        tuple: (scraped_df, scraped_products) - scraped data and list of product names scraped
    """
    import requests
    from yahoo_auction_scraper import scrape_product, _get_out_cols
    
    has_box = "BoxNo" in products_df.columns
    out_cols = _get_out_cols(has_box)
    
    all_rows = []
    session = requests.Session()
    products_scraped = 0
    scraped_products = []  # Track which products were scraped
    
    for idx, (_, row) in enumerate(products_df.iterrows(), start=1):
        # Check if stop was requested
        if scraping_status['stop_requested']:
            logger.log(f"⏹ Stop requested. Scraped {products_scraped} products so far.", progress=70, total=100)
            break
            
        keyword = str(row["Details"]).strip()
        brand = str(row.get("Brand", "")).strip() if "Brand" in products_df.columns else ""
        rank = str(row.get("Rank", "")).strip() if "Rank" in products_df.columns else ""
        box_no = row.get("BoxNo", "") if has_box else ""
        branch_no = row.get("BranchNo", "") if has_box else ""
        
        if not keyword:
            continue
        
        # Build search URL for display
        from yahoo_auction_scraper import CLOSED_SEARCH_URL
        from urllib.parse import urlencode
        search_url = f"{CLOSED_SEARCH_URL}?{urlencode({'p': keyword})}"
        
        logger.log(
            f"Scraping product {products_scraped + 1}/{total_products}: {keyword[:50]}...",
            url=search_url,
            progress=round(10 + (products_scraped / total_products) * 60, 1),
            total=100
        )

        listings = scrape_product(keyword, session)

        for item in listings:
            item["Product"] = keyword
            item["Brand"] = brand
            item["Rank"] = rank
            if has_box:
                item["BoxNo"] = box_no
                item["BranchNo"] = branch_no

        all_rows.extend(listings)
        scraped_products.append(keyword)  # Track this product
        products_scraped += 1

        # Update progress after scraping this product
        progress_after = round(10 + (products_scraped / total_products) * 60, 1)
        logger.log(
            f"✓ Done {products_scraped}/{total_products}",
            progress=progress_after,
            total=100
        )
        
        # Save progress periodically
        if idx % 10 == 0:
            df = pd.DataFrame(all_rows, columns=out_cols)
            df.to_excel(output_file, index=False, engine="openpyxl")
    
    # Final save
    df = pd.DataFrame(all_rows, columns=out_cols)
    df.to_excel(output_file, index=False, engine="openpyxl")
    
    return df, scraped_products


@app.route('/upload_priority', methods=['POST'])
def upload_priority_file():
    """Handle bulk priority bids file upload (CSV or Excel).
    
    Expected format: two columns — keyword (model/series) and amount (bid amount).
    Column names are flexible: first column = keyword, second column = amount.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    
    if not file.filename or file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
        return jsonify({'error': 'Please upload a CSV or Excel file'}), 400
    
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"priority_{filename}")
    file.save(filepath)
    
    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath, engine='openpyxl')
        
        if len(df.columns) < 2:
            return jsonify({'error': 'File must have at least 2 columns (keyword, amount)'}), 400
        
        # Use first two columns regardless of header names
        keywords_col = df.columns[0]
        amounts_col = df.columns[1]
        
        priority_bids = []
        for _, row in df.iterrows():
            keyword = str(row[keywords_col]).strip()
            try:
                amount = int(float(row[amounts_col]))
            except (ValueError, TypeError):
                continue
            if keyword and keyword.lower() != 'nan' and amount > 0:
                priority_bids.append({'keyword': keyword, 'amount': amount})
        
        return jsonify({
            'success': True,
            'priority_bids': priority_bids,
            'count': len(priority_bids)
        })
    except Exception as e:
        return jsonify({'error': f'Failed to parse file: {str(e)}'}), 400
    finally:
        # Clean up temp file
        try:
            os.remove(filepath)
        except OSError:
            pass


@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload (supports multiple files)."""
    if 'files' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    files = request.files.getlist('files')
    
    if not files or all(f.filename == '' for f in files):
        return jsonify({'error': 'No file selected'}), 400
    
    saved_filepaths = []
    saved_filenames = []
    
    for file in files:
        if not file.filename or file.filename == '':
            continue
        if not file.filename.endswith(('.xlsx', '.xls')):
            return jsonify({'error': f'Invalid file: {file.filename}. Please upload Excel files (.xlsx or .xls)'}), 400
        
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        saved_filepaths.append(filepath)
        saved_filenames.append(file.filename)
    
    if not saved_filepaths:
        return jsonify({'error': 'No valid files uploaded'}), 400
    
    return jsonify({
        'success': True,
        'filenames': saved_filenames,
        'filepaths': saved_filepaths
    })


@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    """Start the scraping process."""
    global scraping_status
    
    if scraping_status['running']:
        return jsonify({'error': 'Scraping already in progress'}), 400
    
    data = request.json
    input_files = data.get('filepaths', [])
    
    # Backward compat: single filepath
    if not input_files:
        single = data.get('filepath')
        if single:
            input_files = [single]
    
    if not input_files or not all(os.path.exists(f) for f in input_files):
        return jsonify({'error': 'Input file(s) not found'}), 400
    
    # Get customizable settings from request
    profit_margin = data.get('profit_margin', 0.50)
    marketplace_fee = data.get('marketplace_fee', 0.10)
    consumption_tax = data.get('consumption_tax', 0.10)
    
    # Get priority bids list and convert to dict
    priority_bids_list = data.get('priority_bids', [])
    priority_bids = {}
    for item in priority_bids_list:
        keyword = item.get('keyword', '').strip()
        amount = item.get('amount', 0)
        grades = item.get('grades', [])  # [] = all grades
        if keyword and amount > 0:
            priority_bids[keyword] = {'amount': int(amount), 'grades': grades}

    # Get grade adjustments (UI overrides); fall back to config defaults
    import config as _config
    raw_grades = data.get('grade_adjustments', {})
    grade_adjustments = {
        'S': float(raw_grades.get('S', _config.GRADE_ADJUSTMENTS.get('S', 1.10))),
        'A': float(raw_grades.get('A', _config.GRADE_ADJUSTMENTS.get('A', 1.00))),
        'B': float(raw_grades.get('B', _config.GRADE_ADJUSTMENTS.get('B', 0.90))),
        'C': float(raw_grades.get('C', _config.GRADE_ADJUSTMENTS.get('C', 0.80))),
        'J': float(raw_grades.get('J', _config.GRADE_ADJUSTMENTS.get('J', 0.50))),
        'Ｊ': float(raw_grades.get('J', _config.GRADE_ADJUSTMENTS.get('J', 0.50))),  # fullwidth J
    }

    # Validate settings
    if not (0 <= profit_margin <= 1):
        return jsonify({'error': 'Invalid profit margin'}), 400
    if not (0 <= marketplace_fee <= 1):
        return jsonify({'error': 'Invalid marketplace fee'}), 400
    if not (0 <= consumption_tax <= 1):
        return jsonify({'error': 'Invalid consumption tax'}), 400
    
    # Create a unique queue ID for this scraping session
    queue_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    message_queues[queue_id] = Queue()
    
    # Output file
    output_file = os.path.join(
        app.config['OUTPUT_FOLDER'],
        f"scraped_data_{queue_id}.xlsx"
    )
    
    # Start scraping in background thread
    thread = threading.Thread(
        target=scrape_worker,
        args=(input_files, output_file, queue_id, profit_margin, marketplace_fee, consumption_tax, priority_bids, grade_adjustments)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'success': True,
        'queue_id': queue_id,
        'message': 'Scraping started'
    })


@app.route('/status')
def get_status():
    """Get current scraping status."""
    return jsonify(scraping_status)


@app.route('/stop', methods=['POST'])
def stop_scraping():
    """Stop the scraping process gracefully."""
    global scraping_status
    
    if not scraping_status['running']:
        return jsonify({'error': 'No scraping in progress'}), 400
    
    scraping_status['stop_requested'] = True
    
    return jsonify({
        'success': True,
        'message': 'Stop requested. Finishing current product and saving data...'
    })


@app.route('/stream/<queue_id>')
def stream(queue_id):
    """Server-Sent Events stream for real-time updates."""
    
    def event_stream():
        if queue_id not in message_queues:
            yield f"data: {json.dumps({'error': 'Invalid queue ID'})}\n\n"
            return
        
        queue = message_queues[queue_id]
        
        while True:
            try:
                message = queue.get(timeout=30)
                if message == "DONE":
                    break
                yield f"data: {message}\n\n"
            except:
                # Send keepalive
                yield f"data: {json.dumps({'keepalive': True})}\n\n"
    
    response = Response(event_stream(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return response


@app.route('/download/<filename>')
def download_file(filename):
    """Download the output file."""
    filepath = os.path.join(app.config['OUTPUT_FOLDER'], os.path.basename(filename))
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(
        filepath,
        as_attachment=True,
        download_name=os.path.basename(filename)
    )


@app.route('/open_folder', methods=['POST'])
def open_output_folder():
    """List output files available for download."""
    folder_path = os.path.abspath(app.config['OUTPUT_FOLDER'])

    try:
        files = []
        for f in sorted(Path(folder_path).glob('bid_results_*.xlsx'), reverse=True):
            files.append({
                'name': f.name,
                'size_kb': round(f.stat().st_size / 1024, 1),
                'modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })
        return jsonify({'success': True, 'files': files, 'path': folder_path})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('FLASK_PORT', os.environ.get('PORT', 5001)))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() in ('true', '1', 'yes')
    app.run(debug=debug, host='0.0.0.0', port=port, threaded=True)
