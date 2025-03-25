import os
import time
import json
import requests
import pandas as pd
import yfinance as yf
from ta import add_all_ta_features

# Configuration for Indian stocks
TOP_STOCKS = [
    {"tw": "RELIANCE:NSE", "yf": "RELIANCE.NS"},
    {"tw": "TCS:NSE", "yf": "TCS.NS"},
    {"tw": "HDFCBANK:NSE", "yf": "HDFCBANK.NS"},
    {"tw": "INFY:NSE", "yf": "INFY.NS"},
    {"tw": "ICICIBANK:NSE", "yf": "ICICIBANK.NS"},
    {"tw": "HINDUNILVR:NSE", "yf": "HINDUNILVR.NS"},
    {"tw": "SBIN:NSE", "yf": "SBIN.NS"},
    {"tw": "KOTAKBANK:NSE", "yf": "KOTAKBANK.NS"},
    {"tw": "BAJFINANCE:NSE", "yf": "BAJFINANCE.NS"},
    {"tw": "ITC:NSE", "yf": "ITC.NS"}
]

HISTORICAL_YEARS = 5
API_DELAY = 1.2  # seconds between API calls

# API Key for Twelve Data
API_KEYS = {
    'twelve_data': 'a8fafdd224b94900a09762192bcccc93'
}

# File paths
BASE_DIR = 'stock_data'
os.makedirs(BASE_DIR, exist_ok=True)
PROGRESS_FILE = 'progress.json'

def initialize_progress():
    """Initializes progress file if it doesn't exist."""
    if not os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({"completed": []}, f)

def update_progress(symbol):
    """Adds a symbol to the progress file."""
    with open(PROGRESS_FILE, 'r+') as f:
        progress = json.load(f)
        progress["completed"].append(symbol)
        f.seek(0)
        json.dump(progress, f)
        f.truncate()

def is_completed(symbol):
    """Checks if a stock symbol has already been processed."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
            return symbol in progress.get("completed", [])
    return False

def log_api_usage(api_name, endpoint):
    log_entry = {'timestamp': time.time(), 'api': api_name, 'endpoint': endpoint}
    with open('api_usage_log.json', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

def get_historical_data(tw_symbol, yf_symbol):
    # Try Twelve Data
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={tw_symbol}&interval=1day&outputsize={HISTORICAL_YEARS*365}&apikey={API_KEYS['twelve_data']}"
        response = requests.get(url)
        log_api_usage('twelve_data', 'time_series')
        if response.status_code == 200:
            data = response.json()
            if 'values' in data:
                df = pd.DataFrame(data['values'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
                df = df.rename(columns={
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                })
                df = add_all_ta_features(
                    df,
                    open="Open",
                    high="High",
                    low="Low",
                    close="Close",
                    volume="Volume",
                    fillna=True
                )
                return df
    except Exception as e:
        print(f"Twelve Data failed for {tw_symbol}: {str(e)}")
    
    # Fallback to yfinance
    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=f"{HISTORICAL_YEARS}y")
        df = df.rename(columns={
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low',
            'Close': 'Close',
            'Volume': 'Volume'
        })
        df = add_all_ta_features(
            df,
            open="Open",
            high="High",
            low="Low",
            close="Close",
            volume="Volume",
            fillna=True
        )
        log_api_usage('yfinance', 'history')
        return df
    except Exception as e:
        print(f"yfinance fallback failed for {yf_symbol}: {str(e)}")
        return None

def process_stock(stock_info, finished_count, total_stocks):
    tw_symbol = stock_info["tw"]
    yf_symbol = stock_info["yf"]
    symbol_clean = tw_symbol.split(":")[0]  # e.g. RELIANCE from RELIANCE:NSE
    
    # Check if stock is already processed
    if is_completed(symbol_clean):
        print(f"Skipping {symbol_clean}, already processed.")
        return finished_count

    stock_dir = os.path.join(BASE_DIR, symbol_clean)
    os.makedirs(stock_dir, exist_ok=True)
    
    df = get_historical_data(tw_symbol, yf_symbol)
    if df is None:
        print(f"Failed to download data for {symbol_clean}")
        return finished_count

    # Define raw price columns
    price_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    
    # Save historical price data
    if set(price_cols).issubset(df.columns):
        historical_data = df[price_cols]
        historical_data.to_csv(os.path.join(stock_dir, 'historical_data.csv'))
    else:
        print(f"Price data columns missing for {symbol_clean}.")
    
    # Save technical indicators (all columns except price data)
    technical_cols = [col for col in df.columns if col not in price_cols]
    if technical_cols:
        technical_data = df[technical_cols]
        technical_data.to_csv(os.path.join(stock_dir, 'technical.csv'))
    else:
        print(f"No technical indicator columns found for {symbol_clean}.")

    finished_count += 1
    print(f"Finished processing {symbol_clean} ({finished_count}/{total_stocks})")
    
    # Log progress to a file
    with open('progress_log.txt', 'a') as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Finished processing {symbol_clean} ({finished_count}/{total_stocks})\n")
    
    # Update progress file
    update_progress(symbol_clean)
    return finished_count

def main():
    initialize_progress()
    finished = 0
    total = len(TOP_STOCKS)
    for stock in TOP_STOCKS:
        finished = process_stock(stock, finished, total)
        time.sleep(API_DELAY)

if __name__ == "__main__":
    main()
