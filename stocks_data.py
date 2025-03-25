import os
import time
import json
import requests
import pandas as pd
import yfinance as yf
from ta import add_all_ta_features

# Configuration
HISTORICAL_YEARS = 5
API_DELAY = 1.2  # seconds between API calls
BASE_DIR = 'stock_data'
os.makedirs(BASE_DIR, exist_ok=True)

# Ask for user details
user_name = input("Enter your name: ").strip().lower()
api_key = input(f"Enter your API key for Twelve Data, {user_name}: ").strip()

# File paths
ticker_file = f"{user_name}_tickers.txt"
progress_file = f"jeevan_progress.json"
if not os.path.exists(progress_file):
    with open(progress_file, 'w') as f:
        json.dump({"completed": []}, f)

# Read tickers from file
if not os.path.exists(ticker_file):
    print(f"Ticker file '{ticker_file}' not found!")
    exit()

with open(ticker_file, 'r') as f:
    tickers = [line.strip() for line in f.readlines() if line.strip()]

# Format tickers for Twelve Data and Yahoo Finance
stock_mappings = [{"tw": f"{ticker}:NSE", "yf": f"{ticker}.NS"} for ticker in tickers]

# Load progress
def is_completed(symbol):
    """Checks if a stock symbol has already been processed."""
    with open(progress_file, 'r') as f:
        progress = json.load(f)
        return symbol in progress.get("completed", [])

def update_progress(symbol):
    """Adds a symbol to the progress file."""
    with open(progress_file, 'r+') as f:
        progress = json.load(f)
        progress["completed"].append(symbol)
        f.seek(0)
        json.dump(progress, f)
        f.truncate()

# API logging
def log_api_usage(api_name, endpoint):
    log_entry = {'timestamp': time.time(), 'api': api_name, 'endpoint': endpoint}
    with open(f'{user_name}_api_usage_log.json', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

# Function to get stock data
def get_historical_data(tw_symbol, yf_symbol):
    """Fetch stock data from Twelve Data or Yahoo Finance."""
    # Try Twelve Data
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={tw_symbol}&interval=1day&outputsize={HISTORICAL_YEARS*365}&apikey={api_key}"
        response = requests.get(url)
        log_api_usage('twelve_data', 'time_series')
        if response.status_code == 200:
            data = response.json()
            if 'values' in data:
                df = pd.DataFrame(data['values'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High', 'low': 'Low',
                    'close': 'Close', 'volume': 'Volume'
                })
                df = add_all_ta_features(df, open="Open", high="High", low="Low", close="Close", volume="Volume", fillna=True)
                return df
    except Exception as e:
        print(f"Twelve Data failed for {tw_symbol}: {str(e)}")

    # Fallback to yfinance
    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=f"{HISTORICAL_YEARS}y")
        df = add_all_ta_features(df, open="Open", high="High", low="Low", close="Close", volume="Volume", fillna=True)
        log_api_usage('yfinance', 'history')
        return df
    except Exception as e:
        print(f"yfinance fallback failed for {yf_symbol}: {str(e)}")
        return None

# Processing function
def process_stock(stock_info, finished_count, total_stocks):
    tw_symbol = stock_info["tw"]
    yf_symbol = stock_info["yf"]
    symbol_clean = tw_symbol.split(":")[0]  # Extract stock name

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

    # Save data
    df.to_csv(os.path.join(stock_dir, 'historical_data.csv'))
    
    finished_count += 1
    print(f"Finished processing {symbol_clean} ({finished_count}/{total_stocks})")

    # Update progress
    update_progress(symbol_clean)
    return finished_count

# Main script execution
def main():
    finished = 0
    total = len(stock_mappings)
    for stock in stock_mappings:
        finished = process_stock(stock, finished, total)
        time.sleep(API_DELAY)

if __name__ == "__main__":
    main()
