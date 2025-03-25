import os
import time
import json
import requests
import pandas as pd
import yfinance as yf
from ta import add_all_ta_features

# Configuration
TOP_STOCKS = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'META', 'GOOGL', 'GOOG', 'TSLA', 'BRK-B', 'JPM']
HISTORICAL_YEARS = 5
API_DELAY = 1.2  # Seconds between API calls

# API Keys
API_KEYS = {
    'twelve_data': 'a8fafdd224b94900a09762192bcccc93',
    'finnhub': 'cvh31uhr01qi76d68rv0cvh31uhr01qi76d68rvg',
    'fmp': 'daAb1P5GdjsrSsw52brVyKzLtFylIaPQ',
    'newsapi': 'd419d653d7834787b58583906ace65e0'
}

# File paths
BASE_DIR = 'stock_data'
os.makedirs(BASE_DIR, exist_ok=True)

def initialize_progress():
    if not os.path.exists('progress.json'):
        with open('progress.json', 'w') as f:
            json.dump({'completed': [], 'current_stock': None}, f)

def log_api_usage(api_name, endpoint):
    log_entry = {'timestamp': time.time(), 'api': api_name, 'endpoint': endpoint}
    with open('api_usage_log.json', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

def get_historical_data(symbol):
    try:
        # Try Twelve Data
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize={HISTORICAL_YEARS*365}&apikey={API_KEYS['twelve_data']}"
        response = requests.get(url)
        log_api_usage('twelve_data', 'time_series')
        
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data['values'])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
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
        print(f"Twelve Data failed: {str(e)}")
    
    # Fallback to yfinance
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=f"{HISTORICAL_YEARS}y")
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
        print(f"yFinance fallback failed: {str(e)}")
        return None

def get_fundamental_data(symbol):
    try:
        # Try Finnhub
        url = f"https://finnhub.io/api/v1/stock/financials?symbol={symbol}&token={API_KEYS['finnhub']}"
        response = requests.get(url)
        log_api_usage('finnhub', 'financials')
        
        if response.status_code == 200:
            data = response.json()
            # Convert to pandas DataFrame and save
            income_df = pd.DataFrame(data['income_statement'])
            balance_df = pd.DataFrame(data['balance_sheet'])
            cashflow_df = pd.DataFrame(data['cash_flow'])
            
            return {
                'income': income_df,
                'balance': balance_df,
                'cashflow': cashflow_df
            }
    except Exception as e:
        print(f"Finnhub failed: {str(e)}")
    
    try:
        # Fallback to Financial Modeling Prep (FMP)
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?apikey={API_KEYS['fmp']}"
        response = requests.get(url)
        log_api_usage('fmp', 'income_statement')
        
        if response.status_code == 200:
            income_data = response.json()
            income_df = pd.DataFrame(income_data)
            
            url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?apikey={API_KEYS['fmp']}"
            response = requests.get(url)
            log_api_usage('fmp', 'balance_sheet')
            
            if response.status_code == 200:
                balance_data = response.json()
                balance_df = pd.DataFrame(balance_data)
                
                url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?apikey={API_KEYS['fmp']}"
                response = requests.get(url)
                log_api_usage('fmp', 'cash_flow')
                
                if response.status_code == 200:
                    cashflow_data = response.json()
                    cashflow_df = pd.DataFrame(cashflow_data)
                    
                    return {
                        'income': income_df,
                        'balance': balance_df,
                        'cashflow': cashflow_df
                    }
    except Exception as e:
        print(f"FMP fallback failed: {str(e)}")
    
    try:
        # Final fallback to yfinance
        ticker = yf.Ticker(symbol)
        info = ticker.info
        # Note: yfinance does not provide detailed financial statements like FMP or Finnhub
        #       You might need to manually extract relevant info from ticker.info
        print("yfinance fallback for financials not fully implemented.")
        return None
    except Exception as e:
        print(f"yFinance fallback failed: {str(e)}")
        return None

def get_news_data(symbol):
    try:
        # Try NewsAPI
        url = f"https://newsapi.org/v2/everything?q={symbol}&apiKey={API_KEYS['newsapi']}"
        response = requests.get(url)
        log_api_usage('newsapi', 'everything')
        
        if response.status_code == 200:
            data = response.json()
            articles = data['articles']
            df = pd.DataFrame(articles)
            return df
    except Exception as e:
        print(f"NewsAPI failed: {str(e)}")
    
    try:
        # Fallback to FMP news endpoint (if available)
        # Note: FMP does not provide a free news endpoint.
        #       You might need to use another free news API or implement a paid service.
        print("FMP news fallback not implemented.")
        return None
    except Exception as e:
        print(f"FMP news fallback failed: {str(e)}")
        return None

def process_stock(symbol):
    stock_dir = os.path.join(BASE_DIR, symbol)
    os.makedirs(stock_dir, exist_ok=True)
    
    # Historical data with technical indicators
    hist_data = get_historical_data(symbol)
    if hist_data is not None:
        hist_data.to_csv(os.path.join(stock_dir, 'historical.csv'))
    
    # Fundamental data
    fundamental_data = get_fundamental_data(symbol)
    if fundamental_data:
        for statement_type, data in fundamental_data.items():
            data.to_csv(os.path.join(stock_dir, f'{statement_type}.csv'))
    
    # News data
    news_data = get_news_data(symbol)
    if news_data is not None:
        news_data.to_csv(os.path.join(stock_dir, 'news.csv'))

def main():
    initialize_progress()
    
    with open('progress.json', 'r+') as f:
        progress = json.load(f)
        
        for symbol in TOP_STOCKS:
            if symbol not in progress['completed']:
                progress['current_stock'] = symbol
                f.seek(0)
                json.dump(progress, f)
                f.truncate()
                
                process_stock(symbol)
                progress['completed'].append(symbol)
                time.sleep(API_DELAY)

if __name__ == "__main__":
    main()
