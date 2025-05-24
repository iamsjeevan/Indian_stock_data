import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path("/workspaces/Indian_stock_data/stock_data")
CLEANED_DATA_SUBFOLDER = "cleaned_data"

def parse_balance_sheet_date(date_str):
    try:
        month_str, year_short_str = date_str.split(" ")
        year_prefix = "20"
        year = int(f"{year_prefix}{year_short_str.strip()}")
        month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                     'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
        return pd.Timestamp(year=year, month=month_map[month_str.strip()], day=1) + pd.offsets.MonthEnd(0)
    except ValueError:
        return pd.NaT
    except Exception:
        return pd.NaT

def parse_quarterly_date(date_str):
    try:
        dt_obj = datetime.strptime(date_str.strip(), "%b '%y")
        return pd.Timestamp(dt_obj) + pd.offsets.MonthEnd(0)
    except ValueError:
        return pd.NaT
    except Exception:
        return pd.NaT

def load_and_clean_financial_data(file_path, date_parser_func):
    """Loads and cleans balance sheet or quarterly financial data."""
    try:
        df = pd.read_csv(file_path, low_memory=False)
        if 'Item' not in df.columns:
            print(f"Warning: 'Item' column not found in {file_path}. Skipping.")
            return None

        df = df.set_index('Item')
        df = df.replace(['--', ''], np.nan)

        df = df.T
        
        parsed_dates_index = []
        for date_str in df.index:
            parsed_date = date_parser_func(str(date_str))
            parsed_dates_index.append(parsed_date)
        
        df.index = pd.DatetimeIndex(parsed_dates_index)
        df.index.name = 'Date'
        
        df = df[df.index.notna()]
        df = df.sort_index()

        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.dropna(how='all', axis=1) # Drop columns (original items) if all values are NaN
        df = df.dropna(how='all', axis=0) # Drop rows (dates) if all values for that date are NaN
        df = df.fillna(0.0)               # Fill remaining NaNs in financial items with 0.0

        return df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def load_and_clean_historical_data(file_path):
    """Loads and cleans historical price data."""
    try:
        df = pd.read_csv(file_path, low_memory=False)
        if 'Date' not in df.columns:
            print(f"Warning: 'Date' column not found in {file_path}. Skipping.")
            return None

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date'])
        df = df.set_index('Date')
        df = df.sort_index()

        cols_to_convert = [col for col in df.columns if col != 'Date']
        for col in cols_to_convert:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.ffill()
        # df = df.bfill() # Optional
        return df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# To process all stocks, we remove or comment out max_stocks_to_process
# processed_stocks_count = 0
# max_stocks_to_process = 5 # For testing, limit the number of stocks

total_stocks_processed = 0
failed_stocks = []

for stock_dir in BASE_DIR.iterdir():
    # if processed_stocks_count >= max_stocks_to_process:
    #     print(f"Reached max stocks to process: {max_stocks_to_process}. Stopping.")
    #     break

    if stock_dir.is_dir():
        stock_name = stock_dir.name
        print(f"\nProcessing stock: {stock_name}")
        
        output_dir_for_stock = stock_dir / CLEANED_DATA_SUBFOLDER
        output_dir_for_stock.mkdir(parents=True, exist_ok=True)
        
        stock_has_errors = False

        # --- Process and Save Balance Sheet ---
        balance_sheet_file = stock_dir / f"{stock_name}_balance_VI_merged_financials.csv"
        if balance_sheet_file.exists():
            print(f"  Loading balance sheet: {balance_sheet_file.name}")
            df_balance = load_and_clean_financial_data(balance_sheet_file, parse_balance_sheet_date)
            if df_balance is not None and not df_balance.empty:
                save_path = output_dir_for_stock / f"{stock_name}_balance_sheet_cleaned.csv"
                try:
                    df_balance.to_csv(save_path)
                    print(f"    Saved cleaned balance sheet to: {save_path}")
                except Exception as e:
                    print(f"    ERROR saving cleaned balance sheet for {stock_name}: {e}")
                    stock_has_errors = True
            elif df_balance is None: # Error during loading/cleaning
                stock_has_errors = True
            elif df_balance.empty:
                 print(f"    Cleaned balance sheet for {stock_name} is empty. Not saving.")
        else:
            print(f"  Balance sheet not found for {stock_name}")

        # --- Process and Save Quarterly Financials ---
        quarterly_file = stock_dir / f"{stock_name}_quarterly_merged_financials.csv"
        if quarterly_file.exists():
            print(f"  Loading quarterly financials: {quarterly_file.name}")
            df_quarterly = load_and_clean_financial_data(quarterly_file, parse_quarterly_date)
            if df_quarterly is not None and not df_quarterly.empty:
                save_path = output_dir_for_stock / f"{stock_name}_quarterly_cleaned.csv"
                try:
                    df_quarterly.to_csv(save_path)
                    print(f"    Saved cleaned quarterly financials to: {save_path}")
                except Exception as e:
                    print(f"    ERROR saving cleaned quarterly financials for {stock_name}: {e}")
                    stock_has_errors = True
            elif df_quarterly is None: # Error during loading/cleaning
                stock_has_errors = True
            elif df_quarterly.empty:
                 print(f"    Cleaned quarterly financials for {stock_name} is empty. Not saving.")
        else:
            print(f"  Quarterly financials not found for {stock_name}")

        # --- Process and Save Historical Data ---
        historical_file = stock_dir / "historical_data.csv"
        if historical_file.exists():
            print(f"  Loading historical data: {historical_file.name}")
            df_historical = load_and_clean_historical_data(historical_file)
            if df_historical is not None and not df_historical.empty:
                save_path = output_dir_for_stock / f"{stock_name}_historical_cleaned.csv"
                try:
                    df_historical.to_csv(save_path)
                    print(f"    Saved cleaned historical data to: {save_path}")
                except Exception as e:
                    print(f"    ERROR saving cleaned historical data for {stock_name}: {e}")
                    stock_has_errors = True
            elif df_historical is None: # Error during loading/cleaning
                stock_has_errors = True
            elif df_historical.empty:
                 print(f"    Cleaned historical data for {stock_name} is empty. Not saving.")
        else:
            print(f"  Historical data not found for {stock_name}")
        
        # processed_stocks_count += 1 # If using max_stocks_to_process
        total_stocks_processed +=1
        if stock_has_errors:
            failed_stocks.append(stock_name)


print(f"\n--- Processing Summary ---")
print(f"Total stock directories scanned and attempted: {total_stocks_processed}")
if failed_stocks:
    print(f"Number of stocks with one or more errors during processing/saving: {len(failed_stocks)}")
    print(f"Stocks with errors: {', '.join(failed_stocks)}")
else:
    print("All processed stocks completed without reported errors during saving cleaned files.")
print(f"Cleaned files (if any) are saved in a '{CLEANED_DATA_SUBFOLDER}' subfolder within each processed stock's directory.")