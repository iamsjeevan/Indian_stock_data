import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
from urllib.parse import urlparse, parse_qs
# import random # No longer needed if not using random proxy selection from a list

# --- Configurations ---
BASE_URL_FINANCIALS = "https://www.moneycontrol.com/stocks/company_info/print_financials.php"
BASE_URL_SUGGESTION_API = "https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php"
# PROXY_API_URL = "https://freeapiproxies.azurewebsites.net/proxyapi" # Not used
REPORT_TYPE = "balance_VI"
MAX_PAGES_PER_STOCK = 10
REQUEST_DELAY = 2        # Base delay for financial page navigation
API_REQUEST_DELAY = 1    # Base delay for suggestion API calls
# PROXY_REQUEST_TIMEOUT = 20 # Not used

HEADERS_FINANCIALS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.6", "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Sec-Fetch-Dest": "iframe", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1", "Sec-GPC": "1", "Upgrade-Insecure-Requests": "1",
}
HEADERS_API = {
    'accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.7', 'priority': 'u=1, i',
    'referer': 'https://www.moneycontrol.com/',
    'sec-ch-ua': '"Chromium";v="136", "Brave";v="136", "Not.A/Brand";v="99"', 'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin', 'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}
# PROXY_API_HEADERS not needed

# --- REMOVED PROXY RELATED FUNCTIONS ---
# def get_random_proxy_from_api(session): ...

# Helper functions (clean_value, get_form_params, parse_financial_table_from_soup)
# These remain IDENTICAL to your last working version.
def clean_value(value_str):
    if value_str is None: return None
    text = value_str.strip()
    if text == '-' or text == '': return None
    try: return float(text.replace(',', ''))
    except ValueError: return text

def get_form_params(soup):
    form = soup.find('form', {'name': 'finyear_frm'})
    if not form: return None
    params = {}
    for inp in form.find_all('input', {'type': 'hidden'}): params[inp.get('name')] = inp.get('value')
    return params

def parse_financial_table_from_soup(soup, company_name, page_num_for_debug=""):
    all_candidate_tables = soup.find_all('table', class_='table4', width="100%", cellspacing="0", cellpadding="0", bgcolor="#ffffff")
    financial_table = None
    for idx, candidate_table in enumerate(all_candidate_tables):
        first_few_rows = candidate_table.find_all('tr', limit=5)
        for tr_candidate in first_few_rows:
            tds = tr_candidate.find_all('td')
            if len(tds) > 1 and "detb" in tds[0].get('class', []) and \
               tds[1].text.strip().startswith("Mar ") and "detb" in tds[1].get('class', []):
                financial_table = candidate_table; break
        if financial_table: break
    if not financial_table: return [], []
    rows = financial_table.find_all('tr')
    table_data, column_headers, header_found_flag = [], [], False
    for i, row in enumerate(rows):
        cols = row.find_all('td')
        if not cols: continue
        if not header_found_flag and len(cols) > 1 and "detb" in cols[0].get('class', []) and cols[1].text.strip().startswith("Mar "):
            column_headers = ["Item"] + [col.text.strip() for col in cols[1:]]; header_found_flag = True; continue
        if not header_found_flag: continue
        if len(cols) > 1 and "12 mths" in cols[1].text.strip() and ("det" in cols[1].get('class', []) or "detb" in cols[1].get('class', [])): continue
        if len(cols) == 1 and cols[0].get('colspan') and cols[0].find('img'): continue
        first_col_classes = cols[0].get('class', [])
        item_name = cols[0].text.replace('\xa0', ' ').strip()
        if "Source :" in item_name: break
        if not item_name and not any(c.text.strip() for c in cols[1:] if c.text is not None): continue
        is_section_header = "detb" in first_col_classes and (cols[0].get('colspan') == '2' or len(cols) < len(column_headers) -1 if column_headers else True)
        if is_section_header and item_name:
            all_other_cells_empty = True
            if len(cols) > 1 and column_headers:
                if len(cols[1:]) < len(column_headers[1:]): pass
                else:
                    for k_idx, _ in enumerate(column_headers[1:]):
                        if k_idx < len(cols[1:]) and cols[k_idx+1].text.strip() != "": all_other_cells_empty = False; break
            if all_other_cells_empty:
                rd = {"Item": item_name}; table_data.append(rd); continue
        if item_name and column_headers:
            if len(cols[1:]) == len(column_headers[1:]):
                vals = [clean_value(c.text) for c in cols[1:]]; rd = {"Item": item_name}
                for k, v in zip(column_headers[1:], vals): rd[k] = v
                table_data.append(rd)
            elif item_name and len(cols) <= 2 and "detb" in first_col_classes:
                rd = {"Item": item_name}; table_data.append(rd)
    return table_data, column_headers

# --- MODIFIED Function to get sc_id (no proxy argument) ---
def get_moneycontrol_sc_did_from_api(ticker_query, session): # Removed current_proxies
    params = {'classic': 'true', 'query': ticker_query, 'type': '1', 'format': 'json', 'callback': 'suggest1'}
    sc_id_to_use = None
    try:
        print(f"   API Call: Searching for sc_id for '{ticker_query}'...")
        response = session.get(BASE_URL_SUGGESTION_API, params=params, headers=HEADERS_API, 
                               timeout=10) # Normal timeout
        response.raise_for_status()
        response_text = response.text
        if response_text.startswith('suggest1(') and response_text.endswith(')'):
            json_str = response_text[len('suggest1('):-1]
            suggestions = json.loads(json_str)
            if suggestions and isinstance(suggestions, list) and len(suggestions) > 0:
                for sug in suggestions: 
                    if 'link_src' in sug and sug['link_src']:
                        path_segments = sug['link_src'].strip('/').split('/')
                        if path_segments:
                            potential_id = path_segments[-1]
                            if potential_id and potential_id.isalnum():
                                if 'pdt_dis_nm' in sug and ticker_query.upper() in sug['pdt_dis_nm'].upper().replace("&NBSP;", " "):
                                    sc_id_to_use = potential_id; print(f"   API Success (link_src, relevant): ID '{sc_id_to_use}' for '{ticker_query}'."); return sc_id_to_use
                                elif not sc_id_to_use: sc_id_to_use = potential_id
                if not sc_id_to_use: 
                    for sug in suggestions:
                        if 'sc_id' in sug and sug['sc_id']:
                            potential_id = sug['sc_id']
                            if 'pdt_dis_nm' in sug and ticker_query.upper() in sug['pdt_dis_nm'].upper().replace("&NBSP;", " "):
                                sc_id_to_use = potential_id; break 
                            elif not sc_id_to_use: sc_id_to_use = potential_id
                if not sc_id_to_use: 
                    for sug in suggestions:
                        if 'link_track' in sug and sug['link_track']:
                            query_params = parse_qs(urlparse(sug['link_track']).query)
                            if 'id' in query_params and query_params['id'] and query_params['id'][0]:
                                sc_id_to_use = query_params['id'][0]; break 
                if sc_id_to_use: print(f"   API Final Choice: Using ID '{sc_id_to_use}' for '{ticker_query}'."); return sc_id_to_use
                print(f"   API Warning: No suitable ID for '{ticker_query}'. Suggestions: {suggestions[:1]}")
            else: print(f"   API Warning: No suggestions for '{ticker_query}'.")
        else: print(f"   API Error: Unexpected response format for '{ticker_query}'.")
    except requests.exceptions.Timeout: print(f"   API Request Timeout for '{ticker_query}'.")
    except requests.RequestException as e: print(f"   API Request Error for '{ticker_query}': {e}")
    except json.JSONDecodeError as e: print(f"   API JSON Decode Error for '{ticker_query}': {e}.")
    return None


# --- MODIFIED Main Scraping and Merging Function (no proxy argument) ---
def scrape_and_save_financials_for_stock(sc_id_to_scrape, original_ticker_name, output_folder_path, session): # Removed current_proxies
    print(f"\n--- Processing Financials for: {original_ticker_name} (using sc_id: {sc_id_to_scrape}) ---")
    all_pages_data_dictionaries = []
    master_year_headers = set()
    session.headers.update(HEADERS_FINANCIALS)
    initial_params_get = {'sc_did': sc_id_to_scrape, 'type': REPORT_TYPE}
    current_page_html, response_obj_for_referer = None, None
    request_successful = True 

    try:
        print(f"   Fetching initial financial page...")
        response = session.get(BASE_URL_FINANCIALS, params=initial_params_get, 
                               timeout=25) # Normal timeout
                               # proxies=current_proxies REMOVED
        response.raise_for_status(); current_page_html = response.text; response_obj_for_referer = response
    except requests.exceptions.HTTPError as http_err:
        if response and response.status_code == 404: print(f"   Error 404: Financials page not found for {original_ticker_name} (sc_id: {sc_id_to_scrape}).");
        else: print(f"   HTTP error initial financials for {original_ticker_name}: {http_err}");
        return False 
    except requests.exceptions.Timeout:
        print(f"   Timeout fetching initial financials for {original_ticker_name}.")
        return False 
    except requests.RequestException as e: print(f"   Request error initial financials for {original_ticker_name}: {e}"); return False
    if not current_page_html: print(f"   Failed initial financials for {original_ticker_name}."); return False

    page_count = 1
    while page_count <= MAX_PAGES_PER_STOCK:
        soup = BeautifulSoup(current_page_html, 'lxml')
        page_company_name_tag = soup.find('td', class_='det') 
        page_company_name_display = original_ticker_name
        if page_company_name_tag and page_company_name_tag.b: page_company_name_display = page_company_name_tag.b.text.strip()
        elif page_company_name_tag: page_company_name_display = page_company_name_tag.text.strip()
        page_table_data, page_headers = parse_financial_table_from_soup(soup, page_company_name_display, str(page_count))
        if not page_table_data and page_count == 1:
            print(f"   No table data found on the first financial page for {original_ticker_name}. This sc_id might be incorrect for financials.")
            return False 
        if page_table_data:
            if page_headers and len(page_headers) > 1:
                for header in page_headers[1:]:
                    if header.startswith("Mar "): master_year_headers.add(header)
            all_pages_data_dictionaries.extend(page_table_data)
        
        prev_years_link = soup.find('a', onclick=lambda x: x and "post_prevnext('2')" in x)
        if prev_years_link and response_obj_for_referer:
            form_params = get_form_params(soup)
            if not form_params: break 
            form_params['nav'] = 'next' 
            post_referer = response_obj_for_referer.url
            post_financials_headers = HEADERS_FINANCIALS.copy()
            post_financials_headers.update({'Referer': post_referer, 'Origin': "https://www.moneycontrol.com", 'Content-Type': 'application/x-www-form-urlencoded'})
            
            time.sleep(REQUEST_DELAY)
            try:
                print(f"   Fetching next financial page (POST)...")
                response = session.post(BASE_URL_FINANCIALS, data=form_params, headers=post_financials_headers, 
                                        timeout=25) # Normal timeout
                                        # proxies=next_page_proxies REMOVED
                response.raise_for_status(); current_page_html = response.text; response_obj_for_referer = response; page_count += 1
            except requests.exceptions.Timeout:
                print(f"      Timeout fetching next financials page (POST) for {original_ticker_name}.")
                request_successful = False; break 
            except requests.RequestException as e: 
                print(f"      Error next financials page (POST) for {original_ticker_name}: {e}")
                request_successful = False; break 
        else:
            break 
            
    if not request_successful: 
        print(f"   Pagination interrupted by error for {original_ticker_name}. Not saving partial data.")
        return False 

    if not all_pages_data_dictionaries: 
        print(f"   No financial data collected after pagination for {original_ticker_name} (sc_id: {sc_id_to_scrape}).")
        return False

    try: # Merging and saving logic remains the same
        merged_items_data = {}
        for row_dict in all_pages_data_dictionaries:
            item_name = row_dict.get("Item")
            if not item_name: continue
            if item_name not in merged_items_data: merged_items_data[item_name] = {"Item": item_name}
            for key, value in row_dict.items():
                if key.startswith("Mar ") and value is not None:
                    if merged_items_data[item_name].get(key) is None or value is not None: merged_items_data[item_name][key] = value
                elif key != "Item" and value is not None :
                    if merged_items_data[item_name].get(key) is None: merged_items_data[item_name][key] = value
        final_merged_list = list(merged_items_data.values())
        if not final_merged_list: print(f"   Merged data is empty for {original_ticker_name}. Not saving CSV."); return False
        sorted_year_headers = sorted(list(master_year_headers), reverse=True)
        final_ordered_columns = ["Item"] + sorted_year_headers
        all_collected_keys = set(); [all_collected_keys.update(item_data.keys()) for item_data in final_merged_list]
        other_columns = [key for key in all_collected_keys if key not in final_ordered_columns]
        final_ordered_columns.extend(other_columns)
        df = pd.DataFrame(final_merged_list)
        for col in final_ordered_columns: 
            if col not in df.columns: df[col] = pd.NA
        df = df[final_ordered_columns] 
        if df.empty or len(df.columns) <= 1 : print(f"   Merged DataFrame is empty for {original_ticker_name}. Not saving CSV."); return False
        csv_filename = f"{original_ticker_name}_{REPORT_TYPE}_merged_financials.csv"
        csv_filepath = os.path.join(output_folder_path, csv_filename)
        df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
        print(f"   Saved Financials: '{csv_filepath}' (Shape: {df.shape})")
        return True 
    except Exception as e:
        print(f"   Error during data merging or CSV saving for {original_ticker_name}: {e}")
        return False 


# --- MODIFIED Function to iterate through stock folders ---
def process_stock_folders(main_data_folder, stocks_to_process_tickers=None):
    if not os.path.isdir(main_data_folder): print(f"Error: Main data folder '{main_data_folder}' does not exist."); return
    
    print(f"Starting financial data scraping process (without proxies)...") # Modified message
    folders_processed_count, successful_scrapes = 0, 0
    skipped_tickers_info = {} 

    target_ticker_folders = []
    if stocks_to_process_tickers:
        target_ticker_folders = [ticker for ticker in stocks_to_process_tickers if os.path.isdir(os.path.join(main_data_folder, ticker))]
        if len(target_ticker_folders) != len(stocks_to_process_tickers): print("Warning: Some specified tickers do not have folders.")
        print(f"Processing limited list of {len(target_ticker_folders)} stock folders: {target_ticker_folders}")
    else:
        target_ticker_folders = [f for f in os.listdir(main_data_folder) if os.path.isdir(os.path.join(main_data_folder, f))]
        print(f"Found {len(target_ticker_folders)} subfolders to process.")

    with requests.Session() as session:
        for original_ticker_name in target_ticker_folders:
            print(f"\n========= Processing Ticker Folder: {original_ticker_name} =========")
            ticker_folder_path = os.path.join(main_data_folder, original_ticker_name)
            expected_csv_filename = f"{original_ticker_name}_{REPORT_TYPE}_merged_financials.csv"
            expected_csv_filepath = os.path.join(ticker_folder_path, expected_csv_filename)

            if os.path.exists(expected_csv_filepath):
                print(f"Skipping {original_ticker_name}: Financials CSV '{expected_csv_filepath}' already exists.")
                folders_processed_count +=1; successful_scrapes +=1 
                if folders_processed_count < len(target_ticker_folders): time.sleep(0.5)
                continue
            
            # current_proxy_for_stock = get_random_proxy_from_api(session) # REMOVED
            # time.sleep(API_REQUEST_DELAY / 2) # REMOVED

            session.headers.update(HEADERS_API) 
            sc_id = get_moneycontrol_sc_did_from_api(original_ticker_name, session) # Removed proxy argument
            time.sleep(API_REQUEST_DELAY)

            if sc_id:
                if scrape_and_save_financials_for_stock(sc_id, original_ticker_name, ticker_folder_path, session): # Removed proxy argument
                    successful_scrapes += 1
                else: 
                    skipped_tickers_info[original_ticker_name] = f"Failed during financial scrape (sc_id: {sc_id})"
            else:
                skipped_tickers_info[original_ticker_name] = "sc_id lookup failed"
            
            folders_processed_count += 1
            if folders_processed_count < len(target_ticker_folders):
                overall_delay = REQUEST_DELAY # Normal delay
                print(f"--- Waiting for {overall_delay:.1f}s before next stock ---")
                time.sleep(overall_delay) 

    print(f"\n--- Overall Process Complete ---")
    print(f"Total ticker folders attempted: {folders_processed_count}")
    print(f"Successfully scraped and saved data for: {successful_scrapes} tickers.")
    if skipped_tickers_info:
        print(f"Tickers skipped or failed: {len(skipped_tickers_info)}")
        for i, (ticker, reason) in enumerate(skipped_tickers_info.items()):
            print(f"  {i+1}. {ticker} - Reason: {reason}")
    else:
        print("All attempted tickers were processed successfully or already had existing files.")


# --- Main Execution ---
if __name__ == "__main__":
    main_folder = "stock_data"

    ticker_file = "failed_ticker.txt" # Using your specified filename

    ticker_file = "kushal_tickers.txt" # Using your specified filename


    # PySocks check is not strictly necessary if not using SOCKS proxies, but harmless to keep
    try:
        import socks
        # print("PySocks library found.") # Optional print
    except ImportError:
        print("NOTE: PySocks library not found. This is okay if not using SOCKS proxies.")

    if not os.path.isdir(main_folder):
        print(f"Default data folder '{main_folder}' was not found.")
        custom_path = input(f"Please enter the full path to your main stock data folder: ")
        if os.path.isdir(custom_path): main_folder = custom_path
        else: print(f"The path '{custom_path}' is not valid. Exiting."); exit()
    
    # print("INFO: Dynamic proxy fetching from API will be attempted for each stock.") # REMOVED
    print("INFO: Running script without proxies.")


    tickers_to_run = []
    if os.path.exists(ticker_file):
        try:
            with open(ticker_file, 'r') as f:
                tickers_to_run = [line.strip() for line in f if line.strip()]
            if not tickers_to_run: print(f"Warning: Ticker file '{ticker_file}' is empty.")
            else: print(f"Read {len(tickers_to_run)} tickers from '{ticker_file}'.")
        except Exception as e: print(f"Error reading '{ticker_file}': {e}")
    else:
        print(f"Ticker file '{ticker_file}' not found. Exiting as ticker file is required.")
        exit()

    if not tickers_to_run: print("No tickers loaded to process. Exiting."); exit()
    
    print("\nSetting up subfolders for tickers (if they don't exist)...")
    for stock_ticker in tickers_to_run:
        safe_folder_name = re.sub(r'[<>:"/\\|?*]', '_', stock_ticker)
        if safe_folder_name != stock_ticker:
            print(f"   Note: Using safe folder name '{safe_folder_name}' for ticker '{stock_ticker}'")
        ticker_specific_folder_path = os.path.join(main_folder, safe_folder_name)
        if not os.path.exists(ticker_specific_folder_path):
            try: os.makedirs(ticker_specific_folder_path); print(f"Created data folder: {ticker_specific_folder_path}")
            except OSError as e: print(f"Error creating data folder {ticker_specific_folder_path}: {e}")
    
    print(f"\n--- RUNNING SCRIPT FOR {len(tickers_to_run)} STOCKS (from '{ticker_file}') WITHOUT PROXIES ---")
    process_stock_folders(main_folder, stocks_to_process_tickers=tickers_to_run)