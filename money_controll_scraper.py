import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
from urllib.parse import urlparse, parse_qs # For parsing URL query parameters

# --- Configurations and other functions (clean_value, get_form_params, parse_financial_table_from_soup, scrape_and_save_financials_for_stock)
# --- remain EXACTLY THE SAME as the previous version.
# --- Only get_moneycontrol_sc_did_from_api and the main execution block are shown for brevity.

# Configurations
BASE_URL_FINANCIALS = "https://www.moneycontrol.com/stocks/company_info/print_financials.php"
BASE_URL_SUGGESTION_API = "https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php"
REPORT_TYPE = "balance_VI"
MAX_PAGES_PER_STOCK = 10
REQUEST_DELAY = 3
API_REQUEST_DELAY = 1.5

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
                rd = {"Item": item_name}; table_data.append(rd); continue # Section headers have None for year values implicitly
        if item_name and column_headers:
            if len(cols[1:]) == len(column_headers[1:]):
                vals = [clean_value(c.text) for c in cols[1:]]; rd = {"Item": item_name}
                for k, v in zip(column_headers[1:], vals): rd[k] = v
                table_data.append(rd)
            elif item_name and len(cols) <= 2 and "detb" in first_col_classes: # Bold sub-header like 'NON-CURRENT ASSETS'
                rd = {"Item": item_name} # No year values for these, will be filled with None/NaN by DataFrame
                table_data.append(rd)
    return table_data, column_headers

# --- MODIFIED Function to get sc_id using the API with link_src as priority ---
def get_moneycontrol_sc_did_from_api(ticker_query, session):
    params = {
        'classic': 'true', 'query': ticker_query, 'type': '1',
        'format': 'json', 'callback': 'suggest1'
    }
    sc_id_to_use = None
    try:
        print(f"   API Call: Searching for sc_id for '{ticker_query}'...")
        response = session.get(BASE_URL_SUGGESTION_API, params=params, headers=HEADERS_API, timeout=10)
        response.raise_for_status()
        response_text = response.text
        if response_text.startswith('suggest1(') and response_text.endswith(')'):
            json_str = response_text[len('suggest1('):-1]
            suggestions = json.loads(json_str)

            if suggestions and isinstance(suggestions, list) and len(suggestions) > 0:
                # Iterate through suggestions to find the best match
                for sug in suggestions:
                    # Priority 1: Extract ID from link_src
                    if 'link_src' in sug and sug['link_src']:
                        path_segments = sug['link_src'].strip('/').split('/')
                        if path_segments:
                            potential_id_from_link_src = path_segments[-1]
                            # Basic validation: not empty and often alphanumeric
                            if potential_id_from_link_src and potential_id_from_link_src.isalnum():
                                # Check if ticker_query is part of the display name for relevance
                                if 'pdt_dis_nm' in sug and ticker_query.upper() in sug['pdt_dis_nm'].upper().replace("&NBSP;", " "):
                                    sc_id_to_use = potential_id_from_link_src
                                    print(f"   API Success (from link_src, relevant): Found ID '{sc_id_to_use}' for '{ticker_query}'.")
                                    return sc_id_to_use
                                elif not sc_id_to_use: # If no ID found yet, take the first valid one from link_src
                                    sc_id_to_use = potential_id_from_link_src
                                    print(f"   API Info (from link_src, first valid): Tentatively using ID '{sc_id_to_use}' for '{ticker_query}'.")
                
                # If link_src didn't yield a relevant ID, try direct sc_id as a fallback
                if not sc_id_to_use:
                    for sug in suggestions:
                        if 'sc_id' in sug and sug['sc_id']:
                            potential_direct_sc_id = sug['sc_id']
                            if 'pdt_dis_nm' in sug and ticker_query.upper() in sug['pdt_dis_nm'].upper().replace("&NBSP;", " "):
                                sc_id_to_use = potential_direct_sc_id
                                print(f"   API Info (from direct sc_id, relevant): Using ID '{sc_id_to_use}' for '{ticker_query}'.")
                                return sc_id_to_use
                            elif not sc_id_to_use: # First valid direct sc_id
                                sc_id_to_use = potential_direct_sc_id
                                print(f"   API Info (from direct sc_id, first valid): Tentatively using ID '{sc_id_to_use}' for '{ticker_query}'.")

                # Fallback to link_track ID if others failed
                if not sc_id_to_use:
                    for sug in suggestions:
                        if 'link_track' in sug and sug['link_track']:
                            parsed_url = urlparse(sug['link_track'])
                            query_params = parse_qs(parsed_url.query)
                            if 'id' in query_params and query_params['id'] and query_params['id'][0]:
                                sc_id_to_use = query_params['id'][0]
                                print(f"   API Info (from link_track): Using ID '{sc_id_to_use}' for '{ticker_query}'.")
                                return sc_id_to_use # Return this as the sc_id

                if sc_id_to_use: # If any ID was tentatively set but not returned due to relevance checks
                    print(f"   API Final Choice: Using ID '{sc_id_to_use}' for '{ticker_query}'.")
                    return sc_id_to_use

                print(f"   API Warning: Could not extract a suitable ID for '{ticker_query}' from any source. Suggestions: {suggestions[:1]}")
                return None
            else:
                print(f"   API Warning: No suggestions or empty list for '{ticker_query}'.")
        else:
            print(f"   API Error: Unexpected response format for '{ticker_query}'.")

    except requests.RequestException as e: print(f"   API Request Error for '{ticker_query}': {e}")
    except json.JSONDecodeError as e: print(f"   API JSON Decode Error for '{ticker_query}': {e}.")
    return sc_id_to_use # Return whatever was found, or None


# --- Main Scraping and Merging Function for a SINGLE stock (scrape_and_save_financials_for_stock) ---
# --- This function REMAINS THE SAME as the last fully working version you approved. ---
# --- For brevity, it's not fully repeated here. Assume it's correctly defined. ---
def scrape_and_save_financials_for_stock(sc_id_to_scrape, original_ticker_name, output_folder_path, session):
    print(f"\n--- Processing Financials for: {original_ticker_name} (using sc_id: {sc_id_to_scrape}) ---")
    all_pages_data_dictionaries = []
    master_year_headers = set()
    session.headers.update(HEADERS_FINANCIALS)
    initial_params_get = {'sc_did': sc_id_to_scrape, 'type': REPORT_TYPE}
    current_page_html, response_obj_for_referer = None, None
    try:
        response = session.get(BASE_URL_FINANCIALS, params=initial_params_get, timeout=25)
        response.raise_for_status(); current_page_html = response.text; response_obj_for_referer = response
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404: print(f"   Error 404: Financials page not found for {original_ticker_name} (sc_id: {sc_id_to_scrape}). Skipping."); return False
        else: print(f"   HTTP error initial financials for {original_ticker_name}: {http_err}"); return False
    except requests.RequestException as e: print(f"   Request error initial financials for {original_ticker_name}: {e}"); return False
    if not current_page_html: print(f"   Failed initial financials for {original_ticker_name}. Skipping."); return False

    page_count = 1
    while page_count <= MAX_PAGES_PER_STOCK:
        soup = BeautifulSoup(current_page_html, 'lxml')
        page_company_name_tag = soup.find('td', class_='det') 
        page_company_name_display = original_ticker_name
        if page_company_name_tag and page_company_name_tag.b: page_company_name_display = page_company_name_tag.b.text.strip()
        elif page_company_name_tag: page_company_name_display = page_company_name_tag.text.strip()
        page_table_data, page_headers = parse_financial_table_from_soup(soup, page_company_name_display, str(page_count))
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
                response = session.post(BASE_URL_FINANCIALS, data=form_params, headers=post_financials_headers, timeout=25)
                response.raise_for_status(); current_page_html = response.text; response_obj_for_referer = response; page_count += 1
            except requests.RequestException as e: print(f"      Error next financials page (POST) for {original_ticker_name}: {e}"); break
        else: break
    if all_pages_data_dictionaries:
        merged_items_data = {}
        for row_dict in all_pages_data_dictionaries: # Merging logic
            item_name = row_dict.get("Item")
            if not item_name: continue
            if item_name not in merged_items_data: merged_items_data[item_name] = {"Item": item_name}
            for key, value in row_dict.items():
                if key.startswith("Mar ") and value is not None:
                    if merged_items_data[item_name].get(key) is None or value is not None: merged_items_data[item_name][key] = value
                elif key != "Item" and value is not None :
                    if merged_items_data[item_name].get(key) is None: merged_items_data[item_name][key] = value
        final_merged_list = list(merged_items_data.values())
        sorted_year_headers = sorted(list(master_year_headers), reverse=True)
        final_ordered_columns = ["Item"] + sorted_year_headers
        all_collected_keys = set(); [all_collected_keys.update(item_data.keys()) for item_data in final_merged_list]
        other_columns = [key for key in all_collected_keys if key not in final_ordered_columns]
        final_ordered_columns.extend(other_columns)
        df = pd.DataFrame(final_merged_list)
        for col in final_ordered_columns:
            if col not in df.columns: df[col] = pd.NA
        df = df[final_ordered_columns]
        csv_filename = f"{original_ticker_name}_{REPORT_TYPE}_merged_financials.csv"
        csv_filepath = os.path.join(output_folder_path, csv_filename)
        df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
        print(f"   Saved Financials: '{csv_filepath}' (Shape: {df.shape})")
        return True
    else:
        print(f"   No financial data for {original_ticker_name} (sc_id: {sc_id_to_scrape}). CSV not created.")
        return False

# --- Function to iterate through stock folders (process_stock_folders) ---
# --- This function REMAINS THE SAME as the last fully working version you approved. ---
def process_stock_folders(main_data_folder, stocks_to_process_tickers=None):
    if not os.path.isdir(main_data_folder): print(f"Error: Main data folder '{main_data_folder}' does not exist."); return
    print(f"Starting financial data scraping process...")
    folders_processed_count, successful_scrapes = 0, 0
    target_ticker_folders = []
    if stocks_to_process_tickers:
        target_ticker_folders = [ticker for ticker in stocks_to_process_tickers if os.path.isdir(os.path.join(main_data_folder, ticker))]
        if len(target_ticker_folders) != len(stocks_to_process_tickers): print("Warning: Some specified tickers do not have folders.")
        print(f"Processing limited list of {len(target_ticker_folders)} stock folders: {target_ticker_folders}")
    else:
        target_ticker_folders = [f for f in os.listdir(main_data_folder) if os.path.isdir(os.path.join(main_data_folder, f))]
        print(f"Found {len(target_ticker_folders)} subfolders to process.")

    with requests.Session() as session: # Persistent session
        for original_ticker_name in target_ticker_folders:
            ticker_folder_path = os.path.join(main_data_folder, original_ticker_name)
            expected_csv_filename = f"{original_ticker_name}_{REPORT_TYPE}_merged_financials.csv"
            expected_csv_filepath = os.path.join(ticker_folder_path, expected_csv_filename)

            if os.path.exists(expected_csv_filepath):
                print(f"Skipping {original_ticker_name}: Financials CSV '{expected_csv_filepath}' already exists.")
                folders_processed_count +=1; successful_scrapes +=1
                continue
            
            session.headers.update(HEADERS_API) # Set headers for API call
            sc_id = get_moneycontrol_sc_did_from_api(original_ticker_name, session)
            time.sleep(API_REQUEST_DELAY)

            if sc_id:
                # Now scrape financials using the obtained sc_id
                if scrape_and_save_financials_for_stock(sc_id, original_ticker_name, ticker_folder_path, session):
                    successful_scrapes += 1
            else:
                print(f"Could not find valid sc_id for {original_ticker_name}. Skipping financial data scraping.")
            
            folders_processed_count += 1
            if folders_processed_count < len(target_ticker_folders): # Don't sleep after the last one
                print(f"--- Waiting for {REQUEST_DELAY * 1.2:.1f}s before next stock ---")
                time.sleep(REQUEST_DELAY * 1.2) # Overall delay between processing different stocks
    print(f"\n--- Overall Process Complete ---")
    print(f"Total ticker folders attempted: {folders_processed_count}")
    print(f"Successfully scraped and saved data for: {successful_scrapes} tickers.")


# --- Main Execution (Test with 10 stocks) ---
# --- Main Execution (Test with tickers from ticker.txt) ---
if __name__ == "__main__":
    main_folder = "stock_data"
    ticker_file = "ticker.txt" # Name of your ticker file

    if not os.path.isdir(main_folder):
        print(f"Default folder '{main_folder}' not found.")
        custom_path = input(f"Enter full path to stock data folder: ")
        if os.path.isdir(custom_path): main_folder = custom_path
        else: print(f"Path '{custom_path}' invalid. Exiting."); exit()

    # --- Read tickers from ticker.txt ---
    test_tickers = []
    if os.path.exists(ticker_file):
        try:
            with open(ticker_file, 'r') as f:
                test_tickers = [line.strip() for line in f if line.strip()] # Read and strip whitespace/newlines
            if not test_tickers:
                print(f"Warning: '{ticker_file}' is empty or contains no valid tickers.")
            else:
                print(f"Read {len(test_tickers)} tickers from '{ticker_file}': {test_tickers[:5]}...") # Print first 5
        except Exception as e:
            print(f"Error reading '{ticker_file}': {e}")
    else:
        print(f"Ticker file '{ticker_file}' not found. Please create it with one ticker per line.")
        # Optionally, fall back to a default list or exit
        # test_tickers = ["RELIANCE", "INFY"] # Example fallback
        # exit()


    if not test_tickers: # If no tickers were read or file not found
        print("No tickers to process. Exiting.")
        exit()
    
    print("\nSetting up dummy folders for testing (if they don't exist)...")
    for stock_ticker in test_tickers:
        dummy_folder_path = os.path.join(main_folder, stock_ticker)
        if not os.path.exists(dummy_folder_path):
            try: os.makedirs(dummy_folder_path); print(f"Created dummy folder: {dummy_folder_path}")
            except OSError as e: print(f"Error creating dummy folder {dummy_folder_path}: {e}")
    
    print(f"\n--- RUNNING TEST SCRIPT FOR {len(test_tickers)} STOCKS FROM '{ticker_file}' (Using API for sc_id) ---")
    process_stock_folders(main_folder, stocks_to_process_tickers=test_tickers)

    # --- TO RUN FOR ALL STOCKS LATER (after testing and ensuring ticker.txt has all 500) ---
    # print(f"\n--- RUNNING SCRIPT FOR ALL STOCKS FROM '{ticker_file}' ---")
    # process_stock_folders(main_folder, stocks_to_process_tickers=test_tickers) # if ticker.txt contains all
    # OR to process all folders irrespective of ticker.txt:
    # process_stock_folders(main_folder)