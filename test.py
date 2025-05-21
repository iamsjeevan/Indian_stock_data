import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
from urllib.parse import urlparse, parse_qs
from datetime import datetime # For sorting period headers

# --- Configurations ---
BASE_URL_FINANCIALS = "https://www.moneycontrol.com/stocks/company_info/print_financials.php"
BASE_URL_SUGGESTION_API = "https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php"

# --- CHOOSE THE ACTIVE REPORT TYPE ---
ACTIVE_REPORT_TYPE = "quarterly"
# ACTIVE_REPORT_TYPE = "balance_VI"
# --- END OF REPORT TYPE CHOICE ---

MAX_PAGES_PER_STOCK = 10
REQUEST_DELAY = 2
API_REQUEST_DELAY = 1

HEADERS_FINANCIALS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Sec-Fetch-Dest": "iframe", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1", "Sec-GPC": "1", "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.moneycontrol.com/stocks/company_info/print_main.php",
}
HEADERS_API = {
    'accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.7', 'priority': 'u=1, i', 'referer': 'https://www.moneycontrol.com/',
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

def parse_financial_table_from_soup(soup, company_name_for_debug, page_num_for_debug="", report_type_for_debug=""):
    """
    Parses the financial data table based on the provided HTML structure.
    """
    print(f"DEBUG PARSER (Page {page_num_for_debug}, {company_name_for_debug}, {report_type_for_debug}): Starting table search.")
    
    # Find all tables that are candidates (class='table4' seems consistent for these print views)
    candidate_tables = soup.find_all('table', class_='table4', attrs={'bgcolor': '#ffffff', 'width': '100%'})
    financial_table_soup = None
    
    print(f"DEBUG PARSER: Found {len(candidate_tables)} candidate tables with class 'table4', bgcolor, width.")

    for idx, table_candidate in enumerate(candidate_tables):
        # Look for the specific header row structure within this candidate table
        # Header: <td class="detb"> </td> <td class="detb">Mar '25</td> <td class="detb">Dec '24</td> ...
        rows = table_candidate.find_all('tr', limit=10) # Check first few rows for the header
        for r_idx, tr_header_candidate in enumerate(rows):
            tds = tr_header_candidate.find_all('td')
            if len(tds) > 1:
                # Check first TD for class 'detb' (even if empty text)
                # Check second TD for class 'detb' and "Mon 'YY" pattern
                first_td_classes = tds[0].get('class', [])
                second_td_classes = tds[1].get('class', [])
                second_td_text = tds[1].text.strip()

                # Check if the first item column header is present (can be empty text like  )
                # and the second column is a date with 'detb' class.
                if "detb" in first_td_classes and \
                   "detb" in second_td_classes and \
                   re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s'\d{2}$", second_td_text): # Note the added apostrophe
                    
                    # Verify subsequent cells also look like period headers with 'detb'
                    is_correct_header_row = True
                    if len(tds) > 2: # If there are more than two columns
                        for h_idx in range(2, min(len(tds), 5)): # Check next few columns
                            if not ("detb" in tds[h_idx].get('class', []) and \
                                    re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s'\d{2}$", tds[h_idx].text.strip())):
                                is_correct_header_row = False
                                break
                    if is_correct_header_row:
                        financial_table_soup = table_candidate
                        print(f"DEBUG PARSER: Identified financial table (candidate {idx}) using specific header pattern. Header starts with '{second_td_text}'")
                        break
            if financial_table_soup: break
        if financial_table_soup: break

    if not financial_table_soup:
        print(f"ERROR (Page {page_num_for_debug}, {company_name_for_debug}, {report_type_for_debug}): Could not identify the main financial data table.")
        return [], []

    all_rows_in_table = financial_table_soup.find_all('tr')
    table_data = []
    column_headers = []
    header_row_found_idx = -1

    # Extract column headers from the identified table
    for i, row_tr in enumerate(all_rows_in_table):
        cols_td = row_tr.find_all('td')
        if len(cols_td) > 1 and \
           "detb" in cols_td[0].get('class', []) and \
           "detb" in cols_td[1].get('class', []) and \
           re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s'\d{2}$", cols_td[1].text.strip()):
            column_headers = ["Item"] + [col.text.strip() for col in cols_td[1:]]
            header_row_found_idx = i
            print(f"DEBUG PARSER: Column Headers extracted: {column_headers}")
            break
            
    if not column_headers:
        print(f"ERROR (Page {page_num_for_debug}, {company_name_for_debug}, {report_type_for_debug}): Could not extract column headers even after identifying table.")
        return [], []

    # Process data rows
    for i, row_tr in enumerate(all_rows_in_table):
        if i <= header_row_found_idx:  # Skip header row and anything before it
            continue

        cols_td = row_tr.find_all('td')
        if not cols_td: continue

        item_name = cols_td[0].text.replace('\xa0', ' ').strip()

        if "Source :" in item_name:
            # print(f"DEBUG PARSER: Reached 'Source :', ending data extraction for this page.")
            break
        
        # Skip rows that look like dividers or are completely empty after the first cell
        if len(cols_td) == 1 and cols_td[0].find('img'): continue
        if not item_name and not any(c.text.strip() for c in cols_td[1:]): continue
        if "12 mths" in item_name.lower() and len(cols_td) ==1 : continue #skip "12 mths" header if it takes full row

        # Handle section headers (e.g., "EXPENDITURE")
        # These often have class 'detb' and might have colspan on the first td
        is_section_header = "detb" in cols_td[0].get('class', []) and \
                            (cols_td[0].get('colspan') or len(cols_td) < len(column_headers) -1 or \
                             all(c.text.strip() == '' or c.text.strip() == '-' for c in cols_td[1:]))

        if item_name: # Only process if there's an item name
            row_data_dict = {"Item": item_name}
            if is_section_header:
                # For section headers, fill period columns with None
                for ch in column_headers[1:]:
                    row_data_dict[ch] = None
            else:
                # For data rows, extract values
                # Ensure we match the number of data columns to available headers
                values_from_row = [clean_value(col.text) for col in cols_td[1:]]
                for k_idx, header_key in enumerate(column_headers[1:]):
                    if k_idx < len(values_from_row):
                        row_data_dict[header_key] = values_from_row[k_idx]
                    else:
                        row_data_dict[header_key] = None # If row has fewer data cells than headers
            
            table_data.append(row_data_dict)

    if not table_data:
        print(f"WARN (Page {page_num_for_debug}, {company_name_for_debug}, {report_type_for_debug}): No data rows extracted from the table.")

    return table_data, column_headers

def get_moneycontrol_sc_did_from_api(ticker_query, session):
    params = {'classic': 'true', 'query': ticker_query, 'type': '1', 'format': 'json', 'callback': 'suggest1'}
    sc_id_to_use = None
    try:
        print(f"   API Call: Searching for sc_id for '{ticker_query}'...")
        response = session.get(BASE_URL_SUGGESTION_API, params=params, headers=HEADERS_API, timeout=15)
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
                                # More robust name matching, ignore " Ltd", " Limited" etc.
                                query_upper = ticker_query.upper().replace(" LTD","").replace(" LIMITED","").replace(".","")
                                sug_name_upper = sug.get('pdt_dis_nm','').upper().replace("&NBSP;", " ").replace(" LTD","").replace(" LIMITED","").replace(".","")
                                if query_upper in sug_name_upper:
                                    sc_id_to_use = potential_id; print(f"   API Success (link_src, relevant name): ID '{sc_id_to_use}' for '{ticker_query}'."); return sc_id_to_use
                                elif not sc_id_to_use: sc_id_to_use = potential_id # Take first potential if no name match
                if not sc_id_to_use and suggestions[0].get('sc_id'): # Fallback to first sc_id if link_src didn't yield result
                    sc_id_to_use = suggestions[0]['sc_id']
                    print(f"   API Fallback (first sc_id): Using ID '{sc_id_to_use}' for '{ticker_query}'.")
                    return sc_id_to_use
                if sc_id_to_use:  # If sc_id_to_use was set by link_src but not returned due to name mismatch
                    print(f"   API Choice (link_src, less relevant name): Using ID '{sc_id_to_use}' for '{ticker_query}'.")
                    return sc_id_to_use
                print(f"   API Warning: No suitable ID from link_src for '{ticker_query}'. Suggestions: {suggestions[:1]}")
            else: print(f"   API Warning: No suggestions for '{ticker_query}'.")
        else: print(f"   API Error: Unexpected response format for '{ticker_query}'.")
    except requests.exceptions.Timeout: print(f"   API Request Timeout for '{ticker_query}'.")
    except requests.RequestException as e: print(f"   API Request Error for '{ticker_query}': {e}")
    except json.JSONDecodeError as e: print(f"   API JSON Decode Error for '{ticker_query}': {e}.")
    return None

def scrape_and_save_financials_for_stock(sc_id_to_scrape, original_ticker_name, output_folder_path, session):
    report_type_to_fetch = ACTIVE_REPORT_TYPE 

    print(f"\n--- Processing {report_type_to_fetch.replace('_VI','').capitalize()} Financials for: {original_ticker_name} (sc_id: {sc_id_to_scrape}) ---")
    all_pages_data_dictionaries = []
    master_period_headers = set() 
    session.headers.update(HEADERS_FINANCIALS)
    initial_params_get = {'sc_did': sc_id_to_scrape, 'type': report_type_to_fetch}
    current_page_html, response_obj_for_referer = None, None
    request_successful = True 
    page_count = 1

    try:
        print(f"   Fetching initial {report_type_to_fetch} page (Page {page_count}). URL: {BASE_URL_FINANCIALS} PARAMS: {initial_params_get}")
        response = session.get(BASE_URL_FINANCIALS, params=initial_params_get, timeout=30)
        response.raise_for_status()
        current_page_html = response.text
        response_obj_for_referer = response
        print(f"   Initial page fetched successfully. Status: {response.status_code}. URL: {response.url}")
    except requests.exceptions.HTTPError as http_err:
        if response and response.status_code == 404: 
            print(f"   Error 404: {report_type_to_fetch.capitalize()} page not found for {original_ticker_name} (sc_id: {sc_id_to_scrape}).")
        else: 
            print(f"   HTTP error fetching initial {report_type_to_fetch} for {original_ticker_name}: {http_err}")
        if response is not None: print(f"   Response text (first 300): {response.text[:300]}")
        return False 
    except requests.exceptions.Timeout:
        print(f"   Timeout fetching initial {report_type_to_fetch} for {original_ticker_name}.")
        return False 
    except requests.RequestException as e: 
        print(f"   Request error fetching initial {report_type_to_fetch} for {original_ticker_name}: {e}")
        return False
    
    if not current_page_html: 
        print(f"   Failed to get initial {report_type_to_fetch} page content for {original_ticker_name}.")
        return False

    soup_initial_check = BeautifulSoup(current_page_html, 'lxml')
    no_data_message = soup_initial_check.find(
        lambda tag: tag.name == "font" and 
                    tag.get("color") == "#5b5b5b" and 
                    tag.get("size") == "3" and 
                    "Data Not Available" in tag.get_text(strip=True)
    )
    if no_data_message:
        print(f"   Explicit Message: '{no_data_message.get_text(strip=True)}' found for {original_ticker_name}. No data to process.")
        no_data_filename = os.path.join(output_folder_path, f"{original_ticker_name}_{report_type_to_fetch}_NO_DATA.txt")
        try:
            with open(no_data_filename, "w", encoding="utf-8") as f_no_data: # Added encoding
                f_no_data.write(f"Moneycontrol page indicated: {no_data_message.get_text(strip=True)}\n")
                f_no_data.write(f"URL attempted: {response_obj_for_referer.url if response_obj_for_referer else 'N/A'}\n")
            print(f"   Created placeholder file: {no_data_filename}")
        except IOError as e_save_no_data:
            print(f"   Error creating no_data placeholder file: {e_save_no_data}")
        return False

    # Company name extraction specifically for this page structure
    page_company_name_display = original_ticker_name 
    company_name_tag_candidate = soup_initial_check.find('form', {'name': 'finyear_frm'})
    if company_name_tag_candidate:
        # The company name is in a <td><b>Company Name</b></td> structure before the "Previous Years" link
        # within the same table as the form.
        parent_table = company_name_tag_candidate.find_parent('table')
        if parent_table:
            first_td_with_b = parent_table.find('td', class_='det') # Look for <td class="det">
            if first_td_with_b and first_td_with_b.b:
                page_company_name_display = first_td_with_b.b.text.strip()
                print(f"   Company name from form's parent table: '{page_company_name_display}'")
            else:
                print(f"   Warning: Could not find specific company name tag structure. Using H1 or default.")
                h1_tag = soup_initial_check.find('h1') # Fallback to H1
                if h1_tag and len(h1_tag.text.strip()) < 100 : page_company_name_display = h1_tag.text.strip()

    if "copy pasted" in page_company_name_display.lower() or "microsoft excel" in page_company_name_display.lower():
        print(f"   Warning: Extracted display name still seems incorrect ('{page_company_name_display[:100]}...'). Using original ticker name for logs.")
        page_company_name_display_for_log = original_ticker_name # Use original for logs if extraction is bad
    else:
        page_company_name_display_for_log = page_company_name_display


    while page_count <= MAX_PAGES_PER_STOCK:
        print(f"   Processing Page {page_count} for {page_company_name_display_for_log} ({report_type_to_fetch})...")
        soup = BeautifulSoup(current_page_html, 'lxml')
        
        page_table_data, page_headers = parse_financial_table_from_soup(soup, page_company_name_display_for_log, str(page_count), report_type_to_fetch)
        
        if not page_table_data and page_count == 1:
            print(f"   CRITICAL: No parseable table data found on FIRST financial page for {page_company_name_display_for_log} (Report: {report_type_to_fetch}).")
            debug_html_filename = os.path.join(output_folder_path, f"{original_ticker_name}_{report_type_to_fetch}_page{page_count}_FAIL.html")
            try:
                with open(debug_html_filename, "w", encoding="utf-8") as f_debug:
                    f_debug.write(current_page_html)
                print(f"   Saved problematic HTML to: {debug_html_filename}")
            except Exception as e_save: print(f"   Error saving debug HTML: {e_save}")
            return False 
        
        if not page_table_data and page_count > 1:
            print(f"   No parseable table data on page {page_count}. Assuming end of financial data.")
            break

        if page_table_data:
            if page_headers and len(page_headers) > 1:
                for header_text in page_headers[1:]: 
                    if re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s'\d{2}$", header_text): # Match "Mon 'YY"
                        master_period_headers.add(header_text) 
            all_pages_data_dictionaries.extend(page_table_data)
            print(f"   Page {page_count}: Parsed {len(page_table_data)} rows.")
        else: # If page_table_data is empty but it's not page 1 (e.g. parser returned empty list)
             print(f"   WARN: Parser returned no data for page {page_count}, but not first page. Ending pagination.")
             break


        prev_periods_link = soup.find('a', onclick=lambda x: x and "post_prevnext('2')" in x)
        form_params_for_check = get_form_params(soup)
        can_paginate = prev_periods_link and response_obj_for_referer and form_params_for_check and form_params_for_check.get('start_year')

        if can_paginate:
            form_params = form_params_for_check 
            if not form_params: print("   Pagination check: Form params missing."); break
            form_params['nav'] = 'next' 
            post_referer = response_obj_for_referer.url
            post_financials_headers = HEADERS_FINANCIALS.copy()
            post_financials_headers.update({'Referer': post_referer, 'Origin': "https://www.moneycontrol.com", 'Content-Type': 'application/x-www-form-urlencoded'})
            
            time.sleep(REQUEST_DELAY + (page_count * 0.1)) 
            page_count += 1
            try:
                print(f"   Fetching next {report_type_to_fetch} page (Page {page_count}, POST)...")
                response = session.post(BASE_URL_FINANCIALS, data=form_params, headers=post_financials_headers, timeout=30)
                response.raise_for_status(); current_page_html = response.text; response_obj_for_referer = response
                print(f"   Next page ({page_count}) fetched successfully.")
            except requests.exceptions.Timeout:
                print(f"      Timeout fetching next {report_type_to_fetch} page (POST) for {original_ticker_name}.")
                request_successful = False; break 
            except requests.RequestException as e: 
                print(f"      Error next {report_type_to_fetch} page (POST) for {original_ticker_name}: {e}")
                if response:
                    debug_html_filename_post = os.path.join(output_folder_path, f"{original_ticker_name}_{report_type_to_fetch}_page{page_count}_POST_FAIL.html")
                    try:
                        with open(debug_html_filename_post, "w", encoding="utf-8") as f_debug_post: f_debug_post.write(response.text)
                        print(f"      Saved failing POST response HTML to: {debug_html_filename_post}")
                    except: pass
                request_successful = False; break 
        else:
            print(f"   No more pagination for {original_ticker_name} on page {page_count-1 if page_count>1 else 1}.")
            break 
            
    if not request_successful and not all_pages_data_dictionaries: 
        print(f"   Request/Parsing failed and no data collected for {original_ticker_name}.")
        return False 
    elif not request_successful and all_pages_data_dictionaries: 
        print(f"   Pagination interrupted by error for {original_ticker_name}. Processing data from {page_count-1 if page_count > 1 else 1} pages.")

    if not all_pages_data_dictionaries: 
        print(f"   No {report_type_to_fetch} data collected after pagination attempts for {original_ticker_name} (sc_id: {sc_id_to_scrape}).")
        return False

    try:
        merged_items_data = {}
        unique_items_order = [] 

        for row_dict in all_pages_data_dictionaries:
            item_name = row_dict.get("Item")
            if not item_name: continue
            if item_name not in merged_items_data:
                merged_items_data[item_name] = {"Item": item_name}
                unique_items_order.append(item_name)
            for key, value in row_dict.items():
                if key == "Item": continue
                # Use the corrected regex for period headers like "Mar '25"
                if re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s'\d{2}$", key) :
                    if merged_items_data[item_name].get(key) is None or value is not None:
                         merged_items_data[item_name][key] = value
                elif value is not None : # For other potential non-period columns
                    if merged_items_data[item_name].get(key) is None: merged_items_data[item_name][key] = value
        
        final_merged_list = [merged_items_data[item] for item in unique_items_order if item in merged_items_data]

        if not final_merged_list: 
            print(f"   Merged {report_type_to_fetch} data is empty for {original_ticker_name}. Not saving CSV.")
            return False
            
        sorted_period_headers = sorted(
            list(master_period_headers), 
            key=lambda d: datetime.strptime(d, "%b '%y") if isinstance(d, str) and re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s'\d{2}$",d) else datetime.min, 
            reverse=True
        ) # Corrected strptime format for "Mon 'YY"
        final_ordered_columns = ["Item"] + sorted_period_headers
        
        all_collected_keys_in_merge = set()
        for item_data in final_merged_list: 
            all_collected_keys_in_merge.update(item_data.keys())
        other_columns = sorted([key for key in all_collected_keys_in_merge if key not in final_ordered_columns])
        final_ordered_columns.extend(other_columns)
        
        df = pd.DataFrame(final_merged_list)
        for col in final_ordered_columns: 
            if col not in df.columns: df[col] = pd.NA
        df = df[final_ordered_columns]
        
        if df.empty or (len(df.columns) <= 1 and "Item" in df.columns) : 
            print(f"   Merged {report_type_to_fetch} DataFrame is empty or has only Item column for {original_ticker_name}. Not saving CSV.")
            return False
            
        csv_filename = f"{original_ticker_name}_{report_type_to_fetch}_merged_financials.csv"
        csv_filepath = os.path.join(output_folder_path, csv_filename)
        df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
        print(f"   SUCCESS: Saved {report_type_to_fetch.capitalize()} Financials: '{csv_filepath}' (Shape: {df.shape})")
        return True 
    except Exception as e:
        print(f"   CRITICAL Error during data merging or CSV saving for {original_ticker_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_stock_folders(main_data_folder, stocks_to_process_tickers=None):
    report_type_to_fetch = ACTIVE_REPORT_TYPE

    if not os.path.isdir(main_data_folder): print(f"Error: Main data folder '{main_data_folder}' does not exist."); return
    
    report_name_for_log = report_type_to_fetch.replace('_VI','').capitalize()
    print(f"Starting {report_name_for_log} financial data scraping process...")
    folders_processed_count, successful_scrapes = 0, 0
    skipped_tickers_info = {} 

    target_ticker_names_from_list = [] 
    if stocks_to_process_tickers:
        target_ticker_names_from_list = stocks_to_process_tickers 
        print(f"Processing limited list of {len(target_ticker_names_from_list)} tickers for {report_name_for_log}: {target_ticker_names_from_list[:5]}...")
    else:
        target_ticker_names_from_list = [f for f in os.listdir(main_data_folder) if os.path.isdir(os.path.join(main_data_folder, f))]
        print(f"Found {len(target_ticker_names_from_list)} subfolders to process for {report_name_for_log} (treating folder names as tickers).")

    with requests.Session() as session:
        for original_ticker_name in target_ticker_names_from_list: 
            safe_os_folder_name = re.sub(r'[<>:"/\\|?*&]', '_', original_ticker_name) # Ensure '&' is also replaced
            ticker_folder_path = os.path.join(main_data_folder, safe_os_folder_name)
            
            if not os.path.isdir(ticker_folder_path):
                print(f"   Warning: Folder {ticker_folder_path} not found for ticker {original_ticker_name}. Attempting to create.")
                try: os.makedirs(ticker_folder_path, exist_ok=True)
                except OSError as e_mkdir:
                    print(f"   Error creating folder {ticker_folder_path}: {e_mkdir}. Skipping {original_ticker_name}.")
                    skipped_tickers_info[original_ticker_name] = f"Folder creation error ({safe_os_folder_name})"
                    continue
            
            print(f"\n========= Processing Ticker: {original_ticker_name} (Data in Folder: {safe_os_folder_name}) for {report_name_for_log} =========")
            
            expected_csv_filename = f"{original_ticker_name}_{report_type_to_fetch}_merged_financials.csv"
            expected_csv_filepath = os.path.join(ticker_folder_path, expected_csv_filename)

            if os.path.exists(expected_csv_filepath):
                print(f"Skipping {original_ticker_name}: {report_name_for_log} CSV '{expected_csv_filepath}' already exists.")
                folders_processed_count +=1; successful_scrapes +=1 
                if folders_processed_count < len(target_ticker_names_from_list): time.sleep(0.2)
                continue
            
            session.headers.update(HEADERS_API) 
            sc_id = get_moneycontrol_sc_did_from_api(original_ticker_name, session) 
            time.sleep(API_REQUEST_DELAY)

            if sc_id:
                if scrape_and_save_financials_for_stock(sc_id, original_ticker_name, ticker_folder_path, session):
                    successful_scrapes += 1
                else: 
                    skipped_tickers_info[original_ticker_name] = f"Failed during {report_name_for_log} scrape (sc_id: {sc_id})"
            else:
                skipped_tickers_info[original_ticker_name] = f"sc_id lookup failed for {original_ticker_name}"
            
            folders_processed_count += 1
            if folders_processed_count < len(target_ticker_names_from_list):
                overall_delay = REQUEST_DELAY
                print(f"--- Waiting for {overall_delay:.1f}s before next stock ---")
                time.sleep(overall_delay) 

    print(f"\n--- Overall {report_name_for_log} Process Complete ---")
    print(f"Total ticker folders attempted: {folders_processed_count}")
    print(f"Successfully scraped and saved data for: {successful_scrapes} tickers.")
    if skipped_tickers_info:
        print(f"Tickers skipped or failed: {len(skipped_tickers_info)}")
        for i, (ticker, reason) in enumerate(skipped_tickers_info.items()):
            print(f"  {i+1}. {ticker} - Reason: {reason}")
    else:
        print("All attempted tickers were processed successfully or already had existing files.")

if __name__ == "__main__":
    main_folder = "stock_data"
    ticker_file = "kushal_tickers.txt" 

    try: import socks
    except ImportError: print("NOTE: PySocks library not found. This is okay as it's not currently used.")

    if not os.path.isdir(main_folder):
        print(f"Default data folder '{main_folder}' was not found. Creating it.")
        os.makedirs(main_folder, exist_ok=True)
    
    print(f"INFO: Running script without proxies for {ACTIVE_REPORT_TYPE.replace('_VI','').capitalize()} reports.")

    tickers_to_run_from_file = [] 
    if os.path.exists(ticker_file):
        try:
            with open(ticker_file, 'r', encoding='utf-8') as f: # Added encoding
                tickers_to_run_from_file = [line.strip() for line in f if line.strip()]
            if not tickers_to_run_from_file: print(f"Warning: Ticker file '{ticker_file}' is empty.")
            else: print(f"Read {len(tickers_to_run_from_file)} tickers from '{ticker_file}'.")
        except Exception as e: print(f"Error reading '{ticker_file}': {e}")
    else:
        print(f"Ticker file '{ticker_file}' not found. Exiting as ticker file is required.")
        exit()

    if not tickers_to_run_from_file: print("No tickers loaded to process. Exiting."); exit()
    
    print("\nSetting up subfolders for tickers (if they don't exist)...")
    for stock_ticker_original_name in tickers_to_run_from_file:
        safe_os_folder_name = re.sub(r'[<>:"/\\|?*&]', '_', stock_ticker_original_name) 
        if safe_os_folder_name != stock_ticker_original_name:
            print(f"   Note: Original ticker '{stock_ticker_original_name}' will use OS folder name '{safe_os_folder_name}'")
        
        ticker_specific_folder_path = os.path.join(main_folder, safe_os_folder_name)
        if not os.path.exists(ticker_specific_folder_path):
            try:
                os.makedirs(ticker_specific_folder_path)
                print(f"Created data folder: {ticker_specific_folder_path}")
            except OSError as e:
                print(f"Error creating data folder {ticker_specific_folder_path}: {e}. This might cause issues for {stock_ticker_original_name}.")
    
    print(f"\n--- RUNNING SCRIPT FOR {len(tickers_to_run_from_file)} STOCKS (from '{ticker_file}') FOR {ACTIVE_REPORT_TYPE.replace('_VI','').capitalize()} REPORTS ---")
    process_stock_folders(main_folder, stocks_to_process_tickers=tickers_to_run_from_file)