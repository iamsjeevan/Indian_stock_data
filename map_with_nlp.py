import csv
import json
import os
import requests
import time
import spacy
import re # For additional text processing if needed
from collections import defaultdict

# --- Configuration ---
ALPHA_VANTAGE_API_KEY = "XLRCIQPQOGKYSC0Z"  # <<<< PASTE YOUR ALPHA VANTAGE API KEY HERE
TICKER_INPUT_FILE = "tivker.txt" # File with one ticker per line
MANUAL_COMPANY_MAP_FILE = "ticker_company_map.csv"
SLUGS_FILE = "all_slugs.txt"
OUTPUT_JSON_FILE = "consolidated_company_slug_mappings.json"

# Alpha Vantage API settings
MAX_API_CALLS_AV_PER_RUN = 20
AV_CALL_DELAY_SECONDS = 1   # Delay between Alpha Vantage calls (5 per minute limit means 12s, 15s is safer)

# --- Load spaCy Model ---
# Global nlp object. Disable components not strictly needed if performance is critical,
# but ensure 'tagger', 'attribute_ruler' (for lemmatization) and 'ner' (if ent_type_ is used) are enabled.
print("Loading spaCy model 'en_core_web_sm'...")
try:
    # Consider disabling components if not needed, e.g.:
    # nlp = spacy.load("en_core_web_sm", disable=["parser"])
    nlp = spacy.load("en_core_web_sm") # Default: all components enabled
except OSError:
    print("spaCy model 'en_core_web_sm' not found. Please run: python -m spacy download en_core_web_sm")
    exit()
print("spaCy model loaded.")

# Cache for NLP doc objects to avoid reprocessing the same company name text multiple times
nlp_doc_cache = {}
def get_cached_nlp_doc(text):
    if text not in nlp_doc_cache:
        nlp_doc_cache[text] = nlp(text)
    return nlp_doc_cache[text]

def load_manual_company_map(filepath=MANUAL_COMPANY_MAP_FILE):
    mapping = {}
    if not os.path.exists(filepath):
        print(f"Warning: Manual company map file '{filepath}' not found.")
        return mapping
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.reader(infile)
            header = next(reader, None)
            ticker_col_idx, name_col_idx = 0, 1

            if header:
                try:
                    header_upper = [h.strip().upper() for h in header]
                    ticker_col_idx = header_upper.index("TICKER")
                    name_col_idx = header_upper.index("COMPANY_NAME")
                except ValueError:
                    print(f"Warning: Could not find 'TICKER' or 'COMPANY_NAME' in header of '{filepath}'. Assuming col 0 for ticker, 1 for name.")

            for row_num, row in enumerate(reader, 1):
                if len(row) > max(ticker_col_idx, name_col_idx):
                    ticker = row[ticker_col_idx].strip().upper()
                    company_name = row[name_col_idx].strip() # Allow empty company name
                    if ticker:
                        mapping[ticker] = company_name
                elif row: # Row exists but not enough columns
                     print(f"Warning in '{filepath}' (row {row_num+1}): Not enough columns. Expected at least {max(ticker_col_idx, name_col_idx)+1}, found {len(row)}.")
        print(f"Loaded {len(mapping)} company name mappings from '{filepath}'.")
    except Exception as e:
        print(f"Error loading manual company map from '{filepath}': {e}")
    return mapping

def get_tickers_from_file(filepath=TICKER_INPUT_FILE):
    if not os.path.exists(filepath):
        print(f"Error: Ticker list file '{filepath}' not found.")
        return []
    tickers = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                ticker_symbol = line.strip().upper()
                if ticker_symbol:
                    tickers.append(ticker_symbol)
                elif line.strip() == "": # Empty line
                    pass
                else:
                    print(f"Warning in '{filepath}' (line {line_num}): Unusual line content '{line.strip()}' ignored.")
        print(f"Read {len(tickers)} tickers from '{filepath}'.")
        if tickers: print(f"First few tickers read from file: {tickers[:5]}")
        return sorted(list(set(tickers))) # Unique and sorted
    except Exception as e:
        print(f"Error reading tickers from file '{filepath}': {e}")
    return []

def get_company_name_alpha_vantage(ticker_symbol, exchange_hint="India"):
    if not ALPHA_VANTAGE_API_KEY or ALPHA_VANTAGE_API_KEY == "YOUR_ALPHA_VANTAGE_KEY_HERE":
        return None, "NO_API_KEY" # Should be caught earlier, but good failsafe

    search_keyword = ticker_symbol
    url = f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={search_keyword}&apikey={ALPHA_VANTAGE_API_KEY}"
    
    try:
        with requests.Session() as session:
            response = session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()

        if "Note" in data or "Information" in data:
             api_message = data.get('Note') or data.get('Information', '')
             if "call frequency" in api_message.lower() or "higher Alpha Vantage API call frequency" in api_message:
                return None, "API_RATE_LIMIT"
             return None, "API_OTHER_MESSAGE"

        if "bestMatches" in data and data["bestMatches"]:
            for match_priority_type, get_match_condition in [
                ("API_SUCCESS_EXACT_REGION", lambda m: ticker_symbol == m.get("1. symbol", "").upper().split('.')[0] and exchange_hint.lower() in m.get("4. region", "").lower()),
                ("API_SUCCESS_STARTSWITH_REGION", lambda m: m.get("1. symbol", "").upper().startswith(ticker_symbol) and exchange_hint.lower() in m.get("4. region", "").lower()),
            ]:
                for match in data["bestMatches"]:
                    if get_match_condition(match):
                        return match.get("2. name"), match_priority_type
            
            # Fallback: first match if in specified region (use with caution)
            first_match = data["bestMatches"][0]
            if exchange_hint.lower() in first_match.get("4. region", "").lower():
                return first_match.get("2. name"), "API_SUCCESS_FIRST_REGION_MATCH"
            
            # Fallback: If no region match, take first overall match if ticker is exact
            first_match_overall = data["bestMatches"][0]
            if ticker_symbol == first_match_overall.get("1. symbol", "").upper().split('.')[0]:
                return first_match_overall.get("2. name"), "API_SUCCESS_EXACT_ANY_REGION"

            return None, "API_NO_SUITABLE_MATCH"
        else: # No 'bestMatches' key or it's empty
            return None, "API_NO_MATCHES_KEY"

    except requests.exceptions.HTTPError as http_err:
        return None, f"API_HTTP_ERROR_{http_err.response.status_code if http_err.response else 'UNKNOWN'}"
    except requests.exceptions.RequestException:
        return None, "API_REQUEST_ERROR"
    except json.JSONDecodeError:
        return None, "API_JSON_ERROR"

def load_slugs(filepath=SLUGS_FILE):
    if not os.path.exists(filepath):
        print(f"Error: Slugs file '{filepath}' not found.")
        return []
    slugs = set()
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                slugs.add(stripped_line)
    print(f"Loaded {len(slugs)} unique slugs from '{filepath}'.")
    return sorted(list(slugs))

def get_nlp_search_terms(ticker_symbol, company_name_str):
    search_terms = set()
    normalized_ticker = ticker_symbol.lower()
    search_terms.add(normalized_ticker)

    if company_name_str and \
       not any(company_name_str.startswith(p) for p in ["UNKNOWN_", "API_", "NO_API_KEY_", "MANUALLY_EMPTIED_"]):
        doc = get_cached_nlp_doc(company_name_str)
        company_lemmas = set()
        excluded_lemmas = {"ltd", "limited", "inc", "incorporated", "corp", "corporation",
                           "pvt", "private", "group", "fund", "etf", "index", "company",
                           "holding", "holdings", "trust","bank","india"} 
        for token in doc:
            lemma_lower = token.lemma_.lower()
            if lemma_lower and lemma_lower not in excluded_lemmas and not token.is_stop and not token.is_punct:
                if token.ent_type_ in ["ORG", "PRODUCT"] and len(lemma_lower) > 2:
                    company_lemmas.add(lemma_lower)
                elif token.pos_ in ["PROPN", "NOUN"] and len(lemma_lower) > 1:
                    company_lemmas.add(lemma_lower)
        if doc and len(doc) > 0: # First token logic
            first_token_lemma = doc[0].lemma_.lower()
            if doc[0].pos_ == "PROPN" and first_token_lemma and \
               first_token_lemma not in excluded_lemmas and len(first_token_lemma) > 2:
                company_lemmas.add(first_token_lemma)
        search_terms.update(company_lemmas)

    patterns = {"bank":4, "pharma":6, "cem":3, "fin":3, "tech":4, "info":4, "soft":4, "auto":4, "ind":3}
    for p, min_len in patterns.items():
        if p in normalized_ticker and len(normalized_ticker) > min_len:
            stripped = normalized_ticker.replace(p, "")
            if len(stripped) > 2 or (len(stripped) > 0 and stripped.isalpha()):
                is_safe_strip = True
                if p == "ind" and ("india" in normalized_ticker or "industries" in normalized_ticker or "index" in normalized_ticker):
                    if len(stripped) <=2: is_safe_strip = False # Avoid "ia" from "india" if "ind" is stripped
                if is_safe_strip:
                    search_terms.add(stripped)

    final_terms = {term for term in search_terms if len(term) > 2 or term == normalized_ticker or (len(term)>0 and term.isalnum())}
    if not final_terms and normalized_ticker:
        final_terms.add(normalized_ticker)
    return sorted(list(final_terms))


def preprocess_slugs_and_build_inverted_index(all_slugs_list, nlp_model_for_slugs):
    print("Starting slug pre-processing and inverted index building...")
    start_time = time.time()
    lemma_to_slugs_map = defaultdict(set)
    
    unique_slug_parts_text = set()
    # Maps original slug string to a list of its (lowercased) parts
    slug_to_its_parts = defaultdict(list)

    for slug_original_case in all_slugs_list:
        if not slug_original_case: continue
        # Parts are split, lowercased, and then unique parts are collected for NLP
        parts_for_this_slug = [part for part in slug_original_case.lower().split('-') if part]
        slug_to_its_parts[slug_original_case] = parts_for_this_slug # Store original slug string as key
        for part_text in parts_for_this_slug:
            unique_slug_parts_text.add(part_text) # Collect unique parts for batch NLP

    list_of_unique_parts = list(unique_slug_parts_text)
    print(f"  Collected {len(list_of_unique_parts)} unique slug parts for batch NLP.")

    # Lemmatize all unique parts using nlp.pipe
    # This maps a lowercased part_text to its set of lemmas
    part_text_to_lemmas_map = {}
    chunk_size = 10000  # Number of texts per call to nlp.pipe
    pipe_batch_size = 200 # spaCy's internal batching within nlp.pipe

    for i in range(0, len(list_of_unique_parts), chunk_size):
        current_chunk_of_parts = list_of_unique_parts[i:i+chunk_size]
        # nlp_model_for_slugs is the global nlp object
        docs = nlp_model_for_slugs.pipe(current_chunk_of_parts, batch_size=pipe_batch_size)
        for original_part_text, doc in zip(current_chunk_of_parts, docs):
            lemmas_for_this_part = set()
            for token in doc:
                # Slug lemmas are simple: non-stop, non-punct, >1 char
                if not token.is_stop and not token.is_punct and token.lemma_ and len(token.lemma_) > 1:
                    lemmas_for_this_part.add(token.lemma_.lower()) # Ensure lemma is lowercased
            part_text_to_lemmas_map[original_part_text] = lemmas_for_this_part
        
        processed_count = min(i + chunk_size, len(list_of_unique_parts))
        print(f"    Lemmatized {processed_count}/{len(list_of_unique_parts)} unique slug parts...")

    # Build the inverted index: map a lemma to set of original slug strings
    print("  Building inverted index from lemmatized slug parts...")
    for original_slug_string, list_of_part_texts in slug_to_its_parts.items():
        all_lemmas_for_this_slug = set()
        for part_text in list_of_part_texts: # part_text is already lowercased
            all_lemmas_for_this_slug.update(part_text_to_lemmas_map.get(part_text, set()))
        
        for lemma in all_lemmas_for_this_slug:
            lemma_to_slugs_map[lemma].add(original_slug_string) # Use original case slug string
    
    end_time = time.time()
    print(f"Finished slug pre-processing and inverted index building in {end_time - start_time:.2f} seconds.")
    print(f"  Inverted index contains {len(lemma_to_slugs_map)} unique lemmas mapping to slugs.")
    return dict(lemma_to_slugs_map) # Convert defaultdict to dict


def find_slugs_with_inverted_index(search_terms_list, lemma_to_slugs_idx):
    matched_slugs = set()
    # search_terms_list elements are already lemmatized and lowercased.
    # Keys in lemma_to_slugs_idx are also lowercased lemmas.
    for term in search_terms_list:
        matched_slugs.update(lemma_to_slugs_idx.get(term, set()))
    return sorted(list(matched_slugs))

def load_existing_mappings(filepath=OUTPUT_JSON_FILE):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode existing JSON from '{filepath}'. Starting fresh.")
        except Exception as e: # Catch other potential errors like permission issues
            print(f"Error loading existing mappings from '{filepath}': {e}. Starting fresh.")
    return {}

def save_mappings(data, filepath=OUTPUT_JSON_FILE):
    try:
        # Ensure the directory exists before trying to write the file
        output_dir = os.path.dirname(filepath)
        if output_dir and not os.path.exists(output_dir): # Check if output_dir is not empty string
            os.makedirs(output_dir)
            
        with open(filepath, 'w', encoding='utf-8') as f_json:
            json.dump(data, f_json, indent=4, sort_keys=True)
        print(f"\nAll mappings saved to '{filepath}'.")
    except Exception as e:
        print(f"Error saving mappings to '{filepath}': {e}")

def main():
    global ALPHA_VANTAGE_API_KEY # Allow modification
    script_start_time = time.time()

    # Prompt for API key if default or placeholder is used
    if not ALPHA_VANTAGE_API_KEY or ALPHA_VANTAGE_API_KEY == "XLRCIQPQOGKYSC0Z": # Check against your default key
        user_key_input = input(f"Alpha Vantage API Key is default or not set. Enter your key (or press Enter to skip API features): ").strip()
        if user_key_input:
            ALPHA_VANTAGE_API_KEY = user_key_input
            print("Alpha Vantage API Key accepted for this session.")
        else:
            ALPHA_VANTAGE_API_KEY = None # Explicitly set to None if user skips
            print("Alpha Vantage API calls will be skipped for new/failed tickers this session.")

    all_available_slugs_list = load_slugs()
    if not all_available_slugs_list:
        print("No slugs loaded from SLUGS_FILE. Slug matching will yield no results. Processing will continue without slug matches.")
        # Allow continuation, find_slugs_with_inverted_index will just return empty list.
        # If slugs are essential, you might want to `return` here.

    # Preprocess slugs and build inverted index (once, using the global nlp object)
    lemma_to_slugs_inverted_index = preprocess_slugs_and_build_inverted_index(all_available_slugs_list, nlp)

    tickers_from_input_file = get_tickers_from_file()
    if not tickers_from_input_file:
        print(f"No tickers found in '{TICKER_INPUT_FILE}'. Exiting.")
        return

    manual_company_data = load_manual_company_map()
    existing_mappings = load_existing_mappings() # Load previous results
    print(f"Loaded {len(existing_mappings)} existing ticker mappings from '{OUTPUT_JSON_FILE}'.")

    processed_in_this_run_count = 0 # Counts tickers whose data in JSON was actually changed/added
    av_api_calls_made_this_run = 0
    
    tickers_to_evaluate = tickers_from_input_file

    for ticker_symbol in tickers_to_evaluate:
        print(f"\n--- Evaluating Ticker: {ticker_symbol} ---")
        
        # Store a representation of the original data for this ticker to check for actual changes
        original_ticker_data_json = None
        if ticker_symbol in existing_mappings:
            original_ticker_data_json = json.dumps(existing_mappings[ticker_symbol], sort_keys=True)

        current_company_name = None
        current_name_source = "INIT" # Initial status
        
        # Try to use existing valid data from previous runs first
        previous_data_for_ticker = existing_mappings.get(ticker_symbol)
        if previous_data_for_ticker:
            prev_name_val = previous_data_for_ticker.get("company_name")
            prev_source_val = previous_data_for_ticker.get("company_name_source")
            # Check if the previous name was NOT an error/placeholder
            is_placeholder_or_error = not prev_name_val or \
                                   any(prev_name_val.startswith(p) for p in ["API_", "NO_API_KEY_", "UNKNOWN_", "MANUALLY_EMPTIED_"])
            if not is_placeholder_or_error:
                current_company_name = prev_name_val
                current_name_source = prev_source_val
                print(f"  Using provisional name from existing valid data: '{current_company_name}' (Source: {current_name_source})")

        # 1. Manual Map Override (has the highest priority for company name)
        if ticker_symbol in manual_company_data:
            manual_name_val = manual_company_data[ticker_symbol]
            # If manual map provides a name (even empty) different from current, or if source wasn't manual
            if manual_name_val != current_company_name or current_name_source != "MANUAL_MAP":
                print(f"  Applying manual map name: '{manual_name_val}'")
                current_name_source = "MANUAL_MAP"
                if not manual_name_val: # If manual map explicitly blanks the name
                    current_company_name = f"MANUALLY_EMPTIED_FOR_{ticker_symbol}"
                else:
                    current_company_name = manual_name_val


        # 2. Alpha Vantage API Call if needed
        # Needs API if: (no valid name yet OR (previous name was a retryable error AND current name not from manual map))
        needs_api_attempt = False
        if current_company_name is None : # No name from existing valid data or manual map
            needs_api_attempt = True
            print(f"  No valid company name determined yet. Will attempt API.")
        elif current_name_source != "MANUAL_MAP" and previous_data_for_ticker: # Not manual, so API might improve previous
            prev_name_for_retry_check = previous_data_for_ticker.get("company_name", "")
            # Check if previous name indicates a temporary API issue
            if any(prev_name_for_retry_check.startswith(p) for p in ["API_RATE_LIMIT_", "NO_API_KEY_", "API_HTTP_ERROR", "API_REQUEST_ERROR", "API_JSON_ERROR"]):
                needs_api_attempt = True
                print(f"  Previous name '{prev_name_for_retry_check}' suggests a retryable API issue. Will attempt API.")
        
        if needs_api_attempt:
            if ALPHA_VANTAGE_API_KEY and av_api_calls_made_this_run < MAX_API_CALLS_AV_PER_RUN:
                print(f"  Attempting Alpha Vantage (Call #{av_api_calls_made_this_run + 1} this run)...")
                fetched_name_av, av_status = get_company_name_alpha_vantage(ticker_symbol)
                av_api_calls_made_this_run += 1
                
                print(f"  Alpha Vantage raw result: Name='{fetched_name_av}', Status='{av_status}'")
                if fetched_name_av: # API returned a name
                    current_company_name = fetched_name_av
                    current_name_source = f"ALPHA_VANTAGE ({av_status})"
                else: # API call made, but no name returned or an error status from API
                    current_company_name = f"{av_status}_FOR_{ticker_symbol}"
                    current_name_source = f"ALPHA_VANTAGE_STATUS ({av_status})"
                
                if av_api_calls_made_this_run < MAX_API_CALLS_AV_PER_RUN and MAX_API_CALLS_AV_PER_RUN > 1 :
                    print(f"  Waiting {AV_CALL_DELAY_SECONDS}s before next potential AV API call...")
                    time.sleep(AV_CALL_DELAY_SECONDS)

            elif not ALPHA_VANTAGE_API_KEY and current_name_source != "MANUAL_MAP": # If API key wasn't provided and not manually set
                current_company_name = f"NO_API_KEY_FOR_{ticker_symbol}"
                current_name_source = "FALLBACK_NO_API_KEY"
                print(f"  Skipping Alpha Vantage: No API Key provided for this session.")
            elif current_name_source != "MANUAL_MAP": # API key exists but limit reached for this run
                current_company_name = f"API_RATE_LIMIT_PENDING_FOR_{ticker_symbol}"
                current_name_source = "FALLBACK_API_LIMIT_THIS_RUN"
                print(f"  Skipping Alpha Vantage for {ticker_symbol}: API call limit for this run reached.")
        
        # 3. Final Fallback for company_name if it's somehow still None after all steps
        if current_company_name is None:
            current_company_name = f"UNKNOWN_FINAL_FOR_{ticker_symbol}"
            current_name_source = "FALLBACK_UNKNOWN_FINAL"
            print(f"  Setting to fallback unknown name for '{ticker_symbol}' after all steps.")

        # Now, current_company_name and current_name_source are determined.
        # Decide if NLP terms and slugs need to be re-calculated.
        
        recalculate_nlp_slugs = False
        if ticker_symbol not in existing_mappings: # Ticker is new to our JSON file
            recalculate_nlp_slugs = True
            print("  New ticker to mappings, will calculate NLP/slugs.")
        elif previous_data_for_ticker: # Ticker exists, check if data changed
            old_entry = previous_data_for_ticker
            if old_entry.get("company_name") != current_company_name or \
               old_entry.get("company_name_source") != current_name_source:
                recalculate_nlp_slugs = True
                print(f"  Company name or source changed for '{ticker_symbol}'. Will recalculate NLP/slugs.")
            # Also recalculate if essential keys are missing (e.g. from older script versions)
            elif "nlp_derived_search_terms" not in old_entry or "matched_slugs" not in old_entry:
                recalculate_nlp_slugs = True
                print(f"  Missing NLP terms or slugs in existing data for '{ticker_symbol}'. Will recalculate.")
            else:
                print(f"  Company name and source for '{ticker_symbol}' appear unchanged, and NLP/slugs exist. No recalculation triggered.")
        else: # Should not happen if ticker_symbol in existing_mappings was false, but as a safeguard
            recalculate_nlp_slugs = True 
            print(f"  '{ticker_symbol}' was not in existing_mappings (logic check). Will calculate NLP/slugs.")


        if recalculate_nlp_slugs:
            print(f"  Recalculating NLP search terms and matching slugs for '{ticker_symbol}' using name '{current_company_name}' (Source: {current_name_source}).")
            nlp_search_terms = get_nlp_search_terms(ticker_symbol, current_company_name)
            matched_slugs = find_slugs_with_inverted_index(nlp_search_terms, lemma_to_slugs_inverted_index)

            current_ticker_final_data = {
                "company_name": current_company_name,
                "company_name_source": current_name_source,
                "nlp_derived_search_terms": nlp_search_terms,
                "matched_slugs": matched_slugs
            }
            
            # Compare new data with original to see if an update is truly needed
            current_ticker_final_data_json = json.dumps(current_ticker_final_data, sort_keys=True)

            if current_ticker_final_data_json != original_ticker_data_json:
                existing_mappings[ticker_symbol] = current_ticker_final_data
                processed_in_this_run_count += 1
                print(f"  Data for '{ticker_symbol}' was updated in mappings.")
            else:
                # This implies recalculation resulted in the exact same data as before.
                # If it was a new ticker (original_ticker_data_json was None), it still needs to be added.
                if original_ticker_data_json is None and ticker_symbol not in existing_mappings : # Ticker was new
                    existing_mappings[ticker_symbol] = current_ticker_final_data # Add it
                    processed_in_this_run_count += 1
                    print(f"  Data for new ticker '{ticker_symbol}' was calculated and added to mappings.")
                else:
                    print(f"  Recalculation for '{ticker_symbol}' resulted in identical data to stored. No change recorded in mappings.")
            
            # Print details of derived terms and slugs if recalculated
            print(f"    Derived NLP search terms: {nlp_search_terms}")
            print(f"    Found {len(matched_slugs)} related slugs." + (f" First 5: {matched_slugs[:5]}..." if matched_slugs else ""))

        else: # No recalculation needed
            print(f"  Skipping NLP/slug recalculation for '{ticker_symbol}', data is considered up-to-date from previous run.")

    # Save all mappings at the end if any changes were made or if the file is new
    if processed_in_this_run_count > 0 :
        print(f"\nProcessed or updated data for {processed_in_this_run_count} tickers in this run.")
        save_mappings(existing_mappings, OUTPUT_JSON_FILE)
    # Save even if no processing, if the output file didn't exist but we had tickers (to create initial file)
    elif not os.path.exists(OUTPUT_JSON_FILE) and tickers_from_input_file : 
        print(f"\nOutput file '{OUTPUT_JSON_FILE}' did not exist. Saving current state of all evaluated tickers.")
        save_mappings(existing_mappings, OUTPUT_JSON_FILE) # This will save all tickers evaluated, even if "unchanged" from a blank slate
    else:
        print("\nNo tickers required data updates in this run. Output file remains unchanged if it already existed and was up-to-date.")

    if ALPHA_VANTAGE_API_KEY and av_api_calls_made_this_run >= MAX_API_CALLS_AV_PER_RUN:
        print(f"\nWarning: Reached the Alpha Vantage API call limit for this run ({MAX_API_CALLS_AV_PER_RUN}).")
        print("  Some tickers might have placeholder names like 'API_RATE_LIMIT_PENDING_FOR_...'.")
        print("  Re-run the script later to process these remaining tickers that might need API calls.")
    
    script_end_time = time.time()
    print(f"\nScript finished in {script_end_time - script_start_time:.2f} seconds.")

if __name__ == "__main__":
    main()