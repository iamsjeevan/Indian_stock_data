import json
import os
import time
import spacy
import re
from joblib import Parallel, delayed
import multiprocessing

# --- Configuration ---
TICKER_AND_COMPANY_INPUT_FILE = "tivker.txt"
SLUGS_FILE = "all_slugs.txt"
OUTPUT_JSON_FILE = "consolidated_company_slug_mappings.json"

nlp_worker = None

# Terms to heavily deprioritize or exclude as standalone search keywords from company names
# and to be cautious about when matching slugs.
# In your main mapping script (e.g., map_with_nlp.py)

# Updated list based on data-driven analysis and your feedback
HIGHLY_GENERIC_TERMS = {
    # From your data-driven list (excluding specific company group names)
    "bank",
    "bharat", # Often part of PSU names, can be generic without full context
    "chemicals",
    "energy",
    "engineering",
    "enterprises", # Can be generic
    "finance",
    "financial",
    "gas",
    "gujarat", # State name, can be generic if not part of specific company identifier
    "hindustan", # Often part of PSU names, can be generic
    "housing",
    "india",     # Already highly prioritized for exclusion as standalone
    "indian",    # Already highly prioritized for exclusion as standalone
    "industries",
    "insurance",
    "international",
    "laboratories",
    "life",      # "Life Insurance Corporation" - "life" can be generic
    # "ltd.", # This should be handled by MANUAL_EXCLUDED_LEMMAS as it's a suffix, not a keyword
    "management",
    "pharma",
    "pharmaceuticals", # or just "pharmaceutical" (lemma)
    "power",
    "services",
    "systems",
    "technologies", # or "technology" (lemma)

    # Adding a few more from the initial list that are generally good to keep as generic
    "limited",   # Should be in MANUAL_EXCLUDED_LEMMAS
    "pvt",       # Should be in MANUAL_EXCLUDED_LEMMAS
    "private",   # Should be in MANUAL_EXCLUDED_LEMMAS
    "inc",       # Should be in MANUAL_EXCLUDED_LEMMAS
    "corp",      # Should be in MANUAL_EXCLUDED_LEMMAS
    "company",   # Should be in MANUAL_EXCLUDED_LEMMAS
    "group",     # Can be generic, but also part of specific names. Context is key. Let's keep it here.
    "holdings",
    "investment",
    "capital",
    "solutions",
    "global",
    "enterprise",
    "consulting",
    "ventures",
    "construction",
    "projects",
    "infra", "infrastructure",
    "automotive", "motor", "motors", # "auto" is often the ticker part
    "electric",
    "metals",
    "textiles", "mills",
    "paper",
    "sugar",
    "cement",
    "realty", "properties",
    "food", "products", "agro",
    "logistics", "shipping",
    "media", "communications", "telecom",
    "consumer", "retail",
     # "state" by itself is generic. "State Bank" needs to be handled as a phrase.
    "national", "federal", "central","small", "large", "medium", "big","chemicals","wealth","star","coal","technology","chennai","mumbai","delhi","bengaluru","bangalore","karnataka","maharashtra","tamilnadu","gujarat","rajasthan","punjab","haryana","uttarpradesh",
    "petroleum","industrials","agriculture","agri","agro","fertilizers","world","global","international","asia","africa","europe","americas","north","south","east","west",
    "services","solutions","systems","products","technologies","ventures","holdings","capital","investments","partners","group","enterprise","consulting","management",
    "financial","finance","banking","insurance","healthcare","hospitality","realestate","realty","properties","infrastructure","construction","engineering","manufacturing",
    "transportation","logistics","shipping","distribution","retail","wholesale","consumer","food","beverages","agriculture","agri","fertilizers",
    "chemicals","pharmaceuticals","healthcare","biotechnology","medical","devices","equipment","supplies","services","consulting","research","development",
    "education","training","e-learning","media","entertainment","telecommunications","telecom","broadcasting","publishing","advertising",
    "marketing","digital","online","internet","software","hardware","cloud","data","analytics","cybersecurity",
    "artificial","intelligence","machine","learning","blockchain","cryptocurrency","fintech","insurtech","proptech","edtech",
    "agritech","healthtech","medtech","cleantech","greentech","renewable","energy","solar","wind","hydro",
    "biomass","geothermal","nuclear","oil","gas","petroleum","mining","metals","minerals",
    "steel","aluminium","copper","zinc","lead","nickel","gold","silver",
    "platinum","palladium","diamonds","precious","stones","jewellery",
    "textiles","apparel","clothing","fashion","footwear","accessories",
    "automotive","motor","vehicles","transportation","logistics","shipping",
    "distribution","retail","wholesale","consumer","goods","services",
    "hospitality","travel","tourism","leisure","entertainment","mobility",
    "health","wellness","fitness","nutrition","food","beverages",
    "science","technology","engineering","research","development","ashok","asian"
}

# Lemmatized versions of common suffixes/parts to ALWAYS exclude from company name processing
# This list is for structural parts of names, not keywords.
EXCLUDED_LEMMAS_FROM_COMPANY = {
    "ltd", "limited", "inc", "incorporated", "corp", "corporation", 
    "pvt", "private", "co", # "company" is already above, "co" for "Co."
    "&", "of", "the", "and", "in", "for", "on", "at", "to", "with", "by",
    "is", "are", "be", "was", "were",
    # These were identified as very frequent suffixes/parts by your analysis script
    # and are not keywords themselves.
    "group", # Reconsidering: "group" is often part of the actual name like "Adani Group"
             # If we exclude "group" here, "Adani Group" becomes "Adani".
             # Let's keep "group" out of MANUAL_EXCLUDED_LEMMAS and let HIGHLY_GENERIC_TERMS
             # and phrase detection handle it. If "group" is the ONLY remaining word, it's generic.
             # If it's "Adani Group", "adani-group" should be a specific search term.
    "fund", "etf", "index", "holding", "trust", "plc", "llp", "llc"
}


def init_worker_nlp():
    global nlp_worker
    if nlp_worker is None:
        # print(f"Initializing spaCy for worker (PID: {os.getpid()}). Disabling parser, ner.")
        nlp_worker = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    return nlp_worker

def load_tickers_and_companies_from_single_file(filepath=TICKER_AND_COMPANY_INPUT_FILE):
    # ... (same as your last version)
    data_pairs = []
    if not os.path.exists(filepath):
        print(f"Error: Input file '{filepath}' not found.")
        return data_pairs
    print(f"Loading tickers and company names from '{filepath}'...")
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as infile:
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split(':', 1)
                if len(parts) == 2:
                    ticker = parts[0].strip().upper()
                    company_name = parts[1].strip()
                    if ticker and company_name: data_pairs.append((ticker, company_name))
                    else: print(f"Warning in '{filepath}' (line {line_num}): Invalid format: '{line}'")
                else: print(f"Warning in '{filepath}' (line {line_num}): Could not parse: '{line}'")
        print(f"Loaded {len(data_pairs)} ticker-company pairs.")
        if data_pairs: print(f"First few pairs: {data_pairs[:3]}")
    except Exception as e:
        print(f"Error loading data from '{filepath}': {e}")
    return data_pairs


def load_slugs(filepath=SLUGS_FILE):
    # ... (same as your last version, with main_thread_nlp for pre-lemmatization)
    if not os.path.exists(filepath):
        print(f"Error: Slugs file '{filepath}' not found.")
        return [], {} 
    slugs = []
    with open(filepath, 'r', encoding='utf-8') as f:
        slugs = [line.strip() for line in f if line.strip()]
    
    print(f"Pre-lemmatizing {len(slugs)} slugs (main thread)...")
    slug_lemmatized_map = {}
    main_thread_nlp = None
    try:
        # print("Main thread loading spaCy instance for pre-lemmatization...")
        main_thread_nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    except Exception as e:
        print(f"CRITICAL: Error loading spaCy for main thread pre-lemmatization: {e}. Slug matching will be impaired.")
        # If spaCy fails to load here, pre-lemmatization won't happen, which will affect matching.
        # We could choose to exit or continue with degraded functionality.
        # For now, it will continue, and slug_lemmatized_map will be empty or partially filled if error is intermittent.
        # However, the worker processes still try to load spaCy.

    if main_thread_nlp: # Proceed only if spaCy loaded
        for slug_idx, slug_text in enumerate(slugs):
            if slug_text:
                 slug_lemmatized_map[slug_text] = lemmatize_slug_parts_worker(slug_text, main_thread_nlp)
            if (slug_idx + 1) % 10000 == 0: # Increased progress interval
                print(f"  Pre-lemmatized {slug_idx + 1}/{len(slugs)} slugs...")
        print("Slug pre-lemmatization complete.")
    else:
        print("Skipping slug pre-lemmatization due to spaCy load failure in main thread.")

    return slugs, slug_lemmatized_map


def get_nlp_search_terms_worker(ticker_symbol, company_name, nlp_instance):
    search_terms = set()
    normalized_ticker = ticker_symbol.lower()
    search_terms.add(normalized_ticker) # Ticker is ALWAYS a primary search term

    # Process company name to find *specific, non-generic* keywords
    if company_name and not company_name.startswith("UNKNOWN_"):
        doc = nlp_instance(company_name)
        company_specific_lemmas = set()
        
        # Extract potential multi-word entities (proper nouns, orgs)
        # This helps capture things like "State Bank" together if it's an entity
        for ent in doc.ents:
            if ent.label_ == "ORG" or (ent.root.pos_ == "PROPN" and len(ent.text.split()) > 1):
                ent_lemma = ent.lemma_.lower()
                # Check if the whole entity text is not just generic terms
                is_ent_generic = all(token.lemma_.lower() in HIGHLY_GENERIC_TERMS for token in ent if not token.is_punct)
                if not is_ent_generic and len(ent_lemma) > 2:
                    company_specific_lemmas.add(ent_lemma.replace(" ", "-")) # for slug-like matching
                    # also add individual non-generic parts of the entity
                    for token in ent:
                        if not token.is_punct and token.lemma_.lower() not in EXCLUDED_LEMMAS_FROM_COMPANY and \
                           token.lemma_.lower() not in HIGHLY_GENERIC_TERMS and len(token.lemma_) > 2:
                            company_specific_lemmas.add(token.lemma_.lower())


        # Extract individual specific lemmas (nouns, proper nouns)
        for token in doc:
            lemma_lower = token.lemma_.lower()
            # Is it a specific, non-excluded, non-highly-generic term?
            if not token.is_stop and not token.is_punct and \
               lemma_lower not in EXCLUDED_LEMMAS_FROM_COMPANY and \
               lemma_lower not in HIGHLY_GENERIC_TERMS:
                
                if token.pos_ in ["NOUN", "PROPN"] or \
                   (len(lemma_lower) > 2 and token.ent_type_ in ["ORG", "PRODUCT"]):
                    company_specific_lemmas.add(lemma_lower)
        
        search_terms.update(company_specific_lemmas)

    # Ticker patterns (remove parts like "bank", "ind" if the remainder is specific)
    ticker_patterns = {"bank":4, "pharma":6, "cem":3, "fin":3, "tech":4, "info":4, "soft":4, "auto":4, "ind":3, "infra":5, "power":5, "steel":5}
    for p_key, min_len in ticker_patterns.items():
        if p_key in normalized_ticker and len(normalized_ticker) > min_len:
            stripped = normalized_ticker.replace(p_key, "")
            # Add stripped part ONLY if it's not generic and is of reasonable length
            if stripped and (len(stripped) > 1 or (stripped.isalpha() and stripped != normalized_ticker)) and \
               stripped not in HIGHLY_GENERIC_TERMS:
                 search_terms.add(stripped)
            # Special handling for "ind" if it results in a generic term like "ia" from "india"
            if p_key == "ind" and stripped in HIGHLY_GENERIC_TERMS and ("india" in normalized_ticker or "industries" in normalized_ticker):
                if stripped in search_terms: search_terms.remove(stripped)


    # Final cleanup: ensure terms are useful length, and ticker is always present
    final_terms = {term for term in search_terms if len(term) > 2 or term == normalized_ticker}
    if not final_terms: # Should not happen if ticker is always added
        final_terms = {normalized_ticker}
    
    # If the only terms beyond the ticker are highly generic, remove them.
    # This forces matching to rely on the ticker or more specific parts of the company name (if any were found).
    non_ticker_terms = final_terms - {normalized_ticker}
    if non_ticker_terms and all(term in HIGHLY_GENERIC_TERMS for term in non_ticker_terms):
        # print(f"    INFO [{ticker_symbol}]: Derived search terms were too generic; defaulting to ticker primarily.")
        final_terms = {normalized_ticker}
        # Optionally add back a very few, less generic terms if needed, but ticker is key.

    return sorted(list(final_terms))


def lemmatize_slug_parts_worker(slug, nlp_instance):
    # ... (same as your last version)
    lemmatized_parts = set()
    try:
        # Ensure slug is a string before processing
        if not isinstance(slug, str):
            # print(f"Warning: Non-string slug received: {slug} (type: {type(slug)}). Skipping lemmatization.")
            return lemmatized_parts
        doc = nlp_instance(slug.lower())
        for token in doc:
            if not token.is_stop and not token.is_punct:
                lemmatized_parts.add(token.lemma_)
    except Exception as e:
        print(f"Error lemmatizing slug part '{str(slug)[:50]}...': {e}")
    return lemmatized_parts


def find_and_match_slugs_nlp_worker(search_terms, original_slugs_list, pre_lemmatized_slug_map, ticker_symbol):
    matched_slugs_set = set()
    normalized_ticker = ticker_symbol.lower()
    
    # Separate search terms: ticker, specific company terms, and (de-prioritized) generic terms
    specific_company_terms = set(search_terms) - {normalized_ticker} - HIGHLY_GENERIC_TERMS
    # generic_company_terms = set(search_terms) & HIGHLY_GENERIC_TERMS # We won't use these directly for matching

    # print(f"    DEBUG [{ticker_symbol}]: Effective specific search terms: {specific_company_terms}")

    for original_slug in original_slugs_list:
        slug_lemmatized_parts = pre_lemmatized_slug_map.get(original_slug, set())
        if not slug_lemmatized_parts: continue

        # --- Strict Matching Logic ---
        # Rule 1: Direct ticker match in slug (highest priority)
        if normalized_ticker in slug_lemmatized_parts:
            matched_slugs_set.add(original_slug)
            # print(f"    MATCH [{ticker_symbol}]: Slug '{original_slug}' via TICKER.")
            continue

        # Rule 2: Match on specific (non-generic) company-derived terms.
        # The slug MUST contain at least one of these specific terms.
        if specific_company_terms: # Only if we have specific terms to search for
            found_specific_term_in_slug = False
            for spec_term in specific_company_terms:
                if spec_term in slug_lemmatized_parts:
                    found_specific_term_in_slug = True
                    break
            
            if found_specific_term_in_slug:
                # If a specific term is found, we can be a bit more lenient if the ticker is also there,
                # or if the slug isn't overly generic.
                # For now, any specific term match is a strong signal.
                matched_slugs_set.add(original_slug)
                # print(f"    MATCH [{ticker_symbol}]: Slug '{original_slug}' via SPECIFIC_TERM.")
                continue
        
        # Rule 3 (Optional, very conservative): Match if slug contains ticker AND a non-highly-generic company name part.
        # This is mostly covered if the specific_company_terms are generated well.
        # We are intentionally NOT matching slugs that *only* contain "india" or "finance" if those
        # were the only search terms beyond the ticker, UNLESS the slug also contains the ticker.

    # Print matched slugs (moved to here from inside process_ticker_task for consolidated printing)
    # This will be called once per ticker by the worker.
    if matched_slugs_set:
        print(f"  --- Matched Slugs for {ticker_symbol} ({len(matched_slugs_set)} found): ---")
        sorted_slugs = sorted(list(matched_slugs_set))
        for s_idx, m_slug in enumerate(sorted_slugs):
            print(f"    {s_idx + 1}. {m_slug}")
            if s_idx >= 9 and len(sorted_slugs) > 10: # Print max 10, then summary
                print(f"    ... and {len(sorted_slugs) - 10} more slugs.")
                break
        print(f"  --- End Matched Slugs for {ticker_symbol} ---")
    # else:
        # print(f"  --- No slugs matched for {ticker_symbol} with current criteria. ---")


    return sorted(list(matched_slugs_set))


def process_ticker_task(ticker_data_tuple, all_original_slugs_global, pre_lemmatized_slug_map_global):
    # ... (same as your last version)
    ticker, company_name, name_source = ticker_data_tuple
    local_nlp = init_worker_nlp()
    
    # print(f"Worker (PID {os.getpid()}) processing: {ticker}")
    nlp_derived_search_terms = get_nlp_search_terms_worker(ticker, company_name, local_nlp)
    # print(f"    DEBUG [{ticker}]: Generated NLP Search Terms: {nlp_derived_search_terms}")

    matched_slugs_for_ticker = find_and_match_slugs_nlp_worker(
        nlp_derived_search_terms, 
        all_original_slugs_global,
        pre_lemmatized_slug_map_global,
        ticker
    )
    result = {
        "company_name": company_name,
        "company_name_source": name_source,
        "nlp_derived_search_terms": nlp_derived_search_terms,
        "matched_slugs": matched_slugs_for_ticker
    }
    return ticker, result

def load_existing_mappings(filepath=OUTPUT_JSON_FILE):
    # ... (same as your last version)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f: return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode existing JSON from '{filepath}'. Starting fresh.")
    return {}

def save_mappings(data, filepath=OUTPUT_JSON_FILE):
    # ... (same as your last version)
    try:
        with open(filepath, 'w', encoding='utf-8') as f_json:
            json.dump(data, f_json, indent=4, sort_keys=True)
        print(f"\nAll mappings saved to '{filepath}'.")
    except Exception as e:
        print(f"Error saving mappings to '{filepath}': {e}")


def main():
    # ... (same as your last version, ensure nlp_worker=None at start)
    global nlp_worker 
    nlp_worker = None 

    all_original_slugs, pre_lemmatized_slug_map_global = load_slugs()
    if not all_original_slugs:
        print("No slugs loaded. Exiting.")
        return

    ticker_company_pairs = load_tickers_and_companies_from_single_file()
    if not ticker_company_pairs:
        print(f"No ticker-company pairs loaded. Exiting.")
        return

    existing_mappings = load_existing_mappings()
    print(f"Loaded {len(existing_mappings)} existing mappings.")

    tasks_for_parallel = []
    for ticker, company_name in ticker_company_pairs:
        name_source = "DIRECT_INPUT_FILE"
        tasks_for_parallel.append( (ticker, company_name, name_source) )

    if not tasks_for_parallel:
        print("No tasks to process.")
        if not os.path.exists(OUTPUT_JSON_FILE) and not existing_mappings :
            save_mappings(existing_mappings)
        return

    num_cores = multiprocessing.cpu_count()
    # Set n_jobs to 1 if you want to debug without parallelism, otherwise num_cores
    # effective_jobs = 1 # FOR DEBUGGING
    effective_jobs = num_cores
    print(f"\nStarting processing for {len(tasks_for_parallel)} tasks using up to {effective_jobs} cores...")
    
    results = Parallel(n_jobs=effective_jobs, backend='loky', verbose=5)(
        delayed(process_ticker_task)(
            task_data_tuple, all_original_slugs, pre_lemmatized_slug_map_global
        ) for task_data_tuple in tasks_for_parallel
    )
    
    print("\nMerging results...")
    updated_or_new_mappings_count = 0
    final_mappings = existing_mappings.copy() # Start with old, update with new/processed

    for ticker_key, processed_data in results:
        is_new_or_updated = False
        if ticker_key not in final_mappings or \
           final_mappings[ticker_key].get("company_name") != processed_data["company_name"] or \
           final_mappings[ticker_key].get("company_name_source") != processed_data["company_name_source"] or \
           set(final_mappings[ticker_key].get("nlp_derived_search_terms", [])) != set(processed_data["nlp_derived_search_terms"]) or \
           set(final_mappings[ticker_key].get("matched_slugs", [])) != set(processed_data["matched_slugs"]):
            is_new_or_updated = True
        
        if is_new_or_updated:
            updated_or_new_mappings_count += 1
        
        final_mappings[ticker_key] = processed_data

    if updated_or_new_mappings_count > 0 or not os.path.exists(OUTPUT_JSON_FILE):
        print(f"{updated_or_new_mappings_count} entries processed/updated.")
        save_mappings(final_mappings)
    else:
        print("No data changed. Output file remains the same.")


if __name__ == "__main__":
    multiprocessing.freeze_support() 
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds.")