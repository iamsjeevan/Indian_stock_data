import json
import os
# Note: Other imports like requests, time, spacy, bs4, dateutil, vaderSentiment
# will be imported inside the functions where they are first needed by a worker process
# or in the main thread context if used there first (like for pre-lemmatization).
from joblib import Parallel, delayed
import multiprocessing
import time


# --- Configuration (GLOBAL SCOPE) ---
INPUT_MAPPINGS_FILE = "consolidated_company_slug_mappings.json"
OUTPUT_ROOT_DIR = "stock_data" 
BASE_SLUG_URL_PREFIX = "https://www.financialexpress.com/about/"
SLUGS_FILE = "all_slugs.txt" # Now globally defined
OUTPUT_JSON_FILE = "consolidated_company_slug_mappings.json" # For load_existing_mappings

REQUEST_DELAY_SECONDS = 0.1 
MAX_PAGES_PER_SLUG_TEST = 3   
MAX_ARTICLE_AGE_YEARS = 5
MAX_TICKERS_TO_TEST = 10 

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}

# --- Global Variables for Workers ---
nlp_worker = None 
sentiment_analyzer_worker = None


def init_worker_all():
    import spacy 
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    global nlp_worker, sentiment_analyzer_worker
    if nlp_worker is None:
        nlp_worker = spacy.load("en_core_web_sm", disable=["parser"])
    if sentiment_analyzer_worker is None:
        sentiment_analyzer_worker = SentimentIntensityAnalyzer()
    return nlp_worker, sentiment_analyzer_worker

def load_json_file(filepath):
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception as e:
        print(f"Error loading '{filepath}': {e}")
        return None

def fetch_page_content_with_retries(url, retries=2, delay=1):
    import requests 
    import time     
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            if "/page/" in url and response.url != url:
                base_of_paginated_url = url.split("/page/")[0].rstrip('/') + "/"
                current_response_url_norm = response.url.rstrip('/') + "/"
                if current_response_url_norm == base_of_paginated_url or response.status_code == 404:
                    return None, 404 
            response.raise_for_status()
            return response.text, response.status_code
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404: return None, 404
            if attempt == retries - 1: return None, e.response.status_code
        except requests.exceptions.RequestException:
            if attempt == retries - 1: return None, 0 
        time.sleep(delay)
    return None, 0

def parse_article_date(date_string):
    from dateutil.parser import parse as parse_date 
    if not date_string: return None
    try:
        date_string_cleaned = date_string.replace(" IST", "").replace("IST", "").strip()
        dt_aware = parse_date(date_string_cleaned)
        return dt_aware.replace(tzinfo=None) if dt_aware.tzinfo else dt_aware
    except: return None

def get_sentiment(text, analyzer):
    if not text or not analyzer: return 0.0, "NEUTRAL"
    vs = analyzer.polarity_scores(text)
    compound_score = vs['compound']
    if compound_score >= 0.05: return compound_score, "POSITIVE"
    elif compound_score <= -0.05: return compound_score, "NEGATIVE"
    else: return compound_score, "NEUTRAL"

def extract_headlines_dates_and_sentiment(html_content, base_page_url_for_relative_links, current_slug, sentiment_analyzer_instance):
    from bs4 import BeautifulSoup 
    from urllib.parse import urljoin
    from datetime import datetime, timedelta

    items = []
    oldest_article_date_on_page = None
    five_years_ago = datetime.now() - timedelta(days=MAX_ARTICLE_AGE_YEARS * 365.25)

    if not html_content: return items, oldest_article_date_on_page
    soup = BeautifulSoup(html_content, 'html.parser')
    
    article_tags = []
    main_content_area = soup.find('div', class_=lambda c: c and 'ie-network-grid__lhs' in c)
    if main_content_area:
        story_blocks = main_content_area.find_all('div', class_=lambda c: c and 'wp-block-newspack-blocks-ie-stories' in c)
        for block in story_blocks:
            article_tags.extend(block.find_all('article'))
    if not article_tags: article_tags = soup.find_all('article')

    for article_tag in article_tags:
        headline_text, headline_link, article_date_str = None, None, None
        title_tag = article_tag.find(['div', 'h3'], class_='entry-title') or article_tag.find(['h1','h2','h3','h4'])
        if title_tag:
            link_tag = title_tag.find('a')
            if link_tag and link_tag.get_text(strip=True):
                headline_text = link_tag.get_text(strip=True)
                headline_link = link_tag.get('href')
        date_tag = article_tag.find('time', class_='entry-date')
        if date_tag: article_date_str = date_tag.get('datetime') or date_tag.get_text(strip=True)

        if headline_text and headline_link:
            if headline_link and not headline_link.startswith(('http://', 'https://')):
                headline_link = urljoin(base_page_url_for_relative_links, headline_link)
            
            parsed_date_obj = parse_article_date(article_date_str)
            if parsed_date_obj:
                if parsed_date_obj < five_years_ago:
                    oldest_article_date_on_page = parsed_date_obj
                    break 
                
                sentiment_score, sentiment_label = get_sentiment(headline_text, sentiment_analyzer_instance)
                items.append({
                    "headline": headline_text,
                    "link": headline_link,
                    "publication_date": parsed_date_obj.strftime('%Y-%m-%d %H:%M:%S'),
                    "sentiment_score": f"{sentiment_score:.4f}",
                    "sentiment_label": sentiment_label,
                    "source_slug": current_slug
                })
                if oldest_article_date_on_page is None or parsed_date_obj < oldest_article_date_on_page:
                    oldest_article_date_on_page = parsed_date_obj
    return items, oldest_article_date_on_page

def lemmatize_slug_parts_worker(slug, nlp_instance): 
    lemmatized_parts = set()
    try:
        if not isinstance(slug, str): return lemmatized_parts
        doc = nlp_instance(slug.lower())
        for token in doc:
            if not token.is_stop and not token.is_punct:
                lemmatized_parts.add(token.lemma_)
    except Exception as e:
        print(f"Error lemmatizing slug part '{str(slug)[:50]}...': {e}")
    return lemmatized_parts

def load_slugs(filepath=SLUGS_FILE): # SLUGS_FILE is now globally defined
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
        import spacy 
        main_thread_nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"]) 
    except Exception as e:
        print(f"CRITICAL: Error loading spaCy for main thread pre-lemmatization: {e}.")
    
    if main_thread_nlp:
        for slug_idx, slug_text in enumerate(slugs):
            if slug_text:
                 slug_lemmatized_map[slug_text] = lemmatize_slug_parts_worker(slug_text, main_thread_nlp)
            if (slug_idx + 1) % 10000 == 0: 
                print(f"  Pre-lemmatized {slug_idx + 1}/{len(slugs)} slugs...")
        print("Slug pre-lemmatization complete.")
    else:
        print("Skipping slug pre-lemmatization due to spaCy load failure in main thread.")
    return slugs, slug_lemmatized_map

def scrape_all_pages_for_slug(slug_info_tuple, shared_slug_cache):
    import time 
    from datetime import datetime, timedelta 
    slug, base_slug_url_for_slug_param = slug_info_tuple
    if slug in shared_slug_cache:
        return slug, shared_slug_cache[slug]

    _, sentiment_analyzer_instance_worker = init_worker_all()
    all_headlines_for_this_slug = []
    current_page = 1
    # MAX_ARTICLE_AGE_YEARS is global
    five_years_ago = datetime.now() - timedelta(days=MAX_ARTICLE_AGE_YEARS * 365.25) 

    while current_page <= MAX_PAGES_PER_SLUG_TEST:
        page_url = f"{base_slug_url_for_slug_param}page/{current_page}/" if current_page > 1 else base_slug_url_for_slug_param
        html_content, status_code = fetch_page_content_with_retries(page_url)
        if status_code == 404 or html_content is None: break

        page_items, oldest_date_on_page = extract_headlines_dates_and_sentiment(
            html_content, base_slug_url_for_slug_param, slug, sentiment_analyzer_instance_worker
        )
        if not page_items and current_page > 1: break
        all_headlines_for_this_slug.extend(page_items)
        if oldest_date_on_page and oldest_date_on_page < five_years_ago: break
        current_page += 1
        time.sleep(REQUEST_DELAY_SECONDS)
    
    if all_headlines_for_this_slug: shared_slug_cache[slug] = all_headlines_for_this_slug 
    return slug, all_headlines_for_this_slug

def process_ticker_and_print(ticker_task_data, shared_slug_cache_param, all_original_slugs_global_unused, pre_lemmatized_slug_map_global_unused):
    # The last two arguments are passed by Parallel but not used in this simplified test logic
    # because headline extraction doesn't use pre-lemmatized slugs directly for matching in this flow.
    from urllib.parse import urljoin # Ensure urljoin is available in worker

    ticker, company_data = ticker_task_data
    print(f"\n--- Processing Ticker: {ticker} (Company: {company_data.get('company_name', 'N/A')}) ---")
    
    _ , sentiment_analyzer_instance_worker = init_worker_all() 

    ticker_headlines_aggregated = []
    unique_slugs_for_ticker = set(company_data.get("matched_slugs", []))
    
    for slug_name in unique_slugs_for_ticker:
        print(f"  Handling slug: '{slug_name}' for ticker '{ticker}'")
        base_url_for_slug = urljoin(BASE_SLUG_URL_PREFIX, f"{slug_name}/")
        
        if slug_name in shared_slug_cache_param:
            print(f"    Retrieving '{slug_name}' from cache.")
            slug_specific_headlines = shared_slug_cache_param[slug_name]
        else:
            print(f"    '{slug_name}' not in cache, scraping...")
            _, slug_specific_headlines = scrape_all_pages_for_slug(
                (slug_name, base_url_for_slug), 
                shared_slug_cache_param
            )
        
        if slug_specific_headlines:
            ticker_headlines_aggregated.extend(slug_specific_headlines)

    print(f"\n--- Headlines for Ticker: {ticker} (Total: {len(ticker_headlines_aggregated)}) ---")
    if ticker_headlines_aggregated:
        for i, item in enumerate(ticker_headlines_aggregated):
            print(f"  {i+1}. Headline: {item['headline']}")
            print(f"     Link: {item['link']}")
            print(f"     Date: {item.get('publication_date', 'N/A')}")
            print(f"     Sentiment: {item.get('sentiment_label', 'N/A')} ({item.get('sentiment_score', 'N/A')})")
            print(f"     Source Slug: {item['source_slug']}")
    else:
        print(f"  No headlines found for ticker {ticker} from its matched slugs.")
    
    return ticker, ticker_headlines_aggregated

def load_existing_mappings(filepath=OUTPUT_JSON_FILE): # OUTPUT_JSON_FILE is now global
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f: return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode existing JSON from '{filepath}'. Starting fresh.")
    return {}

def main():
    global nlp_worker, sentiment_analyzer_worker
    nlp_worker = None 
    sentiment_analyzer_worker = None 

    company_mappings = load_json_file(INPUT_MAPPINGS_FILE)
    if not company_mappings:
        print(f"Could not load mappings. Exiting.")
        return

    all_original_slugs, _ = load_slugs() # Load slugs for context, though map not directly used by simplified worker

    with multiprocessing.Manager() as manager:
        shared_slug_cache = manager.dict()
        
        ticker_processing_tasks_data = []
        count = 0
        for ticker, data in company_mappings.items():
            if count >= MAX_TICKERS_TO_TEST:
                break
            ticker_processing_tasks_data.append((ticker, data))
            count += 1

        if not ticker_processing_tasks_data:
            print("No tickers selected for processing.")
            return

        num_cores = multiprocessing.cpu_count()
        effective_jobs = min(num_cores, len(ticker_processing_tasks_data))
        
        print(f"\nStarting parallel processing for {len(ticker_processing_tasks_data)} tickers using up to {effective_jobs} cores...")
        
        _ = Parallel(n_jobs=effective_jobs, backend='loky', verbose=5)(
            delayed(process_ticker_and_print)(
                ticker_task_tuple, 
                shared_slug_cache,
                all_original_slugs, 
                {} 
            ) for ticker_task_tuple in ticker_processing_tasks_data
        )
        
        print("\n--- All Test Processing Complete ---")

if __name__ == "__main__":
    multiprocessing.freeze_support() 
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\nTotal execution time for test: {end_time - start_time:.2f} seconds.")