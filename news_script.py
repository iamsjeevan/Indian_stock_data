import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse as parse_date
import time
import json
import os # For directory and file path operations
import csv # For writing data to CSV files

# --- For Sentiment Analysis (VADER) ---
# --- For Sentiment Analysis (VADER) ---
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Ensure NLTK's VADER lexicon is downloaded.
# This will run only once if the lexicon isn't present.
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError: # <--- CHANGE THIS LINE
    print("[NLTK] Downloading 'vader_lexicon' for sentiment analysis...")
    nltk.download('vader_lexicon')
    print("[NLTK] 'vader_lexicon' downloaded successfully.")

# Initialize VADER sentiment analyzer globally
sentiment_analyzer = SentimentIntensityAnalyzer()

# --- Configuration ---
# Path to your JSON file containing company slug mappings
COMPANIES_JSON_FILE = "/workspaces/Indian_stock_data/consolidated_company_slug_mappings.json"

# Base directory where output CSVs will be saved
OUTPUT_BASE_DIR = "stock_data" 

BASE_SLUG_URL_PREFIX = "https://www.financialexpress.com/about/"
MAX_PAGES_TO_TEST = 50  # Max pages to attempt for each slug (adjust as needed)
MAX_ARTICLE_AGE_YEARS = 5 # Articles older than this will be filtered out and can stop pagination
REQUEST_DELAY_SECONDS = 0.1 # Delay between fetching pages for the same slug
COMPANY_SLUG_DELAY_SECONDS = 0.1 # Additional delay between different slugs for the same company
COMPANY_DELAY_SECONDS = 0.5 # Additional delay between processing different companies

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}


def fetch_page_content_with_retries(url, retries=3, delay=2):
    """
    Fetches HTML content from a URL with retry mechanism and redirection handling.
    Returns (html_content, status_code).
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            
            # Special handling for pagination: if a /page/N/ URL redirects back to the base slug URL,
            # it means we've hit the end of available paginated content.
            if "/page/" in url and response.url != url:
                base_of_paginated_url = url.split("/page/")[0].rstrip('/') + "/"
                current_response_url_norm = response.url.rstrip('/') + "/"
                if current_response_url_norm == base_of_paginated_url:
                    # print(f"    [INFO] Redirected from {url} to {response.url}. Likely end of pagination.")
                    return None, 404 

            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            return response.text, response.status_code
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # print(f"    [WARN] Page not found (404): {url}")
                return None, 404
            print(f"    [ERROR] HTTP error on {url} (attempt {attempt + 1}): {e.response.status_code}")
            if attempt == retries - 1: return None, e.response.status_code
        except requests.exceptions.RequestException as e:
            print(f"    [ERROR] Request error on {url} (attempt {attempt + 1}): {e}")
            if attempt == retries - 1: return None, 0 
        time.sleep(delay) # Wait before retrying
    return None, 0 # Return None and 0 if all retries fail

def parse_article_date(date_string):
    """
    Parses a date string into an offset-naive datetime object (converted to UTC equivalent).
    Returns None if parsing fails.
    """
    if not date_string:
        return None
    try:
        # Clean up common suffixes like "IST"
        date_string_cleaned = date_string.replace(" IST", "").replace("IST", "").strip()
        dt_aware = parse_date(date_string_cleaned)
        
        # Convert to UTC and then make it naive for consistent comparison across different timezones
        if dt_aware.tzinfo is not None:
            dt_naive_utc = dt_aware.astimezone(timezone.utc).replace(tzinfo=None)
            return dt_naive_utc
        else:
            # If dateutil returns an already naive datetime, assume it's local time
            # and treat it as UTC naive for consistent comparison.
            return dt_aware 
            
    except (ValueError, TypeError) as e:
        # print(f"      [DEBUG] Could not parse date: '{date_string}'. Error: {e}")
        return None

def extract_headlines_and_dates_from_html_page(html_content, base_page_url_for_relative_links):
    """
    Extracts headlines, links, and dates from the HTML content of a Financial Express page.
    It applies the age filter and skips sections after 'Related News' or 'More News' headers.
    It also explicitly skips the generic "Latest News" section identified by its specific div structure.
    """
    items = []
    oldest_article_date_on_page = None
    
    now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    age_threshold_utc_naive = now_utc_naive - timedelta(days=MAX_ARTICLE_AGE_YEARS * 365.25)

    if not html_content:
        return items, oldest_article_date_on_page

    soup = BeautifulSoup(html_content, 'html.parser')
    
    relevant_article_tags = []
    
    # Target the primary content area where company-specific articles are listed
    main_content_area = soup.find('div', class_=lambda c: c and 'ie-network-grid__lhs' in c)

    if main_content_area:
        # Flag to indicate if we've passed a "stop" header (like Related News)
        stop_collecting_flag = False
        
        # Iterate through the direct children of the main content area.
        for child in main_content_area.children:
            # Skip non-tag elements (like NavigableString for whitespace)
            if not hasattr(child, 'name'): 
                continue

            # --- Logic to skip the specific "Latest News" block ---
            # This block is typically structured with a <div class="article-section-title"> containing "Latest News"
            if child.name == 'div':
                title_div = child.find('div', class_='article-section-title')
                if title_div and "latest news" in title_div.get_text(strip=True).lower():
                    # print(f"      [INFO] Skipping generic 'Latest News' section (based on structure).")
                    continue # Skip this entire div block and move to the next sibling
            # --- End of skipping "Latest News" logic ---

            # Check for headers that indicate we should stop collecting news (sections that come *after* main content)
            if child.name in ['h2', 'h3']:
                header_text = child.get_text(strip=True).lower()
                # Stop conditions: "Related News", "More News", "Also Read" usually mark generic sections.
                if "related news" in header_text or "more news" in header_text or "also read" in header_text:
                    print(f"      [INFO] Encountered section '{child.get_text(strip=True)}'. Stopping article extraction for this page.")
                    stop_collecting_flag = True
                    continue # Stop processing this header and move to next child, but the flag is now set

            # If the stop_collecting_flag is true, skip this child and all subsequent ones
            if stop_collecting_flag:
                continue 

            # If it's a block containing articles and we haven't hit a stop header, add its articles
            if child.name == 'div' and 'wp-block-newspack-blocks-ie-stories' in child.get('class', []):
                articles_in_block = child.find_all('article')
                relevant_article_tags.extend(articles_in_block)
    else:
        # This can happen if the page structure is completely different or empty
        # print("    [DEBUG] No 'ie-network-grid__lhs' found, no articles will be extracted.")
        return items, oldest_article_date_on_page

    # Process all identified relevant article tags
    # print(f"    [DEBUG] Processing {len(relevant_article_tags)} relevant article elements.")
    for article_tag in relevant_article_tags:
        headline_text, headline_link, article_date_str = None, None, None
        
        # Extract title and link
        title_tag = article_tag.find(['div', 'h3'], class_='entry-title') 
        if not title_tag: 
            title_tag = article_tag.find(['h1','h2','h3','h4']) 

        if title_tag:
            link_tag = title_tag.find('a')
            if link_tag and link_tag.get_text(strip=True):
                headline_text = link_tag.get_text(strip=True)
                headline_link = link_tag.get('href')

        # Extract date
        date_tag = article_tag.find('time', class_='entry-date') 
        if date_tag:
            article_date_str = date_tag.get('datetime') or date_tag.get_text(strip=True)

        if headline_text and headline_link:
            # Resolve relative URLs
            if not headline_link.startswith(('http://', 'https://')):
                headline_link = urljoin(base_page_url_for_relative_links, headline_link)
            
            parsed_date_obj = parse_article_date(article_date_str) if article_date_str else None
            
            if parsed_date_obj:
                # Only add articles that are within the specified age limit
                if parsed_date_obj >= age_threshold_utc_naive:
                    items.append({
                        "headline": headline_text,
                        "link": headline_link,
                        "date_str": article_date_str, 
                        "parsed_date_iso": parsed_date_obj.isoformat() 
                    })
                    # Keep track of the oldest article's date (that is still relevant) on this page
                    if oldest_article_date_on_page is None or parsed_date_obj < oldest_article_date_on_page:
                        oldest_article_date_on_page = parsed_date_obj
                # else:
                #     print(f"      [DEBUG] Skipping old article: '{headline_text}' ({parsed_date_obj.date()})")
            elif article_date_str is None: 
                 # If no date is found but a headline is present, add it.
                 items.append({
                    "headline": headline_text,
                    "link": headline_link,
                    "date_str": None,
                    "parsed_date_iso": None
                })
    
    return items, oldest_article_date_on_page


def calculate_sentiment(text):
    """
    Calculates VADER compound sentiment score for a given text.
    Returns a float between -1.0 (most negative) and +1.0 (most positive).
    Returns None if text is empty.
    """
    if not text:
        return None
    scores = sentiment_analyzer.polarity_scores(text)
    return scores['compound']


def scrape_financial_express_slug(target_slug: str):
    """
    Scrapes paginated headlines for a given company slug on Financial Express.
    Returns a list of dictionaries, each representing a headline.
    """
    all_headlines_for_slug = []
    base_url_for_slug = urljoin(BASE_SLUG_URL_PREFIX, f"{target_slug}/")
    
    # Calculate the age threshold relative to current UTC time for stop condition
    now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    age_threshold_utc_naive = now_utc_naive - timedelta(days=MAX_ARTICLE_AGE_YEARS * 365.25)

    print(f"  [SCRAPER] Starting scraping for slug: {target_slug}")

    for page_num in range(1, MAX_PAGES_TO_TEST + 1):
        # print(f"    [SCRAPER] Fetching page {page_num} for '{target_slug}'...")
        if page_num == 1:
            current_url = base_url_for_slug
        else:
            current_url = f"{base_url_for_slug}page/{page_num}/"
        
        html_content, status_code = fetch_page_content_with_retries(current_url)

        if status_code == 404 or html_content is None:
            print(f"    [INFO] Stopping pagination for '{target_slug}' at page {page_num} due to 404 or fetch error.")
            break
        
        page_items, oldest_date_on_page = extract_headlines_and_dates_from_html_page(html_content, base_url_for_slug)

        if not page_items and page_num > 1: # No items found on a subsequent page, likely end of content
            print(f"    [INFO] No new items found on page {page_num}. Assuming end of content for '{target_slug}'.")
            break
        
        # print(f"    [SCRAPER] Page {page_num}: Found {len(page_items)} relevant items.")
        
        all_headlines_for_slug.extend(page_items)

        # If the oldest article found on this page (and within age limit) is older than our threshold, stop pagination.
        # This condition is crucial for limiting the depth of scraping for efficiency.
        if oldest_date_on_page and oldest_date_on_page < age_threshold_utc_naive:
            print(f"    [INFO] Oldest relevant article on page {page_num} ({oldest_date_on_page.date()}) is older than {MAX_ARTICLE_AGE_YEARS} years. Stopping for '{target_slug}'.")
            break
        
        if page_num < MAX_PAGES_TO_TEST: 
            time.sleep(REQUEST_DELAY_SECONDS)
            
    print(f"  [SCRAPER] Finished scraping for slug: {target_slug}. Collected {len(all_headlines_for_slug)} headlines.")
    return all_headlines_for_slug

def main():
    """Main function to load company data, scrape headlines, add sentiment, and save to CSVs."""
    # 1. Load company data from JSON file
    try:
        with open(COMPANIES_JSON_FILE, 'r', encoding='utf-8') as f:
            companies_data = json.load(f)
    except FileNotFoundError:
        print(f"[CRITICAL ERROR] Company JSON file not found at: {COMPANIES_JSON_FILE}")
        print("Please ensure the path is correct or place the file in the specified location.")
        return
    except json.JSONDecodeError as e:
        print(f"[CRITICAL ERROR] Error decoding JSON data from {COMPANIES_JSON_FILE}: {e}")
        print("Please ensure the JSON file is correctly formatted.")
        return

    # 2. Create the base output directory if it doesn't exist
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
    print(f"[INFO] Output directory '{OUTPUT_BASE_DIR}/' ensured.")

    total_companies = len(companies_data)

    for i, (ticker, company_info) in enumerate(companies_data.items()):
        company_name = company_info.get("company_name", ticker)
        matched_slugs = company_info.get("matched_slugs", [])

        print(f"\n--- Processing Company {i+1}/{total_companies}: {company_name} ({ticker}) ---")

        # 3. Create ticker-specific directory within the base output directory
        ticker_output_dir = os.path.join(OUTPUT_BASE_DIR, ticker)
        os.makedirs(ticker_output_dir, exist_ok=True)
        print(f"  [INFO] Directory '{ticker_output_dir}/' ensured.")

        if not matched_slugs:
            print(f"  [INFO] Skipping {company_name} ({ticker}): No matched slugs found. No CSV will be created.")
            continue

        company_headlines = []
        for slug in matched_slugs:
            headlines_for_slug = scrape_financial_express_slug(slug)
            company_headlines.extend(headlines_for_slug)
            time.sleep(COMPANY_SLUG_DELAY_SECONDS) # Delay between different slugs for the same company

        # Remove duplicate headlines based on their link (URL)
        unique_headlines = []
        seen_links = set()
        for headline in company_headlines:
            link = headline.get("link")
            # Only add if link exists and hasn't been seen, or if no link exists and it's not an exact duplicate
            if link and link not in seen_links:
                unique_headlines.append(headline)
                seen_links.add(link)
            elif not link and headline not in unique_headlines: 
                unique_headlines.append(headline)
        
        # 4. Add sentiment score to each unique headline
        for headline_item in unique_headlines:
            headline_text = headline_item.get("headline")
            sentiment_score = calculate_sentiment(headline_text)
            headline_item['sentiment_score'] = sentiment_score
        
        # 5. Save unique headlines (with sentiment) to a CSV file
        csv_filename = os.path.join(ticker_output_dir, f"{ticker}_headlines.csv")
        if unique_headlines:
            # Define CSV column headers explicitly to ensure order and inclusion of new fields
            fieldnames = ["headline", "link", "date_str", "parsed_date_iso", "sentiment_score"]
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore') # 'ignore' skips keys not in fieldnames
                writer.writeheader() # Writes the header row
                writer.writerows(unique_headlines) # Writes all headline dictionaries as rows
            print(f"--- Saved {len(unique_headlines)} unique headlines for {company_name} to {csv_filename} ---")
        else:
            print(f"--- No relevant headlines found for {company_name} ({ticker}). No CSV file created. ---")

        # Add a delay between processing different companies to be polite to the server
        if i < total_companies - 1:
            time.sleep(COMPANY_DELAY_SECONDS) 

    print(f"\n--- Scraping complete. All scraped headlines with sentiment scores are saved in the '{OUTPUT_BASE_DIR}' directory. ---")
    print(f"Summary: Processed {total_companies} companies.")


if __name__ == "__main__":
    main()