import requests
import time
import json # To try and pretty-print if it's valid JSON after stripping

# --- Configuration for API Call ---
BASE_URL_SUGGESTION_API = "https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php"
API_REQUEST_DELAY = 1 # Seconds between API calls

HEADERS_API = { # Headers from your cURL example
    'accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://www.moneycontrol.com/', # General referer
    'sec-ch-ua': '"Chromium";v="136", "Brave";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

def fetch_and_print_api_suggestion(ticker_query, session):
    """
    Fetches the suggestion from Moneycontrol's API and prints the raw response.
    """
    params = {
        'classic': 'true',
        'query': ticker_query,
        'type': '1',  # '1' seems to be for equity
        'format': 'json',
        'callback': 'suggest1' # This will be part of the response text
    }
    print(f"\n--- Fetching API suggestion for: {ticker_query} ---")
    try:
        response = session.get(BASE_URL_SUGGESTION_API, params=params, headers=HEADERS_API, timeout=10)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        print(f"Status Code: {response.status_code}")
        print("Raw Response Text:")
        print(response.text)
        print("--------------------------------------------------")

        # Optional: Try to parse and pretty print the JSON part
        if response.text.startswith('suggest1(') and response.text.endswith(')'):
            json_str = response.text[len('suggest1('):-1]
            try:
                parsed_json = json.loads(json_str)
                print("\nParsed JSON Content (Pretty Printed):")
                print(json.dumps(parsed_json, indent=2))
                print("--------------------------------------------------")
            except json.JSONDecodeError as e:
                print(f"\nCould not parse the inner content as JSON: {e}")
        
    except requests.RequestException as e:
        print(f"Request Error for '{ticker_query}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred for '{ticker_query}': {e}")

# --- Main Execution ---
if __name__ == "__main__":
    tickers_to_check = ["AXISBANK", "KOTAKBANK"] # Stocks you want to inspect

    with requests.Session() as session: # Use a session for potential cookie handling if needed
        for ticker in tickers_to_check:
            fetch_and_print_api_suggestion(ticker, session)
            if ticker != tickers_to_check[-1]: # Add delay unless it's the last one
                time.sleep(API_REQUEST_DELAY)