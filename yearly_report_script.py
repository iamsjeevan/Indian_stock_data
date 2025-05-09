import requests
import json
import random
import time

# --- Configuration ---
PROXY_API_URL = "https://freeapiproxies.azurewebsites.net/proxyapi"
PROXY_API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}
NUMBER_OF_PROXIES_TO_TEST = 10
TEST_URL = "https://api.ipify.org?format=json"
PROXY_TEST_TIMEOUT = 15 # Increased timeout a bit for potentially slower SOCKS

def fetch_proxies_from_api(session):
    """Fetches a list of proxies from the PROXY_API_URL."""
    print(f"Fetching proxy list from {PROXY_API_URL}...")
    try:
        response = session.get(PROXY_API_URL, headers=PROXY_API_HEADERS, timeout=10)
        response.raise_for_status()
        proxies_list = response.json()
        if proxies_list and isinstance(proxies_list, list):
            print(f"Successfully fetched {len(proxies_list)} proxies from the API.")
            return proxies_list
        else:
            print("API returned no proxies or unexpected format.")
            return []
    except requests.exceptions.Timeout:
        print("Proxy API request timed out.")
        return []
    except requests.RequestException as e:
        print(f"Proxy API request error: {e}")
        return []
    except json.JSONDecodeError:
        print(f"Proxy API response was not valid JSON: {response.text[:200] if response else 'No response text'}") # Added check for response
        return []

def test_proxy(proxy_info, session):
    """
    Tests a single proxy by making a request to TEST_URL.
    """
    ip = proxy_info.get("ip")
    port = proxy_info.get("port")
    proxy_type = proxy_info.get("type", "http").lower()

    if not ip or not port:
        print(f"  [Invalid Data] Proxy info missing IP or Port: {proxy_info}")
        return False

    # *** MODIFIED PROXY URL FORMATTING ***
    if proxy_type == "http" or proxy_type == "https":
        # For HTTP/HTTPS proxies, requests library often handles the scheme correctly
        # when you provide http:// in the proxy dict.
        # Some systems/libraries might prefer explicit https:// if the proxy is an HTTPS proxy,
        # but generally http://ip:port works for both http and https traffic through an HTTP proxy.
        proxy_url_formatted = f"http://{ip}:{port}"
    elif proxy_type == "socks4":
        proxy_url_formatted = f"socks4://{ip}:{port}"
    elif proxy_type == "socks5":
        proxy_url_formatted = f"socks5://{ip}:{port}"
    else:
        print(f"  [Unsupported Type] Proxy type '{proxy_type}' not directly formatted for testing: {ip}:{port}.")
        return False

    proxies_for_request = {
        "http": proxy_url_formatted,
        "https": proxy_url_formatted
    }

    print(f"  Testing proxy: {proxy_url_formatted} (Original API type: {proxy_type})... ", end="")
    try:
        response = session.get(TEST_URL, proxies=proxies_for_request, timeout=PROXY_TEST_TIMEOUT)
        response.raise_for_status()
        print(f"SUCCESS (Status: {response.status_code}, IP via proxy: {response.json().get('ip', 'N/A')})")
        return True
    except requests.exceptions.Timeout:
        print("FAILED (Timeout)")
        return False
    except requests.exceptions.RequestException as e:
        error_type = type(e).__name__
        # The "InvalidSchema" error occurs if PySocks is not installed or requests can't handle the SOCKS scheme.
        if "InvalidSchema" in error_type:
             print(f"FAILED ({error_type} - Is PySocks installed? 'pip install PySocks')")
        else:
            print(f"FAILED ({error_type})")
        return False
    except json.JSONDecodeError:
        print(f"FAILED (Could not decode JSON response from test URL: {response.text[:100] if response else 'No response text'})")
        return False


if __name__ == "__main__":
    # Check if PySocks is available, if not, SOCKS proxies will likely fail
    try:
        import socks
        print("PySocks library found.")
    except ImportError:
        print("WARNING: PySocks library not found. SOCKS proxies will likely fail. Please install it: pip install PySocks")


    with requests.Session() as session:
        available_proxies = fetch_proxies_from_api(session)

        if not available_proxies:
            print("No proxies fetched from API. Cannot proceed with testing.")
            exit()
        
        source_list_for_sampling = available_proxies

        num_to_sample = min(NUMBER_OF_PROXIES_TO_TEST, len(source_list_for_sampling))
        if num_to_sample == 0:
            print("No proxies available to sample and test.")
            exit()
            
        proxies_to_test_sample = random.sample(source_list_for_sampling, num_to_sample)

        print(f"\n--- Testing {num_to_sample} randomly selected proxies ---")
        working_proxies_count = 0
        tested_proxies_count = 0

        for proxy_info in proxies_to_test_sample:
            tested_proxies_count += 1
            if test_proxy(proxy_info, session):
                working_proxies_count += 1
            if tested_proxies_count < len(proxies_to_test_sample):
                time.sleep(0.5) 

        print("\n--- Proxy Test Summary ---")
        print(f"Total proxies selected for testing: {num_to_sample}")
        print(f"Number of working proxies: {working_proxies_count}")
        print(f"Success rate: { (working_proxies_count / num_to_sample * 100) if num_to_sample > 0 else 0:.2f}%")

        if working_proxies_count == 0 and num_to_sample > 0:
            print("\nWARNING: None of the randomly tested proxies worked. The proxy source might be unreliable, or PySocks might be needed for SOCKS proxies.")
            