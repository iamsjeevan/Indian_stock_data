import requests
import os

# --- Configuration ---
STOCK_ID_TO_CHECK = "KSB"  # <--- Change this to the sc_id you want to inspect
REPORT_TYPE = "balance_VI" # Or "profit", "cashflow" etc.
OUTPUT_HTML_FILENAME = f"{STOCK_ID_TO_CHECK}_financial_page_content.html"

BASE_URL_FINANCIALS = "https://www.moneycontrol.com/stocks/company_info/print_financials.php"

HEADERS_FINANCIALS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.6",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36", # Keep your UA updated
    # Add other headers from your main script if you suspect they are critical for initial page load
}

def fetch_and_save_financial_page(sc_id, report_type, output_filename):
    """
    Fetches the initial financial print page for a given sc_id and report type,
    and saves its HTML content to a file.
    """
    params = {'sc_did': sc_id, 'type': report_type}
    
    print(f"Attempting to fetch: {BASE_URL_FINANCIALS} with params: {params}")

    try:
        with requests.Session() as session: # Use a session for good practice
            session.headers.update(HEADERS_FINANCIALS)
            response = session.get(BASE_URL_FINANCIALS, params=params, timeout=20)
            
            print(f"Status Code: {response.status_code}")
            print(f"Final URL after request: {response.url}")

            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

            html_content = response.text

            try:
                with open(output_filename, "w", encoding="utf-8") as f:
                    f.write(html_content)
                print(f"Successfully saved HTML content to: {output_filename}")
                print(f"File size: {os.path.getsize(output_filename) / 1024:.2f} KB")
            except IOError as e:
                print(f"Error saving HTML to file '{output_filename}': {e}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} (Status code: {response.status_code if 'response' in locals() else 'N/A'})")
        if 'response' in locals() and response.status_code == 404:
            print("This might indicate the sc_id or report type is incorrect, or the page doesn't exist.")
        # Optionally save error page content
        if 'response' in locals() and response.text:
             error_filename = f"{sc_id}_error_page.html"
             try:
                 with open(error_filename, "w", encoding="utf-8") as f_err:
                     f_err.write(response.text)
                 print(f"Saved error page content to: {error_filename}")
             except IOError:
                 pass # Ignore if can't save error page
    except requests.exceptions.Timeout:
        print("The request timed out.")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    print(f"Fetching financial page for sc_id: '{STOCK_ID_TO_CHECK}', Report Type: '{REPORT_TYPE}'")
    fetch_and_save_financial_page(STOCK_ID_TO_CHECK, REPORT_TYPE, OUTPUT_HTML_FILENAME)
    print("\nScript finished. Please check the generated HTML file.")