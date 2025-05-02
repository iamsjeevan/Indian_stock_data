import requests
import json
import time # To add delays between requests

# List of stocks with their names and BSE Scrip Codes
stocks_to_process = [
    {'name': 'Reliance Industries Ltd', 'scrip_code': '500325'},
    {'name': 'Tata Consultancy Services Ltd (TCS)', 'scrip_code': '532540'},
    {'name': 'HDFC Bank Ltd', 'scrip_code': '500180'},
    {'name': 'Infosys Ltd', 'scrip_code': '500209'},
    {'name': 'ICICI Bank Ltd', 'scrip_code': '532174'},
    {'name': 'Hindustan Unilever Ltd', 'scrip_code': '500696'},
    {'name': 'State Bank of India (SBI)', 'scrip_code': '500112'},
    {'name': 'Bajaj Finance Ltd', 'scrip_code': '500034'},
    {'name': 'Bharti Airtel Ltd', 'scrip_code': '532454'},
    {'name': 'ITC Ltd', 'scrip_code': '500875'},
    {'name': 'Larsen & Toubro Ltd (L&T)', 'scrip_code': '500510'}
]

# Base API URL format
base_api_url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnualReport_New/w?scripcode={}'

# Common Headers (essential for the API call)
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Origin': 'https://www.bseindia.com',
    'Referer': 'https://www.bseindia.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"'
}

# Keys for JSON parsing
report_list_key = 'Table'
link_key = 'PDFDownload'

# Loop through each stock
for stock in stocks_to_process:
    stock_name = stock['name']
    scrip_code = stock['scrip_code']
    api_url = base_api_url.format(scrip_code) # Construct the specific API URL

    print(f"\n--- Processing: {stock_name} ({scrip_code}) ---")
    print(f"Calling API: {api_url}")

    pdf_links = [] # Reset links for each stock
    try:
        response = requests.get(api_url, headers=headers, timeout=20)
        response.raise_for_status() # Check for HTTP errors
        print("API call successful.")

        data = response.json() # Parse JSON response

        # Extract links
        if report_list_key in data and isinstance(data[report_list_key], list):
            reports = data[report_list_key]
            for report in reports:
                if isinstance(report, dict) and link_key in report:
                    link_url = report[link_key]
                    if link_url and isinstance(link_url, str) and link_url.lower().endswith(('.pdf', '.zip')): # Allow for zip files too
                        # Clean up potential stray backslashes
                        full_link = link_url.replace('\\', '')
                        pdf_links.append(full_link)

        else:
             print(f"Warning: Could not find key '{report_list_key}' or it's not a list in the response for {stock_name}.")

    except requests.exceptions.RequestException as e:
        print(f"API Request Error for {stock_name}: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON Parsing Error for {stock_name}: {e}")
        print(f"Response Text (first 500 chars): {response.text[:500]}...")
    except KeyError as e:
        print(f"Key Error processing {stock_name}: Could not find key '{e}'.")
    except Exception as e:
        print(f"An unexpected error occurred processing {stock_name}: {e}")

    # Print the found links for the current stock
    if pdf_links:
        print(f"Found PDF/ZIP Links for {stock_name}:")
        unique_links = sorted(list(set(pdf_links))) # Remove duplicates and sort
        for link in unique_links:
            print(link)
        print(f"Total unique links found: {len(unique_links)}")
    else:
        print(f"No valid Annual Report PDF/ZIP links found for {stock_name}.")

    # --- IMPORTANT: Add a delay to avoid overwhelming the server ---
    print("Pausing for 2 seconds...")
    time.sleep(2) # Pause for 2 seconds between requests

print("\n--- Finished processing all stocks. ---")
