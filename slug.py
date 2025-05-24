import requests
import json
import time

def fetch_all_slugs():
    base_url = "https://www.financialexpress.com/wp-json/wp/v2/tags/"
    all_slugs = []
    page = 1
    # The WordPress API often allows up to 100 items per page, which is more efficient
    per_page = 100

    # Mimic some browser headers, especially User-Agent.
    # The cookies from your cURL are session-specific and might not be needed
    # or could be problematic if they expire. We'll try without them first.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01', # More generic accept
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': '/',
        'X-Requested-With': 'XMLHttpRequest'
    }

    # Parameters for the request
    params = {
        'per_page': per_page,
        'orderby': 'name',
        'order': 'asc',
        'hide_empty': 'false', # As per your cURL
        '_fields': 'slug,id', # Request only the fields we need to be more efficient
        'page': page
    }

    print(f"Fetching initial page to determine total pages...")

    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=20)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        # Extract total pages from headers
        total_pages_str = response.headers.get('X-WP-TotalPages')
        if not total_pages_str:
            print("Error: 'X-WP-TotalPages' header not found. Cannot determine total pages.")
            print("Attempting to scrape page by page until an empty response is found.")
            # Fallback: iterate until no more data
            total_pages = float('inf') # effectively loop indefinitely until break
        else:
            total_pages = int(total_pages_str)
            print(f"API reports {total_pages} total pages.")

        # Process the first page
        data = response.json()
        if data:
            for item in data:
                if 'slug' in item:
                    all_slugs.append(item['slug'])
            print(f"Processed page {page}/{total_pages if total_pages != float('inf') else '?'}. Found {len(data)} items. Total slugs: {len(all_slugs)}")
        else: # No data on first page
            print(f"No data found on the first page. Exiting.")
            return []


        # Loop through the remaining pages
        page += 1
        while page <= total_pages:
            params['page'] = page
            print(f"Fetching page {page}/{total_pages if total_pages != float('inf') else '?' }...")
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=20)
                response.raise_for_status()
                current_page_data = response.json()

                if not current_page_data:
                    print(f"No more data found on page {page}. Stopping.")
                    break  # Exit loop if a page returns no data

                for item in current_page_data:
                    if 'slug' in item:
                        all_slugs.append(item['slug'])
                print(f"Processed page {page}/{total_pages if total_pages != float('inf') else '?'}. Found {len(current_page_data)} items. Total slugs: {len(all_slugs)}")

                page += 1
                time.sleep(0.5)  # Be polite to the server, wait 0.5 seconds

            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page}: {e}")
                print("Stopping due to error.")
                break
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on page {page}: {e}")
                print(f"Response text: {response.text[:500]}...") # Show first 500 chars
                print("Stopping due to JSON error.")
                break
        
        if total_pages == float('inf') and not current_page_data and page > 1:
             print(f"Reached an empty page ({page-1} was the last with data), assuming end of content.")


    except requests.exceptions.RequestException as e:
        print(f"Error on initial request: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON on initial request: {e}")
        print(f"Response text: {response.text[:500]}...")
        return []

    return all_slugs

if __name__ == "__main__":
    slugs = fetch_all_slugs()
    output_filename = "all_slugs.txt"

    if slugs:
        with open(output_filename, "w", encoding="utf-8") as f:
            for slug in slugs:
                f.write(slug + "\n")
        print(f"\nSuccessfully fetched {len(slugs)} slugs and saved them to '{output_filename}'")
    else:
        print("\nNo slugs were fetched.")