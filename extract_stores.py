import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import time
import argparse

SITEMAP_FILE = "kmart.com.au-sitemap-au-storelocation-sitemap.xml.xml"
verbose = False

def extract_urls_from_sitemap(filepath):
    """Extract store URLs from the sitemap XML file."""
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        # Handle the namespace
        namespace = {'ns': 'https://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = [elem.text for elem in root.findall('.//ns:loc', namespace)]
        return urls
    except Exception as e:
        print(f"Error parsing sitemap: {e}", file=sys.stderr)
        return []

def get_store_details(url):
    """Fetch a store page and extract details from the JSON data."""
    try:
        if verbose:
            print(f"Fetching: {url}", file=sys.stderr)
        
        with urlopen(url, timeout=10) as response:
            html = response.read().decode('utf-8')
        
        # Extract JSON from __NEXT_DATA__ script tag
        start_marker = '"__NEXT_DATA__":'
        start_idx = html.find(start_marker)
        
        if start_idx == -1:
            # Try alternate marker
            start_marker = 'id="__NEXT_DATA__"'
            start_idx = html.find(start_marker)
            if start_idx == -1:
                return None
            # Find the JSON content after the tag
            start_idx = html.find('>', start_idx) + 1
            end_idx = html.find('</script>', start_idx)
        else:
            start_idx += len(start_marker)
            # Find the end of the JSON object
            brace_count = 0
            in_string = False
            escape_next = False
            end_idx = start_idx
            
            for i, char in enumerate(html[start_idx:], start=start_idx):
                if escape_next:
                    escape_next = False
                    continue
                    
                if char == '\\':
                    escape_next = True
                    continue
                    
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                
                if not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = i + 1
                            break
        
        json_str = html[start_idx:end_idx]
        data = json.loads(json_str)
        
        # Navigate to the location data
        location = data.get('props', {}).get('pageProps', {}).get('location', {})
        
        if not location:
            return None
        
        store_data = {
            'locationId': location.get('locationId'),
            'publicName': location.get('publicName'),
            'phoneNumber': location.get('phoneNumber'),
            'address1': location.get('address1'),
            'address2': location.get('address2'),
            'address3': location.get('address3'),
            'city': location.get('city'),
            'state': location.get('state'),
            'postcode': location.get('postcode'),
            'latitude': location.get('latitude'),
            'longitude': location.get('longitude'),
            'tradingHours': location.get('tradingHours'),
            'typename': location.get('__typename'),
            'url': url
        }
        
        return store_data
        
    except (URLError, HTTPError) as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {url}: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error processing {url}: {e}", file=sys.stderr)
        return None

def main():
    """Main function to scrape all stores and output as JSON."""
    global verbose
    
    parser = argparse.ArgumentParser(description='Extract Kmart store details from sitemap.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    verbose = args.verbose
    
    if not Path(SITEMAP_FILE).exists():
        print(f"Error: Sitemap file '{SITEMAP_FILE}' not found.", file=sys.stderr)
        sys.exit(1)
    
    urls = extract_urls_from_sitemap(SITEMAP_FILE)
    
    if verbose:
        print(f"Found {len(urls)} stores in sitemap", file=sys.stderr)
    
    all_stores = []
    errors = []
    
    for i, url in enumerate(urls, 1):
        store_data = get_store_details(url)
        if store_data:
            all_stores.append(store_data)
            if verbose:
                print(f"  [{i}/{len(urls)}] {store_data.get('publicName', 'Unknown')}", file=sys.stderr)
        else:
            errors.append((i, url))
            if verbose:
                print(f"  [{i}/{len(urls)}] Failed to extract", file=sys.stderr)
        
        # Be polite to the server
        time.sleep(0.5)
    
    # Print summary
    print(f"Extracted {len(all_stores)} stores", file=sys.stderr)
    if errors:
        print(f"Failed to extract {len(errors)} stores:", file=sys.stderr)
        for idx, url in errors:
            print(f"  [{idx}] {url}", file=sys.stderr)
    
    all_stores_sorted = sorted(all_stores, key=lambda x: x.get('locationId', ''))
    print(json.dumps(all_stores_sorted, indent=2))

if __name__ == "__main__":
    main()
