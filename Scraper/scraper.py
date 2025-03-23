import requests
from bs4 import BeautifulSoup
import csv
import os
import re

main_url = "https://infosys.beckhoff.com/content/1033/tc3ncerrcode/1521556875.html"

def fetch_html(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    iframe = soup.find('iframe')
    if iframe:
        iframe_src = iframe['src']
        html = fetch_html(iframe_src)
        soup = BeautifulSoup(html, 'html.parser')
    return soup

def extract_table_data(soup):
    table = soup.find('table')
    if table is None:
        raise ValueError("No table found on the webpage")

    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    rows = []
    for tr in table.find_all('tr')[1:]:
        cells = []
        for td in tr.find_all('td'):
            if td == tr.find_all('td')[3]:  # For the description column
                cells.append(str(td.decode_contents()))
            else:
                cells.append(td.get_text(strip=True))
        description_html = BeautifulSoup(cells[3], 'html.parser')
        strong_text = description_html.find('strong').get_text(strip=True) if description_html.find('strong') else ""
        description_text = ' '.join([text for text in description_html.stripped_strings if text != strong_text])
        cells[3] = description_text.replace(strong_text, "").strip()
        cells.append(strong_text)
        rows.append(cells)

    return headers, rows

def format_identifier(identifier):
    # Convert to uppercase and replace spaces with underscores
    formatted = identifier.upper().replace(' ', '_')
    # Remove any special characters except underscores
    formatted = ''.join(c for c in formatted if c.isalnum() or c == '_')
    # Remove multiple consecutive underscores and trim
    formatted = '_'.join(filter(None, formatted.split('_')))
    return formatted

def get_page_title(soup):
    title = soup.find('title')
    if title:
        return format_identifier(title.get_text(strip=True))
    return "NC"

def make_unique_identifier(base_identifier, description, used_identifiers):
    if base_identifier not in used_identifiers:
        used_identifiers[base_identifier] = 1
        return base_identifier
    
    # Extract distinguishing text from description
    # For "Missing process image" cases, extract the interface type
    if "interface" in description.lower():
        interface_type = description.split("interface")[0].strip().replace(' ', '_').replace('-', '_').upper()
        # Remove any non-alphanumeric characters except underscores
        interface_type = ''.join(c for c in interface_type if c.isalnum() or c == '_')
        # Remove multiple consecutive underscores
        interface_type = '_'.join(filter(None, interface_type.split('_')))
        unique_id = f"{base_identifier}_{interface_type}"
        return unique_id
    
    # For other cases, append a counter
    count = used_identifiers[base_identifier]
    used_identifiers[base_identifier] = count + 1
    return f"{base_identifier}_{count}"

def process_table_data(headers, rows, soup):
    headers = [headers[1], headers[3], "Identifier"]
    page_title = get_page_title(soup)
    used_identifiers = {}
    
    processed_rows = []
    for row in rows:
        base_id = f"{page_title}_{format_identifier(row[4])}"
        unique_id = make_unique_identifier(base_id, row[3], used_identifiers)
        processed_rows.append([row[1], row[3], unique_id])
    
    return headers, processed_rows

def write_to_csv(filename, headers, rows):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

def get_page_links(soup):
    """Extract links from the 'Further Information' section"""
    links = []
    # Look for elements that might contain "Further Information"
    for element in soup.find_all(['h2', 'h3', 'h4', 'div']):
        if 'Further Information' in element.get_text():
            # Get the next ul or ol element
            list_element = element.find_next(['ul', 'ol'])
            if list_element:
                for li in list_element.find_all('li'):
                    a_tag = li.find('a')
                    if a_tag and 'href' in a_tag.attrs:
                        href = a_tag['href']
                        # Convert relative URLs to absolute
                        if not href.startswith('http'):
                            base_url = "https://infosys.beckhoff.com/content/1033/tc3ncerrcode/"
                            href = base_url + href
                        links.append(href)
    return links

def extract_page_id(url):
    """Extract a page identifier from the URL to use in filenames"""
    match = re.search(r'(\d+)\.html', url)
    if match:
        return match.group(1)
    return 'unknown'

def main():
    try:
        # Get the main page HTML
        html = fetch_html(main_url)
        main_soup = parse_html(html)
        
        # Extract links from the "Further Information" section
        links = get_page_links(main_soup)
        
        # If no links found, fall back to the original single page scraping
        if not links:
            print("No links found, scraping current page only")
            links = [main_url]
        
        # Process each link
        all_rows = []
        all_used_identifiers = {}
        
        for link in links:
            try:
                print(f"Scraping {link}")
                html = fetch_html(link)
                soup = parse_html(html)
                page_title = get_page_title(soup)
                
                try:
                    headers, rows = extract_table_data(soup)
                    
                    # Process each row's identifier
                    for row in rows:
                        base_id = f"{page_title}_{format_identifier(row[4])}"
                        unique_id = make_unique_identifier(base_id, row[3], all_used_identifiers)
                        all_rows.append([row[1], row[3], unique_id])
                    
                except ValueError as ve:
                    print(f"  Warning: {ve}")
                
            except Exception as e:
                print(f"Error processing {link}: {e}")
        
        # Write a single CSV file with all data
        write_to_csv("c:\\Users\\Saeed\\Documents\\TcXaeShell\\Samples\\TcError\\tc3ncerrcode.csv", 
                     ["Error(Dec)", "Description", "Identifier"], all_rows)
        
        print(f"Completed! Scraped {len(all_rows)} error codes")
                
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
