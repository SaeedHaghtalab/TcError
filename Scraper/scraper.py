import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import logging
from typing import Tuple, List, Dict, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
MAIN_URL = "https://infosys.beckhoff.com/content/1033/tc3ncerrcode/1521556875.html"
BASE_URL = "https://infosys.beckhoff.com/content/1033/tc3ncerrcode/"
OUTPUT_FILE = "c:\\Users\\Saeed\\Documents\\TcXaeShell\\Samples\\TcError\\tc3ncerrcode.csv"

class ErrorCodeScraper:
    """Class to scrape TwinCAT error codes from Beckhoff documentation."""
    
    def __init__(self, main_url: str, base_url: str, output_file: str):
        """Initialize the scraper with main URL, base URL, and output file path."""
        self.main_url = main_url
        self.base_url = base_url
        self.output_file = output_file
        
    def fetch_html(self, url: str) -> bytes:
        """Fetch HTML content from the specified URL."""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to fetch URL {url}: {e}")
            raise
    
    def parse_html(self, html: bytes) -> BeautifulSoup:
        """Parse HTML content and handle iframe if present."""
        soup = BeautifulSoup(html, 'html.parser')
        iframe = soup.find('iframe')
        if (iframe and 'src' in iframe.attrs):
            iframe_src = iframe['src']
            html = self.fetch_html(iframe_src)
            soup = BeautifulSoup(html, 'html.parser')
        return soup
    
    def extract_table_data(self, soup: BeautifulSoup) -> Tuple[List[str], List[List[str]], int]:
        """Extract table headers and row data from the parsed HTML."""
        # Find all tables in the page
        tables = soup.find_all('table')
        if not tables:
            raise ValueError("No table found on the webpage")
        
        all_headers = []
        all_rows = []
        symbol_column_index = -1
        
        # Process each table found
        for table_idx, table in enumerate(tables):
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            if not headers:
                logger.warning(f"Table #{table_idx+1} has no headers, skipping")
                continue
                
            logger.info(f"Processing table #{table_idx+1} with headers: {headers}")
            
            # Check if table has specific columns we need
            has_error_code = any(h.lower() in ['error(dec)', 'error code', 'error'] for h in headers)
            has_description = any(h.lower() in ['description', 'text'] for h in headers)
            
            # Find header indexes
            error_code_idx = next((i for i, h in enumerate(headers) if h.lower() in ['error(dec)', 'error code', 'error']), 0)
            description_idx = next((i for i, h in enumerate(headers) if h.lower() in ['description', 'text']), 1)
            
            # Check for Symbol column
            if not all_headers or table_idx == 0:
                all_headers = headers
                # Find the index of the Symbol column if it exists
                for i, header in enumerate(headers):
                    if header.lower() == 'symbol':
                        symbol_column_index = i
                        logger.info(f"Found Symbol column at index {i}")
                        break
            elif headers != all_headers:
                logger.info(f"Found table with different headers: {headers} vs {all_headers}")
                # Check for Symbol column in this table too
                for i, header in enumerate(headers):
                    if header.lower() == 'symbol':
                        symbol_column_index = i
                        logger.info(f"Found Symbol column at index {i} in secondary table")
                        break
            
            for tr in table.find_all('tr')[1:]:  # Skip header row
                cells = []
                tds = tr.find_all('td')
                
                # Skip empty rows
                if not tds:
                    continue
                    
                # Handle tables with different structures
                if len(tds) >= 2:  # Need at least error code and description
                    # Extract error code and description based on identified indexes
                    # Ensure we don't go out of bounds
                    error_code = tds[error_code_idx].get_text(strip=True) if error_code_idx < len(tds) else ""
                    
                    # Get description - handle as HTML to preserve formatting
                    if description_idx < len(tds):
                        description_html = str(tds[description_idx].decode_contents())
                    else:
                        description_html = ""
                    
                    # Ensure we have both an error code and a description
                    if error_code and description_html:
                        # Create a standardized row structure with placeholders
                        std_row = [""] * 5  # Initialize with 5 empty fields
                        std_row[1] = error_code  # Error code is at index 1
                        std_row[3] = description_html  # Description is at index 3
                        
                        # Handle the case when the description column structure is different
                        try:
                            desc_soup = BeautifulSoup(description_html, 'html.parser')
                            strong_text = desc_soup.find('strong')
                            strong_text = strong_text.get_text(strip=True) if strong_text else ""
                            
                            # Check if all text is in bold
                            all_text = ' '.join([text for text in desc_soup.stripped_strings])
                            
                            # If the strong text is the entire content (accounting for whitespace differences)
                            if strong_text and strong_text.strip() == all_text.strip():
                                # Use the strong text as the description since it's all in bold
                                std_row[3] = strong_text
                            else:
                                # Extract non-bold text
                                description_text = ' '.join([text for text in desc_soup.stripped_strings if text != strong_text])
                                std_row[3] = description_text.replace(strong_text, "").strip()
                            
                            # Add the strong text to be used as identifier if no Symbol column
                            if symbol_column_index == -1:
                                std_row[4] = strong_text if strong_text else "UNKNOWN_IDENTIFIER"
                            
                            # For tables with a Symbol column, make sure we capture it
                            if symbol_column_index != -1 and symbol_column_index < len(tds):
                                symbol_text = tds[symbol_column_index].get_text(strip=True)
                                if symbol_text:
                                    std_row[symbol_column_index] = symbol_text
                            
                            all_rows.append(std_row)
                        except (IndexError, AttributeError) as e:
                            logger.warning(f"Could not process row in table #{table_idx+1}: {e}")
                            # Still try to add the row with basic processing
                            if symbol_column_index == -1:
                                std_row[4] = "UNKNOWN_IDENTIFIER"
                            all_rows.append(std_row)
        
        logger.info(f"Processed {len(tables)} tables with a total of {len(all_rows)} rows")
        logger.info(f"Symbol column found: {'Yes' if symbol_column_index != -1 else 'No'}")
        return all_headers, all_rows, symbol_column_index
    
    @staticmethod
    def format_identifier(identifier: str) -> str:
        """Format identifier to uppercase with underscores."""
        # Convert to uppercase and replace spaces with underscores
        formatted = identifier.upper().replace(' ', '_')
        # Remove any special characters except underscores
        formatted = ''.join(c for c in formatted if c.isalnum() or c == '_')
        # Remove multiple consecutive underscores and trim
        formatted = '_'.join(filter(None, formatted.split('_')))
        return formatted
    
    def get_page_title(self, soup: BeautifulSoup) -> str:
        """Extract page title from the parsed HTML."""
        title = soup.find('title')
        if title:
            return self.format_identifier(title.get_text(strip=True))
        return "NC"
    
    def get_page_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract links from the 'Further Information' section."""
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
                                href = self.base_url + href
                            links.append(href)
        return links
    
    def write_to_csv(self, headers: List[str], rows: List[List[str]]) -> None:
        """Write the extracted data to a CSV file."""
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            logger.info(f"Successfully wrote {len(rows)} rows to {self.output_file}")
        except IOError as e:
            logger.error(f"Failed to write to CSV file: {e}")
            raise
    
    def run(self) -> None:
        """Execute the scraping process."""
        try:
            # Get the main page HTML
            html = self.fetch_html(self.main_url)
            main_soup = self.parse_html(html)
            
            # Extract links from the "Further Information" section
            links = self.get_page_links(main_soup)
            
            # If no links found, fall back to the original single page scraping
            if not links:
                logger.info("No links found, scraping current page only")
                links = [self.main_url]
            else:
                logger.info(f"Found {len(links)} links to process")
                
                # Remove duplicates while preserving order
                seen_links = set()
                unique_links = []
                for link in links:
                    if link not in seen_links:
                        seen_links.add(link)
                        unique_links.append(link)
                links = unique_links
                logger.info(f"Processing {len(links)} unique links")
            
            # Process each link - First collect all rows
            all_raw_rows = []
            symbol_column_indices = {}  # Track symbol column index for each page
            
            for link in links:
                try:
                    logger.info(f"Collecting data from {link}")
                    html = self.fetch_html(link)
                    soup = self.parse_html(html)
                    page_title = self.get_page_title(soup)
                    
                    try:
                        headers, rows, symbol_column_index = self.extract_table_data(soup)
                        symbol_column_indices[link] = symbol_column_index
                        
                        if len(rows) == 0:
                            logger.warning(f"No rows found in table at {link}")
                            continue
                        
                        # Store all raw rows with their page title
                        for row in rows:
                            try:
                                all_raw_rows.append((page_title, row, symbol_column_index))
                            except IndexError:
                                logger.warning(f"Row doesn't have expected columns: {row}")
                        
                    except ValueError as ve:
                        logger.warning(f"Table extraction issue: {ve}")
                    except Exception as e:
                        logger.error(f"Error processing table in {link}: {e}")
                    
                except Exception as e:
                    logger.error(f"Error processing link {link}: {e}")
            
            # Create a map to track how many times each identifier appears
            all_identifiers = []
            
            for page_title, row, symbol_column_index in all_raw_rows:
                try:
                    # Use Symbol column if available, otherwise use the strong text
                    if symbol_column_index != -1 and len(row) > symbol_column_index and row[symbol_column_index].strip():
                        base_id = f"{page_title}_{self.format_identifier(row[symbol_column_index])}"
                    else:
                        # Use the strong text (which is at index 4 if no symbol column was found)
                        idx = 4 if symbol_column_index == -1 else len(row) - 1
                        base_id = f"{page_title}_{self.format_identifier(row[idx])}"
                    
                    all_identifiers.append(base_id)
                except (IndexError, KeyError):
                    continue
            
            # Count occurrences of each identifier
            identifier_counts = {}
            for identifier in all_identifiers:
                identifier_counts[identifier] = identifier_counts.get(identifier, 0) + 1
            
            # Process all rows and create final identifiers
            all_rows = []
            identifier_counters = {}
            unique_count = 0
            duplicate_count = 0
            
            for page_title, row, symbol_column_index in all_raw_rows:
                try:
                    # Use Symbol column if available, otherwise use the strong text
                    if symbol_column_index != -1 and len(row) > symbol_column_index and row[symbol_column_index].strip():
                        base_id = f"{page_title}_{self.format_identifier(row[symbol_column_index])}"
                    else:
                        # Use the strong text (which is at index 4 if no symbol column was found)
                        idx = 4 if symbol_column_index == -1 else len(row) - 1
                        base_id = f"{page_title}_{self.format_identifier(row[idx])}"
                    
                    # Add a suffix to all identifiers that appear more than once
                    if identifier_counts[base_id] > 1:
                        # Get or initialize the counter for this base identifier
                        counter = identifier_counters.get(base_id, 0) + 1
                        identifier_counters[base_id] = counter
                        
                        # Add a suffix for identifiers that appear multiple times
                        final_id = f"{base_id}_{counter}"
                        duplicate_count += 1
                    else:
                        # Use the base identifier for unique identifiers
                        final_id = base_id
                        unique_count += 1
                    
                    all_rows.append([row[1], row[3], final_id])
                except (IndexError, KeyError) as e:
                    logger.warning(f"Error creating identifier: {e}")
                    continue
            
            # Write the CSV file with all the processed data
            self.write_to_csv(
                ["Error(Dec)", "Description", "Identifier"], 
                all_rows
            )
            
            logger.info(f"Scraping completed successfully! Scraped {len(all_rows)} error codes")
            logger.info(f"Unique identifiers: {unique_count}, Identifiers with suffix: {duplicate_count}")
                    
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)

def main():
    """Main entry point for the script."""
    scraper = ErrorCodeScraper(MAIN_URL, BASE_URL, OUTPUT_FILE)
    scraper.run()

if __name__ == "__main__":
    main()
