"""
TwinCAT Error Code Scraper

This script scrapes error codes from Beckhoff TwinCAT documentation.
It extracts error codes, descriptions, and identifiers from HTML tables
and saves them to CSV and TwinCAT PLC files for easy reference.
"""

# Standard library imports
import csv
import logging
import os
import re
from typing import Tuple, List, Dict, Optional, Any, Set, NamedTuple

# Third-party imports
import requests
from bs4 import BeautifulSoup

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
MAIN_URL: str = "https://infosys.beckhoff.com/content/1033/tc3ncerrcode/1521556875.html"
BASE_URL: str = "https://infosys.beckhoff.com/content/1033/tc3ncerrcode/"
OUTPUT_FILE: str = "tc3ncerrcode.csv"


class ErrorCode(NamedTuple):
    """Structure to hold error code data."""
    code: str
    description: str
    identifier: str


class ErrorCodeScraper:
    """
    Scraper for TwinCAT error codes from Beckhoff documentation.
    
    This class handles fetching HTML content, parsing tables containing error codes,
    extracting relevant information, and saving the results to a CSV file and TwinCAT PLC files.
    """
    
    def __init__(self, main_url: str, base_url: str, output_file: str):
        """
        Initialize the scraper with configuration parameters.
        
        Args:
            main_url: URL of the main page containing error code tables or links
            base_url: Base URL used to construct absolute URLs from relative links
            output_file: Path to the CSV file where results will be saved
        """
        self.main_url = main_url
        self.base_url = base_url
        self.output_file = output_file
        
        # Define output files for TwinCAT PLC components
        # Change output directory to the specified NC directory
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(output_file)), "TcError", "TcError", "NC")
        
        # Make sure the output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set the output file paths
        self.enum_file = os.path.join(self.output_dir, "NcErrorCodes.TcDUT")
        self.description_file = os.path.join(self.output_dir, "NcErrorCodeDescription.TcPOU")
        self.converter_file = os.path.join(self.output_dir, "ToNcErrorCode.TcPOU")

    # Web scraping methods
    def fetch_html(self, url: str) -> bytes:
        """
        Fetch HTML content from the specified URL with error handling.
        
        Args:
            url: The URL to fetch content from
            
        Returns:
            Raw HTML content as bytes
            
        Raises:
            requests.RequestException: If the request fails
        """
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to fetch URL {url}: {e}")
            raise
    
    def parse_html(self, html: bytes) -> BeautifulSoup:
        """
        Parse HTML content and handle iframe if present.
        
        Args:
            html: Raw HTML content
            
        Returns:
            BeautifulSoup object of the parsed HTML
        """
        soup = BeautifulSoup(html, 'html.parser')
        iframe = soup.find('iframe')
        if iframe and 'src' in iframe.attrs:
            iframe_src = iframe['src']
            html = self.fetch_html(iframe_src)
            soup = BeautifulSoup(html, 'html.parser')
        return soup
    
    def get_page_title(self, soup: BeautifulSoup) -> str:
        """
        Extract page title from the parsed HTML.
        
        Args:
            soup: BeautifulSoup object of the parsed HTML
            
        Returns:
            Formatted page title
        """
        title = soup.find('title')
        if title:
            return self.format_identifier(title.get_text(strip=True))
        return "NC"
    
    def get_page_links(self, soup: BeautifulSoup) -> List[str]:
        """
        Extract links from the 'Further Information' section.
        
        Args:
            soup: BeautifulSoup object of the parsed HTML
            
        Returns:
            List of URLs
        """
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
    
    def extract_table_data(self, soup: BeautifulSoup) -> Tuple[List[str], List[List[str]], int]:
        """
        Extract table headers and row data from the parsed HTML.
        
        Args:
            soup: BeautifulSoup object of the parsed HTML
            
        Returns:
            Tuple containing (headers, rows, symbol_column_index)
            
        Raises:
            ValueError: If no table is found on the page
        """
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
            has_error_code = any(h.lower() in ['error(dec)', 'error code', 'error', 'code (dec)'] for h in headers)
            has_description = any(h.lower() in ['description', 'text'] for h in headers)
            
            # Find header indexes
            error_code_idx = next((i for i, h in enumerate(headers) if h.lower() in ['error(dec)', 'error code', 'error', 'code (dec)']), 0)
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
    
    # Data processing methods
    @staticmethod
    def format_identifier(identifier: str) -> str:
        """
        Format identifier to uppercase with underscores.
        
        Args:
            identifier: Raw identifier string
            
        Returns:
            Formatted identifier
        """
        # Convert to uppercase and replace spaces with underscores
        formatted = identifier.upper().replace(' ', '_')
        # Remove any special characters except underscores
        formatted = ''.join(c for c in formatted if c.isalnum() or c == '_')
        # Remove multiple consecutive underscores and trim
        formatted = '_'.join(filter(None, formatted.split('_')))
        return formatted
    
    def process_error_codes(self, all_raw_rows: List[Tuple[str, List[str], int]]) -> List[ErrorCode]:
        """
        Process raw data rows into structured ErrorCode objects.
        
        Args:
            all_raw_rows: List of raw data rows
            
        Returns:
            List of ErrorCode objects
        """
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
        error_codes = []
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
                
                error_codes.append(ErrorCode(row[1], row[3], final_id))
            except (IndexError, KeyError) as e:
                logger.warning(f"Error creating identifier: {e}")
                continue
        
        logger.info(f"Processed error codes: {len(error_codes)} total")
        logger.info(f"Unique identifiers: {unique_count}, Identifiers with suffix: {duplicate_count}")
        
        return error_codes
    
    # File output methods
    def write_to_csv(self, error_codes: List[ErrorCode]) -> None:
        """
        Write error codes to a CSV file.
        
        Args:
            error_codes: List of ErrorCode objects
            
        Raises:
            IOError: If writing to the file fails
        """
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Error(Dec)", "Description", "Identifier"])
                writer.writerows([(ec.code, ec.description, ec.identifier) for ec in error_codes])
            logger.info(f"Successfully wrote {len(error_codes)} rows to {self.output_file}")
        except IOError as e:
            logger.error(f"Failed to write to CSV file: {e}")
            raise
    
    def write_enum_definition(self, error_codes: List[ErrorCode]) -> None:
        """
        Generate a TwinCAT PLC enumeration type definition file (NcErrorCodes.TcDUT).
        
        Args:
            error_codes: List of ErrorCode objects
        """
        try:
            # Sort error codes by code number
            sorted_codes = sorted(
                error_codes, 
                key=lambda x: int(x.code.split('-')[0]) if x.code.split('-')[0].isdigit() else 0
            )
            
            with open(self.enum_file, 'w', encoding='utf-8') as f:
                f.write('<?xml version="1.0" encoding="utf-8"?>\n')
                f.write('<TcPlcObject Version="1.1.0.1">\n')
                f.write('  <DUT Name="NcErrorCodes" Id="{00000000-0000-0000-0000-000000000000}">\n')
                f.write('    <Declaration><![CDATA[{attribute \'qualified_only\'}\n')
                f.write('{attribute \'strict\'}\n')
                f.write('(*\n')
                f.write('NC Error codes\n\n')
                f.write(f'Source: {self.main_url}\n')
                f.write('*)\n')
                f.write('TYPE NcErrorCodes : (\n')
                f.write('    NO_ERROR := 0,\n')
                
                # Write each error code enum
                for ec in sorted_codes:
                    # Skip if missing essential data
                    if not ec.code or not ec.identifier:
                        continue
                    
                    # For error code ranges (containing '-'), take only the first number
                    code = ec.code.split('-')[0] if '-' in ec.code else ec.code
                    
                    f.write(f'    {ec.identifier} := {code},\n')
                
                f.write('    ERR_UNKNOWN := 65535\n')
                f.write(');\n')
                f.write('END_TYPE\n')
                f.write(']]></Declaration>\n')
                f.write('  </DUT>\n')
                f.write('</TcPlcObject>')
                
            logger.info(f"Successfully wrote enum definition to {self.enum_file}")
        except IOError as e:
            logger.error(f"Failed to write enum definition file: {e}")
    
    def write_description_function(self, error_codes: List[ErrorCode]) -> None:
        """
        Generate a TwinCAT PLC function for error descriptions (NcErrorCodeDescription.TcPOU).
        
        Args:
            error_codes: List of ErrorCode objects
        """
        try:
            # Sort error codes by code number
            sorted_codes = sorted(
                error_codes, 
                key=lambda x: int(x.code.split('-')[0]) if x.code.split('-')[0].isdigit() else 0
            )
            
            with open(self.description_file, 'w', encoding='utf-8') as f:
                f.write('<?xml version="1.0" encoding="utf-8"?>\n')
                f.write('<TcPlcObject Version="1.1.0.1">\n')
                f.write('  <POU Name="NcErrorCodeDescription" Id="{00000000-0000-0000-0000-000000000000}" SpecialFunc="None">\n')
                f.write('    <Declaration><![CDATA[(*\n')
                f.write('Returns a description of the error from the NcErrorCodes datatype.\n\n')
                f.write('## Example\n')
                f.write('```\n')
                f.write('ncErrorCode := NcErrorCodes.CONTROLLER_ERROR_LAG_ERROR_WINDOW_VELOCITY_NOT_ALLOWED;\n')
                f.write('errorDescription := NcErrorCodeDescription(ncErrorCode);\n')
                f.write('```\n\n')
                f.write(f'## Source\n{self.main_url}\n')
                f.write('*)\n')
                f.write('FUNCTION NcErrorCodeDescription : T_MaxString\n')
                f.write('VAR_INPUT\n')
                f.write('    ncErrorCode : NcErrorCodes;\n')
                f.write('END_VAR\n')
                f.write(']]></Declaration>\n')
                f.write('    <Implementation>\n')
                f.write('      <ST><![CDATA[CASE ncErrorCode OF\n')
                
                # Write each error code case
                for ec in sorted_codes:
                    # Skip if missing essential data
                    if not ec.code or not ec.identifier or not ec.description:
                        continue
                    
                    # Replace single quotes with double quotes in description and format
                    description = ec.description.replace("'", "\"").replace('\n', ' ').strip()
                    
                    f.write(f'    NcErrorCodes.{ec.identifier}:\n')
                    f.write(f"        NcErrorCodeDescription := '{description}';\n\n")
                
                f.write('    NcErrorCodes.ERR_UNKNOWN:\n')
                f.write("        NcErrorCodeDescription := 'Unknown NC error code.';\n\n")
                f.write('ELSE\n')
                f.write("    NcErrorCodeDescription := 'Error code not recognized';\n")
                f.write('END_CASE\n')
                f.write(']]></ST>\n')
                f.write('    </Implementation>\n')
                f.write('  </POU>\n')
                f.write('</TcPlcObject>')
                
            logger.info(f"Successfully wrote description function to {self.description_file}")
        except IOError as e:
            logger.error(f"Failed to write description function file: {e}")
    
    def write_converter_function(self, error_codes: List[ErrorCode]) -> None:
        """
        Generate a TwinCAT PLC function for converting UDINT to NcErrorCodes (ToNcErrorCode.TcPOU).
        
        Args:
            error_codes: List of ErrorCode objects
        """
        try:
            # Sort error codes by code number
            sorted_codes = sorted(
                error_codes, 
                key=lambda x: int(x.code.split('-')[0]) if x.code.split('-')[0].isdigit() else 0
            )
            
            with open(self.converter_file, 'w', encoding='utf-8') as f:
                f.write('<?xml version="1.0" encoding="utf-8"?>\n')
                f.write('<TcPlcObject Version="1.1.0.1">\n')
                f.write('  <POU Name="ToNcErrorCode" Id="{00000000-0000-0000-0000-000000000000}" SpecialFunc="None">\n')
                f.write('    <Declaration><![CDATA[(*\n')
                f.write('Convert a NC error code of type UDINT to the NcErrorCodes datatype.\n\n')
                f.write('## Example\n')
                f.write('```\n')
                f.write('ncErrorId := 17693;\n')
                f.write('ncErrorCode := ToNcErrorCode(ncErrorId);\n')
                f.write('```\n\n')
                f.write(f'## Source\n{self.main_url}\n')
                f.write('*)\n')
                f.write('FUNCTION ToNcErrorCode : NcErrorCodes\n')
                f.write('VAR_INPUT\n')
                f.write('    errorCode : UDINT;\n')
                f.write('END_VAR\n')
                f.write(']]></Declaration>\n')
                f.write('    <Implementation>\n')
                f.write('      <ST><![CDATA[CASE errorCode OF\n')
                
                # Write each error code case
                for ec in sorted_codes:
                    # Skip if missing essential data
                    if not ec.code or not ec.identifier:
                        continue
                    
                    # For error code ranges (containing '-'), replace with '..' for case statements
                    case_code = ec.code
                    if '-' in case_code:
                        start, end = case_code.split('-')
                        case_code = f"{start}..{end}"
                    
                    f.write(f'    {case_code}:\n')
                    f.write(f'        ToNcErrorCode := NcErrorCodes.{ec.identifier};\n\n')
                
                f.write('ELSE\n')
                f.write('    ToNcErrorCode := NcErrorCodes.ERR_UNKNOWN;\n')
                f.write('END_CASE\n')
                f.write(']]></ST>\n')
                f.write('    </Implementation>\n')
                f.write('  </POU>\n')
                f.write('</TcPlcObject>')
                
            logger.info(f"Successfully wrote converter function to {self.converter_file}")
        except IOError as e:
            logger.error(f"Failed to write converter function file: {e}")
    
    def run(self) -> None:
        """Execute the complete scraping process."""
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
                links = list(dict.fromkeys(links))
                logger.info(f"Processing {len(links)} unique links")
            
            # Process each link - First collect all rows
            all_raw_rows = []
            
            for link in links:
                try:
                    logger.info(f"Collecting data from {link}")
                    html = self.fetch_html(link)
                    soup = self.parse_html(html)
                    page_title = self.get_page_title(soup)
                    
                    try:
                        headers, rows, symbol_column_index = self.extract_table_data(soup)
                        
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
            
            # Process raw data into structured error codes
            error_codes = self.process_error_codes(all_raw_rows)
            
            # Write output files
            self.write_to_csv(error_codes)
            self.write_enum_definition(error_codes)
            self.write_description_function(error_codes)
            self.write_converter_function(error_codes)
            
            logger.info(f"Scraping completed successfully! Generated files:")
            logger.info(f"- CSV: {self.output_file}")
            logger.info(f"- Enum definition: {self.enum_file}")
            logger.info(f"- Description function: {self.description_file}")
            logger.info(f"- Converter function: {self.converter_file}")
                    
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)


def main():
    """Main entry point for the script."""
    scraper = ErrorCodeScraper(MAIN_URL, BASE_URL, OUTPUT_FILE)
    scraper.run()


if __name__ == "__main__":
    main()
