"""
Aviation Safety Network Web Scraper
Scrapes accident data from aviation-safety.net for years 2023-2025
with realistic delays, rotating user agents, and browser fingerprinting
Supports resuming interrupted scrapes with progress logging
"""

import json
import time
import random
import os
from datetime import datetime
from typing import Dict, List, Optional, Set
from curl_cffi import requests
from bs4 import BeautifulSoup
import re

class AviationSafetyScraper:
    def __init__(self, proxy: Optional[str] = None):
        """
        Initialize scraper with optional proxy support
        
        Args:
            proxy: Proxy URL in format 'http://user:pass@host:port' (optional)
        """
        self.base_url = "https://aviation-safety.net"
        self.proxy = proxy
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
        ]
        
    def _get_random_delay(self, min_delay: float = 0.2, max_delay: float = 0.5) -> float:
        """Generate realistic random delay with slight variations"""
        base_delay = random.uniform(min_delay, max_delay)
        # Add occasional longer pauses to mimic human behavior
        if random.random() < 0.1:  # 10% chance of longer pause
            base_delay += random.uniform(1, 1)
        return base_delay
    
    def _get_session(self) -> requests.Session:
        """Create a session with curl_cffi for browser fingerprinting"""
        session = requests.Session()
        return session
    
    def _make_request(self, url: str, session: requests.Session) -> Optional[str]:
        """Make HTTP request with realistic browser fingerprinting"""
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        
        proxies = {'http': self.proxy, 'https': self.proxy} if self.proxy else None
        
        try:
            # Use curl_cffi's impersonate feature to mimic real browsers
            response = session.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=30,
                impersonate="chrome120"  # Mimics Chrome 120 browser fingerprint
            )
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None  # Return None for 404 (will be handled by caller)
            print(f"HTTP Error fetching {url}: {str(e)}")
            return None
        except Exception as e:
            print(f"Error fetching {url}: {str(e)}")
            return None
    
    def _parse_accident_list(self, html: str) -> List[str]:
        """Extract accident detail URLs from year listing page"""
        soup = BeautifulSoup(html, 'html.parser')
        accident_urls = []
        
        # Find all accident links in the table
        for row in soup.find_all('tr', class_='list'):
            link = row.find('a', href=True)
            if link and '/wikibase/' in link['href']:
                full_url = f"{self.base_url}{link['href']}"
                accident_urls.append(full_url)
        
        return accident_urls
    
    def _extract_country(self, location_text: str, soup: BeautifulSoup) -> str:
        """Extract country from location field"""
        # Try to find country link in the location row
        location_row = soup.find('td', class_='caption', string=re.compile('Location:'))
        if location_row:
            location_cell = location_row.find_next_sibling('td')
            if location_cell:
                country_link = location_cell.find('a', href=re.compile('/asndb/country/'))
                if country_link:
                    return country_link.text.strip()
        
        # Fallback: extract from text
        if '-' in location_text:
            parts = location_text.split('-')
            if len(parts) >= 2:
                return parts[-1].strip()
        
        return location_text.strip()
    
    def _parse_accident_detail(self, html: str, url: str) -> Optional[Dict]:
        """Parse individual accident detail page"""
        soup = BeautifulSoup(html, 'html.parser')
        accident_data = {'url': url}
        
        # Parse table data
        table = soup.find('table')
        if not table:
            return None
        
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                caption = cells[0].text.strip().replace(':', '').lower()
                value = cells[1].text.strip()
                
                if 'date' in caption:
                    accident_data['date'] = value
                elif 'time' in caption:
                    accident_data['time'] = value if value else None
                elif 'type' in caption and 'aircraft' not in caption:
                    # Extract aircraft type from link
                    type_link = cells[1].find('a')
                    accident_data['type'] = type_link.text.strip() if type_link else value
                elif 'owner/operator' in caption:
                    accident_data['owner_operator'] = value
                elif 'registration' in caption:
                    accident_data['registration'] = value
                elif 'msn' in caption:
                    accident_data['msn'] = value
                elif 'year of manufacture' in caption:
                    accident_data['year_of_manufacture'] = value
                elif 'fatalities' in caption and 'other' not in caption:
                    accident_data['fatalities'] = value
                elif 'aircraft damage' in caption:
                    accident_data['aircraft_damage'] = value
                elif 'location' in caption:
                    accident_data['location'] = self._extract_country(value, soup)
                elif 'phase' in caption:
                    accident_data['phase'] = value
                elif 'nature' in caption:
                    accident_data['nature'] = value
                elif 'departure airport' in caption:
                    accident_data['departure_airport'] = value if value else None
                elif 'destination airport' in caption:
                    accident_data['destination_airport'] = value if value else None
                elif 'confidence rating' in caption:
                    accident_data['confidence_rating'] = value
        
        # Parse narrative
        narrative_tag = soup.find('span', class_='caption', string='Narrative:')
        if narrative_tag:
            narrative_span = narrative_tag.find_next('span')
            if narrative_span:
                accident_data['narrative'] = narrative_span.text.strip()
            else:
                # Sometimes narrative follows directly after caption
                narrative_text = narrative_tag.find_next_sibling(string=True)
                if narrative_text:
                    accident_data['narrative'] = narrative_text.strip()
        
        # Parse sources
        sources_div = soup.find('div', class_='captionhr', string='Sources:')
        if sources_div:
            sources = []
            next_element = sources_div.find_next_sibling()
            while next_element and next_element.name != 'div':
                if next_element.name == 'a':
                    sources.append(next_element.get('href', next_element.text).strip())
                elif hasattr(next_element, 'find_all'):
                    for link in next_element.find_all('a'):
                        sources.append(link.get('href', link.text).strip())
                next_element = next_element.find_next_sibling()
            accident_data['sources'] = sources
        
        return accident_data
    
    def scrape_year(self, year: int) -> List[Dict]:
        """Scrape all accidents for a given year by paginating through all pages"""
        print(f"\n{'='*60}")
        print(f"Starting scrape for year {year}")
        print(f"{'='*60}\n")
        
        session = self._get_session()
        all_accidents = []
        page_number = 1
        
        # Loop through pages until we get a 404
        while True:
            page_url = f"{self.base_url}/asndb/year/{year}/{page_number}"
            print(f"\nFetching page {page_number}: {page_url}")
            
            # Realistic delay between page requests
            delay = self._get_random_delay()
            time.sleep(delay)
            
            # Fetch page
            html = self._make_request(page_url, session)
            if not html:
                print(f"Reached end of pages for year {year} (page {page_number} returned 404 or error)")
                break
            
            # Extract accident URLs from this page
            accident_urls = self._parse_accident_list(html)
            if not accident_urls:
                print(f"No accidents found on page {page_number}, stopping pagination")
                break
            
            print(f"Found {len(accident_urls)} accidents on page {page_number}")
            
            # Scrape each accident on this page
            for i, url in enumerate(accident_urls, 1):
                print(f"  Accident {i}/{len(accident_urls)}: {url}")
                
                # Realistic delay between requests
                delay = self._get_random_delay()
                print(f"  Waiting {delay:.2f} seconds...")
                time.sleep(delay)
                
                # Fetch accident detail
                html = self._make_request(url, session)
                if html:
                    accident_data = self._parse_accident_detail(html, url)
                    if accident_data:
                        all_accidents.append(accident_data)
                        print(f"  ✓ Successfully scraped: {accident_data.get('type', 'Unknown')} - {accident_data.get('date', 'Unknown date')}")
                    else:
                        print(f"  ✗ Failed to parse accident data")
                else:
                    print(f"  ✗ Failed to fetch accident page")
            
            page_number += 1
        
        return all_accidents
    
    def save_to_json(self, data: List[Dict], year: int, filename: Optional[str] = None):
        """Save scraped data to JSON file"""
        if filename is None:
            filename = f"aviation_accidents_{year}.json"
        
        output = {
            'year': year,
            'total_accidents': len(data),
            'scraped_at': datetime.now().isoformat(),
            'accidents': data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*60}")
        print(f"Saved {len(data)} accidents to {filename}")
        print(f"{'='*60}\n")


def main():
    """Main execution function"""
    # Optional: Configure proxy if you have one
    # proxy = "http://username:password@proxy-host:port"
    proxy = None
    
    scraper = AviationSafetyScraper(proxy=proxy)
    
    # Scrape years 2024, and 2025
    years = [2010]

    for year in years:
        try:
            accidents = scraper.scrape_year(year)
            scraper.save_to_json(accidents, year)
            
            if year != years[-1]:
                delay = random.uniform(10, 20)
                print(f"\nWaiting {delay:.2f} seconds before next year...\n")
                time.sleep(delay)
                
        except Exception as e:
            print(f"Error scraping year {year}: {str(e)}")
            continue
    
    print("\n" + "="*60)
    print("Scraping completed!")
    print("="*60)


if __name__ == "__main__":
    main()