import requests
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

class NTSBScraper:
    def __init__(self, output_dir="ntsb_data"):
        self.base_url = "https://data.ntsb.gov/carol-main-public/api/Query/FileExport"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'data.ntsb.gov',
            'Origin': 'https://data.ntsb.gov',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0'
        }
    
    def get_month_date_range(self, year, month):
        """Get the first and last day of a given month"""
        first_day = datetime(year, month, 1)
        
        # Get last day of month
        if month == 12:
            last_day = datetime(year, 12, 31)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")
    
    def create_payload(self, year, month, session_id=227230):
        """Create the POST request payload for a specific month"""
        first_day, last_day = self.get_month_date_range(year, month)
        
        payload = {
            "QueryGroups": [{
                "QueryRules": [
                    {
                        "RuleType": "Simple",
                        "Values": [first_day],
                        "Columns": ["Event.EventDate"],
                        "Operator": "is on or after",
                        "overrideColumn": "",
                        "selectedOption": {
                            "FieldName": "EventDate",
                            "DisplayText": "Event date",
                            "Columns": ["Event.EventDate"],
                            "Selectable": True,
                            "InputType": "Date",
                            "RuleType": 0,
                            "Options": None,
                            "TargetCollection": "cases",
                            "UnderDevelopment": True
                        }
                    },
                    {
                        "RuleType": "Simple",
                        "Values": [last_day],
                        "Columns": ["Event.EventDate"],
                        "Operator": "is on or before",
                        "selectedOption": {
                            "FieldName": "EventDate",
                            "DisplayText": "Event date",
                            "Columns": ["Event.EventDate"],
                            "Selectable": True,
                            "InputType": "Date",
                            "RuleType": 0,
                            "Options": None,
                            "TargetCollection": "cases",
                            "UnderDevelopment": True
                        },
                        "overrideColumn": ""
                    },
                    {
                        "RuleType": "Simple",
                        "Values": ["Aviation"],
                        "Columns": ["Event.Mode"],
                        "Operator": "is",
                        "selectedOption": {
                            "FieldName": "Mode",
                            "DisplayText": "Investigation mode",
                            "Columns": ["Event.Mode"],
                            "Selectable": True,
                            "InputType": "Dropdown",
                            "RuleType": 0,
                            "Options": None,
                            "TargetCollection": "cases",
                            "UnderDevelopment": True
                        },
                        "overrideColumn": ""
                    }
                ],
                "AndOr": "and",
                "inLastSearch": False,
                "editedSinceLastSearch": False
            }],
            "AndOr": "and",
            "TargetCollection": "cases",
            "ExportFormat": "data",
            "SessionId": session_id,
            "ResultSetSize": 500,
            "SortDescending": True
        }
        
        return payload
    
    def download_month(self, year, month, delay=2):
        """Download data for a specific month"""
        filename = f"ntsb_{year}_{month:02d}.zip"
        filepath = self.output_dir / filename
        
        # Skip if already downloaded
        if filepath.exists():
            print(f"✓ Already exists: {filename}")
            return True
        
        # Update referer header for this specific request
        referer = f"https://data.ntsb.gov/carol-main-public/query-builder?month={month}&year={year}"
        headers = self.headers.copy()
        headers['Referer'] = referer
        
        # Create payload
        payload = self.create_payload(year, month)
        
        try:
            print(f"Downloading: {filename}... ", end="", flush=True)
            
            response = requests.post(
                self.base_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content) / 1024  # kb
            print(f"✓ Success ({file_size:.2f} KB)")
            
            time.sleep(delay)
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed: {str(e)}")
            return False
    
    def download_all(self, start_year=2010, end_year=2025, delay=2):
        total = 0
        successful = 0
        failed = 0
        
        current_date = datetime.now()
        
        for year in range(start_year, end_year + 1):
            print(f"\n{'='*50}")
            print(f"Year: {year}")
            print(f"{'='*50}")
            
            # Determine last month to download for this year
            if year == current_date.year:
                last_month = current_date.month
            else:
                last_month = 12
            
            for month in range(1, last_month + 1):
                total += 1
                if self.download_month(year, month, delay):
                    successful += 1
                else:
                    failed += 1
        
        # Summary
        print(f"\n{'='*50}")
        print(f"Download Summary")
        print(f"{'='*50}")
        print(f"Total attempts: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Files saved to: {self.output_dir.absolute()}")

if __name__ == "__main__":
    scraper = NTSBScraper(output_dir="ntsb_data")
    

    scraper.download_all(start_year=2010, end_year=2025, delay=2)