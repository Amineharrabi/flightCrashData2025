import json
from pathlib import Path

# Directory with extracted files
extracted_dir = Path("c:\\Users\\asus\\Desktop\\FlightCrashes\\NTSB_scraping\\ntsb_data\\extracted")

# Get all JSON case files (not readme.txt)
json_files = sorted([f for f in extracted_dir.glob("*.json") if "cases" in f.name])

print(f"Found {len(json_files)} JSON files to merge")

all_cases = []
total_records = 0

for json_file in json_files:
    print(f"Processing {json_file.name}...", end=" ")
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Handle if the JSON is a list or a dict
            if isinstance(data, list):
                all_cases.extend(data)
                total_records += len(data)
            elif isinstance(data, dict):
                # Check common keys for case data
                if 'cases' in data:
                    all_cases.extend(data['cases'])
                    total_records += len(data['cases'])
                else:
                    all_cases.append(data)
                    total_records += 1
        
        print(f"✓ ({len(data) if isinstance(data, list) else 1} records)")
    except Exception as e:
        print(f"✗ Error: {e}")

# Save merged data
output_file = extracted_dir / "merged_all_cases.json"

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(all_cases, f, indent=2, ensure_ascii=False)

print(f"\n✓ Merged {total_records} total records")
print(f"Saved to: {output_file}")
print(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
