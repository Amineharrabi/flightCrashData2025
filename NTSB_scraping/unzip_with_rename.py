import zipfile
import os
from pathlib import Path

# Directory containing zip files
ntsb_data_dir = Path("c:\\Users\\asus\\Desktop\\FlightCrashes\\NTSB_scraping\\ntsb_data")
output_dir = ntsb_data_dir / "extracted"

# Create output directory if it doesn't exist
output_dir.mkdir(exist_ok=True)

# Get all zip files
zip_files = sorted(ntsb_data_dir.glob("ntsb_*.zip"))

print(f"Found {len(zip_files)} zip files to extract")

for zip_path in zip_files:
    # Extract year and month from filename (e.g., ntsb_2010_01.zip)
    stem = zip_path.stem  # ntsb_2010_01
    parts = stem.split("_")
    year = parts[1]
    month = parts[2]
    
    print(f"Extracting {zip_path.name}...", end=" ")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get all files in the zip
            file_list = zip_ref.namelist()
            
            for file_info in zip_ref.infolist():
                # Extract the file
                file_content = zip_ref.read(file_info.filename)
                
                # Create a new filename with year_month prefix
                original_name = Path(file_info.filename).name
                if original_name:  # Skip directories
                    new_name = f"{year}_{month}_{original_name}"
                    output_path = output_dir / new_name
                    
                    # Write the file
                    with open(output_path, 'wb') as f:
                        f.write(file_content)
        
        print(f"✓ ({len(file_list)} files)")
    except Exception as e:
        print(f"✗ Error: {e}")

# Count extracted files
extracted_files = list(output_dir.glob("*"))
print(f"\nTotal files extracted: {len(extracted_files)}")
print(f"Extracted to: {output_dir}")
