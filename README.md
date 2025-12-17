# Flight Crash Data Repository

A comprehensive multi-source aviation accident data pipeline that aggregates flight crash information from multiple authoritative sources (Aviation Safety Network and NTSB) into a unified data warehouse. This repository provides tools to scrape, process, and analyze aviation accident data for research, analysis, and reporting purposes.
Watch the youtube video :

https://www.youtube.com/watch?v=T0hBFzDCDDM

![thumbnail(1)](https://github.com/user-attachments/assets/696a0373-d78c-4766-bea8-57339b1653d0)


## Table of Contents

- [Project Overview](#project-overview)
- [Data Sources](#data-sources)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup Instructions](#detailed-setup-instructions)
  - [1. Database Initialization](#1-database-initialization)
  - [2. Data Collection](#2-data-collection)
  - [3. Data Loading](#3-data-loading)
- [Scraping Instructions](#scraping-instructions)
- [Database Schema](#database-schema)
- [Project Architecture](#project-architecture)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## Project Overview

This project provides a complete ETL (Extract, Transform, Load) pipeline for aviation accident data. It combines data from three major sources:

1. **Aviation Safety Network (ASN)** - Comprehensive accident database from aviation-safety.net
2. **NTSB** - National Transportation Safety Board official accident reports
3. **CSV Data** - Additional structured accident data

The data is organized into a star-schema data warehouse with dimensions for dates, times, locations, aircraft, and operators, enabling sophisticated analytical queries.

---

## Data Sources

### Aviation Safety Network (ASN)

- **Source**: https://aviation-safety.net
- **Coverage**: 2010-2025 (and ongoing)
- **Method**: Web scraping with realistic delays and browser fingerprinting
- **Data**: Comprehensive accident summaries with links to full reports

### NTSB (National Transportation Safety Board)

- **Source**: https://data.ntsb.gov
- **Coverage**: Monthly case files from 2010 onwards
- **Method**: API-based extraction
- **Data**: Detailed accident investigation reports and case information

### CSV Data

- **Source**: Structured CSV files
- **Usage**: Supplementary accident information and historical data

---

## Repository Structure

```
flightCrashData/
├── ASN_scraping/                    # Aviation Safety Network scraper
│   ├── scraper.py                   # Main ASN web scraper
│   ├── aviation_accidents_YYYY.json # Year-specific accident data
│   ├── merged_all_accidents.json    # Combined raw data
│   ├── merged_all_accidents_cleaned.json  # Cleaned/deduplicated data
│   ├── proxies.txt                  # Proxy configuration (optional)
│   └── scraper_progress.json        # Resume capability for interrupted scrapes
│
├── NTSB_scraping/                   # NTSB data extraction
│   ├── script.py                    # Main NTSB scraper
│   ├── merge_extracted_json.py      # JSON merging utility
│   ├── unzip_with_rename.py         # Archive extraction utility
│   └── ntsb_data/
│       ├── extracted/               # Monthly case files and metadata
│       └── readme.txt               # NTSB-specific documentation
│
├── TL/                              # Transform & Load (ETL)
│   ├── commands.sql                 # Database schema and ETL SQL
│   ├── load_staging.py              # Python script to load staging tables
│   ├── ASN.json                     # Processed ASN data
│   ├── NTSB.json                    # Processed NTSB data
│   ├── CSV.csv                      # CSV source data
│   └── README.md                    # ETL-specific documentation
│
└── README.md                        # This file
```

---

## Prerequisites

### System Requirements

- **OS**: Windows, macOS, or Linux
- **Python**: 3.8 or higher
- **Database**: PostgreSQL 12 or higher

### Python Dependencies

Install required packages:

```bash
pip install requests
pip install beautifulsoup4
pip install curl-cffi
pip install psycopg2-binary
pip install lxml
```

Or use the requirements file (create if needed):

```bash
pip install -r requirements.txt
```

### Database Setup

- PostgreSQL server running and accessible
- Sufficient disk space for accident data (estimated 500MB-2GB)
- Database user with CREATE privileges

---

## Quick Start

### 1. Initialize the Database

```bash
cd TL
psql -U postgres -h localhost -d postgres -f commands.sql
```

This creates the complete schema with dimensions and fact tables.

### 2. Run the ASN Scraper (Optional - Data Already Included)

```bash
cd ASN_scraping
python scraper.py
```

The repository already includes pre-scraped data from 2010-2025.

### 3. Load Data into Database

```bash
cd TL
python load_staging.py
```

This loads data from JSON files into PostgreSQL staging tables.

### 4. Query Your Data

```sql
-- Connect to the database
psql -U postgres -d FlightAccidentMain

-- Example: Count accidents by year
SELECT
    DATE_PART('year', d.full_date) as year,
    COUNT(*) as accident_count
FROM fact_accidents fa
JOIN dim_date d ON fa.date_key = d.date_key
GROUP BY DATE_PART('year', d.full_date)
ORDER BY year DESC;
```

---

## Detailed Setup Instructions

### 1. Database Initialization

#### Step 1: Connect to PostgreSQL

```bash
psql -U postgres -h localhost
```

#### Step 2: Create the Database

```sql
CREATE DATABASE FlightAccidentMain;
\c FlightAccidentMain
```

#### Step 3: Run Schema Creation

```bash
psql -U postgres -d FlightAccidentMain -f TL/commands.sql
```

This SQL script creates:

- **Dimension Tables**: date, time, location, aircraft, operator
- **Fact Table**: fact_accidents (central fact table)
- **Staging Tables**: stg_source1_aviation_safety, stg_source2_ntsb, stg_source3_csv
- **Indexes**: For optimal query performance

#### Verify Installation

```sql
\dt  -- List all tables
\di  -- List all indexes
```

You should see all dimension, fact, and staging tables listed.

---

### 2. Data Collection

#### Option A: Using Pre-collected Data (Recommended)

The repository includes pre-scraped data files:

- `ASN_scraping/merged_all_accidents_cleaned.json`
- `NTSB_scraping/ntsb_data/extracted/*.json`
- `TL/CSV.csv`

#### Option B: Collect Fresh Data

#### ASN Scraper

**Location**: `ASN_scraping/scraper.py`

**Features**:

- Scrapes aviation-safety.net from 2010-2025
- Realistic delays and rotating user agents
- Resume capability (saves progress to `scraper_progress.json`)
- Proxy support (configure in `proxies.txt`)
- Browser fingerprinting to avoid blocking

**Run**:

```bash
cd ASN_scraping
python scraper.py
```

**Output**:

- Year-specific files: `aviation_accidents_2010.json` through `aviation_accidents_2025.json`
- Merged file: `merged_all_accidents.json`
- Cleaned file: `merged_all_accidents_cleaned.json` (deduplicated)

**Configuration** (in `scraper.py`):

```python
# Modify years to scrape
years_to_scrape = [2023, 2024, 2025]

# Adjust delays (seconds)
min_delay = 0.2
max_delay = 0.5
```

#### NTSB Scraper

**Location**: `NTSB_scraping/script.py`

**Features**:

- Extracts monthly accident data from NTSB API
- Handles date ranges efficiently
- Exports to JSON format
- Merge utility for combining outputs

**Run**:

```bash
cd NTSB_scraping
python script.py
```

**Output**:

- Monthly case files in `ntsb_data/extracted/`
- Format: `YYYY_MM_cases2025-11-23_14-43.json`

**Merge Extracted Data**:

```bash
python merge_extracted_json.py
```

---

### 3. Data Loading

#### Prepare Data Files

Ensure the following files exist in the `TL/` directory:

- `ASN.json` - From ASN scraper (merged and cleaned)
- `NTSB.json` - From NTSB scraper (merged monthly files)
- `CSV.csv` - CSV data source

#### Load into Staging Tables

```bash
cd TL
python load_staging.py
```

**What this does**:

1. Reads JSON files from ASN, NTSB, and CSV sources
2. Extracts unique identifiers for each accident
3. Loads raw data into staging tables:
   - `stg_source1_aviation_safety`
   - `stg_source2_ntsb`
   - `stg_source3_csv`
4. Marks records with status `PENDING` for processing

#### Verify Loading

```sql
SELECT COUNT(*) FROM stg_source1_aviation_safety;
SELECT COUNT(*) FROM stg_source2_ntsb;
SELECT COUNT(*) FROM stg_source3_csv;
```

---

## Scraping Instructions

### Aviation Safety Network (ASN)

#### Before You Start

- Ensure Python 3.8+ is installed
- Install dependencies: `pip install curl-cffi beautifulsoup4 requests`
- The script includes delays to be respectful to the website

#### Running the Scraper

```bash
cd ASN_scraping
python scraper.py
```

#### Configuration

Edit `scraper.py` to customize:

```python
# Target years
target_years = [2023, 2024, 2025]

# Delays between requests (seconds)
delay_min = 0.2
delay_max = 0.5

# Proxy (optional)
proxy = None  # or "http://user:pass@proxy.com:port"
```

#### Resume Interrupted Scrapes

The scraper automatically saves progress to `scraper_progress.json`. If interrupted:

```bash
python scraper.py  # Automatically resumes from last checkpoint
```

#### Output Files

| File                                | Description               |
| ----------------------------------- | ------------------------- |
| `aviation_accidents_YYYY.json`      | Per-year data files       |
| `merged_all_accidents.json`         | Combined raw data         |
| `merged_all_accidents_cleaned.json` | Deduplicated/cleaned data |
| `scraper_progress.json`             | Resume checkpoint         |

### NTSB Scraper

#### Before You Start

- Ensure Python 3.8+ is installed
- Install dependencies: `pip install requests`
- No authentication required for NTSB API

#### Running the Scraper

```bash
cd NTSB_scraping
python script.py
```

#### Output

Files are saved to `ntsb_data/extracted/`:

- Format: `YYYY_MM_cases2025-11-23_14-43.json`
- Includes monthly case summaries and metadata

#### Merge Results

```bash
python merge_extracted_json.py
```

This combines all monthly files into a single `NTSB.json`.

---

## Database Schema

### Dimension Tables

#### `dim_date`

- **Purpose**: Temporal dimension for accident dates
- **Key Fields**: full_date, year, quarter, month, day_of_month, day_name, month_name
- **Key**: date_key

#### `dim_time`

- **Purpose**: Temporal dimension for accident times
- **Key Fields**: time_value, hour, minute, second
- **Key**: time_key

#### `dim_location`

- **Purpose**: Geographic and airport information
- **Key Fields**: country, state_province, city, airport_code, airport_name, latitude, longitude
- **Key**: location_key
- **Unique**: (data_source, source_location_id)

#### `dim_aircraft`

- **Purpose**: Aircraft specifications and details
- **Key Fields**: type_name, manufacturer, model, registration_number, serial_number, number_of_engines
- **Key**: aircraft_key
- **Unique**: (data_source, source_aircraft_id)

#### `dim_operator`

- **Purpose**: Airline and flight operation information
- **Key Fields**: operator_name, operator_type, owner_name, flight_operation_type
- **Key**: operator_key
- **Unique**: (data_source, source_operator_id)

### Fact Table

#### `fact_accidents`

- **Purpose**: Central fact table containing all accident records
- **Key Fields**:
  - Flight information: flight_number, route_departure, route_destination
  - Injury metrics: total_aboard, fatalities_total, fatalities_crew, fatalities_passengers, ground_fatalities
  - Data source tracking: data_source, source_unique_id
- **Foreign Keys**: date_key, time_key, location_key, aircraft_key, operator_key
- **Key**: accident_id

### Staging Tables

Intermediate tables for data ingestion:

- `stg_source1_aviation_safety` - ASN raw data
- `stg_source2_ntsb` - NTSB raw data
- `stg_source3_csv` - CSV data

---

## Project Architecture

### Data Flow

```
ASN Website          NTSB API           CSV Files
      ↓                  ↓                   ↓
   scraper.py        script.py          CSV reader
      ↓                  ↓                   ↓
  JSON Files        JSON Files          JSON Format
      ↓                  ↓                   ↓
   ┌──────────────────────────────────────┐
   │    Staging Tables (PostgreSQL)       │
   │  - stg_source1_aviation_safety       │
   │  - stg_source2_ntsb                  │
   │  - stg_source3_csv                   │
   └──────────────────────────────────────┘
                       ↓
   ┌──────────────────────────────────────┐
   │   Transformation & Deduplication     │
   │   (ETL process)                      │
   └──────────────────────────────────────┘
                       ↓
   ┌──────────────────────────────────────┐
   │  Dimension & Fact Tables             │
   │  - dim_date, dim_time                │
   │  - dim_location, dim_aircraft        │
   │  - dim_operator                      │
   │  - fact_accidents                    │
   └──────────────────────────────────────┘
                       ↓
   ┌──────────────────────────────────────┐
   │  Analytics & Reporting               │
   │  (Your queries here)                 │
   └──────────────────────────────────────┘
```

### Technology Stack

- **Data Extraction**: Python (requests, curl_cffi, BeautifulSoup)
- **Data Storage**: PostgreSQL 12+
- **Data Processing**: Python, SQL
- **Format**: JSON, CSV, JSONB (PostgreSQL)

---

## Troubleshooting

### Database Connection Issues

**Problem**: `psql: could not translate host name "localhost" to address`

**Solution**:

```bash
# Use IP address instead
psql -U postgres -h 127.0.0.1 -d FlightAccidentMain

# Or check PostgreSQL service status (Windows)
Get-Service PostgreSQL*
```

### Scraper Getting Blocked

**Problem**: 403 Forbidden or Connection Timeout

**Solution**:

1. Increase delays in scraper configuration
2. Use proxy (configure in `proxies.txt`)
3. Restart after a few minutes

```python
delay_min = 1.0  # 1 second minimum
delay_max = 3.0  # 3 second maximum
```

### Duplicate Key Errors During Loading

**Problem**: `duplicate key value violates unique constraint`

**Solution**:

```sql
-- Clear staging tables and retry
TRUNCATE TABLE stg_source1_aviation_safety CASCADE;
TRUNCATE TABLE stg_source2_ntsb CASCADE;
TRUNCATE TABLE stg_source3_csv CASCADE;
```

### Out of Memory During Scraping

**Problem**: Python runs out of memory with large JSON files

**Solution**:

1. Process data in smaller time ranges
2. Delete intermediate files after merging
3. Increase system virtual memory

### Database Not Found

**Problem**: `database "FlightAccidentMain" does not exist`

**Solution**:

```sql
-- Create database
CREATE DATABASE FlightAccidentMain;

-- Re-run schema
psql -U postgres -d FlightAccidentMain -f TL/commands.sql
```

---

## Example Queries

### Count Accidents by Year

```sql
SELECT
    DATE_PART('year', d.full_date) as year,
    COUNT(*) as accident_count,
    SUM(fa.fatalities_total) as total_fatalities
FROM fact_accidents fa
JOIN dim_date d ON fa.date_key = d.date_key
GROUP BY DATE_PART('year', d.full_date)
ORDER BY year DESC;
```

### Top 10 Most Dangerous Aircraft Types

```sql
SELECT
    da.type_name,
    COUNT(*) as accident_count,
    AVG(fa.fatalities_total) as avg_fatalities
FROM fact_accidents fa
JOIN dim_aircraft da ON fa.aircraft_key = da.aircraft_key
WHERE fa.fatalities_total > 0
GROUP BY da.type_name
ORDER BY accident_count DESC
LIMIT 10;
```

### Accidents by Country (Last 5 Years)

```sql
SELECT
    dl.country,
    COUNT(*) as accident_count,
    SUM(fa.fatalities_total) as total_fatalities
FROM fact_accidents fa
JOIN dim_location dl ON fa.location_key = dl.location_key
JOIN dim_date d ON fa.date_key = d.date_key
WHERE DATE_PART('year', d.full_date) >= DATE_PART('year', CURRENT_DATE) - 5
GROUP BY dl.country
ORDER BY accident_count DESC;
```

---

## Contributing

To contribute to this project:

1. Report issues or suggest improvements via GitHub Issues
2. Submit enhancements via Pull Requests
3. Ensure data quality and completeness
4. Test scraping changes before submitting

---

## Contact & Support

For questions or support regarding this repository, please open an issue or contact the project maintainers.

---

## License

Please refer to the LICENSE file in the repository for licensing information.

---

**Last Updated**: December 2025  
**Data Coverage**: 2010-2025  
**Sources**: Aviation Safety Network, NTSB, CSV Data
