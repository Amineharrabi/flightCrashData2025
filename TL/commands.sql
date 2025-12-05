CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    time_value TIME NOT NULL UNIQUE,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    hour_name VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_time_hour ON dim_time(hour);


CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    location_id VARCHAR(100) UNIQUE,
    
    country VARCHAR(100),
    state_province VARCHAR(100),
    city VARCHAR(100),
    
    airport_code VARCHAR(10),
    airport_name VARCHAR(255),
    
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    accident_site_condition VARCHAR(50),
    
    data_source VARCHAR(50),
    source_location_id VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE dim_location
ALTER COLUMN latitude TYPE DOUBLE PRECISION,
ALTER COLUMN longitude TYPE DOUBLE PRECISION;


CREATE INDEX idx_dim_location_country_city ON dim_location(country, city);
CREATE INDEX idx_dim_location_airport ON dim_location(airport_code);
CREATE UNIQUE INDEX idx_dim_location_unique_id ON dim_location(data_source, source_location_id);

CREATE TABLE dim_aircraft (
    aircraft_key SERIAL PRIMARY KEY,
    aircraft_id VARCHAR(100) UNIQUE,
    
    type_name VARCHAR(255),
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    registration_number VARCHAR(50),
    serial_number VARCHAR(100),
    msn VARCHAR(100),
    
    number_of_engines INTEGER,
    engine_type VARCHAR(100),
    aircraft_category VARCHAR(50),
    
    amateur_built BOOLEAN,
    air_medical BOOLEAN,
    
    data_source VARCHAR(50),
    source_aircraft_id VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Add a unique constraint on (data_source, source_aircraft_id)
ALTER TABLE dim_aircraft
ADD CONSTRAINT uq_dim_aircraft_source UNIQUE (data_source, source_aircraft_id);


CREATE INDEX idx_dim_aircraft_registration ON dim_aircraft(registration_number);
CREATE INDEX idx_dim_aircraft_manufacturer_model ON dim_aircraft(manufacturer, model);
CREATE UNIQUE INDEX idx_dim_aircraft_unique_id ON dim_aircraft(data_source, source_aircraft_id);


CREATE TABLE dim_operator (
    operator_key SERIAL PRIMARY KEY,
    operator_id VARCHAR(100) UNIQUE,
    
    operator_name VARCHAR(255) NOT NULL,
    operator_type VARCHAR(100),
    owner_name VARCHAR(255),
    
    flight_operation_type VARCHAR(100),
    flight_scheduled_type VARCHAR(100),
    flight_service_type VARCHAR(100),
    
    regulation_flight_conducted_under VARCHAR(50),
    
    data_source VARCHAR(50),
    source_operator_id VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_operator_name ON dim_operator(operator_name);
CREATE UNIQUE INDEX idx_dim_operator_unique_id ON dim_operator(data_source, source_operator_id);

-- ============================================================
-- PHASE 2: STAGING TABLES (For raw data ingestion)
-- ============================================================

-- Staging for Source 1 (Aviation Safety JSON)
CREATE TABLE stg_source1_aviation_safety (
    stg_id SERIAL PRIMARY KEY,
    source_unique_id VARCHAR(100),
    raw_json JSONB,
    processing_status VARCHAR(50) DEFAULT 'PENDING',
    error_message TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE stg_source1_aviation_safety
ADD CONSTRAINT stg_source1_unique UNIQUE (source_unique_id);

-- Staging for Source 2 (NTSB JSON)
CREATE TABLE stg_source2_ntsb (
    stg_id SERIAL PRIMARY KEY,
    source_unique_id VARCHAR(100),
    raw_json JSONB,
    processing_status VARCHAR(50) DEFAULT 'PENDING',
    error_message TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE stg_source2_ntsb
ADD CONSTRAINT stg_source2_unique UNIQUE (source_unique_id);


-- Staging for Source 3 (CSV)
CREATE TABLE stg_source3_csv (
    stg_id SERIAL PRIMARY KEY,
    source_unique_id VARCHAR(100),
    raw_data JSONB,
    processing_status VARCHAR(50) DEFAULT 'PENDING',
    error_message TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE stg_source3_csv
ADD CONSTRAINT stg_source3_unique UNIQUE (source_unique_id);

-- ============================================================
-- PHASE 3: FACT TABLE (Depends on all dimensions)
-- ============================================================

CREATE TABLE fact_accidents (
    accident_id SERIAL PRIMARY KEY,
    
    -- Data source tracking
    data_source VARCHAR(50) NOT NULL,
    source_unique_id VARCHAR(100) NOT NULL,
    
    -- Foreign keys to dimensions
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    location_key INTEGER NOT NULL REFERENCES dim_location(location_key),
    aircraft_key INTEGER NOT NULL REFERENCES dim_aircraft(aircraft_key),
    operator_key INTEGER NOT NULL REFERENCES dim_operator(operator_key),
    
    -- Flight Information
    flight_number VARCHAR(50),
    route_departure VARCHAR(100),
    route_destination VARCHAR(100),
    flight_phase VARCHAR(100),
    flight_nature VARCHAR(100),
    
    -- Injury & Fatality Metrics
    total_aboard INTEGER,
    fatalities_total INTEGER DEFAULT 0,
    fatalities_crew INTEGER DEFAULT 0,
    fatalities_passengers INTEGER DEFAULT 0,
    ground_fatalities INTEGER DEFAULT 0,
    
    injuries_serious INTEGER DEFAULT 0,
    injuries_minor INTEGER DEFAULT 0,
    injuries_none INTEGER DEFAULT 0,
    
    -- Damage & Investigation
    aircraft_damage_level VARCHAR(50),
    confidence_rating VARCHAR(255),
    investigation_status VARCHAR(50),
    investigation_report_url VARCHAR(500),
    
    -- Narrative Fields
    narrative_summary TEXT,
    probable_cause TEXT,
    analysis_narrative TEXT,
    factual_narrative TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint on data source + source ID
    UNIQUE(data_source, source_unique_id)
);

CREATE INDEX idx_fact_accidents_date ON fact_accidents(date_key);
CREATE INDEX idx_fact_accidents_location ON fact_accidents(location_key);
CREATE INDEX idx_fact_accidents_aircraft ON fact_accidents(aircraft_key);
CREATE INDEX idx_fact_accidents_operator ON fact_accidents(operator_key);
CREATE INDEX idx_fact_accidents_source ON fact_accidents(data_source, source_unique_id);
CREATE INDEX idx_fact_accidents_fatalities ON fact_accidents(fatalities_total);

-- ============================================================
-- PHASE 4: BRIDGE TABLES (Many-to-Many relationships)
-- ============================================================

-- Event Sequence Details
CREATE TABLE bridge_accident_event_sequence (
    event_seq_id SERIAL PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES fact_accidents(accident_id) ON DELETE CASCADE,
    
    sequence_number INTEGER NOT NULL,
    event_phase VARCHAR(100),
    event_description VARCHAR(500),
    event_code VARCHAR(50),
    is_defining_event BOOLEAN,
    
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bridge_event_accident ON bridge_accident_event_sequence(accident_id);
CREATE INDEX idx_bridge_event_sequence ON bridge_accident_event_sequence(accident_id, sequence_number);

-- Findings & Root Causes
CREATE TABLE bridge_accident_findings (
    finding_id SERIAL PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES fact_accidents(accident_id) ON DELETE CASCADE,
    
    finding_number INTEGER,
    finding_code VARCHAR(50),
    finding_text TEXT,
    finding_report_text VARCHAR(500),
    in_probable_cause BOOLEAN DEFAULT FALSE,
    
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bridge_finding_accident ON bridge_accident_findings(accident_id);
CREATE INDEX idx_bridge_finding_code ON bridge_accident_findings(finding_code);

-- Flight Crew Information
CREATE TABLE bridge_accident_flight_crew (
    crew_id SERIAL PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES fact_accidents(accident_id) ON DELETE CASCADE,
    
    crew_role VARCHAR(100),
    crew_category VARCHAR(100),
    experience_hours INTEGER,
    license_type VARCHAR(100),
    age INTEGER,
    
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bridge_crew_accident ON bridge_accident_flight_crew(accident_id);

-- Injury Matrix (detailed breakdown)
CREATE TABLE bridge_accident_injury_matrix (
    injury_matrix_id SERIAL PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES fact_accidents(accident_id) ON DELETE CASCADE,
    
    category VARCHAR(100),
    fatal_count INTEGER DEFAULT 0,
    serious_count INTEGER DEFAULT 0,
    minor_count INTEGER DEFAULT 0,
    none_count INTEGER DEFAULT 0,
    
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bridge_injury_accident ON bridge_accident_injury_matrix(accident_id);
CREATE INDEX idx_bridge_injury_category ON bridge_accident_injury_matrix(category);


-- ============================================================
-- PHASE 5: METADATA & AUDIT TABLES
-- ============================================================

-- Data Quality & Lineage
CREATE TABLE dw_data_quality_log (
    quality_log_id SERIAL PRIMARY KEY,
    
    table_name VARCHAR(100),
    record_count INTEGER,
    quality_check VARCHAR(255),
    check_result VARCHAR(50),
    check_details TEXT,
    
    data_source VARCHAR(50),
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL Execution Log
CREATE TABLE dw_etl_log (
    etl_log_id SERIAL PRIMARY KEY,
    
    etl_process_name VARCHAR(100),
    data_source VARCHAR(50),
    
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    error_message TEXT
);

-- ============================================================
-- DATA POPULATION SCRIPTS
-- ============================================================

-- Populate dim_date for years 1908-2028
INSERT INTO dim_date
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
    date_series as full_date,
    EXTRACT(YEAR FROM date_series)::INTEGER as year,
    EXTRACT(QUARTER FROM date_series)::INTEGER as quarter,
    EXTRACT(MONTH FROM date_series)::INTEGER as month,
    EXTRACT(DAY FROM date_series)::INTEGER as day_of_month,
    EXTRACT(DOW FROM date_series)::INTEGER as day_of_week,
    EXTRACT(WEEK FROM date_series)::INTEGER as week_of_year,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    TO_CHAR(date_series, 'Day') as day_name,
    TO_CHAR(date_series, 'Month') as month_name
FROM generate_series('1900-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) as date_series
ON CONFLICT (date_key) DO NOTHING;


-- Populate dim_time for all minutes in a day (00:00 to 23:59)
INSERT INTO dim_time
SELECT
    -- Calcul de time_key (HHMM)
    (time_series / 3600)::INTEGER * 100 + (time_series % 3600 / 60)::INTEGER as time_key,
    
    -- Conversion de l'entier (secondes) en TIME
    ('00:00:00'::TIME + (time_series * '1 second'::INTERVAL)) as time_value,
    
    -- Extraction de l'heure
    (time_series / 3600)::INTEGER as hour,
    
    -- Extraction de la minute
    (time_series % 3600 / 60)::INTEGER as minute,
    
    -- Extraction de la seconde (ici toujours 0 car on génère par minute)
    (time_series % 60)::INTEGER as second,
    
    -- Formatage de l'heure
    TO_CHAR(('00:00:00'::TIME + (time_series * '1 second'::INTERVAL)), 'HH24:MI') as hour_name
FROM 
    -- Générer une série d'entiers de 0 à (24 * 60 - 1) * 60 secondes, par pas de 60 secondes (1 minute)
    generate_series(0, 86340, 60) AS time_series -- 86340 = (23h * 3600) + (59m * 60)
ON CONFLICT (time_value) DO NOTHING;

-- Create Unknown/Missing dimension entries
INSERT INTO dim_location (location_id, country, city, data_source) 
VALUES ('UNKNOWN', 'Unknown', 'Unknown', 'SYSTEM') ON CONFLICT (location_id) DO NOTHING;

INSERT INTO dim_aircraft (aircraft_id, type_name, manufacturer, data_source)
VALUES ('UNKNOWN', 'Unknown', 'Unknown', 'SYSTEM') ON CONFLICT (aircraft_id) DO NOTHING;

INSERT INTO dim_operator (operator_id, operator_name, data_source)
VALUES ('UNKNOWN', 'Unknown', 'SYSTEM') ON CONFLICT (operator_id) DO NOTHING;


---Date/Time: You convert everything to YYYYMMDD integers. Source 1 requires text parsing (Monday 9...), Source 2 is ISO 8601, Source 3 is US format (MM/DD/YYYY).

---Location: You prioritize granular data. If Source 1 only gives "Taiwan", you leave City/State NULL. If Source 2 gives exact lat/long, you populate it.

---Fatalities: You must calculate totals. Source 1 combines them in a string; you must parse them out to match the integer columns in fact_accidents.

---Keys: You generate the keys (date_key, time_key) during the INSERT into the Fact table by casting the raw timestamp.

-- Vider les tables dans l'ordre inverse des dépendances pour éviter les erreurs de clés étrangères
TRUNCATE TABLE bridge_accident_findings, bridge_accident_event_sequence, fact_accidents CASCADE;
TRUNCATE TABLE dim_aircraft, dim_location, dim_operator CASCADE;
TRUNCATE TABLE stg_source1_aviation_safety, stg_source2_ntsb, stg_source3_csv CASCADE;

--- loading staging data 

INSERT INTO stg_source1_aviation_safety (source_unique_id, raw_json)
VALUES 
('464908', '{
      "url": "https://aviation-safety.net/wikibase/464908",
      "date": "Monday 9 December 2024",
      "time": "c. 10:55 LT",
      "type": "Fly Synthesis Storch CL",
      "owner_operator": "Taiwan Aviation Master Activity Association",
      "registration": "JJ2258",
      "msn": "CAA-113-028",
      "fatalities": "Fatalities: 0 / Occupants: 2",
      "aircraft_damage": "Substantial",
      "location": "Taiwan",
      "phase": "Initial climb",
      "nature": "Training",
      "departure_airport": "Saijia Jiehao Ultralightport",
      "destination_airport": "Saijia Jiehao Ultralightport",
      "confidence_rating": "Accident investigation report completed and information captured",
      "narrative": "On December 9, 2024, at 1055 Taipei time, a STORCH CL ultralight operated by the Taiwan Aviation Master Activity Association, carrying 1 operator and 1 occupant, experienced engine failure shortly after taking off from Runway 26 at the Pingtung Sai Jia Jaie Haour Ultralight Airfield and crash-landed approximately 340 meters west of the Runway 08 threshold. The ultralight was destroyed, and both occupants were hospitalized for treatment.  The finding related to probable causes is as follows: 1. During the initial climb to approximately 100 feet, the occurrence vehicle experienced an internal engine mechanical failure. The circlip securing the gudgeon pin of cylinder no. 4 detached during engine operation, resulting in abnormal lateral movement of the gudgeon pin. While the engine continued running, the end of gudgeon pin scraped against the inner wall of the cylinder during the pistons reciprocating motion and struck the lower flange structure of the crankcase as the piston approached bottom dead center. As the conrod remained driven by the crankshaft\u2019s rotation, the piston body was torn apart and the conrod became bent and deformed. Eventually, the deformed conrod, during its erratic impacts at the bottom of the cylinder, punctured the upper casing of the crankcase and simultaneously embedding into the rotating crankshaft, which causing the engine to seize immediately.",
      "sources": [
        "https://www.ttsb.gov.tw/media/8165/jj2258%E4%BA%8B%E6%95%85%E5%88%9D%E6%AD%A5%E5%A0%B1%E5%91%8A.pdf",
        "https://udn.com/news/story/7320/8413393",
        "https://www.taiwannews.com.tw/news/5987799",
        "https://www.youtube.com/watch?v=87Y2jrKVR80",
        "https://www.ttsb.gov.tw/english/18609/18610/44578/post"
      ]
    }'::jsonb);

INSERT INTO stg_source2_ntsb (source_unique_id, raw_json)
VALUES 
('WPR26LA036', ' {
    "cm_mkey": 201955,
    "airportId": "SEZ",
    "airportName": "SEDONA",
    "cm_closed": false,
    "cm_completionStatus": "In work",
    "cm_hasSafetyRec": false,
    "cm_highestInjury": "None",
    "cm_isStudy": false,
    "cm_mode": "Aviation",
    "cm_ntsbNum": "WPR26LA036",
    "cm_originalPublishedDate": null,
    "cm_vehicles": [
      {
        "cm_vehicleNum": 1,
        "DamageLevel": "Substantial",
        "ExplosionType": "None",
        "FireType": "None",
        "cm_injuries": [
          {
            "cm_hasonBoardInjuryMatrix": true,
            "cm_crew_Fatal": 0,
            "cm_crew_Minor": 0,
            "cm_crew_None": 1,
            "cm_crew_Serious": 0,
            "cm_passengers_Fatal": 0,
            "cm_passengers_Minor": 0,
            "cm_passengers_None": 1,
            "cm_passengers_Serious": 0
          }
        ],
        "SerialNumber": "2159",
        "aircraftCategory": "AIR",
        "amateurBuilt": false,
        "make": "CIRRUS DESIGN CORP",
        "model": "SR22",
        "numberOfEngines": 1,
        "registrationNumber": "N237RJ",
        "gaFlight": true,
        "cm_events": [
          {
            "cm_eventNum": 1,
            "cicttEventSOEGroup": "System/Component Failure (Powerplant)",
            "cicttPhaseSOEGroup": "Takeoff",
            "cm_eventCode": "300341",
            "cm_isDefiningEvent": true,
            "cm_sequenceNum": 1,
            "cm_tier1Name": "Takeoff",
            "cm_tier1Num": "300",
            "cm_tier2Name": "Loss of engine power (total)",
            "cm_tier2Num": "341"
          }
        ],
        "airMedical": false,
        "airMedicalType": null,
        "flightOperationType": "PERS",
        "flightScheduledType": null,
        "flightServiceType": null,
        "flightTerminalType": null,
        "operatorName": null,
        "registeredOwner": "ARGAEUS LLC",
        "regulationFlightConductedUnder": "091",
        "revenueSightseeing": false,
        "secondPilotPresent": false
      }
    ],
    "cm_recentReportPublishDate": "2025-11-05T00:26:25.829Z",
    "cm_mostRecentReportType": "Prelim",
    "cm_HazmatInvolved": false,
    "cm_Latitude": 34.8374,
    "cm_Longitude": -111.8045,
    "cm_city": "Sedona",
    "cm_country": "USA",
    "cm_eventDate": "2025-11-03T09:30:00Z",
    "cm_state": "AZ",
    "cm_agency": "NTSB",
    "cm_boardLaunch": false,
    "cm_boardMeetingDate": null,
    "cm_docketDate": null,
    "cm_eventType": "ACC",
    "cm_launch": "None",
    "cm_reportDate": "2025-11-18T05:00:00Z",
    "cm_reportNum": null,
    "cm_reportType": "DirectorBrief",
    "analysisNarrative": null,
    "factualNarrative": null,
    "prelimNarrative": "On November 3, 2025, about 0830 mountain standard time, a Cirrus SR-22, N237RJ, sustained substantial damage when it was involved in an accident near Sedona, Arizona. The pilot and passenger were not injured. The airplane was operated as a Title 14 Code of Federal Regulations Part 91 personal flight.&#x0D;\n&#x0D;\nThe pilot of the airplane reported that, during the preflight inspection, no anomalies were observed, and the fuel level was about 30 gallons per side. During the takeoff roll, everything seemed normal. Shortly after rotation, the engine experienced a partial loss of power, however, there was not enough runway remaining to abort the takeoff, and the pilot continued the takeoff sequence. During the initial climb out, the pilot noticed a decrease in airspeed along with an additional loss of engine power. He elected to return to the airport, however, realized he would not be able to make it to the runway, and deployed the Cirrus Airframe Parachute System (CAPS).  Subsequently, the airplane descended under the parachute canopy onto desert terrain.     &#x0D;\n&#x0D;\nPost accident examination of the airplane revealed that forward fuselage and empennage sustained substantial damage. The wreckage was recovered to a secure location for further examination.",
    "cm_injuryOnboardCount": 0,
    "cm_injury_onboard_Fatal": 0,
    "cm_injury_onboard_Minor": 0,
    "cm_injury_onboard_Serious": 0,
    "cm_onboard_None": 2,
    "cm_onboard_Total": 2,
    "cm_fatalInjuryCount": 0,
    "cm_minorInjuryCount": 0,
    "cm_seriousInjuryCount": 0,
    "accidentSiteCondition": "VMC",
    "cm_topicMode": "Aviation"
  }'::jsonb);

INSERT INTO stg_source3_csv (source_unique_id, raw_data)
VALUES 
('0', '{"index": "0", "Date": "09/17/1908", "Time": "17:18", "Location": "Fort Myer, Virginia", "Operator": "Military - U.S. Army", "Type": "Wright Flyer III", "Fatalities": "1.0", "Summary": "First recorded airplane fatality..."}'::jsonb),
('1', '{"index": "1", "Date": "07/12/1912", "Time": "06:30", "Location": "AtlantiCity, New Jersey", "Operator": "Military - U.S. Navy", "Type": "Dirigible", "Fatalities": "5.0", "Summary": "Dirigible exploded."}'::jsonb);

-- Traitement Source 1 (Juste le pays)
INSERT INTO dim_location (location_id, country, data_source, source_location_id)
SELECT DISTINCT 
    'LOC-SRC1-' || (raw_json->>'url'), -- ID Unique généré
    raw_json->>'location',             -- Pays
    'AVIATION_SAFETY',
    raw_json->>'url'
FROM stg_source1_aviation_safety
ON CONFLICT (location_id) DO NOTHING;

-- Traitement Source 2 (Détaillé: Ville, Etat, Lat/Lon)
INSERT INTO dim_location (location_id, country, state_province, city, latitude, longitude, data_source, source_location_id)
SELECT DISTINCT
    'LOC-SRC2-' || (raw_json->>'cm_ntsbNum'),
    UPPER(raw_json->>'cm_country'),
    UPPER(raw_json->>'cm_state'),
    UPPER(raw_json->>'cm_city'),
    (raw_json->>'cm_Latitude')::DOUBLE PRECISION,
    (raw_json->>'cm_Longitude')::DOUBLE PRECISION,
    'NTSB',
    raw_json->>'cm_ntsbNum'
FROM stg_source2_ntsb
ON CONFLICT (location_id) DO NOTHING;


-- Traitement Source 3 (CSV - Ville, Etat combinés)
INSERT INTO dim_location (location_id, city, country, data_source, source_location_id)
SELECT DISTINCT 
    'LOC-SRC3-' || (raw_data->>'index'),
    SPLIT_PART(raw_data->>'Location', ',', 1), -- Prend la partie avant la virgule
    'USA', -- On suppose USA par défaut pour cet exemple historique, ou extraire si possible
    'CSV_HISTORICAL',
    raw_data->>'index'
FROM stg_source3_csv
ON CONFLICT (location_id) DO NOTHING;

-- Source 1
INSERT INTO dim_aircraft (aircraft_id, type_name, registration_number, msn, data_source, source_aircraft_id)
SELECT DISTINCT
    'AC-SRC1-' || (raw_json->>'registration'),
    raw_json->>'type',
    raw_json->>'registration',
    raw_json->>'msn',
    'AVIATION_SAFETY',
    raw_json->>'url'
FROM stg_source1_aviation_safety
ON CONFLICT (aircraft_id) DO NOTHING;

-- Source 2 (NTSB - Données imbriquées dans un tableau JSON)
INSERT INTO dim_aircraft (aircraft_id, manufacturer, model, registration_number, serial_number, data_source, source_aircraft_id)
SELECT DISTINCT
    'AC-SRC2-' || (v.value->>'registrationNumber'),
    v.value->>'make',
    v.value->>'model',
    v.value->>'registrationNumber',
    v.value->>'SerialNumber',
    'NTSB',
    raw_json->>'cm_ntsbNum'
FROM stg_source2_ntsb, jsonb_array_elements(raw_json->'cm_vehicles') as v
ON CONFLICT (aircraft_id) DO NOTHING;

-- Source 3
INSERT INTO dim_aircraft (aircraft_id, type_name, data_source, source_aircraft_id)
SELECT DISTINCT
    'AC-SRC3-' || (raw_data->>'index'),
    raw_data->>'Type',
    'CSV_HISTORICAL',
    raw_data->>'index'
FROM stg_source3_csv
ON CONFLICT (aircraft_id) DO NOTHING;



-- ==========================================
-- SOURCE 1 : Aviation Safety (Champ "owner_operator")
-- ==========================================
INSERT INTO dim_operator (operator_id, operator_name, data_source, source_operator_id)
SELECT DISTINCT
    'OP-SRC1-' || md5(raw_json->>'owner_operator'), -- ID généré via Hash
    COALESCE(TRIM(raw_json->>'owner_operator'), 'Unknown Operator'),
    'AVIATION_SAFETY',
    raw_json->>'url'
FROM stg_source1_aviation_safety
WHERE raw_json->>'owner_operator' IS NOT NULL
ON CONFLICT (operator_id) DO NOTHING;

-- ==========================================
-- SOURCE 2 : NTSB (Champ "operatorName" imbriqué dans vehicles)
-- ==========================================
-- Note : NTSB fournit aussi le Propriétaire ("registeredOwner") et le type de vol.
INSERT INTO dim_operator (
    operator_id, operator_name, owner_name, 
    flight_operation_type, regulation_flight_conducted_under,
    data_source, source_operator_id
)
SELECT DISTINCT
    'OP-SRC2-' || md5((v.value->>'operatorName') || (v.value->>'registrationNumber')), 
    COALESCE(TRIM(v.value->>'operatorName'), 'Unknown Operator'),
    TRIM(v.value->>'registeredOwner'),       -- Propriétaire (ex: TETON LEASING LLC)
    v.value->>'flightOperationType',         -- Type (ex: INST, PERS)
    v.value->>'regulationFlightConductedUnder', -- Régulation (ex: 091, 121)
    'NTSB',
    raw_json->>'cm_ntsbNum'
FROM stg_source2_ntsb,
     jsonb_array_elements(raw_json->'cm_vehicles') as v
WHERE v.value->>'operatorName' IS NOT NULL
ON CONFLICT (operator_id) DO NOTHING;

-- ==========================================
-- SOURCE 3 : CSV (Champ "Operator")
-- ==========================================
INSERT INTO dim_operator (operator_id, operator_name, data_source, source_operator_id)
SELECT DISTINCT
    'OP-SRC3-' || (raw_data->>'index'),
    COALESCE(TRIM(raw_data->>'Operator'), 'Unknown Operator'),
    'CSV_HISTORICAL',
    raw_data->>'index'
FROM stg_source3_csv
ON CONFLICT (operator_id) DO NOTHING;

--------------------------------------------------



-- ==========================================
-- CHARGEMENT SOURCE 1 (Format Date: "Monday 9 December 2024")
-- ==========================================
INSERT INTO fact_accidents (
    data_source, source_unique_id, 
    date_key, location_key, aircraft_key, 
    fatalities_total, total_aboard, 
    flight_phase, flight_nature, narrative_summary
)
SELECT 
    'AVIATION_SAFETY',
    raw_json->>'url',
    -- Conversion Date complexe
    TO_CHAR(TO_DATE(raw_json->>'date', 'Day DD Month YYYY'), 'YYYYMMDD')::INTEGER,
    -- Récupération des IDs (Lookups)
    (SELECT location_key FROM dim_location WHERE source_location_id = s1.raw_json->>'url' AND data_source = 'AVIATION_SAFETY' LIMIT 1),
    (SELECT aircraft_key FROM dim_aircraft WHERE source_aircraft_id = s1.raw_json->>'url' AND data_source = 'AVIATION_SAFETY' LIMIT 1),
    -- Parsing des morts (Regex pour extraire les chiffres du texte)
    NULLIF(SUBSTRING(raw_json->>'fatalities' FROM 'Fatalities: ([0-9]+)')::INTEGER, 0),
    NULLIF(SUBSTRING(raw_json->>'fatalities' FROM 'Occupants: ([0-9]+)')::INTEGER, 0),
    raw_json->>'phase',
    raw_json->>'nature',
    raw_json->>'narrative'
FROM stg_source1_aviation_safety s1
ON CONFLICT (data_source, source_unique_id) DO NOTHING;

-- ==========================================
-- CHARGEMENT SOURCE 2 (Format Date: ISO 8601 "2010-06-30T...")
-- ==========================================
INSERT INTO fact_accidents (
    data_source, source_unique_id, 
    date_key, location_key, aircraft_key, 
    fatalities_total, injuries_serious, injuries_minor,
    probable_cause, factual_narrative, investigation_status
)
SELECT 
    'NTSB',
    raw_json->>'cm_ntsbNum',
    TO_CHAR((raw_json->>'cm_eventDate')::TIMESTAMP, 'YYYYMMDD')::INTEGER,
    (SELECT location_key FROM dim_location WHERE source_location_id = s2.raw_json->>'cm_ntsbNum' AND data_source = 'NTSB' LIMIT 1),
    (SELECT aircraft_key FROM dim_aircraft WHERE source_aircraft_id = s2.raw_json->>'cm_ntsbNum' AND data_source = 'NTSB' LIMIT 1),
    (raw_json->>'cm_fatalInjuryCount')::INTEGER,
    (raw_json->>'cm_seriousInjuryCount')::INTEGER,
    (raw_json->>'cm_minorInjuryCount')::INTEGER,
    raw_json->>'cm_probableCause',
    raw_json->>'factualNarrative',
    raw_json->>'cm_completionStatus'
FROM stg_source2_ntsb s2
ON CONFLICT (data_source, source_unique_id) DO NOTHING;

-- ==========================================
-- CHARGEMENT SOURCE 3 (Format Date: US "09/17/1908")
-- ==========================================
INSERT INTO fact_accidents (
    data_source, source_unique_id, 
    date_key, location_key, aircraft_key, 
    fatalities_total, narrative_summary
)
SELECT 
    'CSV_HISTORICAL',
    raw_data->>'index',
    TO_CHAR(TO_DATE(raw_data->>'Date', 'MM/DD/YYYY'), 'YYYYMMDD')::INTEGER,
    -- Lookups
    (SELECT location_key FROM dim_location WHERE source_location_id = s3.raw_data->>'index' AND data_source = 'CSV_HISTORICAL' LIMIT 1),
    (SELECT aircraft_key FROM dim_aircraft WHERE source_aircraft_id = s3.raw_data->>'index' AND data_source = 'CSV_HISTORICAL' LIMIT 1),
    (raw_data->>'Fatalities')::NUMERIC::INTEGER, -- Parfois float dans CSV
    raw_data->>'Summary'
FROM stg_source3_csv s3
ON CONFLICT (data_source, source_unique_id) DO NOTHING;


---after that we made a python script that loads all 3 sources into staging tables to prepare for the T part (load_staging.py)

INSERT INTO dim_location (location_id, country, data_source, source_location_id)
SELECT DISTINCT 
    'LOC-SRC1-' || (raw_json->>'url'), -- ID Unique généré
    raw_json->>'location',             -- Pays
    'AVIATION_SAFETY',
    raw_json->>'url'
FROM stg_source1_aviation_safety
ON CONFLICT (location_id) DO NOTHING;

-- Traitement Source 2 (Détaillé: Ville, Etat, Lat/Lon)
INSERT INTO dim_location (location_id, country, state_province, city, latitude, longitude, data_source, source_location_id)
SELECT DISTINCT
    'LOC-SRC2-' || (raw_json->>'cm_ntsbNum'),
    UPPER(raw_json->>'cm_country'),
    UPPER(raw_json->>'cm_state'),
    UPPER(raw_json->>'cm_city'),
    (raw_json->>'cm_Latitude')::DOUBLE PRECISION,
    (raw_json->>'cm_Longitude')::DOUBLE PRECISION,
    'NTSB',
    raw_json->>'cm_ntsbNum'
FROM stg_source2_ntsb
ON CONFLICT (location_id) DO NOTHING;


-- Traitement Source 3 (CSV - Ville, Etat combinés)
INSERT INTO dim_location (location_id, city, country, data_source, source_location_id)
SELECT DISTINCT 
    'LOC-SRC3-' || (raw_data->>'index'),
    SPLIT_PART(raw_data->>'Location', ',', 1), -- Prend la partie avant la virgule
    'USA', -- On suppose USA par défaut pour cet exemple historique, ou extraire si possible
    'CSV_HISTORICAL',
    raw_data->>'index'
FROM stg_source3_csv
ON CONFLICT (location_id) DO NOTHING;
INSERT INTO fact_accidents (
    data_source, source_unique_id, 
    date_key, location_key, aircraft_key, operator_key, 
    fatalities_total, total_aboard, 
    flight_phase, flight_nature, narrative_summary
)
SELECT 
    'AVIATION_SAFETY',
    s1.raw_json->>'url',
    
    -- Date Lookups (Robuste et rapide)
    COALESCE(
        TO_CHAR(
            safe_to_date(TRIM(s1.raw_json->>'date'), 'Day DD Month YYYY'), 
            'YYYYMMDD'
        )::INTEGER, 
        19000101 -- Fallback Date
    ) AS date_key,
    
    -- Lookup Location (dl)
    COALESCE(dl.location_key, (SELECT location_key FROM dim_location WHERE location_id = 'UNKNOWN')),
    
    -- Lookup Aircraft (da)
    COALESCE(da.aircraft_key, (SELECT aircraft_key FROM dim_aircraft WHERE aircraft_id = 'UNKNOWN')),

    -- 🔥 Lookup Operator (d_op - CORRIGÉ)
    COALESCE(d_op.operator_key, (SELECT operator_key FROM dim_operator WHERE operator_id = 'UNKNOWN')),

    -- Data Fields
    NULLIF(SUBSTRING(s1.raw_json->>'fatalities' FROM 'Fatalities: ([0-9]+)')::INTEGER, 0),
    NULLIF(SUBSTRING(s1.raw_json->>'fatalities' FROM 'Occupants: ([0-9]+)')::INTEGER, 0),
    s1.raw_json->>'phase',
    s1.raw_json->>'nature',
    s1.raw_json->>'narrative'
    
FROM 
    stg_source1_aviation_safety s1

-- JOINS (pour la rapidité)
LEFT JOIN dim_location dl ON dl.source_location_id = s1.raw_json->>'url' AND dl.data_source = 'AVIATION_SAFETY'
LEFT JOIN dim_aircraft da ON da.source_aircraft_id = s1.raw_json->>'url' AND da.data_source = 'AVIATION_SAFETY'
LEFT JOIN dim_operator d_op ON d_op.source_operator_id = s1.raw_json->>'url' AND d_op.data_source = 'AVIATION_SAFETY' -- 🔥 CORRIGÉ

ON CONFLICT (data_source, source_unique_id) DO NOTHING;


CREATE OR REPLACE FUNCTION safe_to_date(text, text)
RETURNS date AS
$$
BEGIN
    RETURN to_date($1, $2);
EXCEPTION
    WHEN others THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

INSERT INTO dim_operator (operator_id, operator_name, data_source)
VALUES ('UNKNOWN', 'Unknown Operator', 'SYSTEM')
ON CONFLICT (operator_id) DO NOTHING;


INSERT INTO fact_accidents (
    data_source, source_unique_id, 
    date_key, location_key, aircraft_key, operator_key, 
    fatalities_total, injuries_serious, injuries_minor,
    probable_cause, factual_narrative, investigation_status
)
SELECT 
    'NTSB',
    s2.raw_json->>'cm_ntsbNum',
    
    -- Date Conversion (ISO format is simple, so no custom function needed)
    TO_CHAR((s2.raw_json->>'cm_eventDate')::TIMESTAMP, 'YYYYMMDD')::INTEGER,
    
    -- Lookup Location (Optimized JOIN)
    COALESCE(dl.location_key, (SELECT location_key FROM dim_location WHERE location_id = 'UNKNOWN')),
    
    -- Lookup Aircraft (Optimized JOIN)
    COALESCE(da.aircraft_key, (SELECT aircraft_key FROM dim_aircraft WHERE aircraft_id = 'UNKNOWN')),

    -- 🔥 Lookup Operator (Optimized JOIN + Fallback)
    COALESCE(d_op.operator_key, (SELECT operator_key FROM dim_operator WHERE operator_id = 'UNKNOWN')),

    -- Data Fields
    (s2.raw_json->>'cm_fatalInjuryCount')::INTEGER,
    (s2.raw_json->>'cm_seriousInjuryCount')::INTEGER,
    (s2.raw_json->>'cm_minorInjuryCount')::INTEGER,
    s2.raw_json->>'cm_probableCause',
    s2.raw_json->>'factualNarrative',
    s2.raw_json->>'cm_completionStatus'
    
FROM stg_source2_ntsb s2

-- JOINS (for fast lookup)
LEFT JOIN dim_location dl ON dl.source_location_id = s2.raw_json->>'cm_ntsbNum' AND dl.data_source = 'NTSB'
LEFT JOIN dim_aircraft da ON da.source_aircraft_id = s2.raw_json->>'cm_ntsbNum' AND da.data_source = 'NTSB'
-- Note: Operator lookup uses the NTSB accident number as the source ID
LEFT JOIN dim_operator d_op ON d_op.source_operator_id = s2.raw_json->>'cm_ntsbNum' AND d_op.data_source = 'NTSB' 

ON CONFLICT (data_source, source_unique_id) DO NOTHING;




--Maintenant que la table de faits centrale est remplie, nous pouvons charger les Tables de Liaison (Bridge Tables) pour intégrer les données complexes de la Source 2 (NTSB).

--Ces tables de liaison sont cruciales pour dénormaliser les informations plusieurs-à-plusieurs (par exemple, un accident a plusieurs causes, une cause est liée à plusieurs accidents) et pour fournir une structure utilisable dans l'analyse.

--Nous allons charger deux tables de liaison :

    --bridge_accident_event_sequence : Pour la séquence chronologique des événements.

    --bridge_accident_findings : Pour les causes probables et les facteurs contributifs.