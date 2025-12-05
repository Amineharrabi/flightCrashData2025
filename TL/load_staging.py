import json
import csv
import os
import psycopg2
from psycopg2.extras import execute_batch

DB = {
    "dbname": "FlightAccidentMain",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

conn = psycopg2.connect(**DB)
cur = conn.cursor()

# =====================================================
# JSON SOURCE 1 - Aviation Safety Network
# =====================================================
def load_source1_aviation(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    accidents = data["accidents"]  # <-- focus on the array of records
    rows = []

    for record in accidents:
        source_id = record.get("url").split("/")[-1]  # wikibase ID
        rows.append((source_id, json.dumps(record)))

    execute_batch(cur,
        """
        INSERT INTO stg_source1_aviation_safety (source_unique_id, raw_json)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (source_unique_id) DO NOTHING;
        """,
        rows
    )

    print(f"Loaded {len(rows)} rows → stg_source1_aviation_safety")



# =====================================================
# JSON SOURCE 2 - NTSB
# =====================================================
def load_source2_ntsb(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for record in data:
        # Unique ID = cm_ntsbNum (e.g., 'WPR26LA036')
        source_id = record.get("cm_ntsbNum")
        raw_json = json.dumps(record)

        rows.append((source_id, raw_json))

    execute_batch(cur,
        """
        INSERT INTO stg_source2_ntsb (source_unique_id, raw_json)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (source_unique_id) DO NOTHING;
        """,
        rows
    )

    print(f"Loaded {len(rows)} rows → stg_source2_ntsb")


# =====================================================
# CSV SOURCE 3
# =====================================================
def load_source3_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        rows = []
        for row in reader:
            source_id = row.get("index")      # Your sample uses index as the unique ID
            raw_data = json.dumps(row)

            rows.append((source_id, raw_data))

        execute_batch(cur,
            """
            INSERT INTO stg_source3_csv (source_unique_id, raw_data)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (source_unique_id) DO NOTHING;
            """,
            rows
        )

    print(f"Loaded {len(rows)} rows → stg_source3_csv")


# =====================================================
# MAIN EXECUTION
# =====================================================
if __name__ == "__main__":
    load_source1_aviation("ASN.json")
    load_source2_ntsb("NTSB.json")
    load_source3_csv("CSV.csv")

    conn.commit()
    cur.close()
    conn.close()
    print("✔ Staging load complete")
