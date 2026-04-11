import sqlite3
import json
import requests
import os
from pathlib import Path

DB_PATH = "data/courtfinder/courts.db"
NYC_JSON_URL = "https://www.nycgovparks.org/bigapps/DPR_Basketball_001.json"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS courts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT,
            lat REAL,
            lon REAL,
            county TEXT,
            source TEXT,
            num_courts INTEGER,
            accessible TEXT,
            metadata JSON
        );
    """)
    conn.commit()
    return conn

def ingest_nyc():
    print("[*] Fetching NYC Parks Data...")
    resp = requests.get(NYC_JSON_URL)
    resp.raise_for_status()
    data = resp.json()
    
    conn = sqlite3.connect(DB_PATH)
    count = 0
    for item in data:
        court_id = item.get("Prop_ID", "") + "_" + item.get("Name", "")
        # Basic mapping
        conn.execute("""
            INSERT OR REPLACE INTO courts (id, name, location, lat, lon, county, source, num_courts, accessible, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            court_id,
            item.get("Name", "Unknown Court"),
            item.get("Location", ""),
            item.get("Lat", 0.0),
            item.get("Lon", 0.0),
            "NYC",
            "NYC Open Data",
            item.get("Num_of_Courts", 1),
            item.get("Accessible", "N"),
            json.dumps(item)
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"[✓] Ingested {count} NYC courts.")

def ingest_long_island_manual():
    # Long Island data is less structured, adding major hubs identified
    li_courts = [
        {"name": "Eisenhower Park", "location": "East Meadow", "county": "Nassau", "lat": 40.7294, "lon": -73.5792},
        {"name": "Cedar Creek Park", "location": "Seaford", "county": "Nassau", "lat": 40.6515, "lon": -73.4912},
        {"name": "Tanner Park", "location": "Copiague", "county": "Suffolk", "lat": 40.6654, "lon": -73.3951},
        {"name": "Lake Ronkonkoma County Park", "location": "Lake Ronkonkoma", "county": "Suffolk", "lat": 40.8315, "lon": -73.1248},
    ]
    conn = sqlite3.connect(DB_PATH)
    count = 0
    for c in li_courts:
        conn.execute("""
            INSERT OR REPLACE INTO courts (id, name, location, lat, lon, county, source, num_courts, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"LI_{c['name'].replace(' ', '_')}",
            c['name'],
            c['location'],
            c['lat'],
            c['lon'],
            c['county'],
            "Manual Hub List",
            1,
            json.dumps(c)
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"[✓] Ingested {count} Long Island anchor hubs.")

if __name__ == "__main__":
    init_db()
    ingest_nyc()
    ingest_long_island_manual()
