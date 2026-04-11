import sqlite3
import json
import os

DB_PATH = "data/courtfinder/courts.db"

gyms = [
    {"name": "Life Time Sky", "location": "605 W 42nd St, Manhattan", "county": "Manhattan", "lat": 40.7611, "lon": -73.9972, "access_type": "Membership", "fee": "$200+/mo"},
    {"name": "LA Fitness Farmingville", "location": "Farmingville, NY", "county": "Suffolk", "lat": 40.8415, "lon": -73.0348, "access_type": "Day Pass Available", "fee": "$15 Day Pass"},
    {"name": "YMCA Huntington", "location": "60 Main St, Huntington", "county": "Suffolk", "lat": 40.8725, "lon": -73.4267, "access_type": "Membership/Guest", "fee": "$20 Day Pass"},
    {"name": "Island Garden", "location": "West Hempstead, NY", "county": "Nassau", "lat": 40.7048, "lon": -73.6501, "access_type": "Open Gym/Rental", "fee": "$10 Open Gym"},
    {"name": "24 Hour Fitness Valley Stream", "location": "Green Acres, Valley Stream", "county": "Nassau", "lat": 40.6625, "lon": -73.7214, "access_type": "Membership", "fee": "Varies"}
]

def ingest():
    conn = sqlite3.connect(DB_PATH)
    count = 0
    for g in gyms:
        conn.execute("""
            INSERT OR REPLACE INTO courts (id, name, location, county, source, lat, lon, access_type, fee, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"GYM_{g['name'].replace(' ', '_')}",
            g['name'],
            g['location'],
            g['county'],
            "Partner Gym Data",
            g['lat'],
            g['lon'],
            g['access_type'],
            g['fee'],
            json.dumps(g)
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"Ingested {count} gym locations.")

if __name__ == "__main__":
    ingest()
