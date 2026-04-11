import os
import json
import sqlite3
import requests
from pathlib import Path

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
DB_PATH = "data/courtfinder/courts.db"

def extract_facilities_with_ai(text, search_type):
    if not OPENROUTER_API_KEY:
        print("Error: OPENROUTER_API_KEY not set")
        return []
    
    # Prompt now focuses on complex facilities and access logic
    prompt = f"""
    Extract a list of multi-sport facilities or complexes that have basketball courts from this {search_type} data.
    For each facility, identify:
    1. name: The name of the facility
    2. location: Address or town
    3. access_type: (Membership, Day Pass, Public Open Gym, Private Rental Only)
    4. schedule_notes: Any specific times or days mentioned for basketball
    5. fee: Cost if mentioned
    
    Return ONLY a JSON object with a key 'facilities' which is a list of these objects.
    Text: {text[:15000]}
    """
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "openai/gpt-4o",
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"}
    }
    
    try:
        resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload)
        resp.raise_for_status()
        result = resp.json()
        content = result['choices'][0]['message']['content']
        return json.loads(content).get('facilities', [])
    except Exception as e:
        print(f"AI Extraction failed: {e}")
        return []

def update_db_schema():
    conn = sqlite3.connect(DB_PATH)
    # Add access-related columns
    try:
        conn.execute("ALTER TABLE courts ADD COLUMN access_type TEXT")
        conn.execute("ALTER TABLE courts ADD COLUMN schedule_notes TEXT")
        conn.execute("ALTER TABLE courts ADD COLUMN fee TEXT")
    except sqlite3.OperationalError:
        pass # Columns already exist
    conn.commit()
    conn.close()

def process_complex_facilities():
    conn = sqlite3.connect(DB_PATH)
    firecrawl_dir = Path(".firecrawl")
    
    for json_file in firecrawl_dir.glob("search-*.json"):
        if "hempstead" in json_file.name or "oysterbay" in json_file.name: continue # Skip old city-specific files
        
        print(f"[*] Processing complex facility results in {json_file.name}...")
        
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        all_text = ""
        web_results = data.get('data', {}).get('web', [])
        for result in web_results:
            all_text += f"Source: {result.get('url')}\nTitle: {result.get('title')}\nContent: {result.get('markdown', '')}\n\n"
            
        facilities = extract_facilities_with_ai(all_text, json_file.stem)
        print(f"[+] AI found {len(facilities)} complex facilities.")
        
        for f in facilities:
            conn.execute("""
                INSERT OR REPLACE INTO courts (id, name, location, source, access_type, schedule_notes, fee, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                f"COMPLEX_{f['name'].replace(' ', '_')}",
                f['name'],
                f.get('location', 'Unknown'),
                "Complex Scraper (Multi-Sport/YMCA)",
                f.get('access_type', 'Unknown'),
                f.get('schedule_notes', ''),
                f.get('fee', ''),
                json.dumps(f)
            ))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    update_db_schema()
    process_complex_facilities()
