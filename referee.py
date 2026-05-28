import sqlite3
import math
import os
import json
import base64
import logging
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from google import genai
from google.genai import types

log = logging.getLogger("openbot.referee")
router = APIRouter(prefix="/referee", tags=["referee"])
DB_PATH = "data/courtfinder/courts.db"

# Ensure Chronicles & Hidden Chemistry tables exist at runtime
conn = sqlite3.connect(DB_PATH)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS newsletter_subscribers (
        email TEXT PRIMARY KEY,
        player_id TEXT,
        subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS blog_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        author TEXT NOT NULL,
        tags TEXT,
        published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS hidden_chemistry (
        player_id TEXT,
        teammate_id TEXT,
        PRIMARY KEY (player_id, teammate_id)
    );
""")
conn.commit()
conn.close()

# Vertex AI Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "gcloud-hackathon-hauvzosacm3d0")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

class CheckInRequest(BaseModel):
    court_id: str
    team_id: str
    captain_id: str
    player_count: int = 5
    lat: float
    lon: float

class TransferRequest(BaseModel):
    team_id: str
    new_captain_id: str

class AuditRequest(BaseModel):
    court_id: str
    auditor_id: str
    photo_b64: Optional[str] = None

class GameRecord(BaseModel):
    court_id: str
    winners: List[str] # List of player IDs
    losers: List[str]

class ReviewRecord(BaseModel):
    game_id: int
    reviewer_id: str
    reviewee_id: str
    rating: int
    trait: str

class SubscribeRequest(BaseModel):
    email: str
    player_id: str

class GenerateNewsletterRequest(BaseModel):
    court_id: Optional[str] = None
    player_id: str

class HideChemistryRequest(BaseModel):
    player_id: str
    teammate_id: str

class SuggestCourtRequest(BaseModel):
    name: str
    location: str
    access_type: str  # 'Public', 'Day Pass', etc.
    fee: Optional[str] = None
    num_courts: int = 1
    county: Optional[str] = None

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points in meters."""
    R = 6371000  # Radius of earth in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _get_legacy_nickname(archetypes: List[str], wins: int) -> str:
    """Generate or retrieve a legendary nickname based on chemistry (Duo or Trio)."""
    if wins < 3: return ""
    
    # Sort for consistent lookup
    archetypes.sort()
    count = len(archetypes)
    
    # Trio Logic (The Big Three)
    if count >= 3:
        if archetypes.count("Bucket Getter") >= 2: return "The Run & Gun Trio"
        if "Point God" in archetypes and "Stretch Big" in archetypes: return "The Modern Big Three"
        return "The Showtime Trio"

    # Duo Logic
    if count == 2:
        a1, a2 = archetypes[0], archetypes[1]
        combinations = {
            ("3-and-D Wing", "Bucket Getter"): "Twin Telepaths",
            ("Stretch Big", "Stretch Big"): "Twin Towers",
            ("Iso Scorer", "Point God"): "Pick & Roll Wizards",
            ("3-and-D Wing", "Point God"): "Lockdown Backcourt",
            ("Bucket Getter", "Iso Scorer"): "The Iso Brothers",
            ("Bucket Getter", "Point God"): "The Duo"
        }
        return combinations.get((a1, a2), "The Duo" if wins < 10 else "The Legends")
    
    return ""

@router.post("/check-in")
async def check_in(req: CheckInRequest, db: sqlite3.Connection = Depends(get_db)):
    # 1. Fetch court coordinates
    court = db.execute("SELECT lat, lon FROM courts WHERE id = ?", (req.court_id,)).fetchone()
    if not court:
        raise HTTPException(status_code=404, detail="Court not found")
    
    # 2. Geofence Check (~100 meters)
    if court['lat'] and court['lon']:
        dist = haversine_distance(req.lat, req.lon, court['lat'], court['lon'])
        if dist > 100:
            raise HTTPException(status_code=403, detail=f"Too far from court ({int(dist)}m). Must be within 100m.")

    # 3. Add to queue
    try:
        db.execute("""
            INSERT INTO queues (court_id, team_id, captain_id, player_count, status)
            VALUES (?, ?, ?, ?, 'waiting')
        """, (req.court_id, req.team_id, req.captain_id, req.player_count))
        
        db.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'JOIN', ?)", 
                   (req.team_id, f"Joined at court {req.court_id}"))
        db.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Team already in a queue")

    return {"status": "ok", "message": "Successfully checked in"}

@router.post("/transfer-captain")
async def transfer_captain(req: TransferRequest, db: sqlite3.Connection = Depends(get_db)):
    team = db.execute("SELECT * FROM queues WHERE team_id = ?", (req.team_id,)).fetchone()
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    
    db.execute("UPDATE queues SET captain_id = ?, status = 'waiting' WHERE team_id = ?", 
               (req.new_captain_id, req.team_id))
    db.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'TRANSFER', ?)", 
               (req.team_id, f"Captaincy transferred to {req.new_captain_id}"))
    db.commit()
    
    return {"status": "ok", "message": f"Captaincy transferred to {req.new_captain_id}"}

@router.get("/leaderboard")
async def get_leaderboard(db: sqlite3.Connection = Depends(get_db)):
    """Fetch top squads by win streak."""
    rows = db.execute("""
        SELECT squad_name, win_streak, total_wins, current_court_id 
        FROM squad_rankings 
        ORDER BY win_streak DESC, total_wins DESC 
        LIMIT 5
    """).fetchall()
    return {"leaderboard": [dict(r) for r in rows]}

@router.get("/player/{player_id}")
async def get_player_stats(player_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Fetch AI-generated player bio and stats."""
    row = db.execute("SELECT * FROM player_stats WHERE player_id = ?", (player_id,)).fetchone()
    if not row:
        # Generate a sample player for demo
        return {
            "player_id": player_id,
            "name": player_id.split("_")[0],
            "wins": 12,
            "games_played": 20,
            "archetype": "Sharpshooter",
            "bio": "A lethal threat from deep. Known for silencing the crowd at West 4th."
        }
    return dict(row)

@router.post("/record-game")
async def record_game(req: GameRecord, db: sqlite3.Connection = Depends(get_db)):
    """Record a full game result with individual players."""
    # 1. Create Game entry
    cursor = db.execute("INSERT INTO games (court_id) VALUES (?)", (req.court_id,))
    game_id = cursor.lastrowid
    
    # 2. Update Players and Game Roster
    all_players = [(p, True) for p in req.winners] + [(p, False) for p in req.losers]
    
    for player_id, is_winner in all_players:
        # Upsert player
        db.execute("""
            INSERT INTO players (player_id, name) VALUES (?, ?)
            ON CONFLICT(player_id) DO NOTHING
        """, (player_id, player_id))
        
        # Add to roster
        db.execute("INSERT INTO game_roster (game_id, player_id, is_winner) VALUES (?, ?, ?)",
                   (game_id, player_id, is_winner))
        
        # Update stats
        db.execute("""
            UPDATE players SET 
                total_games = total_games + 1,
                total_wins = total_wins + ?,
                win_rate = CAST((total_wins + ?) AS REAL) / (total_games + 1)
            WHERE player_id = ?
        """, (1 if is_winner else 0, 1 if is_winner else 0, player_id))

    db.commit()
    return {"status": "ok", "game_id": game_id}

@router.get("/player/{player_id}/chemistry")
async def get_player_chemistry(player_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Find teammates this player has won with the most and assign nicknames."""
    # Get current player archetype
    p_row = db.execute("SELECT archetype FROM players WHERE player_id = ?", (player_id,)).fetchone()
    my_archetype = p_row['archetype'] if p_row else "Sharpshooter"

    rows = db.execute("""
        SELECT r2.player_id as teammate, COUNT(*) as wins_together, p2.archetype as teammate_archetype
        FROM game_roster r1
        JOIN game_roster r2 ON r1.game_id = r2.game_id
        JOIN players p2 ON r2.player_id = p2.player_id
        WHERE r1.player_id = ? AND r2.player_id != ? AND r1.is_winner = 1 AND r2.is_winner = 1
          AND r2.player_id NOT IN (
              SELECT teammate_id FROM hidden_chemistry WHERE player_id = ?
          )
        GROUP BY r2.player_id
        ORDER BY wins_together DESC
        LIMIT 5
    """, (player_id, player_id, player_id)).fetchall()
    
    results = []
    for r in rows:
        item = dict(r)
        item["nickname"] = _get_legacy_nickname([my_archetype, item["teammate_archetype"]], item["wins_together"])
        results.append(item)
        
    return {"player_id": player_id, "top_chemistry": results}

@router.post("/review")
async def record_review(req: ReviewRecord, db: sqlite3.Connection = Depends(get_db)):
    """Record a teammate review to influence archetypes."""
    db.execute("""
        INSERT INTO reviews (game_id, reviewer_id, reviewee_id, rating, trait)
        VALUES (?, ?, ?, ?, ?)
    """, (req.game_id, req.reviewer_id, req.reviewee_id, req.rating, req.trait))
    
    # Logic to update archetype could go here (e.g. if many people say 'Passer')
    db.commit()
    return {"status": "ok"}

@router.post("/record-win")
async def record_win(squad_name: str, court_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Record a win, update streak, and handle Court Ownership (King of the Court)."""
    # 1. Update squad rankings
    db.execute("""
        INSERT INTO squad_rankings (squad_name, win_streak, total_wins, current_court_id)
        VALUES (?, 1, 1, ?)
        ON CONFLICT(squad_name) DO UPDATE SET 
            win_streak = win_streak + 1,
            total_wins = total_wins + 1,
            current_court_id = ?
    """, (squad_name, court_id, court_id))
    
    # Get new streak
    new_streak = db.execute("SELECT win_streak FROM squad_rankings WHERE squad_name = ?", (squad_name,)).fetchone()['win_streak']
    
    # 2. King of the Court Logic
    current_king = db.execute("SELECT * FROM court_kings WHERE court_id = ?", (court_id,)).fetchone()
    
    should_conquer = False
    if not current_king:
        should_conquer = True
    elif new_streak > current_king['win_streak']:
        should_conquer = True
        
    if should_conquer:
        db.execute("""
            INSERT OR REPLACE INTO court_kings (court_id, king_id, king_type, win_streak, conquered_at)
            VALUES (?, ?, 'squad', ?, CURRENT_TIMESTAMP)
        """, (court_id, squad_name, new_streak))
        
    db.commit()
    return {
        "status": "ok", 
        "message": f"Win recorded for {squad_name}.",
        "conquered": should_conquer,
        "new_streak": new_streak
    }

@router.get("/court/{court_id}/king")
async def get_court_king(court_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Fetch the current 'King' of a specific court."""
    row = db.execute("SELECT * FROM court_kings WHERE court_id = ?", (court_id,)).fetchone()
    if not row:
        return {"court_id": court_id, "king_id": None, "message": "Court is currently unclaimed."}
    return dict(row)

@router.get("/player/{player_id}/territory")
async def get_player_territory(player_id: str, db: sqlite3.Connection = Depends(get_db)):
    """Find all courts where this player/squad is the King or has significant wins."""
    # Courts where they are the current King (via squad membership)
    # Simplified for demo: checks if their name is in the king_id
    rows = db.execute("""
        SELECT ck.court_id, c.name as court_name, ck.win_streak
        FROM court_kings ck
        JOIN courts c ON ck.court_id = c.id
        WHERE ck.king_id LIKE ? OR ck.king_id IN (
            SELECT squad_name FROM squad_rankings WHERE squad_name LIKE ?
        )
    """, (f"%{player_id}%", f"%{player_id}%")).fetchall()
    
    return {"player_id": player_id, "conquered_courts": [dict(r) for r in rows]}

@router.post("/vibe-check")
async def court_vibe_check(court_id: str, vision_data: Optional[dict] = None):
    """Generate a first-person persona report for the court based on AI Vision."""
    # In a full demo, vision_data would come from Gemini Vision analyzing a photo.
    # For the hackathon, we simulate the persona based on current queue state.
    
    # Simple persona logic:
    vibes = {
        "heavy": "I'm breathing heavy right now. 12 guys on the baseline, the run is elite. Bring your A-game or stay home.",
        "chill": "I'm feeling smooth today. A few hoopers working on their jumpers, plenty of room. Come get some reps in.",
        "ghost": "I'm lonely. Just the wind and some old nets. Where's the heart at? I'm wide open for a run.",
        "sweaty": "It's a battleground out here. Intensity is high, nobody is giving an inch. The asphalt is cooking.",
        "elite": "Legends are born on this concrete. The air is electric. You better show up or get shown up."
    }
    
    # Pick a vibe based on player count or simulated input
    selected_vibe = vision_data.get("vibe", "chill") if vision_data else "chill"
    message = vibes.get(selected_vibe, vibes["chill"])
    
    return {
        "court_id": court_id,
        "persona_name": "The Concrete Sentinel",
        "message": message,
        "vibe_level": selected_vibe
    }


@router.post("/hype-generator")
async def generate_hype(team_id: str, event_type: str = "game_winner"):
    """Generate NBA-style commentary and Squad Card metadata."""
    
    commentators = {
        "mike_breen": {
            "name": "Mike Breen",
            "phrases": ["BANG!", "Puts it in!", "Way downtown!", "It's good!"],
            "template": "{phrase} {team_id} with the {event}! Absolute magic at the Cage!"
        },
        "mark_jackson": {
            "name": "Mark Jackson",
            "phrases": ["Mama, there goes that man!", "Hand down, man down!", "You're better than that!", "Great defense, better offense!"],
            "template": "{phrase} {team_id} is putting on a clinic right now!"
        },
        "gus_johnson": {
            "name": "Gus Johnson",
            "phrases": ["COLD-BLOODED!", "Rise and fire... HEARTBREAK CITY!", "Pure!", "He's got 'get away from the cop' speed!"],
            "template": "{phrase} {team_id}!! ARE YOU KIDDING ME?!"
        }
    }
    
    import random
    key = random.choice(list(commentators.keys()))
    style = commentators[key]
    phrase = random.choice(style["phrases"])
    
    script = style["template"].format(phrase=phrase, team_id=team_id, event=event_type.replace("_", " "))
    
    return {
        "commentator": style["name"],
        "script": script,
        "card_metadata": {
            "title": "KING OF THE COURT",
            "team": team_id,
            "event": event_type.upper().replace("_", " "),
            "vibe": "Elite",
            "timestamp": datetime.now().strftime("%H:%M")
        }
    }


@router.post("/vision-scout")
async def vision_scout(req: AuditRequest):
    """Real AI Vision analysis of a court using Vertex AI."""
    if not req.photo_b64:
        raise HTTPException(status_code=400, detail="Photo data (base64) is required")
    
    prompt = """
    Analyze this photo of a basketball court.
    1. Count the number of players actively playing on the court.
    2. Count the number of people waiting on the sidelines.
    3. Determine the 'vibe' of the run (Elite, Sweaty, Chill, or Ghost).
    4. Write a 1-sentence first-person monologue from the court's perspective about the current state.
    
    Return ONLY a JSON object:
    {"playing": int, "waiting": int, "vibe": str, "monologue": str}
    """
    
    try:
        # Convert b64 to bytes
        image_bytes = base64.b64decode(req.photo_b64)
        
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[
                types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg"),
                types.Part.from_text(text=prompt)
            ],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.2
            )
        )
        
        result = json.loads(response.text)
        
        # Log the event
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'VISION_AUDIT', ?)", 
                   ("SYSTEM", f"Vision audit by {req.auditor_id}: {result['playing']} playing, {result['waiting']} waiting"))
        conn.commit()
        conn.close()
        
        return {
            "status": "verified",
            "data": result,
            "court_id": req.court_id
        }
    except Exception as e:
        log.error("Vertex AI Vision Scout failed: %s", e)
        raise HTTPException(status_code=500, detail=f"AI Analysis failed: {str(e)}")


@router.get("/courts")
async def search_courts(q: str = "", db: sqlite3.Connection = Depends(get_db)):
    """Search for courts by name or location."""
    term = f"%{q.strip().lower()}%"
    rows = db.execute("""
        SELECT id, name, location, county, num_courts, accessible, access_type, fee, source 
        FROM courts 
        WHERE LOWER(name) LIKE ? OR LOWER(location) LIKE ?
        LIMIT 20
    """, (term, term)).fetchall()
    return {"courts": [dict(r) for r in rows]}


@router.get("/status/{court_id}")
async def get_court_status(court_id: str, db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute("""
        SELECT team_id, captain_id, player_count, joined_at, status 
        FROM queues 
        WHERE court_id = ? AND status IN ('waiting', 'playing')
        ORDER BY joined_at ASC
    """, (court_id,)).fetchall()
    
    return {"court_id": court_id, "queue": [dict(r) for r in rows]}

@router.post("/audit-ghost")
async def audit_ghost(req: AuditRequest, db: sqlite3.Connection = Depends(get_db)):
    # In a full implementation, this would trigger a Gemini Vision call.
    # For the MVP, we record the audit request and ping the top team.
    top_team = db.execute("""
        SELECT team_id, captain_id FROM queues 
        WHERE court_id = ? AND status = 'waiting' 
        ORDER BY joined_at ASC LIMIT 1
    """, (req.court_id,)).fetchone()
    
    if not top_team:
        return {"status": "ok", "message": "Queue is empty, no one to audit."}
    
    db.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'AUDIT_REQUEST', ?)", 
               (top_team['team_id'], f"Audit requested by {req.auditor_id}"))
    db.commit()
    
    # Simulate a "Ping" to the captain
    return {
        "status": "ping_sent", 
        "target_team": top_team['team_id'], 
        "message": f"Verification ping sent to Captain {top_team['captain_id']}. They have 2 minutes to respond."
    }


@router.get("/studio-commentary")
async def get_studio_commentary(court_id: Optional[str] = None, db: sqlite3.Connection = Depends(get_db)):
    """Generate dynamic Inside the NBA desk commentary based on court data."""
    court_name = "West 4th St (The Cage)"
    vibe = "Heavy Run"
    player_count = 14
    wait_time = "25 minutes"
    
    if court_id:
        court = db.execute("SELECT name, location FROM courts WHERE id = ?", (court_id,)).fetchone()
        if court:
            court_name = court["name"]
            # Fetch queue info
            queue_rows = db.execute("SELECT count(*) as count FROM queues WHERE court_id = ? AND status = 'waiting'", (court_id,)).fetchone()
            wait_count = queue_rows["count"] if queue_rows else 0
            wait_time = f"{wait_count * 10} minutes"
            playing_rows = db.execute("SELECT count(*) as count FROM queues WHERE court_id = ? AND status = 'playing'", (court_id,)).fetchone()
            player_count = (playing_rows["count"] if playing_rows else 1) * 10
            vibe = "Elite" if wait_count > 2 else "Chill"
            
    prompt = f"""
    You are the Inside the NBA host desk crew: Ernie Johnson (E.J.), Charles Barkley (Chuck), Shaquille O'Neal (Shaq), and Kenny "The Jet" Smith.
    Write a brief, hilarious, and high-energy 4-line transcript discussing a pickup basketball court run during its peak hours.
    
    Court details:
    - Court Name: {court_name}
    - Court Vibe: {vibe}
    - Players checked in: {player_count}
    - Wait time to play: {wait_time} (Current peak hour crowd)
    
    Host personalities:
    - Ernie: Sets the stage professionally as the anchor.
    - Chuck: Criticizes the players/run, complains about waiting in line (e.g. "I wouldn't wait {wait_time} for a run if they had free donuts"), says it's "turrible".
    - Shaq: Talks about dominance, rings ("four rings, Chuck"), or calls the run "barbecue chicken."
    - Kenny: Talks strategic spacing, running the floor, or going to the board.
    
    Return ONLY a JSON array of objects representing the discussion:
    [
      {{"host": "Ernie", "text": "..."}},
      {{"host": "Kenny", "text": "..."}},
      {{"host": "Chuck", "text": "..."}},
      {{"host": "Shaq", "text": "..."}}
    ]
    """
    
    # Fallback default script if AI fails or if API key is not configured
    fallback = [
        {"host": "Ernie", "text": f"Welcome back to Inside the NBA. We're looking at the peak run over at {court_name} with a {wait_time} wait time."},
        {"host": "Kenny", "text": f"That's because of the spacing, Ernie! Everyone wants to play on a court where people run the floor properly. But with a {wait_time} wait, you gotta be ready!"},
        {"host": "Chuck", "text": f"Ernie, that is just turrible. I wouldn't wait {wait_time} to play basketball if they were giving out free Krispy Kreme donuts on the sideline. That's a guarantee!"},
        {"host": "Shaq", "text": f"Chuck, that's because you don't have the stamina to wait. For a dominant big man like me, a {wait_time} wait is just extra time to eat barbecue chicken. Go count the rings!"}
    ]
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.7
            )
        )
        data = json.loads(response.text)
        return {"commentary": data}
    except Exception as e:
        log.error("Failed to generate studio commentary: %s", e)
        return {"commentary": fallback}


@router.post("/newsletter/subscribe")
async def subscribe_newsletter(req: SubscribeRequest, db: sqlite3.Connection = Depends(get_db)):
    """Subscribe a player to the newsletter and reward credits."""
    try:
        db.execute("""
            INSERT OR REPLACE INTO newsletter_subscribers (email, player_id)
            VALUES (?, ?)
        """, (req.email, req.player_id))
        
        # Reward the player 5 credits for subscribing
        db.execute("""
            UPDATE player_stats 
            SET credits = credits + 5 
            WHERE player_id = ?
        """, (req.player_id,))
        
        # Get updated credits
        row = db.execute("SELECT credits FROM player_stats WHERE player_id = ?", (req.player_id,)).fetchone()
        updated_credits = row["credits"] if row else 35
        
        db.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'SUBSCRIBE', ?)", 
                   (req.player_id, f"Subscribed {req.email} to Sentinel Chronicles"))
        db.commit()
        
        return {
            "status": "ok", 
            "message": f"Successfully locked in subscription for {req.email}! +5 Sentinel Credits rewarded.",
            "credits": updated_credits
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/blog/posts")
async def list_blog_posts(db: sqlite3.Connection = Depends(get_db)):
    """Retrieve all blog columns ordered by newest first."""
    rows = db.execute("""
        SELECT id, title, content, author, tags, published_at 
        FROM blog_posts 
        ORDER BY published_at DESC, id DESC
    """).fetchall()
    return {"posts": [dict(r) for r in rows]}


@router.post("/newsletter/generate")
async def generate_newsletter(req: GenerateNewsletterRequest, db: sqlite3.Connection = Depends(get_db)):
    """Dynamically generate a news column based on current court stats using Vertex AI."""
    # 1. Fetch active rankings and stats to context-feed the prompt
    squads = db.execute("SELECT squad_name, win_streak FROM squad_rankings ORDER BY win_streak DESC LIMIT 3").fetchall()
    squads_list = [f"{s['squad_name']} ({s['win_streak']}W streak)" for s in squads]
    
    kings = db.execute("""
        SELECT ck.court_id, c.name as court_name, ck.king_id, ck.win_streak 
        FROM court_kings ck 
        JOIN courts c ON ck.court_id = c.id 
        LIMIT 3
    """).fetchall()
    kings_list = [f"{k['king_id']} ruling {k['court_name']} with a {k['win_streak']}W streak" for k in kings]
    
    # Check preferred court of generating player to dynamically adjust persona
    p_row = db.execute("SELECT preferred_court FROM player_stats WHERE player_id = ?", (req.player_id,)).fetchone()
    pref_court = p_row["preferred_court"] if p_row else "West 4th St (The Cage)"
    
    is_gym = any(x in pref_court for x in ["Life Time", "LA Fitness", "Gym", "Garden", "YMCA"])
    persona = "The Hardwood Sentinel" if is_gym else "The Concrete Sentinel"
    court_type = "Hardwood" if is_gym else "Asphalt/Concrete"
    
    prompt = f"""
    You are {persona}, the sentient, trash-talking guardian of local pickup sports court legacy.
    You watch every run, count every bucket, and keep track of who owns the court.
    Write a hilarious, high-energy, sports-column-style news update about the current state of local pickup runs.
    
    Current Court Facts:
    - Top Active Squads: {", ".join(squads_list) if squads_list else "None active"}
    - Court Kings: {", ".join(kings_list) if kings_list else "None claimed"}
    - Focus Court: {pref_court} ({court_type} court)
    - Featured player in the neighborhood: {req.player_id}
    
    Tone guidelines:
    - First-person perspective ("I am {persona}").
    - Highly entertaining, competitive, slightly arrogant guardian persona.
    - References to streetball culture, modern NBA players, and local legacies.
    - Keep it concise but impact-heavy (about 3 paragraphs).
    - Provide a catchy, bold headline.
    - Provide 2-3 relevant tags (comma separated, e.g. "COURT BEEF,LEGACY").
    
    Return ONLY a JSON object:
    {{
      "title": "A Catchy Bold Headline",
      "tags": "TAG1,TAG2",
      "content": "Full article text..."
    }}
    """
    
    fallback_title = "The Sentinel Has Spoken"
    fallback_tags = "COURT UPDATE,LEGACY"
    fallback_content = f"I am {persona}, and I am watching the runs at {pref_court}. The level of play is high, the lines are long, and players like {req.player_id} are trying to make a name for themselves. Squads like {squads_list[0] if squads_list else 'the neighborhood crews'} are dominating the rankings. Keep running, keep sweating, and respect the game."
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.85
            )
        )
        data = json.loads(response.text)
        title = data.get("title", fallback_title)
        tags = data.get("tags", fallback_tags)
        content = data.get("content", fallback_content)
    except Exception as e:
        log.error("Failed to generate Chronicles column via Gemini: %s", e)
        title = fallback_title
        tags = fallback_tags
        content = fallback_content
        
    try:
        # Insert generated post
        db.execute("""
            INSERT INTO blog_posts (title, content, author, tags)
            VALUES (?, ?, ?, ?)
        """, (title, content, persona, tags))
        
        # Reward user 5 credits for triggering a column generation
        db.execute("""
            UPDATE player_stats 
            SET credits = credits + 5 
            WHERE player_id = ?
        """, (req.player_id,))
        
        # Get updated credits
        row = db.execute("SELECT credits FROM player_stats WHERE player_id = ?", (req.player_id,)).fetchone()
        updated_credits = row["credits"] if row else 35
        
        db.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'BLOG_GENERATE', ?)", 
                   (req.player_id, f"Generated Chronicles column: '{title}'"))
        db.commit()
        
        return {
            "status": "ok",
            "message": "New Chronicles column published successfully! +5 Credits rewarded.",
            "credits": updated_credits,
            "post": {
                "title": title,
                "content": content,
                "author": persona,
                "tags": tags,
                "published_at": datetime.now().isoformat()
            }
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/player/chemistry/hide")
async def hide_player_chemistry(req: HideChemistryRequest, db: sqlite3.Connection = Depends(get_db)):
    """Hide a teammate from player's chemistry list to give them display control."""
    try:
        db.execute("""
            INSERT OR REPLACE INTO hidden_chemistry (player_id, teammate_id)
            VALUES (?, ?)
        """, (req.player_id, req.teammate_id))
        db.commit()
        return {"status": "ok", "message": f"Teammate {req.teammate_id} is now hidden."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/blog/posts/{id}")
async def delete_blog_post(id: int, db: sqlite3.Connection = Depends(get_db)):
    """Delete a blog column/story from the Chronicles feed."""
    try:
        db.execute("DELETE FROM blog_posts WHERE id = ?", (id,))
        db.commit()
        return {"status": "ok", "message": "Chronicles column deleted successfully."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/courts/suggest")
async def suggest_new_court(req: SuggestCourtRequest, db: sqlite3.Connection = Depends(get_db)):
    """Allow players to suggest a new court addition in the neighborhood."""
    try:
        # Slugify the name to create a unique ID
        name_clean = "".join(c if c.isalnum() else "_" for c in req.name.strip().replace(" ", "_"))
        # Clean double underscores
        while "__" in name_clean:
            name_clean = name_clean.replace("__", "_")
        court_id = f"SUGGEST_{name_clean.strip('_')}"
        
        # Check if already exists
        exists = db.execute("SELECT id FROM courts WHERE id = ?", (court_id,)).fetchone()
        if exists:
            raise HTTPException(status_code=400, detail="A court suggestion with this name already exists.")
            
        # We set default latitude and longitude for the suggestion
        lat = 40.7306
        lon = -73.9352
        
        db.execute("""
            INSERT INTO courts (id, name, location, lat, lon, county, source, num_courts, accessible, access_type, fee)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            court_id,
            req.name.strip(),
            req.location.strip(),
            lat,
            lon,
            req.county.strip() if req.county else "GLOBAL",
            "User Suggestion",
            req.num_courts,
            "Pending Verification",
            req.access_type,
            req.fee.strip() if req.fee else None
        ))
        
        # Log suggestion event
        db.execute("INSERT INTO queue_events (team_id, event_type, details) VALUES (?, 'SUGGEST_COURT', ?)", 
                   ("SYSTEM", f"New court suggested: {req.name} at {req.location}"))
        
        db.commit()
        return {
            "status": "ok", 
            "message": f"Successfully submitted suggestion for '{req.name}'! Pending community verification.",
            "court_id": court_id
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))




