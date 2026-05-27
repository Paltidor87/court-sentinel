import sqlite3
import os

DB_PATH = "data/courtfinder/courts.db"

def seed():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Ensure tables exist and clear existing rankings for a fresh start
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
    """)
    conn.execute("DELETE FROM squad_rankings")
    conn.execute("DELETE FROM court_kings")
    conn.execute("DELETE FROM players")
    conn.execute("DELETE FROM player_stats")
    conn.execute("DELETE FROM legacy_nicknames")
    conn.execute("DELETE FROM newsletter_subscribers")
    conn.execute("DELETE FROM blog_posts")

    # 2. Seed Elite Players
    players = [
        ("peggs", "PEGGS", "Bucket Getter", 0.74, 50, 37, "Elite"),
        ("naiqui", "NAIQUI", "3-and-D Wing", 0.72, 48, 35, "Elite"),
        ("jordan_b", "Flight", "Iso Scorer", 0.65, 30, 20, "Verified"),
        ("big_smooth", "Big Smooth", "Stretch Big", 0.80, 25, 20, "Elite"),
        ("d-train", "D-Train", "Point God", 0.60, 40, 24, "Verified")
    ]
    for p in players:
        conn.execute("INSERT INTO players (player_id, name, archetype, win_rate, total_games, total_wins, verified_status) VALUES (?,?,?,?,?,?,?)", p)

    # 2.5 Seed Player Stats (Queried by stats endpoint)
    player_stats_seeds = [
        ("peggs", "PEGGS", 37, 50, "West 4th St (The Cage)", "A lethal bucket getter from deep. Known for silencing the crowd at the West 4th runs.", "Bucket Getter"),
        ("naiqui", "NAIQUI", 35, 48, "West 4th St (The Cage)", "A lock-down defender and reliable corner shooter.", "3-and-D Wing"),
        ("jordan_b", "Flight", 20, 30, "Manor Field Park", "A spectacular isolation threat with explosive bounce.", "Iso Scorer"),
        ("big_smooth", "Big Smooth", 20, 25, "Brooklyn Bridge Park", "A towering stretch big who protects the paint and steps out to hit the trail three.", "Stretch Big"),
        ("d-train", "D-Train", 24, 40, "Astoria Park", "A floor general who commands the offense and delivers perfect dimes.", "Point God")
    ]
    for ps in player_stats_seeds:
        conn.execute("INSERT INTO player_stats (player_id, name, wins, games_played, preferred_court, bio, archetype) VALUES (?,?,?,?,?,?,?)", ps)

    # 3. Seed Squad Rankings
    squads = [
        ("The Twin Telepaths", 12, 120, "NYC_West_4th_Street_Courts"),
        ("Bridge Burners", 8, 85, "NYC_Brooklyn_Bridge_Park"),
        ("Queens Finest", 5, 60, "NYC_Astoria_Park"),
        ("Flight Squad", 4, 45, "LI_Manor_Field_Park"),
        ("Concrete Kings", 3, 30, "NYC_Dyckman_Park")
    ]
    for s in squads:
        conn.execute("INSERT INTO squad_rankings (squad_name, win_streak, total_wins, current_court_id) VALUES (?,?,?,?)", s)

    # 4. Seed Court Kings
    kings = [
        ("NYC_West_4th_Street_Courts", "The Twin Telepaths", "squad", 12),
        ("NYC_Brooklyn_Bridge_Park", "Bridge Burners", "squad", 8),
        ("LI_Manor_Field_Park", "Flight Squad", "squad", 4)
    ]
    for k in kings:
        conn.execute("INSERT INTO court_kings (court_id, king_id, king_type, win_streak) VALUES (?,?,?,?)", k)

    # 5. Seed Legacy Nicknames
    nicknames = [
        ("naiqui,peggs", "Twin Telepaths", 14),
        ("big_smooth,d-train", "Pick & Roll Wizards", 8)
    ]
    for n in nicknames:
        conn.execute("INSERT INTO legacy_nicknames (player_ids, nickname, wins_together) VALUES (?,?,?)", n)

    # 6. Seed Initial Blog Posts
    posts = [
        ("Legend of the Cage: How Peggs Silenced the Bridge Burners", 
         "It was a humid Tuesday evening when the Bridge Burners rolled deep into West 4th St. They had a 7-game streak, talking heavy on the sidelines. But Peggs was waiting. Stepping onto the concrete, the Bucket Getter went to work. Off-dribble pull-ups, contested baseline fadeaways—nothing but net. By the time the dust settled, the Burners were sent packing, and the Cage had a new king. The Concrete Sentinel remembers. Respect the run.",
         "The Concrete Sentinel",
         "COURT BEEF,LEGACY"),
        ("Hardwood Sanctuary: Inside the Life Time Sky Surge",
         "While the parks offer the pure love of the game, the indoor runs at Life Time Sky are raising the stakes. At 10 Credits a day pass, the hardwood is immaculate, the spacing is league-level, and the sweat is real. But don't think the Concrete Sentinel doesn't see you inside. Hardwood or asphalt, you still have to lock in. Sign the waiver, get your QR code, and show us what you're worth.",
         "The Hardwood Sentinel",
         "PREMIUM RUN,FACILITY")
    ]
    for p in posts:
        conn.execute("INSERT INTO blog_posts (title, content, author, tags) VALUES (?,?,?,?)", p)

    conn.commit()
    conn.close()
    print("Database seeded with legendary rankings and columns.")

if __name__ == "__main__":
    seed()
