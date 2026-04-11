import sqlite3
import os

DB_PATH = "data/courtfinder/courts.db"

def seed():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Clear existing rankings for a fresh start
    conn.execute("DELETE FROM squad_rankings")
    conn.execute("DELETE FROM court_kings")
    conn.execute("DELETE FROM players")
    conn.execute("DELETE FROM legacy_nicknames")

    # 2. Seed Elite Players
    players = [
        ("peggs", "PEGGS", "Sharpshooter", 0.74, 50, 37, "Elite"),
        ("naiqui", "NAIQUI", "Sharpshooter", 0.72, 48, 35, "Elite"),
        ("jordan_b", "Flight", "Slasher", 0.65, 30, 20, "Verified"),
        ("big_smooth", "Big Smooth", "Rim Protector", 0.80, 25, 20, "Elite"),
        ("d-train", "D-Train", "Floor General", 0.60, 40, 24, "Verified")
    ]
    for p in players:
        conn.execute("INSERT INTO players (player_id, name, archetype, win_rate, total_games, total_wins, verified_status) VALUES (?,?,?,?,?,?,?)", p)

    # 3. Seed Squad Rankings
    squads = [
        ("The Splash Brothers", 12, 120, "NYC_West_4th_Street_Courts"),
        ("Bridge Burners", 8, 85, "NYC_Brooklyn_Bridge_Park"),
        ("Queens Finest", 5, 60, "NYC_Astoria_Park"),
        ("Flight Squad", 4, 45, "LI_Manor_Field_Park"),
        ("Concrete Kings", 3, 30, "NYC_Dyckman_Park")
    ]
    for s in squads:
        conn.execute("INSERT INTO squad_rankings (squad_name, win_streak, total_wins, current_court_id) VALUES (?,?,?,?)", s)

    # 4. Seed Court Kings
    kings = [
        ("NYC_West_4th_Street_Courts", "The Splash Brothers", "squad", 12),
        ("NYC_Brooklyn_Bridge_Park", "Bridge Burners", "squad", 8),
        ("LI_Manor_Field_Park", "Flight Squad", "squad", 4)
    ]
    for k in kings:
        conn.execute("INSERT INTO court_kings (court_id, king_id, king_type, win_streak) VALUES (?,?,?,?)", k)

    # 5. Seed Legacy Nicknames
    nicknames = [
        ("naiqui,peggs", "Splash Brothers", 14),
        ("big_smooth,d-train", "The Twin Towers", 8)
    ]
    for n in nicknames:
        conn.execute("INSERT INTO legacy_nicknames (player_ids, nickname, wins_together) VALUES (?,?,?)", n)

    conn.commit()
    conn.close()
    print("Database seeded with legendary rankings.")

if __name__ == "__main__":
    seed()
