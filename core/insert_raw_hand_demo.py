#!/usr/bin/env python3
"""Demo script to insert a raw hand from iPoker hand history file."""

import os
import re
import hashlib
from pathlib import Path
from dotenv import load_dotenv
import psycopg

# Load environment variables
load_dotenv()

def get_or_create_user(conn, username):
    """Get or create a user by username. Returns user_id."""
    with conn.cursor() as cur:
        # Try to get existing user
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        result = cur.fetchone()
        if result:
            return result[0]
        
        # Create new user
        cur.execute(
            "INSERT INTO users (username) VALUES (%s) RETURNING id",
            (username,)
        )
        return cur.fetchone()[0]

def upsert_hand(conn, user_id, game_id, raw_text, source_file):
    """Insert or update a hand record. Returns hand id."""
    # Calculate hash of raw text
    raw_text_hash = hashlib.sha256(raw_text.encode('utf-8')).hexdigest()
    
    with conn.cursor() as cur:
        # UPSERT using ON CONFLICT
        cur.execute("""
            INSERT INTO hands (user_id, game_id, raw_text, raw_text_hash, source_file)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_id)
            DO UPDATE SET
                raw_text = EXCLUDED.raw_text,
                raw_text_hash = EXCLUDED.raw_text_hash,
                source_file = EXCLUDED.source_file
            RETURNING id
        """, (user_id, game_id, raw_text, raw_text_hash, source_file))
        
        return cur.fetchone()[0]

def parse_hands_from_file(file_path):
    """Parse hands from iPoker hand history file."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    
    # Split by GAME # pattern
    pattern = re.compile(r'^GAME\s+#\d+', re.MULTILINE)
    matches = list(pattern.finditer(content))
    
    hands = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        hand_text = content[start:end].strip()
        if hand_text:
            hands.append(hand_text)
    
    return hands

def extract_game_id(hand_text):
    """Extract game_id from hand text."""
    match = re.match(r'^GAME\s+#(\d+)', hand_text)
    if match:
        return match.group(1)
    return None

def main():
    """Main execution function."""
    # Database connection
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment")
    
    # File path - EXACT path as specified
    file_path = "/mnt/data/iPoker Network_HandsExport_202601220852_tourney_00001.txt"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        print("Please ensure the file exists at the exact path specified.")
        return
    
    # Parse hands from file
    hands = parse_hands_from_file(file_path)
    
    if not hands:
        print("❌ No hands found in file")
        return
    
    # Get first hand
    first_hand = hands[0]
    game_id = extract_game_id(first_hand)
    
    if not game_id:
        print("❌ Could not extract game_id from hand")
        return
    
    # Connect to database and insert
    with psycopg.connect(database_url) as conn:
        # Get or create demo user
        user_id = get_or_create_user(conn, "demo")
        
        # Upsert hand
        hand_id = upsert_hand(
            conn,
            user_id=user_id,
            game_id=game_id,
            raw_text=first_hand,
            source_file=os.path.basename(file_path)
        )
        
        conn.commit()
        
        print(f"✅ Inserted/updated hand: game_id={game_id} db_id={hand_id}")

if __name__ == "__main__":
    main()
