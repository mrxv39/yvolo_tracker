#!/usr/bin/env python3
# scripts/verify_parsed_data_v1.py
"""Verify that parser v1 has correctly populated relational tables for a user."""

import os
import sys
import argparse
from dotenv import load_dotenv
import psycopg


def get_user_id(conn, username: str) -> int:
    """Get user_id from username."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        result = cur.fetchone()
        if not result:
            raise ValueError(f"User '{username}' not found")
        return result[0]


def get_global_counts(conn, user_id: int) -> dict:
    """Get global counts for the user."""
    counts = {}
    
    with conn.cursor() as cur:
        # Total hands
        cur.execute("SELECT COUNT(*) FROM hands WHERE user_id = %s", (user_id,))
        counts['total_hands'] = cur.fetchone()[0]
        
        # Total players
        cur.execute("SELECT COUNT(*) FROM players WHERE user_id = %s", (user_id,))
        counts['total_players'] = cur.fetchone()[0]
        
        # Total hand_players (only for this user's hands)
        cur.execute("""
            SELECT COUNT(*) 
            FROM hand_players hp
            JOIN hands h ON h.id = hp.hand_id
            WHERE h.user_id = %s
        """, (user_id,))
        counts['total_hand_players'] = cur.fetchone()[0]
        
        # Total actions (only for this user's hands)
        cur.execute("""
            SELECT COUNT(*) 
            FROM actions a
            JOIN hands h ON h.id = a.hand_id
            WHERE h.user_id = %s
        """, (user_id,))
        counts['total_actions'] = cur.fetchone()[0]
        
        # Total streets (only for this user's hands)
        cur.execute("""
            SELECT COUNT(*) 
            FROM streets s
            JOIN hands h ON h.id = s.hand_id
            WHERE h.user_id = %s
        """, (user_id,))
        counts['total_streets'] = cur.fetchone()[0]
    
    return counts


def check_quality_issues(conn, user_id: int) -> dict:
    """Check for various quality issues."""
    issues = {}
    
    with conn.cursor() as cur:
        # Hands without hand_players
        cur.execute("""
            SELECT COUNT(DISTINCT h.id)
            FROM hands h
            LEFT JOIN hand_players hp ON hp.hand_id = h.id
            WHERE h.user_id = %s
            GROUP BY h.id
            HAVING COUNT(hp.id) = 0
        """, (user_id,))
        result = cur.fetchall()
        issues['hands_without_players'] = len(result)
        
        # Hands without actions
        cur.execute("""
            SELECT COUNT(DISTINCT h.id)
            FROM hands h
            LEFT JOIN actions a ON a.hand_id = h.id
            WHERE h.user_id = %s
            GROUP BY h.id
            HAVING COUNT(a.id) = 0
        """, (user_id,))
        result = cur.fetchall()
        issues['hands_without_actions'] = len(result)
        
        # Orphan actions (actions with player_id not belonging to user's players)
        cur.execute("""
            SELECT COUNT(*)
            FROM actions a
            JOIN hands h ON h.id = a.hand_id
            WHERE h.user_id = %s
            AND a.player_id NOT IN (
                SELECT id FROM players WHERE user_id = %s
            )
        """, (user_id, user_id))
        issues['orphan_actions'] = cur.fetchone()[0]
        
        # Invalid street values
        cur.execute("""
            SELECT COUNT(*)
            FROM actions a
            JOIN hands h ON h.id = a.hand_id
            WHERE h.user_id = %s
            AND a.street NOT IN ('preflop', 'flop', 'turn', 'river')
        """, (user_id,))
        issues['invalid_streets'] = cur.fetchone()[0]
    
    return issues


def get_sample_hands(conn, user_id: int, sample_size: int) -> list:
    """Get sample hands with detailed info."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                h.id,
                h.game_id,
                COUNT(DISTINCT hp.id) as player_count,
                COUNT(DISTINCT a.id) as action_count,
                MIN(a.action_no) as first_action_no,
                MAX(a.action_no) as last_action_no,
                STRING_AGG(DISTINCT s.street, ',' ORDER BY s.street) as streets_present
            FROM hands h
            LEFT JOIN hand_players hp ON hp.hand_id = h.id
            LEFT JOIN actions a ON a.hand_id = h.id
            LEFT JOIN streets s ON s.hand_id = h.id AND s.board IS NOT NULL
            WHERE h.user_id = %s
            GROUP BY h.id, h.game_id
            ORDER BY random()
            LIMIT %s
        """, (user_id, sample_size))
        
        return cur.fetchall()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Verify parsed data quality for a user"
    )
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument("--sample", type=int, default=20, help="Number of random hands to inspect")
    
    args = parser.parse_args()
    
    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("ERROR: DATABASE_URL not found in .env file")
        sys.exit(1)
    
    try:
        with psycopg.connect(database_url) as conn:
            # Get user_id
            try:
                user_id = get_user_id(conn, args.user)
            except ValueError as e:
                print(f"ERROR: {e}")
                sys.exit(1)
            
            # Get global counts
            counts = get_global_counts(conn, user_id)
            
            # Check quality issues
            issues = check_quality_issues(conn, user_id)
            
            # Get sample hands
            samples = get_sample_hands(conn, user_id, args.sample)
            
            # Print report
            print("=" * 40)
            print("Parsed Data Verification (v1)")
            print(f"User: {args.user} (id={user_id})")
            print(f"Hands: {counts['total_hands']}")
            print(f"Players: {counts['total_players']}")
            print(f"Hand players rows: {counts['total_hand_players']}")
            print(f"Actions rows: {counts['total_actions']}")
            print(f"Streets rows: {counts['total_streets']}")
            print()
            
            print("Quality checks:")
            print(f"- Hands with 0 hand_players: {issues['hands_without_players']}")
            print(f"- Hands with 0 actions: {issues['hands_without_actions']}")
            print(f"- Orphan actions (wrong user): {issues['orphan_actions']}")
            print(f"- Invalid street values: {issues['invalid_streets']}")
            print()
            
            if samples:
                print(f"Sample ({len(samples)} hands):")
                for sample in samples:
                    hand_id, game_id, player_count, action_count, first_action, last_action, streets = sample
                    
                    # Format action range
                    action_range = "none"
                    if first_action and last_action:
                        action_range = f"{first_action}..{last_action}"
                    
                    # Format streets
                    streets_str = streets if streets else "none"
                    
                    print(f"- hand_id={hand_id}, game_id={game_id}, players={player_count}, "
                          f"actions={action_count}, streets={streets_str}, action_no={action_range}")
            else:
                print("Sample: (no hands to sample)")
            
            print()
            
            # Determine status
            critical_failures = (
                issues['orphan_actions'] > 0 or
                issues['invalid_streets'] > 0
            )
            
            # Check if >10% of hands missing data
            if counts['total_hands'] > 0:
                missing_players_pct = (issues['hands_without_players'] / counts['total_hands']) * 100
                missing_actions_pct = (issues['hands_without_actions'] / counts['total_hands']) * 100
                high_missing_rate = (missing_players_pct > 10 or missing_actions_pct > 10)
            else:
                high_missing_rate = False
            
            is_fail = critical_failures or high_missing_rate
            
            if is_fail:
                print("Status: ❌ FAIL")
                if issues['orphan_actions'] > 0:
                    print(f"  ERROR: Found {issues['orphan_actions']} orphan actions")
                if issues['invalid_streets'] > 0:
                    print(f"  ERROR: Found {issues['invalid_streets']} invalid street values")
                if high_missing_rate:
                    if missing_players_pct > 10:
                        print(f"  ERROR: {missing_players_pct:.1f}% hands missing players (>10% threshold)")
                    if missing_actions_pct > 10:
                        print(f"  ERROR: {missing_actions_pct:.1f}% hands missing actions (>10% threshold)")
            else:
                print("Status: ✅ OK")
            
            print("=" * 40)
            
            if is_fail:
                sys.exit(1)
    
    except psycopg.OperationalError as e:
        print(f"\nERROR: Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
