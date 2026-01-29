#!/usr/bin/env python3
# scripts/verify_import.py
"""Verify that the import of raw hands into PostgreSQL is correct for a user."""

import os
import sys
import argparse
from dotenv import load_dotenv
import psycopg


def get_user_id(conn, username: str) -> int:
    """Get user_id from username. Raises ValueError if not found."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        result = cur.fetchone()
        if not result:
            raise ValueError(f"User '{username}' not found in database")
        return result[0]


def verify_import(database_url: str, username: str):
    """Run verification checks on imported hands."""
    
    with psycopg.connect(database_url) as conn:
        # Get user_id
        try:
            user_id = get_user_id(conn, username)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
        
        with conn.cursor() as cur:
            # A) Count total hands
            cur.execute(
                "SELECT COUNT(*) FROM hands WHERE user_id = %s",
                (user_id,)
            )
            total_hands = cur.fetchone()[0]
            
            # A) Count unique game_ids
            cur.execute(
                "SELECT COUNT(DISTINCT game_id) FROM hands WHERE user_id = %s",
                (user_id,)
            )
            unique_game_ids = cur.fetchone()[0]
            
            # B) Check for duplicate game_ids (should be 0 due to UNIQUE constraint)
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT game_id, COUNT(*) c
                    FROM hands
                    WHERE user_id = %s
                    GROUP BY game_id
                    HAVING COUNT(*) > 1
                ) t
            """, (user_id,))
            duplicate_groups = cur.fetchone()[0]
            
            # C) Top 10 source_file by number of hands
            cur.execute("""
                SELECT COALESCE(source_file, '(null)') AS source_file, COUNT(*) AS c
                FROM hands
                WHERE user_id = %s
                GROUP BY COALESCE(source_file, '(null)')
                ORDER BY c DESC
                LIMIT 10
            """, (user_id,))
            top_sources = cur.fetchall()
            
            # D) Random samples
            cur.execute("""
                SELECT id, game_id, source_file, substring(raw_text from 1 for 200) AS raw_preview
                FROM hands
                WHERE user_id = %s
                ORDER BY random()
                LIMIT 3
            """, (user_id,))
            samples = cur.fetchall()
        
        # Print report
        print("=" * 40)
        print("Import Verification")
        print("-" * 40)
        print(f"User: {username} (id={user_id})")
        print()
        print(f"Total hands: {total_hands}")
        print(f"Distinct game_id: {unique_game_ids}")
        print(f"Duplicate game_id groups: {duplicate_groups}")
        print()
        
        if top_sources:
            print("Top source files:")
            for idx, (source_file, count) in enumerate(top_sources, 1):
                # Truncate long file paths for readability
                display_source = source_file
                if len(display_source) > 50:
                    display_source = "..." + display_source[-47:]
                print(f"  {idx}) {display_source} -> {count}")
            print()
        
        if samples:
            print("Samples:")
            for sample in samples:
                hand_id, game_id, source_file, raw_preview = sample
                source_display = source_file if source_file else "(null)"
                if source_display and len(source_display) > 40:
                    source_display = "..." + source_display[-37:]
                print(f"- id={hand_id}, game_id={game_id}, source={source_display}")
                # Clean up preview text (remove excessive whitespace)
                preview = raw_preview.replace('\n', ' ').replace('\r', '')
                preview = ' '.join(preview.split())  # Normalize whitespace
                if len(preview) > 150:
                    preview = preview[:150] + "..."
                print(f'  preview: "{preview}"')
            print()
        
        # Determine status
        is_valid = (total_hands == unique_game_ids) and (duplicate_groups == 0)
        
        if is_valid:
            print("Status: ✅ OK")
        else:
            print("Status: ❌ FAIL")
            if total_hands != unique_game_ids:
                print(f"  ERROR: total_hands ({total_hands}) != unique_game_ids ({unique_game_ids})")
            if duplicate_groups > 0:
                print(f"  ERROR: Found {duplicate_groups} duplicate game_id groups")
        
        print("=" * 40)
        
        # Exit with appropriate code
        if not is_valid:
            sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Verify imported hands for a user in PostgreSQL"
    )
    parser.add_argument(
        "--user",
        required=True,
        help="Username to verify"
    )
    
    args = parser.parse_args()
    
    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("ERROR: DATABASE_URL not found in .env file")
        sys.exit(1)
    
    try:
        verify_import(database_url, args.user)
    except psycopg.OperationalError as e:
        print(f"\nERROR: Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
