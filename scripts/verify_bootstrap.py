#!/usr/bin/env python3
# scripts/verify_bootstrap.py
"""Verification script to validate PokerTracker bootstrap and database setup."""

import os
import sys
from dotenv import load_dotenv
import psycopg

def check_table_exists(cur, table_name):
    """Check if a table exists in the database."""
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def check_columns_exist(cur, table_name, columns):
    """Check if specific columns exist in a table."""
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = %s
        AND column_name = ANY(%s);
    """, (table_name, columns))
    found_columns = [row[0] for row in cur.fetchall()]
    return all(col in found_columns for col in columns)

def check_unique_constraint(cur, table_name, columns):
    """Check if a UNIQUE constraint exists on specific columns."""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_schema = 'public'
            AND tc.table_name = %s
            AND tc.constraint_type = 'UNIQUE'
            AND ccu.column_name = ANY(%s)
            GROUP BY tc.constraint_name
            HAVING COUNT(DISTINCT ccu.column_name) = %s
        );
    """, (table_name, columns, len(columns)))
    return cur.fetchone()[0]

def main():
    """Main verification function."""
    print("=== PokerTracker Bootstrap Verification ===\n")
    
    try:
        # Load environment variables
        load_dotenv()
        database_url = os.getenv("DATABASE_URL")
        
        if not database_url:
            print("[ERROR] DATABASE_URL not found in .env file")
            sys.exit(1)
        
        # Connect to database
        with psycopg.connect(database_url) as conn:
            print("[OK] Connected to database")
            
            with conn.cursor() as cur:
                # Check tables exist
                if not check_table_exists(cur, "users"):
                    print("[ERROR] Table users does not exist")
                    sys.exit(1)
                print("[OK] Table users exists")
                
                if not check_table_exists(cur, "hands"):
                    print("[ERROR] Table hands does not exist")
                    sys.exit(1)
                print("[OK] Table hands exists")
                
                # Check required columns in hands table
                required_columns = ["id", "user_id", "game_id", "raw_text"]
                if not check_columns_exist(cur, "hands", required_columns):
                    print("[ERROR] hands table missing required columns")
                    sys.exit(1)
                print("[OK] hands has required columns")
                
                # Check UNIQUE constraint
                if not check_unique_constraint(cur, "hands", ["user_id", "game_id"]):
                    print("[ERROR] UNIQUE(user_id, game_id) constraint not found")
                    sys.exit(1)
                print("[OK] UNIQUE(user_id, game_id) constraint exists")
                
                print()  # Empty line for spacing
                
                # Count users
                cur.execute("SELECT COUNT(*) FROM users;")
                users_count = cur.fetchone()[0]
                print(f"Users count: {users_count}")
                
                # Count hands
                cur.execute("SELECT COUNT(*) FROM hands;")
                hands_count = cur.fetchone()[0]
                print(f"Hands count: {hands_count}")
                
                print()  # Empty line for spacing
                
                # Get last hand
                cur.execute("""
                    SELECT id, user_id, game_id, created_at 
                    FROM hands 
                    ORDER BY id DESC 
                    LIMIT 1;
                """)
                last_hand = cur.fetchone()
                
                if last_hand:
                    print("Last hand:")
                    print(f"  id: {last_hand[0]}")
                    print(f"  user_id: {last_hand[1]}")
                    print(f"  game_id: {last_hand[2]}")
                    print(f"  created_at: {last_hand[3]}")
                else:
                    print("Last hand: (no hands in database)")
                
                print()
                print("Bootstrap status: ✅ VALID")
    
    except psycopg.OperationalError as e:
        print(f"\n[ERROR] Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
