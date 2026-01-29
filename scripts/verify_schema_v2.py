#!/usr/bin/env python3
# scripts/verify_schema_v2.py
"""Verify that schema v2 tables and constraints exist in the database."""

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
    found_columns = {row[0] for row in cur.fetchall()}
    return all(col in found_columns for col in columns)


def check_unique_constraint_on_columns(cur, table_name, columns):
    """Check if a UNIQUE constraint exists on specific columns."""
    # Get all unique constraints for the table
    cur.execute("""
        SELECT tc.constraint_name, array_agg(ccu.column_name ORDER BY ccu.column_name) as columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu 
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        WHERE tc.table_schema = 'public'
        AND tc.table_name = %s
        AND tc.constraint_type = 'UNIQUE'
        GROUP BY tc.constraint_name
    """, (table_name,))
    
    constraints = cur.fetchall()
    target_columns = sorted(columns)
    
    for constraint_name, constraint_columns in constraints:
        if sorted(constraint_columns) == target_columns:
            return True
    
    return False


def main():
    """Main verification function."""
    print("=== Schema v2 Verification ===\n")
    
    all_passed = True
    
    try:
        # Load environment
        load_dotenv()
        database_url = os.getenv("DATABASE_URL")
        
        if not database_url:
            print("[ERROR] DATABASE_URL not found in .env file")
            sys.exit(1)
        
        # Connect to database
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Define tables and their required columns
                tables_to_check = {
                    'players': ['id', 'user_id', 'screen_name'],
                    'hand_players': ['id', 'hand_id', 'player_id', 'seat', 'starting_stack', 'is_dealer'],
                    'streets': ['id', 'hand_id', 'street', 'board'],
                    'actions': ['id', 'hand_id', 'street', 'action_no', 'player_id', 'action_type', 'amount', 'is_allin'],
                    'hand_results': ['id', 'hand_id', 'player_id', 'won_amount', 'net_amount']
                }
                
                # 1) Check tables exist
                print("Checking tables existence...")
                for table_name in tables_to_check.keys():
                    exists = check_table_exists(cur, table_name)
                    status = "[OK]" if exists else "[FAIL]"
                    print(f"  {status} Table '{table_name}' exists")
                    if not exists:
                        all_passed = False
                
                print()
                
                # 2) Check columns exist
                print("Checking required columns...")
                for table_name, columns in tables_to_check.items():
                    # Only check columns if table exists
                    if check_table_exists(cur, table_name):
                        has_columns = check_columns_exist(cur, table_name, columns)
                        status = "[OK]" if has_columns else "[FAIL]"
                        columns_str = ", ".join(columns)
                        print(f"  {status} Table '{table_name}' has columns: {columns_str}")
                        if not has_columns:
                            all_passed = False
                    else:
                        print(f"  [SKIP] Table '{table_name}' does not exist")
                        all_passed = False
                
                print()
                
                # 3) Check UNIQUE constraints
                print("Checking UNIQUE constraints...")
                unique_constraints = {
                    'players': ['user_id', 'screen_name'],
                    'hand_players': ['hand_id', 'player_id'],
                    'streets': ['hand_id', 'street'],
                    'hand_results': ['hand_id', 'player_id']
                }
                
                for table_name, columns in unique_constraints.items():
                    # Only check constraints if table exists
                    if check_table_exists(cur, table_name):
                        has_constraint = check_unique_constraint_on_columns(cur, table_name, columns)
                        status = "[OK]" if has_constraint else "[FAIL]"
                        columns_str = ", ".join(columns)
                        print(f"  {status} UNIQUE({columns_str}) on '{table_name}'")
                        if not has_constraint:
                            all_passed = False
                    else:
                        print(f"  [SKIP] Table '{table_name}' does not exist")
                        all_passed = False
                
                print()
                
                # Final status
                if all_passed:
                    print("Status: ✅ OK")
                else:
                    print("Status: ❌ FAIL")
                    sys.exit(1)
    
    except psycopg.OperationalError as e:
        print(f"\n[ERROR] Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
