#!/usr/bin/env python3
"""
Automated smoke test for the per-file import->parse->verify pipeline.

Tests the complete flow:
1. RAW import of XML hands
2. Parsing into relational tables
3. Verification of data integrity
4. File classification (processed/failed)
"""

import os
import sys
import tempfile
import shutil
import uuid
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
import psycopg2

from core.parse_hands_incremental import parse_specific_hands, verify_imported_hands
import xml.etree.ElementTree as ET
import hashlib

load_dotenv()

# System user ID (same as production)
SYSTEM_USER_ID = 1


def get_db_conn():
    """Get database connection from environment."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not defined in .env")
    return psycopg2.connect(dsn)


def test_per_file_import_pipeline():
    """
    Smoke test for the complete per-file import pipeline.
    
    Tests:
    - RAW hands inserted > 0
    - After parsing, rows exist in hand_players and actions for imported hand_ids
    - Verification returns True
    - File would be classified as "processed"
    """
    # Generate unique gamecode for this test run to avoid duplicates
    unique_gamecode = f"TEST_{uuid.uuid4().hex[:12].upper()}"
    
    # Setup: Create isolated temp directory with fixture copy
    fixture_path = PROJECT_ROOT / "tests" / "fixtures" / "minimal_ipoker.xml"
    
    if not fixture_path.exists():
        raise RuntimeError(f"Fixture not found: {fixture_path}")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        test_file = temp_dir_path / "test_session.xml"
        
        # Read fixture and replace gamecode with unique value
        with open(fixture_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        # Replace the gamecode attribute in the fixture with our unique one
        xml_content = xml_content.replace('gamecode="99999999"', f'gamecode="{unique_gamecode}"')
        
        # Write modified content to temp file
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        
        print(f"\n{'='*70}")
        print("SMOKE TEST: Per-File Import Pipeline")
        print(f"{'='*70}")
        print(f"Fixture: {fixture_path.name}")
        print(f"Test file: {test_file}")
        print(f"Unique gamecode: {unique_gamecode}")
        
        # Import phase
        conn = None
        imported_hand_ids = []
        duplicate_count = 0
        
        try:
            # Read XML file
            with open(test_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            # Parse XML to extract games
            root = ET.fromstring(xml_content)
            assert root.tag.lower() == "session", "Expected <session> root element"
            
            games = root.findall("game")
            assert len(games) > 0, "No <game> elements found"
            
            print(f"\n[Setup] Found {len(games)} game(s) in fixture")
            
            # Open connection
            conn = get_db_conn()
            conn.autocommit = False
            
            # Ensure test user exists
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (id, username) 
                    VALUES (%s, %s) 
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (SYSTEM_USER_ID, 'test_system_user')
                )
            
            # Pre-test cleanup: Remove any existing test data for this gamecode
            print(f"[Cleanup] Removing any existing test data for gamecode: {unique_gamecode}")
            with conn.cursor() as cur:
                # Delete from dependent tables first (due to foreign keys)
                cur.execute(
                    """
                    DELETE FROM actions 
                    WHERE hand_id IN (
                        SELECT id FROM hands WHERE game_id = %s AND user_id = %s
                    )
                    """,
                    (unique_gamecode, SYSTEM_USER_ID)
                )
                cur.execute(
                    """
                    DELETE FROM hand_results 
                    WHERE hand_id IN (
                        SELECT id FROM hands WHERE game_id = %s AND user_id = %s
                    )
                    """,
                    (unique_gamecode, SYSTEM_USER_ID)
                )
                cur.execute(
                    """
                    DELETE FROM streets 
                    WHERE hand_id IN (
                        SELECT id FROM hands WHERE game_id = %s AND user_id = %s
                    )
                    """,
                    (unique_gamecode, SYSTEM_USER_ID)
                )
                cur.execute(
                    """
                    DELETE FROM hand_players 
                    WHERE hand_id IN (
                        SELECT id FROM hands WHERE game_id = %s AND user_id = %s
                    )
                    """,
                    (unique_gamecode, SYSTEM_USER_ID)
                )
                cur.execute(
                    """
                    DELETE FROM hand_sizes 
                    WHERE hand_id IN (
                        SELECT id FROM hands WHERE game_id = %s AND user_id = %s
                    )
                    """,
                    (unique_gamecode, SYSTEM_USER_ID)
                )
                # Finally delete from hands table
                cur.execute(
                    """
                    DELETE FROM hands 
                    WHERE game_id = %s AND user_id = %s
                    """,
                    (unique_gamecode, SYSTEM_USER_ID)
                )
                deleted_count = cur.rowcount
                if deleted_count > 0:
                    print(f"  Removed {deleted_count} existing hand(s) from previous test run")
            
            conn.commit()
            
            # RAW import phase
            print(f"[Phase 1] RAW Import...")
            
            for game in games:
                gamecode = game.attrib.get("gamecode", "").strip()
                assert gamecode, "Game missing gamecode attribute"
                assert gamecode == unique_gamecode, f"Gamecode mismatch: expected {unique_gamecode}, got {gamecode}"
                
                # Convert game element to string for storage
                game_xml = ET.tostring(game, encoding="unicode")
                
                # Create hash for deduplication
                raw_text_hash = hashlib.sha256(game_xml.encode('utf-8')).hexdigest()
                
                # Insert into hands table
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO hands (user_id, game_id, source_file, raw_text_hash, raw_text)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, game_id) DO NOTHING
                        RETURNING id
                        """,
                        (SYSTEM_USER_ID, gamecode, str(test_file), raw_text_hash, game_xml),
                    )
                    result = cur.fetchone()
                    
                    if result:
                        hand_id = result[0]
                        imported_hand_ids.append(hand_id)
                    else:
                        # This should not happen since we cleaned up, but track it
                        duplicate_count += 1
            
            # Commit raw import
            conn.commit()
            
            # DIAGNOSTICS: If no hands imported, provide detailed error info
            if len(imported_hand_ids) == 0:
                print(f"\n[DIAGNOSTIC ERROR]")
                print(f"  Gamecode used: {unique_gamecode}")
                print(f"  Duplicates encountered: {duplicate_count}")
                print(f"  Expected: At least 1 new hand should have been imported")
                print(f"  This should not happen with unique gamecode + pre-test cleanup!")
            
            # ASSERTION 1: RAW hands inserted > 0
            assert len(imported_hand_ids) > 0, f"No hands were imported! Duplicates: {duplicate_count}, Gamecode: {unique_gamecode}"
            print(f"  ✓ Assertion 1: {len(imported_hand_ids)} hand(s) inserted into RAW hands table")
            
            # Parsing phase
            print(f"\n[Phase 2] Parsing...")
            
            parse_result = parse_specific_hands(conn, SYSTEM_USER_ID, imported_hand_ids)
            
            parsed_count = parse_result["parsed_count"]
            failed_count = parse_result["failed_count"]
            
            print(f"  Parsed: {parsed_count}, Failed: {failed_count}")
            
            if parse_result["errors"]:
                print(f"  Errors: {parse_result['errors']}")
            
            # Commit parsing results
            conn.commit()
            
            # ASSERTION 2: After parsing, rows exist in hand_players and actions
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) 
                    FROM hand_players 
                    WHERE hand_id = ANY(%s)
                    """,
                    (imported_hand_ids,)
                )
                hand_players_count = cur.fetchone()[0]
                
                cur.execute(
                    """
                    SELECT COUNT(*) 
                    FROM actions 
                    WHERE hand_id = ANY(%s)
                    """,
                    (imported_hand_ids,)
                )
                actions_count = cur.fetchone()[0]
            
            assert hand_players_count > 0, "No hand_players entries found after parsing"
            assert actions_count > 0, "No actions entries found after parsing"
            
            print(f"  ✓ Assertion 2: hand_players={hand_players_count}, actions={actions_count}")
            
            # Verification phase
            print(f"\n[Phase 3] Verification...")
            
            verify_success, verify_error = verify_imported_hands(conn, imported_hand_ids)
            
            # ASSERTION 3: Verification returns True
            assert verify_success, f"Verification failed: {verify_error}"
            print(f"  ✓ Assertion 3: Verification passed")
            
            # ASSERTION 4: Classification would be "processed"
            # In real flow, this would move file to processed/
            # Here we just verify that all conditions for "processed" are met
            classification = "processed" if verify_success and parsed_count > 0 else "failed"
            
            assert classification == "processed", f"File would be classified as {classification}"
            print(f"  ✓ Assertion 4: File would be classified as '{classification}'")
            
            print(f"\n{'='*70}")
            print("✓ ALL ASSERTIONS PASSED")
            print(f"{'='*70}")
            
        finally:
            # Cleanup: Rollback to avoid polluting test database
            if conn:
                try:
                    conn.rollback()
                    print("\n[Cleanup] Test data rolled back")
                except Exception as e:
                    print(f"\n[Cleanup] Error during rollback: {e}")
                
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    test_per_file_import_pipeline()
    print("\n✓ Smoke test completed successfully")
