# C:\Users\Usuario\Dropbox\yvolo_traker\core\parse_ipoker_v1_incremental.py
"""
Incremental parser wrapper for ChampionPoker/iPoker XML hands.
Processes only unparsed hands (NOT EXISTS in hand_players).
"""

import os
import sys
import argparse
from decimal import Decimal

# --- FIX IMPORT PATH (para que "core.*" funcione al ejecutar scripts sueltos) ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# -----------------------------------------------------------------------------

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

# Import the actual functions from parse_ipoker_v1
from core.parse_ipoker_v1 import (
    extract_game_from_raw_xml,
    parse_players,
    parse_boards,
    parse_actions,
    upsert_player,
    upsert_hand_player,
    upsert_street,
    replace_actions,
    replace_hand_results,
    get_user_id,
)

BATCH_SIZE = 500


def get_db_conn():
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set (check .env)")
    return psycopg2.connect(dsn)


def fetch_unparsed_hands(cur, user_id, limit=None, offset=0):
    """
    Fetch hands that have NOT been parsed yet (no entries in hand_players).
    """
    sql = """
        SELECT h.id, h.game_id, h.raw_text
        FROM hands h
        WHERE h.user_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM hand_players hp
              WHERE hp.hand_id = h.id
          )
        ORDER BY h.id
    """
    params = [user_id]

    if limit is not None:
        sql += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    cur.execute(sql, params)
    return cur.fetchall()


def process_hand_incremental(conn, user_id, hand_id, game_id, raw_text):
    """
    Process a single hand using functions from parse_ipoker_v1.py.
    Returns dict with counts of inserted records.
    """
    # Extract and parse XML
    root, game_el = extract_game_from_raw_xml(raw_text)
    players = parse_players(game_el)
    boards = parse_boards(game_el)
    actions = parse_actions(game_el)

    # Build results from players list
    results = []
    for p in players:
        bet_total = p["bet_total"]
        win_total = p["win_total"]
        results.append({
            "player": p["screen_name"],
            "won_amount": win_total,
            "net_amount": (win_total - bet_total),
        })

    # Upsert players and build name->id mapping
    player_name_to_id = {}
    for p in players:
        pid = upsert_player(conn, user_id, p["screen_name"])
        player_name_to_id[p["screen_name"]] = pid

    # Insert hand_players
    for p in players:
        pid = player_name_to_id[p["screen_name"]]
        upsert_hand_player(
            conn,
            hand_id,
            pid,
            p["seat"],
            p["starting_stack"],
            p["is_dealer"],
        )

    # Insert streets (always ensure 4 streets exist)
    for street in ("preflop", "flop", "turn", "river"):
        upsert_street(conn, hand_id, street, boards.get(street))

    # Insert actions and results
    inserted_actions = replace_actions(conn, hand_id, actions, player_name_to_id)
    inserted_results = replace_hand_results(conn, hand_id, results, player_name_to_id)

    return {
        "players_count": len(players),
        "actions_count": inserted_actions,
        "results_count": inserted_results,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Incremental parser wrapper (process only unparsed hands)"
    )
    parser.add_argument("--user", required=True, help="User ID or username")
    parser.add_argument("--limit", type=int, help="Max hands to process (default: all)")
    parser.add_argument("--offset", type=int, default=0, help="Offset (default: 0)")
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't process")
    args = parser.parse_args()

    conn = get_db_conn()
    conn.autocommit = False

    total_parsed = 0
    total_errors = 0
    total_actions = 0
    total_results = 0

    try:
        # Get user_id (handle both numeric ID and username)
        try:
            user_id = int(args.user)
        except ValueError:
            user_id = get_user_id(conn, args.user)

        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Count pending hands
            cur.execute(
                """
                SELECT COUNT(*)
                FROM hands h
                WHERE h.user_id = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM hand_players hp WHERE hp.hand_id = h.id
                  )
                """,
                (user_id,),
            )
            pending = cur.fetchone()[0]

            if pending == 0:
                print("No unparsed hands found. Nothing to do.")
                return

            print(f"Unparsed hands pending: {pending}")

            if args.dry_run:
                print("DRY RUN MODE - No DB writes will be made")
                return

            # Process in batches
            remaining = args.limit if args.limit is not None else pending
            offset = args.offset

            while remaining > 0:
                batch_limit = min(BATCH_SIZE, remaining)
                hands = fetch_unparsed_hands(cur, user_id, limit=batch_limit, offset=offset)
                
                if not hands:
                    break

                print(f"Processing batch: {len(hands)} hand(s)...")

                for h in hands:
                    try:
                        result = process_hand_incremental(
                            conn,
                            user_id,
                            h["id"],
                            h["game_id"],
                            h["raw_text"]
                        )
                        
                        total_parsed += 1
                        total_actions += result["actions_count"]
                        total_results += result["results_count"]

                    except Exception as e:
                        total_errors += 1
                        print(f"ERROR parsing hand_id={h['id']} game_id={h['game_id']}: {e}")
                        conn.rollback()
                        continue

                # Commit batch
                conn.commit()
                print(f"  Committed {len(hands)} hands")

                remaining -= len(hands)
                offset = 0  # After first batch, offset is implicit (no more unparsed at lower IDs)

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    print("=" * 50)
    print(f"Parsed hands: {total_parsed}")
    print(f"Total actions inserted: {total_actions}")
    print(f"Total results inserted: {total_results}")
    print(f"Errors: {total_errors}")
    print("=" * 50)


if __name__ == "__main__":
    main()
