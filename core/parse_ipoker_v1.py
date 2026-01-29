#!/usr/bin/env python3
# C:\Users\Usuario\Dropbox\yvolo_traker\core\parse_ipoker_v1.py
"""
Parse ChampionPoker (iPoker XML) hands stored in hands.raw_text and populate relational tables.

Expected raw_text formats (any of these):
- <hand ...> <game ...> ... </game> </hand>
- <game ...> ... </game>
- <session ...> <game ...> ... </game> ... </session>  (we pick the first game element)

Tables populated:
- players
- hand_players
- streets
- actions
- hand_results
"""

import os
import sys
import argparse
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg


# --- Helpers -----------------------------------------------------------------

def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # remove thousands separators like "1,235"
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def get_user_id(conn, username: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"User '{username}' not found")
        return row[0]


# --- Action type mapping (ChampionPoker XML numeric codes -> our text) --------
# NOTE: If we meet unknown codes, we store TYPE_<n>.
ACTION_TYPE_MAP = {
    0: "FOLD",
    1: "POST_SB",
    2: "POST_BB",
    3: "CALL",
    4: "CHECK",
    5: "BET",
    6: "RAISE",
    7: "ALLIN",       # often call-allin / allin
    15: "POST_ANTE",
    23: "RAISE",      # frequently used for opens/raises in these logs
}


def map_action_type(type_code: Optional[str]) -> str:
    try:
        n = int(str(type_code).strip())
    except Exception:
        return "TYPE_UNKNOWN"
    return ACTION_TYPE_MAP.get(n, f"TYPE_{n}")


def street_from_round_no(round_no: Optional[str]) -> str:
    # Your rule: preflop is round no="1"; flop=2; turn=3; river=4.
    # There is also round no="0" for blinds/antes -> treat as preflop.
    try:
        n = int(str(round_no).strip())
    except Exception:
        return "preflop"
    if n in (0, 1):
        return "preflop"
    if n == 2:
        return "flop"
    if n == 3:
        return "turn"
    if n == 4:
        return "river"
    return "preflop"


# --- XML Extraction -----------------------------------------------------------

def extract_game_from_raw_xml(raw_text: str) -> Tuple[ET.Element, ET.Element]:
    """
    Returns (root, game_element).
    root is the parsed XML root.
    game_element is the <game> element for this hand.
    """
    raw_text = raw_text.strip()
    if not raw_text:
        raise ValueError("Empty raw_text")

    try:
        root = ET.fromstring(raw_text)
    except ET.ParseError as e:
        # Sometimes raw_text may contain invalid leading chars; try to salvage by trimming before '<'
        idx = raw_text.find("<")
        if idx > 0:
            root = ET.fromstring(raw_text[idx:])
        else:
            raise ValueError(f"XML parse error: {e}") from e

    tag = root.tag.lower()

    if tag == "game":
        return root, root

    if tag == "hand":
        game = root.find("./game")
        if game is None:
            # some variants might embed directly, fallback:
            game = root.find(".//game")
        if game is None:
            raise ValueError("No <game> element found inside <hand>")
        return root, game

    if tag == "session":
        game = root.find("./game")
        if game is None:
            game = root.find(".//game")
        if game is None:
            raise ValueError("No <game> element found inside <session>")
        return root, game

    # unknown root, attempt to locate first game anyway
    game = root.find(".//game")
    if game is None:
        raise ValueError(f"Unsupported root tag: {root.tag} (no <game> found)")
    return root, game


def parse_players(game_el: ET.Element) -> List[Dict]:
    """
    Parse players from:
      <general><players><player .../></players></general>
    Returns list of dict:
      {
        screen_name, seat, starting_stack, is_dealer,
        bet_total, win_total
      }
    """
    players: List[Dict] = []

    players_parent = game_el.find("./general/players")
    if players_parent is None:
        return players

    for p in players_parent.findall("./player"):
        name = (p.get("name") or "").strip()
        if not name:
            continue

        seat = p.get("seat")
        dealer = p.get("dealer")
        chips = p.get("chips")  # starting stack
        bet = p.get("bet")      # total invested in hand
        win = p.get("win")      # chips returned/won

        players.append({
            "screen_name": name,
            "seat": int(seat) if seat and seat.isdigit() else None,
            "starting_stack": parse_decimal(chips) or Decimal(0),
            "is_dealer": (str(dealer).strip() == "1"),
            "bet_total": parse_decimal(bet) or Decimal(0),
            "win_total": parse_decimal(win) or Decimal(0),
        })

    return players


def parse_boards(game_el: ET.Element) -> Dict[str, Optional[str]]:
    """
    Parse board cards from rounds:
      <round no="2"><cards type="Flop">D3 HK D2</cards>...
      <round no="3"><cards type="Turn">C3</cards>...
      <round no="4"><cards type="River">SJ</cards>...
    We store:
      preflop: None
      flop: "D3 HK D2"
      turn: "C3"
      river: "SJ"
    """
    board = {"preflop": None, "flop": None, "turn": None, "river": None}

    for rnd in game_el.findall("./round"):
        rno = rnd.get("no")
        street = street_from_round_no(rno)

        # board cards are cards without player attr
        for c in rnd.findall("./cards"):
            ctype = (c.get("type") or "").strip().lower()
            player_attr = c.get("player")

            if player_attr:
                continue

            text = (c.text or "").strip()
            if not text:
                continue

            if ctype == "flop":
                board["flop"] = text
            elif ctype == "turn":
                board["turn"] = text
            elif ctype == "river":
                board["river"] = text
            else:
                # sometimes board may appear without explicit type; use street
                if street in ("flop", "turn", "river"):
                    board[street] = text

    return board


def parse_actions(game_el: ET.Element) -> List[Dict]:
    """
    Parse actions from:
      <round no="X"><action no=".." player=".." sum=".." type=".."/></round>
    Returns list dict:
      {street, action_no, player, action_type, amount, is_allin}
    """
    actions: List[Dict] = []

    for rnd in game_el.findall("./round"):
        rno = rnd.get("no")
        street = street_from_round_no(rno)

        for a in rnd.findall("./action"):
            player = (a.get("player") or "").strip()
            if not player:
                continue

            action_no = a.get("no")
            try:
                ano = int(str(action_no).replace(",", "").strip())
            except Exception:
                # fallback: append at end
                ano = None

            amount = parse_decimal(a.get("sum")) or Decimal(0)
            atype = map_action_type(a.get("type"))

            is_allin = (atype == "ALLIN")

            actions.append({
                "street": street,
                "action_no": ano,  # may be None, we reindex later
                "player": player,
                "action_type": atype,
                "amount": amount,
                "is_allin": is_allin,
            })

    # ensure action_no is sequential and deterministic
    actions.sort(key=lambda x: ({"preflop": 0, "flop": 1, "turn": 2, "river": 3}.get(x["street"], 9),
                                x["action_no"] if x["action_no"] is not None else 10**9))
    for i, a in enumerate(actions, start=1):
        a["action_no"] = i

    return actions


# --- DB upserts ---------------------------------------------------------------

def upsert_player(conn, user_id: int, screen_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO players (user_id, screen_name)
            VALUES (%s, %s)
            ON CONFLICT (user_id, screen_name)
            DO UPDATE SET screen_name = EXCLUDED.screen_name
            RETURNING id
            """,
            (user_id, screen_name),
        )
        return cur.fetchone()[0]


def upsert_hand_player(conn, hand_id: int, player_id: int, seat: Optional[int],
                       starting_stack: Decimal, is_dealer: bool):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hand_players (hand_id, player_id, seat, starting_stack, is_dealer)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (hand_id, player_id)
            DO UPDATE SET
                seat = EXCLUDED.seat,
                starting_stack = EXCLUDED.starting_stack,
                is_dealer = EXCLUDED.is_dealer
            """,
            (hand_id, player_id, seat, starting_stack, is_dealer),
        )


def upsert_street(conn, hand_id: int, street: str, board: Optional[str]):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streets (hand_id, street, board)
            VALUES (%s, %s, %s)
            ON CONFLICT (hand_id, street)
            DO UPDATE SET board = EXCLUDED.board
            """,
            (hand_id, street, board),
        )


def replace_actions(conn, hand_id: int, actions: List[Dict], player_name_to_id: Dict[str, int]) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM actions WHERE hand_id = %s", (hand_id,))

        if not actions:
            return 0

        rows = []
        for a in actions:
            pid = player_name_to_id.get(a["player"])
            if not pid:
                # action references a player not in players list; skip
                continue
            rows.append((
                hand_id,
                a["street"],
                a["action_no"],
                pid,
                a["action_type"],
                a["amount"],
                a["is_allin"],
            ))

        if rows:
            cur.executemany(
                """
                INSERT INTO actions (hand_id, street, action_no, player_id, action_type, amount, is_allin)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )

        return len(rows)


def replace_hand_results(conn, hand_id: int, results: List[Dict], player_name_to_id: Dict[str, int]) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM hand_results WHERE hand_id = %s", (hand_id,))

        if not results:
            return 0

        rows = []
        for r in results:
            pid = player_name_to_id.get(r["player"])
            if not pid:
                continue
            won = r.get("won_amount") or Decimal(0)
            net = r.get("net_amount") or Decimal(0)
            rows.append((hand_id, pid, won, net))

        if rows:
            cur.executemany(
                """
                INSERT INTO hand_results (hand_id, player_id, won_amount, net_amount)
                VALUES (%s, %s, %s, %s)
                """,
                rows,
            )

        return len(rows)


# --- Main processing ----------------------------------------------------------

def process_hand(conn, user_id: int, hand_id: int, game_id: str, raw_text: str, dry_run: bool = False) -> Dict:
    root, game_el = extract_game_from_raw_xml(raw_text)

    players = parse_players(game_el)
    boards = parse_boards(game_el)
    actions = parse_actions(game_el)

    # build results from players list
    results = []
    for p in players:
        bet_total = p["bet_total"]
        win_total = p["win_total"]
        results.append({
            "player": p["screen_name"],
            "won_amount": win_total,
            "net_amount": (win_total - bet_total),
        })

    if dry_run:
        return {
            "game_id": game_id,
            "players": players,
            "boards": boards,
            "actions": actions,
            "results": results,
        }

    # upsert players and mapping
    player_name_to_id: Dict[str, int] = {}
    for p in players:
        pid = upsert_player(conn, user_id, p["screen_name"])
        player_name_to_id[p["screen_name"]] = pid

    # hand_players
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

    # streets: always ensure the 4 streets exist (like your old v1 did)
    for street in ("preflop", "flop", "turn", "river"):
        upsert_street(conn, hand_id, street, boards.get(street))

    # actions + results
    inserted_actions = replace_actions(conn, hand_id, actions, player_name_to_id)
    inserted_results = replace_hand_results(conn, hand_id, results, player_name_to_id)

    return {
        "players_count": len(players),
        "hand_players_count": len(players),
        "streets_count": 4,
        "actions_count": inserted_actions,
        "results_count": inserted_results,
    }


def main():
    parser = argparse.ArgumentParser(description="Parse ChampionPoker iPoker XML (from hands.raw_text) into relational tables")
    parser.add_argument("--user", required=True, help="Username (in your case you used '1')")
    parser.add_argument("--limit", type=int, default=500, help="How many hands to process")
    parser.add_argument("--offset", type=int, default=0, help="Offset")
    parser.add_argument("--dry-run", action="store_true", help="Parse but do not write to DB (prints samples)")
    args = parser.parse_args()

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not found in .env")
        sys.exit(1)

    parsed_hands = 0
    total_players = 0
    total_hand_players = 0
    total_streets = 0
    total_actions = 0
    total_results = 0
    errors = 0

    try:
        with psycopg.connect(database_url) as conn:
            user_id = get_user_id(conn, args.user)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, game_id, source_file, raw_text
                    FROM hands
                    WHERE user_id = %s
                    ORDER BY id ASC
                    LIMIT %s OFFSET %s
                    """,
                    (user_id, args.limit, args.offset),
                )
                hands = cur.fetchall()

            if not hands:
                print(f"No hands found for user '{args.user}' limit={args.limit} offset={args.offset}")
                return

            print(f"Processing {len(hands)} hand(s)...")
            if args.dry_run:
                print("DRY RUN MODE - No DB writes\n")

            shown = 0
            for hand_id, game_id, source_file, raw_text in hands:
                try:
                    r = process_hand(conn, user_id, hand_id, game_id, raw_text, dry_run=args.dry_run)

                    if args.dry_run:
                        if shown < 3:
                            shown += 1
                            print(f"--- Hand game_id={r['game_id']} ---")
                            print(f"Players: {len(r['players'])}")
                            for p in r["players"]:
                                d = " [DEALER]" if p["is_dealer"] else ""
                                print(f"  seat={p['seat']} {p['screen_name']} stack={p['starting_stack']}{d}")
                            print("Boards:")
                            for s in ("flop", "turn", "river"):
                                if r["boards"].get(s):
                                    print(f"  {s}: {r['boards'][s]}")
                            print("Actions (first 20):")
                            for a in r["actions"][:20]:
                                print(f"  {a['action_no']:>3} [{a['street']}] {a['player']}: {a['action_type']} ({a['amount']})")
                            print()
                    else:
                        total_players += r["players_count"]
                        total_hand_players += r["hand_players_count"]
                        total_streets += r["streets_count"]
                        total_actions += r["actions_count"]
                        total_results += r["results_count"]
                        conn.commit()

                    parsed_hands += 1

                except Exception as e:
                    errors += 1
                    print(f"ERROR processing game_id={game_id}, source={source_file}: {e}")
                    if not args.dry_run:
                        conn.rollback()

            print("-" * 40)
            if args.dry_run:
                print(f"DRY RUN - Parsed hands: {parsed_hands}")
                print(f"Errors: {errors}")
            else:
                print(f"Parsed hands: {parsed_hands}")
                print(f"Inserted/updated players: ~{total_players}")
                print(f"Inserted/updated hand_players: {total_hand_players}")
                print(f"Inserted/updated streets: {total_streets}")
                print(f"Inserted actions: {total_actions}")
                print(f"Inserted hand_results: {total_results}")
                print(f"Errors: {errors}")
            print("-" * 40)

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
