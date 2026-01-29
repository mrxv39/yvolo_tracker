#!/usr/bin/env python3
# core/import_ipoker_folder.py
"""
Import hand histories into PostgreSQL.

Supports:
1) Classic iPoker TXT HH:
   - Split by lines starting with: GAME #<id>

2) ChampionPoker / iPoker XML "session" HH:
   - Root <session sessioncode="...">
   - Each <game gamecode="..."> is a hand
   - Rounds: no="1" preflop, no="2" flop, no="3" turn, no="4" river (and sometimes no="0" blinds/antes)
"""

import os
import re
import sys
import time
import hashlib
import argparse
from pathlib import Path
from typing import List, Tuple, Set, Optional
from dotenv import load_dotenv
import psycopg

import xml.etree.ElementTree as ET


def get_or_create_user(conn, username: str) -> int:
    """Get or create a user by username. Returns user_id."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id",
            (username, "x"),
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        return user_id


def find_files(path: str, pattern: str, recursive: bool) -> List[Path]:
    """Find all files matching the pattern in the given path."""
    base_path = Path(path)

    if not base_path.exists():
        raise ValueError(f"Path does not exist: {path}")
    if not base_path.is_dir():
        raise ValueError(f"Path is not a directory: {path}")

    if recursive:
        return list(base_path.rglob(pattern))
    return list(base_path.glob(pattern))


def _read_text_file(file_path: Path) -> str:
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception as e:
        raise IOError(f"Error reading file {file_path}: {e}")


def parse_hands_from_txt(content: str) -> List[Tuple[str, str]]:
    """
    Parse hands from classic iPoker TXT hand history file.
    Returns list of (game_id, raw_text).
    """
    hands_raw = re.split(r"(?=^GAME\s+#\d+)", content, flags=re.MULTILINE)

    hands: List[Tuple[str, str]] = []
    for hand_text in hands_raw:
        hand_text = hand_text.strip()
        if not hand_text:
            continue

        match = re.match(r"^GAME\s+#(\d+)", hand_text, re.MULTILINE)
        if not match:
            continue

        game_id = match.group(1)
        hands.append((game_id, hand_text))

    return hands


def _get_text(node: Optional[ET.Element]) -> str:
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def parse_hands_from_champion_xml(xml_text: str) -> List[Tuple[str, str]]:
    """
    Parse hands from ChampionPoker/iPoker XML 'session' format.
    Each <game gamecode="..."> is a hand.
    Returns list of (game_id, raw_xml_hand_text).
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        # Not valid XML (or not this format)
        return []

    # Expected: <session sessioncode="..."> ... <game gamecode="..."> ... </game> ... </session>
    if root.tag.lower() != "session":
        return []

    sessioncode = root.attrib.get("sessioncode", "").strip()

    # Session-level general metadata (optional but useful)
    session_general = root.find("general")
    nickname = _get_text(session_general.find("nickname")) if session_general is not None else ""
    tablename = _get_text(session_general.find("tablename")) if session_general is not None else ""
    tournamentcode = _get_text(session_general.find("tournamentcode")) if session_general is not None else ""
    tournamentname = _get_text(session_general.find("tournamentname")) if session_general is not None else ""
    startdate = _get_text(session_general.find("startdate")) if session_general is not None else ""

    hands: List[Tuple[str, str]] = []

    for game in root.findall("game"):
        gamecode = (game.attrib.get("gamecode") or "").strip()
        if not gamecode:
            continue

        # Wrap to preserve session metadata per-hand (so parser can be simpler later)
        # We keep it as XML text stored in hands.raw_text
        wrapper_attribs = [
            'source="champion_xml"',
            f'sessioncode="{sessioncode}"' if sessioncode else "",
            f'nickname="{nickname}"' if nickname else "",
            f'tablename="{tablename}"' if tablename else "",
            f'tournamentcode="{tournamentcode}"' if tournamentcode else "",
            f'tournamentname="{tournamentname}"' if tournamentname else "",
            f'startdate="{startdate}"' if startdate else "",
            f'gamecode="{gamecode}"',
        ]
        wrapper_attribs = [a for a in wrapper_attribs if a]

        game_xml = ET.tostring(game, encoding="unicode")
        raw_hand_xml = f"<hand {' '.join(wrapper_attribs)}>{game_xml}</hand>"

        hands.append((gamecode, raw_hand_xml))

    return hands


def parse_hands_from_file(file_path: Path) -> List[Tuple[str, str]]:
    """
    Parse hands from a file (TXT classic or ChampionPoker XML session).
    Returns list of (game_id, raw_text).
    """
    content = _read_text_file(file_path)

    # Heuristic: if it looks like XML or is .xml, try XML parser first
    if file_path.suffix.lower() == ".xml" or content.lstrip().startswith("<"):
        hands_xml = parse_hands_from_champion_xml(content)
        if hands_xml:
            return hands_xml

    # Fallback: classic TXT
    return parse_hands_from_txt(content)


def get_existing_game_ids(conn, user_id: int, game_ids: List[str]) -> Set[str]:
    """Query which game_ids already exist for the user."""
    if not game_ids:
        return set()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT game_id FROM hands WHERE user_id = %s AND game_id = ANY(%s)",
            (user_id, game_ids),
        )
        return {row[0] for row in cur.fetchall()}


def insert_hands_batch(conn, user_id: int, batch: List[dict]) -> Tuple[int, int]:
    """
    Insert a batch of hands using UPSERT.
    Returns (inserted_count, duplicate_count).
    """
    if not batch:
        return 0, 0

    game_ids = [h["game_id"] for h in batch]
    existing_game_ids = get_existing_game_ids(conn, user_id, game_ids)

    duplicates = sum(1 for h in batch if h["game_id"] in existing_game_ids)
    inserted = len(batch) - duplicates

    data = [
        (
            user_id,
            h["game_id"],
            h["source_file"],
            h["raw_text_hash"],
            h["raw_text"],
        )
        for h in batch
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO hands (user_id, game_id, source_file, raw_text_hash, raw_text)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_id)
            DO UPDATE SET
                raw_text_hash = EXCLUDED.raw_text_hash,
                raw_text = EXCLUDED.raw_text,
                source_file = EXCLUDED.source_file
            """,
            data,
        )

    conn.commit()
    return inserted, duplicates


def import_folder(
    database_url: str,
    username: str,
    folder_path: str,
    recursive: bool,
    glob_pattern: str,
    batch_size: int,
):
    """Main import function."""
    start_time = time.time()

    files_processed = 0
    hands_total = 0
    inserted_total = 0
    duplicates_total = 0
    errors_total = 0

    with psycopg.connect(database_url) as conn:
        user_id = get_or_create_user(conn, username)
        files = find_files(folder_path, glob_pattern, recursive)

        if not files:
            print(f"No files found matching pattern '{glob_pattern}' in {folder_path}")
            return

        print(f"Found {len(files)} file(s) to process...\n")

        batch: List[dict] = []

        for file_path in files:
            try:
                print(f"Processing: {file_path}")

                hands = parse_hands_from_file(file_path)

                if not hands:
                    print("  No hands found in file")
                    files_processed += 1
                    continue

                print(f"  Found {len(hands)} hand(s)")
                hands_total += len(hands)

                for game_id, raw_text in hands:
                    try:
                        raw_text_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

                        batch.append(
                            {
                                "game_id": str(game_id),
                                "source_file": str(file_path),
                                "raw_text_hash": raw_text_hash,
                                "raw_text": raw_text,
                            }
                        )

                        if len(batch) >= batch_size:
                            inserted, duplicates = insert_hands_batch(conn, user_id, batch)
                            inserted_total += inserted
                            duplicates_total += duplicates
                            print(f"  Batch inserted: {inserted} new, {duplicates} duplicates")
                            batch = []

                    except Exception as e:
                        print(f"  Error processing hand {game_id}: {e}")
                        errors_total += 1

                files_processed += 1

            except Exception as e:
                print(f"  Error processing file: {e}")
                errors_total += 1
                files_processed += 1

        if batch:
            try:
                inserted, duplicates = insert_hands_batch(conn, user_id, batch)
                inserted_total += inserted
                duplicates_total += duplicates
                print(f"  Final batch inserted: {inserted} new, {duplicates} duplicates")
            except Exception as e:
                print(f"  Error inserting final batch: {e}")
                errors_total += len(batch)

    elapsed = time.time() - start_time

    print("\n" + "=" * 40)
    print("Import Finished")
    print("-" * 40)
    print(f"User: {username}")
    print(f"Files processed: {files_processed}")
    print(f"Hands total found: {hands_total}")
    print(f"Inserted: {inserted_total}")
    print(f"Duplicates: {duplicates_total}")
    print(f"Errors: {errors_total}")
    print(f"Time elapsed: {elapsed:.2f} seconds")
    print("=" * 40)


def main():
    parser = argparse.ArgumentParser(description="Import hand history files into PostgreSQL")
    parser.add_argument("--user", required=True, help="Username in the users table")
    parser.add_argument("--path", required=True, help="Folder path containing hand history files")
    parser.add_argument(
        "--recursive",
        action="store_true",
        default=True,
        help="Search for files recursively (default: True)",
    )
    parser.add_argument("--glob", default="*.txt", help="File pattern to match (default: *.txt)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size (default: 1000)")

    args = parser.parse_args()

    # Ensure we take .env values even if there are stale env vars
    load_dotenv(override=True)

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        print("ERROR: DATABASE_URL not found in .env file")
        sys.exit(1)

    try:
        import_folder(
            database_url=database_url,
            username=args.user,
            folder_path=args.path,
            recursive=args.recursive,
            glob_pattern=args.glob,
            batch_size=args.batch_size,
        )
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
