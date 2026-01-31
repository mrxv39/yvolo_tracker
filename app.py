# C:\Users\Usuario\Desktop\projectos\yvolo_tracker\app.py

import os
import time
import hashlib
import shutil
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")


def get_db_conn():
    """
    Usa DATABASE_URL del .env:
    DATABASE_URL=postgresql://poker:pokerpass@127.0.0.1:5433/pokertracker
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL no está definido en .env")
    return psycopg2.connect(dsn)


@app.route("/ui/players")
def players():
    """
    UI de jugadores:
    - Lista players del pool
    - Filtro por screen_name (NO por ID)
    - Métricas VPIP / PFR separadas 3H / HU
    """
    # Ajusta esto si tienes auth real; por ahora fijo o por querystring
    # Puedes usar /ui/players?user_id=1
    user_id = request.args.get("user_id", "1")
    try:
        user_id_int = int(user_id)
    except ValueError:
        user_id_int = 1

    search_query = (request.args.get("q") or "").strip()

    # ✅ name_filter seguro: nada de % literal en SQL
    name_filter_sql = ""
    extra_params = []
    if search_query:
        name_filter_sql = "AND p.screen_name ILIKE %s"
        extra_params.append(f"%{search_query}%")

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # IMPORTANTE:
            # Este SQL usa %s para user_id múltiples veces (3H y HU).
            # Debes pasar user_id repetido en params en el mismo orden.
            query = f"""
                WITH
                -- =========================
                -- TOTAL HANDS 3H
                -- =========================
                player_hands_3h AS (
                    SELECT
                        a.player_id,
                        COUNT(DISTINCT a.hand_id) AS total_hands_3h
                    FROM actions a
                    JOIN hands h ON a.hand_id = h.id
                    JOIN hand_sizes hs ON a.hand_id = hs.hand_id
                    WHERE h.user_id = %s
                      AND hs.player_count = 3
                      AND a.street = 'preflop'
                    GROUP BY a.player_id
                ),

                -- =========================
                -- TOTAL HANDS HU
                -- =========================
                player_hands_hu AS (
                    SELECT
                        a.player_id,
                        COUNT(DISTINCT a.hand_id) AS total_hands_hu
                    FROM actions a
                    JOIN hands h ON a.hand_id = h.id
                    JOIN hand_sizes hs ON a.hand_id = hs.hand_id
                    WHERE h.user_id = %s
                      AND hs.player_count = 2
                      AND a.street = 'preflop'
                    GROUP BY a.player_id
                ),

                -- =========================
                -- VPIP 3H
                -- =========================
                vpip_3h AS (
                    SELECT
                        a.player_id,
                        COUNT(DISTINCT a.hand_id) AS vpip_hands_3h
                    FROM actions a
                    JOIN hands h ON a.hand_id = h.id
                    JOIN hand_sizes hs ON a.hand_id = hs.hand_id
                    WHERE h.user_id = %s
                      AND hs.player_count = 3
                      AND a.street = 'preflop'
                      AND a.action_type IN ('CALL', 'BET', 'RAISE', 'ALLIN')
                    GROUP BY a.player_id
                ),

                -- =========================
                -- PFR 3H
                -- =========================
                pfr_3h AS (
                    SELECT
                        a.player_id,
                        COUNT(DISTINCT a.hand_id) AS pfr_hands_3h
                    FROM actions a
                    JOIN hands h ON a.hand_id = h.id
                    JOIN hand_sizes hs ON a.hand_id = hs.hand_id
                    WHERE h.user_id = %s
                      AND hs.player_count = 3
                      AND a.street = 'preflop'
                      AND a.action_type IN ('BET', 'RAISE', 'ALLIN')
                    GROUP BY a.player_id
                ),

                -- =========================
                -- VPIP HU
                -- =========================
                vpip_hu AS (
                    SELECT
                        a.player_id,
                        COUNT(DISTINCT a.hand_id) AS vpip_hands_hu
                    FROM actions a
                    JOIN hands h ON a.hand_id = h.id
                    JOIN hand_sizes hs ON a.hand_id = hs.hand_id
                    WHERE h.user_id = %s
                      AND hs.player_count = 2
                      AND a.street = 'preflop'
                      AND a.action_type IN ('CALL', 'BET', 'RAISE', 'ALLIN')
                    GROUP BY a.player_id
                ),

                -- =========================
                -- PFR HU
                -- =========================
                pfr_hu AS (
                    SELECT
                        a.player_id,
                        COUNT(DISTINCT a.hand_id) AS pfr_hands_hu
                    FROM actions a
                    JOIN hands h ON a.hand_id = h.id
                    JOIN hand_sizes hs ON a.hand_id = hs.hand_id
                    WHERE h.user_id = %s
                      AND hs.player_count = 2
                      AND a.street = 'preflop'
                      AND a.action_type IN ('BET', 'RAISE', 'ALLIN')
                    GROUP BY a.player_id
                )

                SELECT
                    p.screen_name,
                    COALESCE(ph3.total_hands_3h, 0) AS hands_3h,
                    COALESCE(phu.total_hands_hu, 0) AS hands_hu,
                    ROUND(100.0 * COALESCE(v3.vpip_hands_3h, 0) / NULLIF(ph3.total_hands_3h, 0), 1) AS vpip_3h_pct,
                    ROUND(100.0 * COALESCE(p3.pfr_hands_3h, 0) / NULLIF(ph3.total_hands_3h, 0), 1) AS pfr_3h_pct,
                    ROUND(100.0 * COALESCE(vu.vpip_hands_hu, 0) / NULLIF(phu.total_hands_hu, 0), 1) AS vpip_hu_pct,
                    ROUND(100.0 * COALESCE(pu.pfr_hands_hu, 0) / NULLIF(phu.total_hands_hu, 0), 1) AS pfr_hu_pct
                FROM players p
                LEFT JOIN player_hands_3h ph3 ON p.id = ph3.player_id
                LEFT JOIN player_hands_hu phu ON p.id = phu.player_id
                LEFT JOIN vpip_3h v3 ON p.id = v3.player_id
                LEFT JOIN pfr_3h p3 ON p.id = p3.player_id
                LEFT JOIN vpip_hu vu ON p.id = vu.player_id
                LEFT JOIN pfr_hu pu ON p.id = pu.player_id
                WHERE p.user_id = %s
                  AND (COALESCE(ph3.total_hands_3h, 0) >= 10 OR COALESCE(phu.total_hands_hu, 0) >= 10)
                  {name_filter_sql}
                ORDER BY (COALESCE(ph3.total_hands_3h, 0) + COALESCE(phu.total_hands_hu, 0)) DESC
                LIMIT 200
            """

            # ✅ Hay 7 placeholders de user_id en el query:
            #  - player_hands_3h: h.user_id = %s
            #  - player_hands_hu: h.user_id = %s
            #  - vpip_3h:         h.user_id = %s
            #  - pfr_3h:          h.user_id = %s
            #  - vpip_hu:         h.user_id = %s
            #  - pfr_hu:          h.user_id = %s
            #  - WHERE p.user_id: %s
            base_params = [user_id_int] * 7

            query_params = tuple(base_params + extra_params)

            cur.execute(query, query_params)
            players_data = cur.fetchall()

    finally:
        conn.close()

    return render_template(
        "players.html",
        players=players_data,
        user_id=user_id_int,
        search_query=search_query,
    )


@app.route("/ui/import")
def import_page():
    """
    Admin page for rare manual imports (PokerTracker, etc).
    """
    user_id = request.args.get("user_id", "1")
    try:
        user_id_int = int(user_id)
    except ValueError:
        user_id_int = 1
    
    project_root = Path(__file__).parent
    inbox_path = project_root / "hands_inbox" / "pokertracker"
    
    # Count files waiting in inbox
    files_count = 0
    if inbox_path.exists():
        files_count = len(list(inbox_path.glob("*.txt")))
    
    # Get import summary from query params (if redirected after import)
    import_summary = None
    if request.args.get("imported"):
        import_summary = {
            'scanned_files': request.args.get('scanned', 0),
            'imported_ok': request.args.get('imported', 0),
            'duplicates_skipped': request.args.get('duplicates', 0),
            'failed_files': request.args.get('failed', 0),
            'elapsed_seconds': request.args.get('elapsed', '0'),
        }
    
    flash_message = request.args.get('message')
    flash_type = request.args.get('type', 'info')
    
    return render_template(
        "import.html",
        user_id=user_id_int,
        inbox_path=str(inbox_path),
        files_count=files_count,
        import_summary=import_summary,
        flash_message=flash_message,
        flash_type=flash_type,
    )


@app.route("/ui/import/pokertracker", methods=["POST"])
def import_pokertracker():
    """
    Import PokerTracker iPoker hands from inbox folder.
    """
    user_id = request.form.get("user_id", "1")
    try:
        user_id_int = int(user_id)
    except ValueError:
        user_id_int = 1
    
    project_root = Path(__file__).parent
    inbox_path = project_root / "hands_inbox" / "pokertracker"
    processed_path = project_root / "hands_processed" / "pokertracker"
    failed_path = project_root / "hands_failed" / "pokertracker"
    
    # Ensure destination folders exist
    processed_path.mkdir(parents=True, exist_ok=True)
    failed_path.mkdir(parents=True, exist_ok=True)
    
    start_time = time.time()
    
    scanned_files = 0
    imported_ok = 0
    duplicates_skipped = 0
    failed_files = 0
    
    # Import the parser
    from core.parse_pokertracker_ipoker import parse_pokertracker_ipoker, store_parsed_hand
    
    conn = get_db_conn()
    
    try:
        # Get all .txt files in inbox
        txt_files = list(inbox_path.glob("*.txt")) if inbox_path.exists() else []
        
        for file_path in txt_files:
            scanned_files += 1
            
            try:
                # Read file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_text = f.read()
                
                # Parse hand
                parsed = parse_pokertracker_ipoker(raw_text)
                game_id = parsed['game_id']
                
                # Create hash for deduplication
                raw_text_hash = hashlib.sha256(raw_text.encode('utf-8')).hexdigest()
                
                # Insert or check if exists in hands table
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO hands (user_id, game_id, source_file, raw_text_hash, raw_text)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, game_id) DO NOTHING
                        RETURNING id
                        """,
                        (user_id_int, game_id, str(file_path), raw_text_hash, raw_text),
                    )
                    result = cur.fetchone()
                    
                    if result:
                        # New hand, store parsed data
                        hand_id = result[0]
                        store_parsed_hand(conn, user_id_int, hand_id, parsed)
                        conn.commit()
                        imported_ok += 1
                        
                        # Move to processed folder
                        dest = processed_path / file_path.name
                        shutil.move(str(file_path), str(dest))
                    else:
                        # Duplicate, skip
                        duplicates_skipped += 1
                        
                        # Move to processed folder anyway
                        dest = processed_path / file_path.name
                        shutil.move(str(file_path), str(dest))
            
            except Exception as e:
                # Log error and move to failed folder
                failed_files += 1
                print(f"Error processing {file_path}: {e}")
                
                try:
                    dest = failed_path / file_path.name
                    shutil.move(str(file_path), str(dest))
                except Exception as move_error:
                    print(f"Error moving failed file: {move_error}")
                
                # Rollback transaction for this file
                conn.rollback()
    
    finally:
        conn.close()
    
    elapsed = round(time.time() - start_time, 2)
    
    # Redirect back to import page with summary
    return redirect(
        url_for(
            'import_page',
            user_id=user_id_int,
            scanned=scanned_files,
            imported=imported_ok,
            duplicates=duplicates_skipped,
            failed=failed_files,
            elapsed=elapsed,
            message=f"Import completed: {imported_ok} imported, {duplicates_skipped} duplicates, {failed_files} failed",
            type='success' if failed_files == 0 else 'info',
        )
    )


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
