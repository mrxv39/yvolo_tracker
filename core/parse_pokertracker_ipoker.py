#!/usr/bin/env python3
"""
Parse PokerTracker iPoker TXT hand histories into database.

Expected format:
  GAME #<id> Version:X.X.X ...
  Table Size N
  Table <name>, <tourney_id>, ...
  Seat X: <player> (€X.XX in chips) [DEALER]
  ...
  *** HOLE CARDS ***
  Dealt to <player> [XX XX]
  <player>: <Action> ...
  *** FLOP *** [XX XX XX]
  ...
  *** SUMMARY ***
  Total pot €X.XX Rake €X.XX
  <player>: wins €X.XX
"""

import re
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    """Parse a decimal value, handling currency symbols and thousands separators."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    
    # Remove currency symbols and thousands separators
    s = s.replace("€", "").replace("$", "").replace(",", "").strip()
    
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def parse_pokertracker_ipoker(raw_text: str) -> Dict:
    """
    Parse a single PokerTracker iPoker hand history.
    
    Returns dict with:
        - game_id: str
        - table_size: int
        - players: List[Dict]  # {screen_name, seat, starting_stack, is_dealer}
        - streets: Dict[str, Optional[str]]  # {preflop, flop, turn, river}
        - actions: List[Dict]  # {street, action_no, player, action_type, amount, is_allin}
        - results: List[Dict]  # {player, won_amount, net_amount}
    """
    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
    
    if not lines:
        raise ValueError("Empty hand text")
    
    # Extract game ID from first line
    first_line = lines[0]
    game_match = re.match(r'^GAME\s+#(\d+)', first_line)
    if not game_match:
        raise ValueError(f"Invalid hand format: missing GAME # line")
    
    game_id = game_match.group(1)
    
    # Parse table size
    table_size = 2  # default
    for line in lines[:10]:
        if line.startswith("Table Size"):
            match = re.search(r'Table Size\s+(\d+)', line)
            if match:
                table_size = int(match.group(1))
                break
    
    # Parse players
    players = []
    for line in lines:
        # Seat X: <name> (€X.XX in chips) [DEALER]
        seat_match = re.match(r'^Seat\s+(\d+):\s+(\S+)\s+\(([€$\d.,]+)\s+in\s+chips\)\s*(DEALER)?', line, re.IGNORECASE)
        if seat_match:
            seat = int(seat_match.group(1))
            screen_name = seat_match.group(2)
            stack_str = seat_match.group(3)
            is_dealer = seat_match.group(4) is not None
            
            starting_stack = parse_decimal(stack_str) or Decimal(0)
            
            players.append({
                'screen_name': screen_name,
                'seat': seat,
                'starting_stack': starting_stack,
                'is_dealer': is_dealer,
            })
    
    if not players:
        raise ValueError("No players found in hand")
    
    # Parse streets and actions
    streets = {'preflop': None, 'flop': None, 'turn': None, 'river': None}
    actions = []
    current_street = 'preflop'
    action_no = 0
    
    # Track pot contributions for net calculation
    player_invested = {p['screen_name']: Decimal(0) for p in players}
    
    for line in lines:
        # Street markers
        if '*** HOLE CARDS ***' in line:
            current_street = 'preflop'
            continue
        elif '*** FLOP ***' in line:
            current_street = 'flop'
            board_match = re.search(r'\[([^\]]+)\]', line)
            if board_match:
                streets['flop'] = board_match.group(1)
            continue
        elif '*** TURN ***' in line:
            current_street = 'turn'
            board_match = re.search(r'\[([^\]]+)\]', line)
            if board_match:
                streets['turn'] = board_match.group(1)
            continue
        elif '*** RIVER ***' in line:
            current_street = 'river'
            board_match = re.search(r'\[([^\]]+)\]', line)
            if board_match:
                streets['river'] = board_match.group(1)
            continue
        elif '*** SUMMARY ***' in line:
            break
        
        # Parse actions: <player>: <action> [amount]
        action_match = re.match(r'^([^:]+):\s+(.*)', line)
        if action_match:
            player = action_match.group(1).strip()
            action_text = action_match.group(2).strip()
            
            # Skip non-action lines
            if action_text.startswith('Seat') or action_text.startswith('Total pot'):
                continue
            
            action_no += 1
            
            # Determine action type and amount
            action_type = None
            amount = Decimal(0)
            is_allin = False
            
            # Check for all-in
            if '(NF)' in action_text or 'all-in' in action_text.lower() or 'allin' in action_text.lower():
                is_allin = True
            
            # Parse action type
            if action_text.startswith('Post SB'):
                action_type = 'POST_SB'
                amount_match = re.search(r'([€$\d.,]+)', action_text)
                if amount_match:
                    amount = parse_decimal(amount_match.group(1)) or Decimal(0)
                    player_invested[player] += amount
            elif action_text.startswith('Post BB'):
                action_type = 'POST_BB'
                amount_match = re.search(r'([€$\d.,]+)', action_text)
                if amount_match:
                    amount = parse_decimal(amount_match.group(1)) or Decimal(0)
                    player_invested[player] += amount
            elif action_text.startswith('Post Ante'):
                action_type = 'POST_ANTE'
                amount_match = re.search(r'([€$\d.,]+)', action_text)
                if amount_match:
                    amount = parse_decimal(amount_match.group(1)) or Decimal(0)
                    player_invested[player] += amount
            elif action_text.startswith('Fold'):
                action_type = 'FOLD'
            elif action_text.startswith('Check'):
                action_type = 'CHECK'
            elif action_text.startswith('Call'):
                action_type = 'CALL'
                amount_match = re.search(r'([€$\d.,]+)', action_text)
                if amount_match:
                    amount = parse_decimal(amount_match.group(1)) or Decimal(0)
                    player_invested[player] += amount
            elif action_text.startswith('Bet'):
                action_type = 'BET'
                amount_match = re.search(r'([€$\d.,]+)', action_text)
                if amount_match:
                    amount = parse_decimal(amount_match.group(1)) or Decimal(0)
                    player_invested[player] += amount
            elif action_text.startswith('Raise'):
                action_type = 'RAISE'
                amount_match = re.search(r'([€$\d.,]+)', action_text)
                if amount_match:
                    amount = parse_decimal(amount_match.group(1)) or Decimal(0)
                    player_invested[player] += amount
            elif action_text.startswith('Dealt to'):
                # Skip hole cards line
                continue
            else:
                # Unknown action, skip
                continue
            
            if action_type:
                actions.append({
                    'street': current_street,
                    'action_no': action_no,
                    'player': player,
                    'action_type': action_type,
                    'amount': amount,
                    'is_allin': is_allin,
                })
    
    # Parse results from summary
    results = []
    in_summary = False
    total_pot = Decimal(0)
    
    for line in lines:
        if '*** SUMMARY ***' in line:
            in_summary = True
            continue
        
        if in_summary:
            # Total pot €X.XX Rake €X.XX
            if line.startswith('Total pot'):
                pot_match = re.search(r'Total pot\s+([€$\d.,]+)', line)
                if pot_match:
                    total_pot = parse_decimal(pot_match.group(1)) or Decimal(0)
            
            # <player>: wins €X.XX
            win_match = re.match(r'^([^:]+):\s+wins\s+([€$\d.,]+)', line)
            if win_match:
                player = win_match.group(1).strip()
                won_amount = parse_decimal(win_match.group(2)) or Decimal(0)
                
                invested = player_invested.get(player, Decimal(0))
                net_amount = won_amount - invested
                
                results.append({
                    'player': player,
                    'won_amount': won_amount,
                    'net_amount': net_amount,
                })
    
    # Add players who didn't win (lost their investment)
    winners = {r['player'] for r in results}
    for player, invested in player_invested.items():
        if player not in winners and invested > 0:
            results.append({
                'player': player,
                'won_amount': Decimal(0),
                'net_amount': -invested,
            })
    
    return {
        'game_id': game_id,
        'table_size': table_size,
        'players': players,
        'streets': streets,
        'actions': actions,
        'results': results,
    }


def upsert_player(conn, user_id: int, screen_name: str) -> int:
    """Get or create player, return player_id."""
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


def store_parsed_hand(conn, user_id: int, hand_id: int, parsed: Dict) -> None:
    """Store parsed hand data into database tables."""
    
    # Map player names to IDs
    player_name_to_id = {}
    for p in parsed['players']:
        pid = upsert_player(conn, user_id, p['screen_name'])
        player_name_to_id[p['screen_name']] = pid
    
    # Insert hand_players
    for p in parsed['players']:
        pid = player_name_to_id[p['screen_name']]
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
                (hand_id, pid, p['seat'], p['starting_stack'], p['is_dealer']),
            )
    
    # Insert streets
    for street, board in parsed['streets'].items():
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
    
    # Delete old actions and insert new ones
    with conn.cursor() as cur:
        cur.execute("DELETE FROM actions WHERE hand_id = %s", (hand_id,))
        
        for a in parsed['actions']:
            pid = player_name_to_id.get(a['player'])
            if not pid:
                continue
            
            cur.execute(
                """
                INSERT INTO actions (hand_id, street, action_no, player_id, action_type, amount, is_allin)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (hand_id, a['street'], a['action_no'], pid, a['action_type'], a['amount'], a['is_allin']),
            )
    
    # Insert hand_results
    with conn.cursor() as cur:
        cur.execute("DELETE FROM hand_results WHERE hand_id = %s", (hand_id,))
        
        for r in parsed['results']:
            pid = player_name_to_id.get(r['player'])
            if not pid:
                continue
            
            cur.execute(
                """
                INSERT INTO hand_results (hand_id, player_id, won_amount, net_amount)
                VALUES (%s, %s, %s, %s)
                """,
                (hand_id, pid, r['won_amount'], r['net_amount']),
            )
    
    # Insert hand_sizes if table exists
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hand_sizes (hand_id, player_count)
                VALUES (%s, %s)
                ON CONFLICT (hand_id) DO UPDATE SET player_count = EXCLUDED.player_count
                """,
                (hand_id, parsed['table_size']),
            )
    except Exception:
        # Table might not exist in older schemas, ignore
        pass
