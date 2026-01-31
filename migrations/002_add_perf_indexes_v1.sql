-- =============================================
-- MIGRATION: 002_add_perf_indexes_v1.sql
-- Description: Add minimal performance indexes for analytics queries
-- Date: 2026-01-29
-- Author: yvolo_tracker project
-- =============================================

-- PURPOSE:
-- Optimize common analytics query patterns:
-- 1. Retrieve ordered actions for hands filtered by user
-- 2. Retrieve hand_players for hands filtered by user with time ordering
-- 3. Support efficient joins between hands and child tables

-- EXISTING INDEXES (already in schema.sql):
-- hands: idx_hands_user_played_at (user_id, played_at)
-- hands: idx_hands_user_game_id (user_id, game_id)
-- actions: idx_actions_hand_id (hand_id)
-- actions: idx_actions_player_id (player_id)
-- actions: idx_actions_hand_street (hand_id, street)
-- actions: idx_actions_hand_action_no (hand_id, action_no)
-- hand_players: idx_hand_players_hand_id (hand_id)
-- hand_players: idx_hand_players_player_id (player_id)
-- streets: idx_streets_hand_id (hand_id)

-- =============================================
-- NEW INDEXES FOR ACTIONS TABLE
-- =============================================

-- Composite index for ordered action retrieval within hands
-- Pattern: SELECT * FROM actions WHERE hand_id = X ORDER BY street, action_no
-- This covers the common case of replaying a hand's action sequence
CREATE INDEX IF NOT EXISTS idx_actions_hand_street_action 
ON actions(hand_id, street, action_no);

-- Note: This supplements existing idx_actions_hand_street and idx_actions_hand_action_no
-- by providing a single covering index for the ORDER BY pattern

-- =============================================
-- NO ADDITIONAL INDEXES NEEDED FOR OTHER TABLES
-- =============================================

-- hand_players: Existing indexes are sufficient
--   - idx_hand_players_hand_id covers JOIN from hands
--   - idx_hand_players_player_id covers player lookups
--   - User filtering happens via JOIN to hands which has idx_hands_user_played_at

-- streets: Existing indexes are sufficient  
--   - idx_streets_hand_id covers JOIN from hands
--   - UNIQUE(hand_id, street) provides additional lookup optimization

-- =============================================
-- ALTERNATIVE: CONCURRENT INDEX CREATION
-- =============================================
-- For production with active traffic, use CONCURRENTLY to avoid blocking writes.
-- IMPORTANT: CONCURRENTLY cannot run inside a transaction, so execute individually.
--
-- To use CONCURRENTLY, run this command directly (not in a transaction):
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_actions_hand_street_action 
-- ON actions(hand_id, street, action_no);

-- =============================================
-- VERIFICATION QUERY
-- =============================================
-- After applying this migration, verify indexes with:
--
-- SELECT tablename, indexname, indexdef 
-- FROM pg_indexes 
-- WHERE schemaname = 'public' 
--   AND tablename IN ('actions', 'hand_players', 'streets', 'hands')
-- ORDER BY tablename, indexname;

