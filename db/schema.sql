-- =============================================
-- CORE TABLES
-- =============================================

-- Users table: stores user accounts
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hands table: stores raw hand history data
CREATE TABLE IF NOT EXISTS hands (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    game_id TEXT NOT NULL,
    played_at TIMESTAMPTZ,
    source_file TEXT,
    raw_text_hash TEXT,
    raw_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, game_id)
);

-- Indexes for hands table
CREATE INDEX IF NOT EXISTS idx_hands_user_played_at ON hands(user_id, played_at);
CREATE INDEX IF NOT EXISTS idx_hands_user_game_id ON hands(user_id, game_id);

-- =============================================
-- PARSED HAND DATA TABLES
-- =============================================

-- Players table: stores unique player screen names per user
CREATE TABLE IF NOT EXISTS players (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    screen_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, screen_name)
);

-- Index for players
CREATE INDEX IF NOT EXISTS idx_players_user_id ON players(user_id);

-- Hand Players table: relationship between hands and players (who played in each hand)
CREATE TABLE IF NOT EXISTS hand_players (
    id BIGSERIAL PRIMARY KEY,
    hand_id BIGINT NOT NULL REFERENCES hands(id) ON DELETE CASCADE,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    seat INT,
    starting_stack NUMERIC,
    is_dealer BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(hand_id, player_id)
);

-- Indexes for hand_players
CREATE INDEX IF NOT EXISTS idx_hand_players_hand_id ON hand_players(hand_id);
CREATE INDEX IF NOT EXISTS idx_hand_players_player_id ON hand_players(player_id);

-- Streets table: stores board cards for each street of a hand
CREATE TABLE IF NOT EXISTS streets (
    id BIGSERIAL PRIMARY KEY,
    hand_id BIGINT NOT NULL REFERENCES hands(id) ON DELETE CASCADE,
    street TEXT NOT NULL CHECK (street IN ('preflop', 'flop', 'turn', 'river')),
    board TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(hand_id, street)
);

-- Indexes for streets
CREATE INDEX IF NOT EXISTS idx_streets_hand_id ON streets(hand_id);

-- Actions table: stores every action taken by players in each hand
CREATE TABLE IF NOT EXISTS actions (
    id BIGSERIAL PRIMARY KEY,
    hand_id BIGINT NOT NULL REFERENCES hands(id) ON DELETE CASCADE,
    street TEXT,
    action_no INT,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    action_type TEXT,
    amount NUMERIC,
    is_allin BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for actions
CREATE INDEX IF NOT EXISTS idx_actions_hand_id ON actions(hand_id);
CREATE INDEX IF NOT EXISTS idx_actions_player_id ON actions(player_id);
CREATE INDEX IF NOT EXISTS idx_actions_hand_street ON actions(hand_id, street);
CREATE INDEX IF NOT EXISTS idx_actions_hand_action_no ON actions(hand_id, action_no);

-- Hand Results table: stores final win/loss amounts for each player in each hand
CREATE TABLE IF NOT EXISTS hand_results (
    id BIGSERIAL PRIMARY KEY,
    hand_id BIGINT NOT NULL REFERENCES hands(id) ON DELETE CASCADE,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    won_amount NUMERIC DEFAULT 0,
    net_amount NUMERIC DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(hand_id, player_id)
);

-- Indexes for hand_results
CREATE INDEX IF NOT EXISTS idx_hand_results_hand_id ON hand_results(hand_id);
CREATE INDEX IF NOT EXISTS idx_hand_results_player_id ON hand_results(player_id);
