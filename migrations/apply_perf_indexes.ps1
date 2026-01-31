# =============================================
# PowerShell Script: Apply and Verify Performance Indexes
# Project: yvolo_tracker
# Migration: 002_add_perf_indexes_v1.sql
# =============================================

$ErrorActionPreference = "Stop"
$CONTAINER_NAME = "pokertracker_db"

# =============================================
# STEP 1: Load DATABASE_URL from .env
# =============================================
Write-Host "=== STEP 1: Loading DATABASE_URL ===" -ForegroundColor Cyan
if (-not (Test-Path ".env")) {
    Write-Host "ERROR: .env file not found" -ForegroundColor Red
    exit 1
}

$envFile = Get-Content .env -Raw
if ($envFile -match 'DATABASE_URL=(.+?)(\r?\n|$)') {
    $DATABASE_URL = $matches[1].Trim().Trim('"').Trim("'")
    Write-Host "DATABASE_URL loaded from .env" -ForegroundColor Green
} else {
    Write-Host "ERROR: DATABASE_URL not found in .env" -ForegroundColor Red
    exit 1
}

# =============================================
# STEP 2: Create docker-safe DATABASE_URL
# =============================================
Write-Host "`n=== STEP 2: Creating docker-safe connection string ===" -ForegroundColor Cyan

# Replace localhost/127.0.0.1 with host.docker.internal for docker exec
$DOCKER_DATABASE_URL = $DATABASE_URL -replace 'localhost', 'host.docker.internal' -replace '127\.0\.0\.1', 'host.docker.internal'
Write-Host "Docker DATABASE_URL: $DOCKER_DATABASE_URL" -ForegroundColor Green

# =============================================
# STEP 3: Verify container is running
# =============================================
Write-Host "`n=== STEP 3: Verifying PostgreSQL container ===" -ForegroundColor Cyan
$containerStatus = docker ps --filter "name=$CONTAINER_NAME" --format "{{.Status}}"
if (-not $containerStatus) {
    Write-Host "ERROR: Container $CONTAINER_NAME is not running" -ForegroundColor Red
    Write-Host "Start it with: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}
Write-Host "Container $CONTAINER_NAME is running: $containerStatus" -ForegroundColor Green

# =============================================
# STEP 4: Capture BEFORE performance
# =============================================
Write-Host "`n=== STEP 4: Capturing BEFORE performance ===" -ForegroundColor Cyan

$query1 = @"
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT a.id, a.hand_id, a.street, a.action_no, a.action_type, a.amount, h.game_id
FROM actions a
JOIN hands h ON a.hand_id = h.id
WHERE h.user_id = 1
ORDER BY a.hand_id, a.street, a.action_no
LIMIT 100;
"@

$query2 = @"
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT hp.id, hp.hand_id, hp.player_id, hp.seat, hp.starting_stack, hp.is_dealer, h.game_id, h.played_at
FROM hand_players hp
JOIN hands h ON hp.hand_id = h.id
WHERE h.user_id = 1
ORDER BY h.played_at DESC
LIMIT 100;
"@

Write-Host "Running Query 1 (hands->actions)..." -ForegroundColor Yellow
$query1 | docker exec -i $CONTAINER_NAME psql "$DOCKER_DATABASE_URL" | Out-File -FilePath "before_query1.txt" -Encoding UTF8
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Query 1 failed" -ForegroundColor Red; exit 1 }
Write-Host "Saved to before_query1.txt" -ForegroundColor Green

Write-Host "Running Query 2 (hands->hand_players)..." -ForegroundColor Yellow
$query2 | docker exec -i $CONTAINER_NAME psql "$DOCKER_DATABASE_URL" | Out-File -FilePath "before_query2.txt" -Encoding UTF8
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Query 2 failed" -ForegroundColor Red; exit 1 }
Write-Host "Saved to before_query2.txt" -ForegroundColor Green

# =============================================
# STEP 5: Apply Migration
# =============================================
Write-Host "`n=== STEP 5: Applying Migration ===" -ForegroundColor Cyan

$migrationFile = "migrations\002_add_perf_indexes_v1.sql"
if (-not (Test-Path $migrationFile)) {
    Write-Host "ERROR: Migration file not found: $migrationFile" -ForegroundColor Red
    exit 1
}

Write-Host "Applying $migrationFile..." -ForegroundColor Yellow
Get-Content $migrationFile | docker exec -i $CONTAINER_NAME psql "$DOCKER_DATABASE_URL"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Migration failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit 1
}
Write-Host "Migration applied successfully!" -ForegroundColor Green

# =============================================
# STEP 6: Verify Indexes
# =============================================
Write-Host "`n=== STEP 6: Verifying Indexes ===" -ForegroundColor Cyan

$verifyQuery = @"
SELECT tablename, indexname, indexdef 
FROM pg_indexes 
WHERE schemaname = 'public' 
  AND tablename IN ('actions', 'hand_players', 'streets', 'hands')
ORDER BY tablename, indexname;
"@

Write-Host "Saving index verification to verify_indexes.txt..." -ForegroundColor Yellow
$verifyQuery | docker exec -i $CONTAINER_NAME psql "$DOCKER_DATABASE_URL" | Out-File -FilePath "verify_indexes.txt" -Encoding UTF8
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Verify query failed" -ForegroundColor Red; exit 1 }
Write-Host "Saved to verify_indexes.txt" -ForegroundColor Green

# =============================================
# STEP 7: Capture AFTER performance
# =============================================
Write-Host "`n=== STEP 7: Capturing AFTER performance ===" -ForegroundColor Cyan

Write-Host "Running Query 1 (hands->actions) AFTER indexes..." -ForegroundColor Yellow
$query1 | docker exec -i $CONTAINER_NAME psql "$DOCKER_DATABASE_URL" | Out-File -FilePath "after_query1.txt" -Encoding UTF8
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Query 1 AFTER failed" -ForegroundColor Red; exit 1 }
Write-Host "Saved to after_query1.txt" -ForegroundColor Green

Write-Host "Running Query 2 (hands->hand_players) AFTER indexes..." -ForegroundColor Yellow
$query2 | docker exec -i $CONTAINER_NAME psql "$DOCKER_DATABASE_URL" | Out-File -FilePath "after_query2.txt" -Encoding UTF8
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Query 2 AFTER failed" -ForegroundColor Red; exit 1 }
Write-Host "Saved to after_query2.txt" -ForegroundColor Green

# =============================================
# STEP 8: Verification
# =============================================
Write-Host "`n=== STEP 8: Verification ===" -ForegroundColor Cyan

$requiredFiles = @("before_query1.txt", "before_query2.txt", "verify_indexes.txt", "after_query1.txt", "after_query2.txt")
$allPass = $true

foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        $size = (Get-Item $file).Length
        if ($size -gt 0) {
            Write-Host "[PASS] $file exists ($size bytes)" -ForegroundColor Green
        } else {
            Write-Host "[FAIL] $file is empty" -ForegroundColor Red
            $allPass = $false
        }
    } else {
        Write-Host "[FAIL] $file not found" -ForegroundColor Red
        $allPass = $false
    }
}

# Check for new index
$verifyContent = Get-Content "verify_indexes.txt" -Raw
if ($verifyContent -match 'idx_actions_hand_street_action') {
    Write-Host "[PASS] New index 'idx_actions_hand_street_action' found in verify_indexes.txt" -ForegroundColor Green
} else {
    Write-Host "[FAIL] New index 'idx_actions_hand_street_action' NOT found in verify_indexes.txt" -ForegroundColor Red
    $allPass = $false
}

# =============================================
# SUMMARY
# =============================================
Write-Host "`n=== SUMMARY ===" -ForegroundColor Cyan
if ($allPass) {
    Write-Host "*** ALL CHECKS PASSED ***" -ForegroundColor Green
    Write-Host "`nFiles created successfully:" -ForegroundColor White
    foreach ($file in $requiredFiles) {
        Write-Host "  - $file" -ForegroundColor Gray
    }
    Write-Host "`nMigration completed successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "*** SOME CHECKS FAILED ***" -ForegroundColor Red
    exit 1
}

