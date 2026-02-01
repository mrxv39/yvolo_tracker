# Tests

## Running Tests

### Smoke Test for Import Pipeline

Test the complete per-file import → parse → verify pipeline:

```bash
# Run directly with Python
python tests/test_import_pipeline.py

# Run with pytest
pytest tests/test_import_pipeline.py -v
```

### Test Details

**test_import_pipeline.py**
- Tests RAW XML import into `hands` table
- Tests parsing into relational tables (`hand_players`, `actions`, `streets`, etc.)
- Tests verification of data integrity
- Tests file classification (processed vs failed)

**Key Features:**
- **Deterministic**: Uses UUID-based unique gamecode per test run
- **Isolated**: Creates temp directory, never modifies original fixture
- **Clean**: Pre-test cleanup removes any existing test data
- **Safe**: Rolls back all changes at end (no database pollution)

**Fixture:**
- Location: `tests/fixtures/minimal_ipoker.xml`
- Format: Valid iPoker XML with 1 game, 2 players, 5 actions
- Automatically modified with unique gamecode per test run

## Requirements

The test uses the same database connection as the production app (from `.env` file).

No additional dependencies required beyond what's in `requirements.txt`.
