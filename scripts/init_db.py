#!/usr/bin/env python3
"""Initialize database schema for pokertracker."""

import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg

# Load environment variables
load_dotenv()

def init_db():
    """Load and execute schema.sql to initialize database."""
    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment")
    
    # Read schema file
    schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    
    # Connect and execute schema
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
    
    print("✅ DB schema applied")

if __name__ == "__main__":
    init_db()
