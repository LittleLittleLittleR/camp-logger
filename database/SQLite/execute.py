import sqlite3
import csv
import os
import shutil
import tempfile
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).resolve().parents[2] / 'database.db'
SYNC_META_TABLE = '__sync_meta'


def resolve_db_path() -> Path:
  """Return the SQLite database path, automatically using temp directory if the packaged DB is read-only."""
  # If DB exists and is writable, use it directly
  if DB_PATH.exists() and os.access(DB_PATH, os.W_OK):
    return DB_PATH

  # Fall back to temp directory (for Vercel/serverless read-only filesystems)
  temp_db_path = Path(tempfile.gettempdir()) / DB_PATH.name

  # Copy packaged DB to temp on first access if it doesn't exist there yet
  if not temp_db_path.exists() and DB_PATH.exists():
    try:
      temp_db_path.parent.mkdir(parents=True, exist_ok=True)
      shutil.copy2(DB_PATH, temp_db_path)
    except Exception:
      pass

  return temp_db_path if temp_db_path.exists() else DB_PATH


def _connect(db_path: Path | None = None):
  return sqlite3.connect(db_path or resolve_db_path())


def list_tables() -> list[str]:
  """Return all non-system table names in the SQLite database."""
  with _connect() as conn:
    cursor = conn.cursor()
    cursor.execute(
      "SELECT name FROM sqlite_master "
      "WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT GLOB '__*';"
    )
    return [row[0] for row in cursor.fetchall()]


def get_db_last_modified_timestamp() -> str | None:
  """Return the database file last-modified timestamp in UTC ISO format."""
  db_path = resolve_db_path()
  if not db_path.exists():
    return None

  dt = datetime.fromtimestamp(db_path.stat().st_mtime, tz=timezone.utc)
  return dt.isoformat().replace('+00:00', 'Z')


def _ensure_sync_meta_table(cursor):
  quoted_table_name = _quote_identifier(SYNC_META_TABLE)
  cursor.execute(
    f"CREATE TABLE IF NOT EXISTS {quoted_table_name} "
    "(meta_key TEXT PRIMARY KEY, meta_value TEXT)"
  )


def get_sync_meta(meta_key: str) -> str | None:
  """Get a sync metadata value from SQLite."""
  quoted_table_name = _quote_identifier(SYNC_META_TABLE)

  with _connect() as conn:
    cursor = conn.cursor()
    _ensure_sync_meta_table(cursor)
    cursor.execute(
      f"SELECT meta_value FROM {quoted_table_name} WHERE meta_key = ?",
      (meta_key,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def set_sync_meta(meta_key: str, meta_value: str):
  """Set a sync metadata value in SQLite."""
  quoted_table_name = _quote_identifier(SYNC_META_TABLE)

  with _connect() as conn:
    cursor = conn.cursor()
    _ensure_sync_meta_table(cursor)
    cursor.execute(
      f"INSERT OR REPLACE INTO {quoted_table_name} (meta_key, meta_value) VALUES (?, ?)",
      (meta_key, meta_value),
    )
    conn.commit()


def read_table(table_name: str):
  """Return a table as (columns, rows)."""
  quoted_table_name = _quote_identifier(table_name)

  with _connect() as conn:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({quoted_table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    cursor.execute(f"SELECT * FROM {quoted_table_name}")
    rows = cursor.fetchall()
    return columns, rows


def replace_table(table_name: str, columns: list[str], rows: list[list]):
  """Drop and recreate a table, then insert all provided rows."""
  quoted_table_name = _quote_identifier(table_name)
  quoted_columns = [_quote_identifier(column) for column in columns]

  with _connect() as conn:
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {quoted_table_name}")

    column_definitions = ", ".join(f"{column} TEXT" for column in quoted_columns)
    cursor.execute(f"CREATE TABLE {quoted_table_name} ({column_definitions})")

    if rows:
      placeholders = ", ".join(["?"] * len(columns))
      insert_columns = ", ".join(quoted_columns)
      cursor.executemany(
        f"INSERT INTO {quoted_table_name} ({insert_columns}) VALUES ({placeholders})",
        rows,
      )

    conn.commit()


def _quote_identifier(identifier: str) -> str:
  escaped = identifier.replace('"', '""')
  return f'"{escaped}"'

def execute_query(sql_command: str) -> str:
  """
  Executes a SQL command on database.db
  - SELECT queries return data as CSV
  - INSERT/DELETE/UPDATE queries return status message
  """
  try:
    conn = _connect()
    cursor = conn.cursor()
    
    # Normalize the query
    query_type = sql_command.strip().upper().split()[0]
    
    if query_type == 'SELECT':
      cursor.execute(sql_command)
      rows = cursor.fetchall()
      columns = [description[0] for description in cursor.description]
      
      # Convert to CSV
      output = StringIO()
      writer = csv.writer(output)
      writer.writerow(columns)
      writer.writerows(rows)
      result = output.getvalue()
      
    else:  # INSERT, UPDATE, DELETE
      cursor.execute(sql_command)
      conn.commit()
      result = f"Success: {cursor.rowcount} row(s) affected"
    
    conn.close()
    return result
    
  except sqlite3.Error as e:
    return f"Error: {str(e)}"
  
if __name__ == "__main__":
  while True:
    sql_command = input("Enter SQL command (or 'exit' to quit): ")
    if sql_command.lower() == 'exit':
      break
    output = execute_query(sql_command)
    print(output)