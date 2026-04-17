import random
import string
from pathlib import Path
import sqlite3

from database.SQLite.execute import get_db_last_modified_timestamp, list_tables


DB_PATH = Path(__file__).resolve().parent / 'database.db'


def insert_random_test_row():
  """Insert a random row into an existing table (defaults to categories)."""
  table_name = 'categories'

  available_tables = list_tables()
  if table_name not in available_tables:
    raise ValueError(f"Table '{table_name}' not found. Available: {available_tables}")

  conn = sqlite3.connect(DB_PATH)
  cursor = conn.cursor()

  cursor.execute(f"PRAGMA table_info({table_name})")
  schema_rows = cursor.fetchall()

  # Required columns: NOT NULL with no default value, excluding auto integer PK.
  required_columns = []
  for _, col_name, col_type, notnull, default_value, pk in schema_rows:
    is_auto_pk = pk == 1 and 'INT' in (col_type or '').upper()
    if notnull == 1 and default_value is None and not is_auto_pk:
      required_columns.append(col_name)

  if not required_columns:
    cursor.execute(f"INSERT INTO {table_name} DEFAULT VALUES")
  else:
    values = []
    for col_name in required_columns:
      if 'id' in col_name.lower():
        values.append(''.join(random.choices(string.digits, k=6)))
      else:
        values.append(''.join(random.choices(string.ascii_letters, k=10)))

    placeholders = ', '.join(['?'] * len(required_columns))
    columns_sql = ', '.join(required_columns)
    cursor.execute(
      f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})",
      tuple(values),
    )

  conn.commit()
  conn.close()
  
  print(f"Inserted into table '{table_name}' in: {DB_PATH}")
  print(f"Database last modified time: {get_db_last_modified_timestamp()}")


if __name__ == '__main__':
  insert_random_test_row()