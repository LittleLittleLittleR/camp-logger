import sqlite3
import csv

def insert_single_row():
  """Option 1: Manually insert a single row into a table"""
  db_path = "database.db"
  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()
  
  # Get table names
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
  tables = [row[0] for row in cursor.fetchall()]
  
  print("Available tables:")
  for i, table in enumerate(tables, 1):
    print(f"{i}. {table}")
  
  table_choice = int(input("Select table number: ")) - 1
  table_name = tables[table_choice]
  
  # Get column names
  cursor.execute(f"PRAGMA table_info({table_name})")
  columns = [row[1] for row in cursor.fetchall()]
  
  print(f"\nEnter data for {table_name}:")
  values = []
  for col in columns:
    value = input(f"{col}: ")
    values.append(value)
  
  placeholders = ", ".join(["?" for _ in columns])
  query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
  
  cursor.execute(query, values)
  conn.commit()
  conn.close()
  print("Row inserted successfully!")


def insert_from_csv():
  """Option 2: Insert multiple rows from CSV, handling linked tables"""
  db_path = "database.db"
  csv_file = input("Enter CSV file path: ")
  
  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()
  
  # Read CSV
  with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
  
  if not rows:
    print("CSV is empty!")
    return
  
  csv_columns = set(rows[0].keys())
  
  # Get all table schemas
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
  all_tables = [row[0] for row in cursor.fetchall()]
  
  table_schemas = {}
  for table in all_tables:
    cursor.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in cursor.fetchall()}
    table_schemas[table] = columns
  
  # Match CSV columns to tables
  matched_tables = {}
  for table, columns in table_schemas.items():
    matching_cols = csv_columns & columns
    if matching_cols:
      matched_tables[table] = list(matching_cols)
  
  if not matched_tables:
    print("CSV columns don't match any table!")
    conn.close()
    return
  
  print(f"Matched tables: {list(matched_tables.keys())}")
  
  # Insert data into respective tables
  try:
    for row in rows:
      for table, cols in matched_tables.items():
        values = tuple(row.get(col) for col in cols)
        placeholders = ", ".join(["?" for _ in cols])
        query = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        cursor.execute(query, values)
    
    conn.commit()
    print(f"Successfully inserted {len(rows)} rows!")
  except Exception as e:
    conn.rollback()
    print(f"Error: {e}")
  finally:
    conn.close()


def main():
  print("=== Database Insert Tool ===")
  print("1. Insert single row manually")
  print("2. Insert from CSV file")
  
  choice = input("Select option (1 or 2): ").strip()
  
  if choice == "1":
    insert_single_row()
  elif choice == "2":
    insert_from_csv()
  else:
    print("Invalid choice!")


if __name__ == "__main__":
  main()