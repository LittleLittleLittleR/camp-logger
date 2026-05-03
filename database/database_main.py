import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from .sheets.sheets_main import GoogleSheets
from .SQLite.execute import (
  DB_PATH,
  list_tables,
  read_table,
  replace_table,
  get_db_last_modified_timestamp,
  get_sync_meta,
  set_sync_meta,
  resolve_db_path,
)

load_dotenv()

INTERNAL_WRITE_SKEW_SECONDS = 5


def _clean_env(value):
  """Normalize .env values by trimming spaces and surrounding quotes."""
  if value is None:
    return None
  return value.strip().strip('"').strip("'")


def _parse_iso(ts):
  if not ts:
    return None
  try:
    normalized = ts.replace('Z', '+00:00')
    return datetime.fromisoformat(normalized)
  except (ValueError, AttributeError):
    return None


def _is_external_edit(last_modified, python_last_write):
  """Return True when last_modified is meaningfully newer than Python's last write."""
  if not last_modified:
    return False
  if not python_last_write:
    return True

  return last_modified > (python_last_write + timedelta(seconds=INTERNAL_WRITE_SKEW_SECONDS))

class DatabaseManager:
  def __init__(self):
    self.sheet_id = _clean_env(os.getenv("SHEET_ID"))

    if not self.sheet_id:
      raise ValueError("SHEET_ID must be set in .env")

    self.google_sheets = GoogleSheets(self.sheet_id)
    self.database_path = resolve_db_path()

  def read_from_google_sheets(self):
    """Read every worksheet tab as a table-like dataset."""
    return self.google_sheets.read_all_sheets()
  
  def write_to_sheet(self):
    """Export every SQLite table to its own worksheet tab and overwrite all data."""
    if not self.database_path.exists():
      raise FileNotFoundError(f"SQLite database not found: {self.database_path}")

    exported_tables = []

    for table_name in list_tables():
      columns, rows = read_table(table_name)

      values = [columns]
      values.extend([list(row) for row in rows])

      self.google_sheets.overwrite_sheet(table_name, values)

      exported_tables.append({
        'tableName': table_name,
        'sheetName': table_name,
        'rowsWritten': len(rows),
      })

    self.google_sheets.set_sync_meta('sheets_python_last_write_ts', self.google_sheets.get_last_modified_timestamp())

    return exported_tables

  def write_to_database(self):
    """Import every Google Sheets tab into SQLite, replacing each table entirely."""
    imported_tables = []

    for table in self.google_sheets.read_all_sheets():
      columns = table['columns']
      rows = table['rows']
      sheet_name = table['sheetName']

      if not columns:
        continue

      replace_table(sheet_name, columns, rows)

      imported_tables.append({
        'tableName': sheet_name,
        'sheetName': sheet_name,
        'rowsWritten': len(rows),
      })

    set_sync_meta('sqlite_python_last_write_ts', get_db_last_modified_timestamp())

    return imported_tables

  def compare_versions(self):
    """
    Compare sheet/database versions using four timestamps and return a sync verdict.

    The four tracked timestamps are:
    - sheets_external_last_edit_ts
    - sqlite_external_last_edit_ts
    - sheets_python_last_write_ts
    - sqlite_python_last_write_ts
    """
    sheets_last_modified_ts = self.google_sheets.get_last_modified_timestamp()
    sqlite_last_modified_ts = get_db_last_modified_timestamp()

    sheets_python_last_write_ts = self.google_sheets.get_sync_meta('sheets_python_last_write_ts')
    sqlite_python_last_write_ts = get_sync_meta('sqlite_python_last_write_ts')

    sheets_last_modified = _parse_iso(sheets_last_modified_ts)
    sqlite_last_modified = _parse_iso(sqlite_last_modified_ts)
    sheets_python_last_write = _parse_iso(sheets_python_last_write_ts)
    sqlite_python_last_write = _parse_iso(sqlite_python_last_write_ts)

    sheets_external_last_edit_ts = (
      sheets_last_modified_ts
      if _is_external_edit(sheets_last_modified, sheets_python_last_write)
      else None
    )
    sqlite_external_last_edit_ts = (
      sqlite_last_modified_ts
      if _is_external_edit(sqlite_last_modified, sqlite_python_last_write)
      else None
    )

    verdict = self._decide_sync_verdict(
      sheets_external_last_edit_ts,
      sqlite_external_last_edit_ts,
    )

    return {
      'sheets_external_last_edit_ts': sheets_external_last_edit_ts,
      'sqlite_external_last_edit_ts': sqlite_external_last_edit_ts,
      'sheets_python_last_write_ts': sheets_python_last_write_ts,
      'sqlite_python_last_write_ts': sqlite_python_last_write_ts,
      'verdict': verdict,
    }

  def _decide_sync_verdict(self, sheets_external_ts, sqlite_external_ts):
    """Decide which side should override the other based on external edit timestamps."""
    sheets_external = _parse_iso(sheets_external_ts)
    sqlite_external = _parse_iso(sqlite_external_ts)

    if sheets_external and not sqlite_external:
      return 'sheets_overrides_sqlite'

    if sqlite_external and not sheets_external:
      return 'sqlite_overrides_sheets'

    if sheets_external and sqlite_external:
      if sheets_external > sqlite_external:
        return 'sheets_overrides_sqlite'
      if sqlite_external > sheets_external:
        return 'sqlite_overrides_sheets'
      return 'conflict_same_external_timestamp'

    return 'already_in_sync_or_no_external_changes'

  def compare_and_sync(self):
    """Compare both sides and apply the verdict when a clear winner exists."""
    comparison = self.compare_versions()
    verdict = comparison['verdict']

    if verdict == 'sheets_overrides_sqlite':
      comparison['action'] = 'write_to_database'
      comparison['sync_result'] = self.write_to_database()
      return comparison

    if verdict == 'sqlite_overrides_sheets':
      comparison['action'] = 'write_to_sheet'
      comparison['sync_result'] = self.write_to_sheet()
      return comparison

    comparison['action'] = 'none'
    comparison['sync_result'] = []
    return comparison
