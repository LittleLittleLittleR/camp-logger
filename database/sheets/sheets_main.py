import gspread
import json
import os
import time
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession


SYNC_META_SHEET = '__sync_meta'

class GoogleSheets:
  def __init__(self, spreadsheet_id):
    credentials_file = os.path.join(os.path.dirname(__file__), 'credentials.json')

    with open(credentials_file, 'r', encoding='utf-8') as f:
      raw_config = json.load(f)

    service_account_keys = {
      'type',
      'project_id',
      'private_key_id',
      'private_key',
      'client_email',
      'client_id',
      'auth_uri',
      'token_uri',
      'auth_provider_x509_cert_url',
      'client_x509_cert_url',
      'universe_domain',
    }
    service_account_info = {
      k: v for k, v in raw_config.items() if k in service_account_keys
    }
    scopes = [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive.metadata.readonly',
    ]
    self.credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    self.gc = gspread.authorize(self.credentials)
    self.authorized_session = AuthorizedSession(self.credentials)

    # Prefer opening by key, then fall back to title when needed.
    try:
      self.spreadsheet = self._open_spreadsheet_with_retry(spreadsheet_id, by_key=True)
    except SpreadsheetNotFound:
      self.spreadsheet = self._open_spreadsheet_with_retry(spreadsheet_id, by_key=False)

  def read_all_sheets(self):
    """Read every worksheet tab as a list of table-like dictionaries."""
    tables = []

    for worksheet in self.spreadsheet.worksheets():
      if worksheet.title == SYNC_META_SHEET:
        continue

      values = worksheet.get_all_values()
      if not values:
        continue

      tables.append({
        'sheetName': worksheet.title,
        'columns': values[0],
        'rows': values[1:],
      })

    return tables

  def overwrite_sheet(self, sheet_name, values, rows=100, cols=20):
    """Clear a worksheet tab and replace it with the provided values."""
    worksheet = self._get_or_create_worksheet(sheet_name, rows=rows, cols=cols)
    worksheet.clear()
    worksheet.update('A1', values, value_input_option='USER_ENTERED')
    return {
      'sheetName': sheet_name,
      'rowsWritten': max(len(values) - 1, 0),
    }

  def get_last_modified_timestamp(self):
    """Return spreadsheet modified timestamp from Google Drive metadata."""
    file_id = self.spreadsheet.id
    url = f'https://www.googleapis.com/drive/v3/files/{file_id}?fields=modifiedTime'
    response = self.authorized_session.get(url, timeout=30)
    response.raise_for_status()
    return response.json().get('modifiedTime')

  def get_sync_meta(self, meta_key):
    """Get a sync metadata value stored in the hidden meta worksheet."""
    worksheet = self._get_or_create_worksheet(SYNC_META_SHEET, rows=20, cols=2)
    self._ensure_meta_headers(worksheet)

    rows = worksheet.get_all_values()
    for row in rows[1:]:
      if len(row) >= 2 and row[0] == meta_key:
        return row[1]
    return None

  def set_sync_meta(self, meta_key, meta_value):
    """Set a sync metadata value in the hidden meta worksheet."""
    worksheet = self._get_or_create_worksheet(SYNC_META_SHEET, rows=20, cols=2)
    self._ensure_meta_headers(worksheet)

    rows = worksheet.get_all_values()
    for index, row in enumerate(rows[1:], start=2):
      if len(row) >= 1 and row[0] == meta_key:
        worksheet.update(f'B{index}', [[meta_value]], value_input_option='USER_ENTERED')
        return

    worksheet.append_row([meta_key, meta_value], value_input_option='USER_ENTERED')

  def _get_or_create_worksheet(self, sheet_name, rows=100, cols=20):
    """Return an existing worksheet by name or create it if missing."""
    try:
      return self.spreadsheet.worksheet(sheet_name)
    except WorksheetNotFound:
      return self.spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)

  def _ensure_meta_headers(self, worksheet):
    """Ensure meta worksheet has key/value headers."""
    values = worksheet.get_all_values()
    if not values:
      worksheet.update('A1:B1', [['meta_key', 'meta_value']], value_input_option='USER_ENTERED')
      return

    header = values[0]
    if len(header) < 2 or header[0] != 'meta_key' or header[1] != 'meta_value':
      worksheet.update('A1:B1', [['meta_key', 'meta_value']], value_input_option='USER_ENTERED')

  def _open_spreadsheet_with_retry(self, spreadsheet_id, by_key=True, retries=3):
    """Open a spreadsheet with retries for transient Google API failures."""
    last_error = None

    for attempt in range(retries):
      try:
        if by_key:
          return self.gc.open_by_key(spreadsheet_id)
        return self.gc.open(spreadsheet_id)
      except APIError as err:
        status_code = err.response.status_code if err.response is not None else None
        if status_code != 500 or attempt == retries - 1:
          raise

        last_error = err
        # Exponential backoff: 1s, 2s, 4s.
        time.sleep(2 ** attempt)

    if last_error is not None:
      raise last_error

    raise RuntimeError("Failed to open spreadsheet for an unknown reason")