import gspread
import json
import os
import time
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession


SYNC_META_SHEET = '__sync_meta'
SERVICE_ACCOUNT_KEYS = {
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
ENV_KEY_MAP = {
  'type': 'GOOGLE_SERVICE_ACCOUNT_TYPE',
  'project_id': 'GOOGLE_PROJECT_ID',
  'private_key_id': 'GOOGLE_PRIVATE_KEY_ID',
  'private_key': 'GOOGLE_PRIVATE_KEY',
  'client_email': 'GOOGLE_CLIENT_EMAIL',
  'client_id': 'GOOGLE_CLIENT_ID',
  'auth_uri': 'GOOGLE_AUTH_URI',
  'token_uri': 'GOOGLE_TOKEN_URI',
  'auth_provider_x509_cert_url': 'GOOGLE_AUTH_PROVIDER_X509_CERT_URL',
  'client_x509_cert_url': 'GOOGLE_CLIENT_X509_CERT_URL',
  'universe_domain': 'GOOGLE_UNIVERSE_DOMAIN',
}
REQUIRED_ENV_KEYS = {
  'GOOGLE_PROJECT_ID',
  'GOOGLE_PRIVATE_KEY_ID',
  'GOOGLE_PRIVATE_KEY',
  'GOOGLE_CLIENT_EMAIL',
  'GOOGLE_CLIENT_ID',
}


def _clean_env(value):
  if value is None:
    return None
  return value.strip().strip('"').strip("'")


def _parse_service_account_file(raw_config):
  if set(raw_config.keys()) & SERVICE_ACCOUNT_KEYS:
    return {k: v for k, v in raw_config.items() if k in SERVICE_ACCOUNT_KEYS}

  if set(raw_config.keys()) & set(ENV_KEY_MAP.values()):
    info = {}
    for service_key, env_key in ENV_KEY_MAP.items():
      value = _clean_env(raw_config.get(env_key))
      if value:
        info[service_key] = value
    return info

  return {}


def _load_service_account_info(credentials_file):
  env_info = {}
  configured_env_keys = set()

  for service_key, env_key in ENV_KEY_MAP.items():
    value = _clean_env(os.getenv(env_key))
    if value:
      configured_env_keys.add(env_key)
      env_info[service_key] = value

  if configured_env_keys:
    missing_required = sorted(REQUIRED_ENV_KEYS - configured_env_keys)
    if missing_required:
      raise ValueError(
        'Missing required Google service account environment variables: '
        + ', '.join(missing_required)
      )

    env_info.setdefault('type', 'service_account')
    env_info.setdefault('auth_uri', 'https://accounts.google.com/o/oauth2/auth')
    env_info.setdefault('token_uri', 'https://oauth2.googleapis.com/token')
    env_info.setdefault('auth_provider_x509_cert_url', 'https://www.googleapis.com/oauth2/v1/certs')
    env_info.setdefault('universe_domain', 'googleapis.com')
    env_info['private_key'] = env_info['private_key'].replace('\\n', '\n')
    return env_info

  if not os.path.exists(credentials_file):
    raise FileNotFoundError(
      'Google credentials not found. Set GOOGLE_* environment variables '
      'or provide database/sheets/credentials.json.'
    )

  with open(credentials_file, 'r', encoding='utf-8') as f:
    raw_config = json.load(f)

  service_account_info = _parse_service_account_file(raw_config)
  if not service_account_info:
    raise ValueError(
      'Invalid credentials.json format. Use service-account JSON keys '
      'or GOOGLE_* key names.'
    )

  if 'private_key' in service_account_info:
    service_account_info['private_key'] = service_account_info['private_key'].replace('\\n', '\n')

  return service_account_info

class GoogleSheets:
  def __init__(self, spreadsheet_id):
    credentials_file = os.path.join(os.path.dirname(__file__), 'credentials.json')
    service_account_info = _load_service_account_info(credentials_file)
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