from database.database_main import DatabaseManager
import json


def export_all_tables_to_google_sheets():
	"""Dummy launcher that writes every SQLite table into its matching Google Sheets tab."""
	manager = DatabaseManager()
	result = manager.write_to_sheet()
	print(result)


def import_all_tables_from_google_sheets():
	"""Dummy launcher that writes every Google Sheets tab back into SQLite."""
	manager = DatabaseManager()
	result = manager.write_to_database()
	print(result)


def test_compare_versions():
	"""Test the compare logic and print the four timestamps plus verdict."""
	manager = DatabaseManager()
	result = manager.compare_versions()
	print(json.dumps(result, indent=2))


def sync_by_compare_verdict():
	"""Compare both sides and execute the correct override automatically."""
	manager = DatabaseManager()
	result = manager.compare_and_sync()
	print(json.dumps(result, indent=2))


if __name__ == '__main__':
	# export_all_tables_to_google_sheets()
	# import_all_tables_from_google_sheets()
	# test_compare_versions()
	sync_by_compare_verdict()
