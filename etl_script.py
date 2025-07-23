# =================================================================
# Python ETL Script: Google Sheets to PostgreSQL
# Description: This script reads trucking data from a Google Sheet
#              and loads it into a PostgreSQL database. It uses a
#              human-readable trip identifier and handles notes.
#
# Prerequisites:
# 1. Python 3 installed.
# 2. A PostgreSQL database set up with the required tables.
# 3. Completed the Google Sheets API setup and have `credentials.json`.
# 4. A `config.py` file with database credentials (see `config.py.example`).
# 5. Required Python libraries installed:
#    pip install -r requirements.txt
# =================================================================

import sys
import numpy as np
import pandas as pd
import psycopg2
import gspread
from gspread_dataframe import get_as_dataframe

# --- Configuration ---
# Attempt to import database configuration from a separate, untracked file.
try:
    from config import DB_CONFIG
except ImportError:
    print("❌ ERROR: `config.py` file not found.", file=sys.stderr)
    print("👉 Please copy `config.py.example` to `config.py` and fill in your database credentials.", file=sys.stderr)
    sys.exit(1)

# The exact name of your Google Sheet file.
GOOGLE_SHEET_NAME = 'trucking-analytics'

# The exact names of the tabs (worksheets) within your Google Sheet.
SHEET_NAMES = {
    "loads": "LOADS",
    "fuel": "FUEL",
    "expenses": "EXPENSES"
}

# Path to your service account credentials file.
SERVICE_ACCOUNT_FILE = 'credentials.json'


def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ ERROR: Could not connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

def clear_database_tables(cursor):
    """Clears all data from the tables to ensure a fresh import."""
    print("🗑️  Clearing existing data from database tables...")
    # TRUNCATE is faster than DELETE and resets SERIAL counters.
    # CASCADE automatically truncates dependent tables (fuel_stops, expenses).
    cursor.execute("TRUNCATE TABLE loads, fuel_stops, expenses RESTART IDENTITY CASCADE;")
    print("✅ Tables cleared.")

def load_data_from_google_sheet():
    """
    Connects to Google Sheets, reads all necessary tabs, and returns a dictionary of pandas DataFrames.
    """
    try:
        print("🔄 Authenticating with Google Sheets...")
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        print(f"✅ Successfully opened Google Sheet: '{GOOGLE_SHEET_NAME}'")

        data = {}
        for key, sheet_name in SHEET_NAMES.items():
            print(f"  - Reading tab: '{sheet_name}'...")
            worksheet = spreadsheet.worksheet(sheet_name)
            df = get_as_dataframe(worksheet, evaluate_formulas=False, header=0)
            df.dropna(how='all', inplace=True)
            # Replace empty strings and numpy NaN with None for database compatibility
            df = df.replace({np.nan: None, '': None})
            data[key] = df

        # --- Robust Data Type Conversion ---
        data['loads']['Load Date'] = pd.to_datetime(data['loads']['Load Date'])
        data['loads']['Total Miles'] = pd.to_numeric(data['loads']['Total Miles'])
        data['loads']['Revenue'] = pd.to_numeric(data['loads']['Revenue'])
        data['loads']['Wait Time Hours'] = pd.to_numeric(data['loads']['Wait Time Hours'])
        data['loads']['Is Drop and Hook'] = data['loads']['Is Drop and Hook'].astype(bool)

        data['fuel']['Stop Date'] = pd.to_datetime(data['fuel']['Stop Date'])
        data['fuel']['Gallons'] = pd.to_numeric(data['fuel']['Gallons'])
        data['fuel']['Total Cost'] = pd.to_numeric(data['fuel']['Total Cost'])

        data['expenses']['Expense Date'] = pd.to_datetime(data['expenses']['Expense Date'])
        data['expenses']['Amount'] = pd.to_numeric(data['expenses']['Amount'])

        print("✅ Successfully loaded and parsed data from all tabs.")
        return data
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ ERROR: Google Sheet '{GOOGLE_SHEET_NAME}' not found.", file=sys.stderr)
        print(f"👉 Make sure the name is correct and you've shared it with the service account email.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR: Could not read the Google Sheet. Error: {e}", file=sys.stderr)
        sys.exit(1)

def insert_data(cursor, data):
    """Inserts all the data into the database tables."""
    trip_to_db_id_map = {}

    # --- 1. Insert Loads and Create Trip Identifier Map ---
    print("  - Processing 'Load Logs' and creating trip map...")
    for _, row in data["loads"].iterrows():
        trip_identifier = f"{row['Load Date'].strftime('%Y-%m-%d')} to {row['Dropoff Location']}"
        sql = "INSERT INTO loads (load_date, pickup_location, dropoff_location, total_miles, revenue, is_drop_and_hook, wait_time_hours, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING load_id;"
        cursor.execute(sql, (row['Load Date'], row['Pickup Location'], row['Dropoff Location'], row['Total Miles'], row['Revenue'], row['Is Drop and Hook'], row['Wait Time Hours'], row['Notes']))
        db_load_id = cursor.fetchone()[0]
        trip_to_db_id_map[trip_identifier] = db_load_id
    print(f"    ↳ {len(trip_to_db_id_map)} trips mapped and inserted.")

    # --- 2. Insert Fuel Stops using the Trip Map ---
    print("  - Processing 'Fuel Stops'...")
    for _, row in data["fuel"].iterrows():
        db_load_id = trip_to_db_id_map.get(row['Trip'])
        if db_load_id:
            sql = "INSERT INTO fuel_stops (load_id, stop_date, gallons, total_cost, location, notes) VALUES (%s, %s, %s, %s, %s, %s);"
            cursor.execute(sql, (db_load_id, row['Stop Date'], row['Gallons'], row['Total Cost'], row['Location'], row['Notes']))
        else:
            print(f"    ⚠️ WARNING: Skipping fuel stop. Could not find matching trip for '{row['Trip']}'.")
    print("    ↳ 'Fuel Stops' data inserted.")

    # --- 3. Insert Expenses using the Trip Map ---
    print("  - Processing 'Receipts'...")
    for _, row in data["expenses"].iterrows():
        db_load_id = trip_to_db_id_map.get(row['Trip'])
        if db_load_id:
            sql = "INSERT INTO expenses (load_id, expense_date, category, amount, description, notes) VALUES (%s, %s, %s, %s, %s, %s);"
            cursor.execute(sql, (db_load_id, row['Expense Date'], row['Category'], row['Amount'], row['Item Description (What)'], row['Notes (Why)']))
        else:
            print(f"    ⚠️ WARNING: Skipping expense. Could not find matching trip for '{row['Trip']}'.")
    print("    ↳ 'Receipts' data inserted.")

def main():
    """Main function to orchestrate the ETL process."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        data = load_data_from_google_sheet()

        clear_database_tables(cursor)

        print("\n--- Starting Data Insertion ---")
        insert_data(cursor, data)

        conn.commit()
        print("\n--- Transaction Committed Successfully ---")

    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}", file=sys.stderr)
        print("--- Transaction Rolled Back ---", file=sys.stderr)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("🔌 Database connection closed.")


if __name__ == "__main__":
    main()