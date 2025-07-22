# =================================================================
# Python ETL Script: Google Sheets to PostgreSQL
# Description: This script reads trucking data from a Google Sheet
#              and loads it into a PostgreSQL database. It uses a
#              human-readable trip identifier and handles notes.
#
# Prerequisites:
# 1. Python 3 installed.
# 2. A PostgreSQL database.
# 3. Completed the Google Sheets API setup and have `credentials.json`.
# 4. Python virtual environment created and activated.
# 5. Required Python libraries (requirements.txt)
# =================================================================

import pandas as pd
import psycopg2
import gspread
from gspread_dataframe import get_as_dataframe
import sys
import numpy as np

# --- Configuration ---
# TODO: Update these details to match your local setup.
DB_CONFIG = {
    "dbname": "db_name",
    "user": "user",
    "password": "your_password", # Replace with your actual password
    "host": "localhost",
    "port": "5432"
}

# The exact name of your Google Sheet file.
GOOGLE_SHEET_NAME = 'Trucking Data'

# The exact names of the tabs (worksheets) within your Google Sheet.
SHEET_NAMES = {
    "loads": "Load Logs",
    "fuel": "Fuel Stops",
    "expenses": "Receipts"
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
        print("👉 Please check your DB_CONFIG and ensure PostgreSQL is running.", file=sys.stderr)
        sys.exit(1)

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

        # Convert data types
        data['loads']['Load Date'] = pd.to_datetime(data['loads']['Load Date'])
        data['loads']['Total Miles'] = pd.to_numeric(data['loads']['Total Miles'])
        # ... (add other conversions as needed)

        print("✅ Successfully loaded and parsed data from all tabs.")
        return data
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ ERROR: Google Sheet '{GOOGLE_SHEET_NAME}' not found.", file=sys.stderr)
        print(f"👉 Make sure the name is correct and you've shared it with the service account email.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR: Could not read the Google Sheet. Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Main function to orchestrate the ETL process."""
    conn = get_db_connection()
    cursor = conn.cursor()

    data = load_data_from_google_sheet()

    trip_to_db_id_map = {}

    try:
        print("\n--- Starting Data Insertion ---")

        print(" Processing 'Load Logs' and creating trip map...")
        for index, row in data["loads"].iterrows():
            trip_identifier = f"{row['Load Date'].strftime('%Y-%m-%d')} to {row['Dropoff Location']}"

            sql_insert_load = """
                INSERT INTO loads (load_date, pickup_location, dropoff_location, total_miles, revenue, is_drop_and_hook, wait_time_hours, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING load_id;
            """
            cursor.execute(sql_insert_load, (
                row['Load Date'], row['Pickup Location'], row['Dropoff Location'],
                row['Total Miles'], row['Revenue'], row['Is Drop and Hook'],
                row['Wait Time Hours'], row['Notes']
            ))

            db_load_id = cursor.fetchone()[0]
            trip_to_db_id_map[trip_identifier] = db_load_id
        print(f"✅ 'Load Logs' processed. {len(trip_to_db_id_map)} trips mapped.")

        print("\n Inserting data into 'fuel_stops' table...")
        for index, fuel_row in data["fuel"].iterrows():
            trip_identifier = fuel_row['Trip']
            db_load_id = trip_to_db_id_map.get(trip_identifier)

            if db_load_id:
                sql_insert_fuel = """
                    INSERT INTO fuel_stops (load_id, stop_date, gallons, total_cost, location, notes)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """
                cursor.execute(sql_insert_fuel, (
                    db_load_id, fuel_row['Stop Date'], fuel_row['Gallons'],
                    fuel_row['Total Cost'], fuel_row['Location'], fuel_row['Notes']
                ))
            else:
                print(f"⚠️ WARNING: Skipping fuel stop. Could not find matching trip for '{trip_identifier}'.")
        print("✅ 'fuel_stops' data inserted.")

        print("\n Inserting data into 'expenses' table...")
        for index, expense_row in data["expenses"].iterrows():
            trip_identifier = expense_row['Trip']
            db_load_id = trip_to_db_id_map.get(trip_identifier)

            if db_load_id:
                sql_insert_expense = """
                    INSERT INTO expenses (load_id, expense_date, category, amount, description, notes)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """
                cursor.execute(sql_insert_expense, (
                    db_load_id, expense_row['Expense Date'], expense_row['Category'],
                    expense_row['Amount'], expense_row['Item Description'], expense_row['Notes']
                ))
            else:
                print(f"⚠️ WARNING: Skipping expense. Could not find matching trip for '{trip_identifier}'.")
        print("✅ 'expenses' data inserted.")

        conn.commit()
        print("\n--- Transaction Committed Successfully ---")

    except Exception as e:
        print(f"\n❌ ERROR during data insertion: {e}", file=sys.stderr)
        print("--- Transaction Rolled Back ---", file=sys.stderr)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("🔌 Database connection closed.")


if __name__ == "__main__":
    main()