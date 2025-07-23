# =================================================================
# Python Analysis & Visualization Script
# Description: This script connects to the PostgreSQL database,
#              runs analysis queries, and generates visualizations
#              to uncover business insights.
#
# Prerequisites:
# 1. The `etl_script.py` has been run successfully to populate the database.
# 2. A `config.py` file with database credentials exists.
# 3. Required Python libraries installed:
#    pip install -r requirements.txt
# =================================================================

import sys
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Configuration ---
# Attempt to import database configuration.
try:
    from config import DB_CONFIG
except ImportError:
    print("❌ ERROR: `config.py` file not found.", file=sys.stderr)
    print("👉 Please ensure `config.py` exists with your database credentials.", file=sys.stderr)
    sys.exit(1)

# Directory to save the generated charts
OUTPUT_DIR = 'reports'

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ ERROR: Could not connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

def run_query(conn, query_path):
    """Executes a SQL query from a file and returns the result as a pandas DataFrame."""
    print(f"🔄 Running query from '{query_path}'...")
    try:
        with open(query_path, 'r') as f:
            sql_query = f.read()
        df = pd.read_sql_query(sql_query, conn)
        print("✅ Query executed successfully.")
        return df
    except Exception as e:
        print(f"❌ ERROR: Failed to execute query from {query_path}. Error: {e}", file=sys.stderr)
        sys.exit(1)

def generate_visualizations(df_profit, df_expenses, df_routes, df_expense_details):
    print("\n--- Generating Visualizations ---")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"📂 Created output directory: '{OUTPUT_DIR}'")

    sns.set_theme(style="whitegrid")

    # Visualization 1: Profit per trip
    plt.figure(figsize=(12, 8))
    trip_labels = df_profit['dropoff_location'] + ' (' + df_profit['load_date'].dt.strftime('%m-%d') + ')'
    sns.barplot(x='net_profit', y=trip_labels, data=df_profit, palette='viridis')
    plt.title('Net Profit per Trip', fontsize=16, weight='bold')
    plt.xlabel('Net Profit ($)')
    plt.ylabel('Trip')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'trip_profitability.png'))
    plt.close()

    # Visualization 2: Expense breakdown
    plt.figure(figsize=(10, 7))
    expense_summary = df_expenses.groupby('category')['total_spent'].sum()
    plt.pie(expense_summary, labels=expense_summary.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
    plt.title('Total Expense Breakdown by Category', fontsize=16, weight='bold')
    plt.ylabel('')
    plt.savefig(os.path.join(OUTPUT_DIR, 'expense_breakdown.png'))
    plt.close()

    # Visualization 3: Route performance
    plt.figure(figsize=(12, 8))
    sns.barplot(x='avg_profit_per_mile', y='dropoff_location', data=df_routes.sort_values('avg_profit_per_mile', ascending=False), palette='coolwarm')
    plt.title('Average Profit per Mile by Route', fontsize=16, weight='bold')
    plt.xlabel('Avg Profit per Mile ($)')
    plt.ylabel('Route')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'route_performance.png'))
    plt.close()

    # Visualization 4: Detailed expenses per trip (optional: maybe heatmap or line chart)
    # You can expand this later to include visualizations for Query 4 if useful

    print("✅ All visualizations generated.")

def print_insights(df_profit, df_expenses):
    """Analyzes the dataframes and prints key insights to the console."""
    print("\n--- Actionable Insights ---")

    # Insight 1: Find the most and least profitable trips
    most_profitable = df_profit.loc[df_profit['net_profit'].idxmax()]
    least_profitable = df_profit.loc[df_profit['net_profit'].idxmin()]

    print("\n📈 Profitability Insights:")
    print(f"  - Most Profitable Trip: to {most_profitable['dropoff_location']} on {most_profitable['load_date'].date()} with a net profit of ${most_profitable['net_profit']:.2f}.")
    print(f"  - Least Profitable Trip: to {least_profitable['dropoff_location']} on {least_profitable['load_date'].date()} with a net profit of ${least_profitable['net_profit']:.2f}.")

    # Insight 2: Analyze the least profitable trip
    if least_profitable['net_profit'] < 0:
        print(f"    ↳ The trip to {least_profitable['dropoff_location']} was unprofitable. Let's dig deeper.")
        total_costs = least_profitable['total_fuel_cost'] + least_profitable['total_other_expenses']
        print(f"      - Revenue was ${least_profitable['revenue']:.2f}, but total costs were ${total_costs:.2f}.")
        print(f"      - High costs were driven by fuel (${least_profitable['total_fuel_cost']:.2f}) and other expenses (${least_profitable['total_other_expenses']:.2f}).")

    # Insight 3: Analyze expense categories
    print("\n💰 Expense Insights:")
    total_food_cost = df_expenses[df_expenses['category'] == 'Food']['total_spent'].sum()
    total_maintenance_cost = df_expenses[df_expenses['category'] == 'Maintenance']['total_spent'].sum()

    print(f"  - Total spent on Food (snacks, meals): ${total_food_cost:.2f}. This is a significant recurring cost.")
    if total_maintenance_cost > 0:
        print(f"  - A single Maintenance event cost ${total_maintenance_cost:.2f}, drastically impacting the profitability of its associated trip.")

def main():
    """Main function to run the analysis and generate reports."""
    conn = get_db_connection()

    try:
        # Load and split all queries
        with open('sql/analysis_queries.sql', 'r') as f:
            queries = f.read().split(';')

        # Clean whitespace and remove blanks
        queries = [q.strip() for q in queries if q.strip()]

        # ✅ Run each query and store results
        df_profitability = pd.read_sql_query(queries[0], conn)
        df_monthly_expenses = pd.read_sql_query(queries[1], conn)
        df_route_analysis = pd.read_sql_query(queries[2], conn)
        df_expense_details = pd.read_sql_query(queries[3], conn)

        # Fix date column for plotting
        df_profitability['load_date'] = pd.to_datetime(df_profitability['load_date'])
        df_expense_details['load_date'] = pd.to_datetime(df_expense_details['load_date'])
        df_expense_details['expense_date'] = pd.to_datetime(df_expense_details['expense_date'])

        # ✅ Run visualizations and insights for each
        generate_visualizations(
            df_profit=df_profitability,
            df_expenses=df_monthly_expenses,
            df_routes=df_route_analysis,
            df_expense_details=df_expense_details
        )

        print_insights(df_profitability, df_monthly_expenses)

    except Exception as e:
        print(f"\n❌ An unexpected error occurred during analysis: {e}", file=sys.stderr)
    finally:
        conn.close()
        print("\n🔌 Database connection closed.")


if __name__ == "__main__":
    main()