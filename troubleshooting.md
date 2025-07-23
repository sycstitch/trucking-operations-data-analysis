1. `❌ An unexpected error occurred during analysis: Could not interpret value `net_profit` for `x`. An entry with this name does not appear in `data`.`

step 1: print dataframe being passed in
```python
# debug: print columns to ensure column exists and no typos
print("🔍 Columns in df_profit:", df_profit.columns.tolist())
print(df_profit.head())
```

step 2: check code

problem: query was run before splitting the file, so query 4 (doesn't have net_profit) was being passed in instead of query 1 (has net_profit).

solution: split the queries into their own variables, then read from each variable.

code before:
```python
# Run the analysis queries
df_profitability = run_query(conn, 'sql/analysis_queries.sql') # Assumes the first query is the main one

# For the expense query, we need to read the file and select the correct query
with open('sql/analysis_queries.sql', 'r') as f:
    queries = f.read().split(';')
```

code after (inside try block, before printing insights):
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