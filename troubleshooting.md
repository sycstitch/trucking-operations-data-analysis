### ❌ Error: `Could not interpret value 'net_profit' for 'x'. An entry with this name does not appear in 'data'.`

#### 🛠️ Summary

A plotting function failed because it was given a DataFrame that didn’t contain the `net_profit` column. This happened because the wrong SQL query was passed in.

---

#### 🔎 Step 1: Check DataFrame contents

**Debug code:**

```python
# debug: print columns to ensure column exists and no typos
print("🔍 Columns in df_profit:", df_profit.columns.tolist())
print(df_profit.head())
```

This confirmed that `net_profit` was missing from the DataFrame being passed to the plotting function.

---

#### 🧠 Root Cause

The SQL file was read as one block before being split into queries. The first query (which includes `net_profit`) wasn’t properly isolated, so a later query (which lacks `net_profit`) was passed to the visual function by mistake.

---

#### ✅ Solution

Split the SQL file into individual queries and assign each one to the correct variable *before* passing it into any analysis or visualization code.

---

#### 📉 Old Code (incorrect):

```python
df_profitability = run_query(conn, 'sql/analysis_queries.sql')  # Assumes first query is correct

with open('sql/analysis_queries.sql', 'r') as f:
    queries = f.read().split(';')
```

---

#### 📈 New Code (corrected):

```python
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

# ✅ Fix date columns
df_profitability['load_date'] = pd.to_datetime(df_profitability['load_date'])
df_expense_details['load_date'] = pd.to_datetime(df_expense_details['load_date'])
df_expense_details['expense_date'] = pd.to_datetime(df_expense_details['expense_date'])

# ✅ Run visualizations and insights
generate_visualizations(
    df_profit=df_profitability,
    df_expenses=df_monthly_expenses,
    df_routes=df_route_analysis,
    df_expense_details=df_expense_details
)
```

---

### Datetime Conversion Error
    * fixes #5
    * **Problem:** The script failed with a `TypeError: Can only use .dt accessor with datetimelike values`. This occurred because the `load_date` column read from the database was not automatically recognized as a proper datetime object by pandas.
    * **Solution:** An explicit conversion step, `df['load_date'] = pd.to_datetime(df['load_date'])`, was added immediately after fetching the data to ensure the column has the correct data type before any plotting operations.

---

### Incorrect Query Execution
    * fixes #6
    * **Problem:** The script failed with a `KeyError` because it was attempting to run all queries from the `analysis_queries.sql` file at once, causing it to use the wrong data for a given visualization.
    * **Solution:** The script's logic was refactored to first read the entire SQL file, then split the content into a list of individual queries. Each query is now executed separately, and the resulting DataFrame is passed to the correct analysis or visualization function, ensuring data integrity.

---

### ❌ Missing "AVG" in Visualization

**Symptom:**
The `trip_label` value counts showed only "LOW" and "HIGH" trips being labeled. No "AVG" label appeared in the plot.

```bash
Low: 5, Avg: 3, High: 3  
trip_label  
LOW\nChicago, IL       3  
HIGH\nCharlotte, NC    1  
Name: count, dtype: int64  
```

**Debugging:**
Printed the candidate DataFrame for `avg` to check what trips were considered and whether it was empty.

```python
# Exclude high and low profit trips from the avg candidate pool  
exclude_ids = {trip_low['load_id'], trip_high['load_id']}  
avg_candidates = sorted_profit[~sorted_profit['load_id'].isin(exclude_ids)]  

# Debug: check 'avg' dataframe (positional indexer)  
print(f"Avg candidates shape: {avg_candidates.shape}")  
print("Avg candidates preview:")  
print(avg_candidates[['load_id', 'net_profit']])
```

**Output:**
```bash
🔍 Avg candidates shape: (4, 12)
Excluded IDs: {np.int64(3), np.int64(5)}
Avg candidates preview:
    load_id  net_profit
5        1      832.05
4        2     1331.54
0        6     1358.10
2        4     1459.90
```

**Cause:**
The same trip was being selected for both "AVG" and "HIGH" (both had `load_id = 3`), which resulted in "AVG" being silently dropped from the labeling process since one trip can't be in two categories.

**Fix:**
Used a set of excluded load IDs to make sure the average trip is different from low and high. Also added a fallback in case no valid avg candidate remains. Replaced the `.iloc` (which caused a later bug) with `.loc` to prevent positional index issues.

```python
if avg_candidates.empty:
    print("⚠️ Not enough trips left to determine an average. Using low-profit trip as fallback.")
    trip_avg = trip_low
else:
    mean_profit = avg_candidates['net_profit'].mean()
    closest_idx = (avg_candidates['net_profit'] - mean_profit).abs().idxmin()
    trip_avg = avg_candidates.loc[closest_idx]
```

**Result:**
After the fix, the visualization correctly included "AVG":
1st run:

```bash
Low: 5, Avg: 1, High: 3  
trip_label  
LOW\nChicago, IL       3  
AVG\nColumbus, OH      1  
HIGH\nCharlotte, NC    1  
```

2nd run:

```bash
Low: 5, Avg: 2, High: 3  
trip_label  
LOW\nChicago, IL       3  
AVG\nNew York, NY      2  
HIGH\nCharlotte, NC    1  
```

---

### ❌ Index Error — "single positional indexer is out-of-bounds"

**Symptom:**
Script crashed during analysis with:

```
An unexpected error occurred during analysis: single positional indexer is out-of-bounds
```

**Debugging:**
Checked the shape and content of `sorted_profit` and `avg_candidates` to confirm the DataFrame wasn’t empty or misaligned.

```python
# Debug: check sorted_profit dataframe  
print("🔍 Sorted profit DataFrame shape:", sorted_profit.shape)  
print("🔍 Sorted_profit preview:\n", sorted_profit[['load_id', 'net_profit']].head())  
if sorted_profit.empty:  
    print("❌ sorted_profit is empty!")  
    return  

# Debug: check avg_candidates  
print(f"Avg candidates shape: {avg_candidates.shape}")  
print("Avg candidates preview:")  
print(avg_candidates[['load_id', 'net_profit']])
```

**Cause:**
Under some conditions, especially with limited data, all trips were either LOW or HIGH, leaving the average selection empty. Using `.iloc[idx]` where `idx` was a label rather than a zero-based integer caused an out-of-bounds error. That line was trying to access a row by label using a method that expects a position.

**Fix:**
Switched from `.iloc[...]` to `.loc[...]` which correctly accesses rows by index label:

```python
trip_avg = avg_candidates.loc[
    (avg_candidates['net_profit'] - mean_profit).abs().idxmin()
]
```

---

### ⚠️ Cut-Off Chart Title

**Old Code (caused issue):**

```python
plt.figure(figsize=(12, 7))  
pivoted.plot(kind='bar', stacked=True, colormap='Set2')  

plt.title('Trip Expense Comparison: Low vs Avg vs High Profit', fontsize=16, weight='bold')  
plt.xlabel('Trip')  
plt.ylabel('Total Expenses ($)')  
plt.legend(title='Expense Type', bbox_to_anchor=(1.05, 1), loc='upper left')  
plt.tight_layout()  
plt.savefig(os.path.join(OUTPUT_DIR, 'trip_expense_comparison.png'))  
plt.close()
```

**Problem:**
The title was getting cropped in the saved PNG image because `plt.tight_layout()` didn’t leave space for it.

**Fix:**
Switched to using `fig.tight_layout(rect=[0, 0, 1, 0.95])` to manually preserve space for the title. Also switched to an explicit `fig, ax` setup for better control:

```python
fig, ax = plt.subplots(figsize=(12, 8))  
pivoted.plot(kind='bar', stacked=True, colormap='Set2', ax=ax)  

ax.set_title('Trip Expense Comparison: Low vs Avg vs High Profit', fontsize=16, weight='bold')  
ax.set_xlabel('Trip')  
ax.set_ylabel('Total Expenses ($)')  
ax.legend(title='Expense Type', bbox_to_anchor=(1.05, 1), loc='upper left')  

fig.tight_layout(rect=[0, 0, 1, 0.95])  
fig.savefig(os.path.join(OUTPUT_DIR, 'trip_expense_comparison.png'))  
```

**Why it’s better:**
Using `fig.tight_layout(rect=[...])` gives finer control over margins. It ensures the title doesn't get clipped when saving the plot, unlike the default behavior of `plt.tight_layout()` which sometimes miscalculates the top margin.
