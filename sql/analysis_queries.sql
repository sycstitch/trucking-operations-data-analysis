-- =================================================================
-- SQL Insight & KPI Queries
-- Description: These queries perform the core analysis of the trucking data.
-- =================================================================

-- Query 1: Comprehensive Load Profitability Report
-- This is the main query. It calculates all key metrics for each load.
SELECT
    l.load_id,
    l.load_date,
    l.pickup_location,
    l.dropoff_location,
    l.revenue,
    l.total_miles,
    COALESCE(SUM(fs.total_cost), 0) AS total_fuel_cost,
    COALESCE(SUM(fs.gallons), 0) AS total_gallons_used,
    COALESCE(SUM(e.amount), 0) AS total_other_expenses,
    -- Calculate Cost Per Mile (CPM)
    (COALESCE(SUM(fs.total_cost), 0) + COALESCE(SUM(e.amount), 0)) / l.total_miles AS cost_per_mile,
    -- Calculate Miles Per Gallon (MPG)
    l.total_miles / NULLIF(COALESCE(SUM(fs.gallons), 0), 0) AS miles_per_gallon,
    -- Calculate Net Profit
    l.revenue - (COALESCE(SUM(fs.total_cost), 0) + COALESCE(SUM(e.amount), 0)) AS net_profit
FROM
    loads l
LEFT JOIN
    fuel_stops fs ON l.load_id = fs.load_id
LEFT JOIN
    expenses e ON l.load_id = e.load_id
GROUP BY
    l.load_id
ORDER BY
    l.load_date DESC;


-- Query 2: Monthly Expense Breakdown by Category
-- Useful for seeing where the money is going over time.
SELECT
    TO_CHAR(expense_date, 'YYYY-MM') AS month,
    category,
    SUM(amount) AS total_spent
FROM
    expenses
GROUP BY
    month,
    category
ORDER BY
    month,
    total_spent DESC;


-- Query 3: Route Performance Analysis
-- Averages key metrics by route (pickup/dropoff pair) to find the best/worst performing lanes.
SELECT
    pickup_location,
    dropoff_location,
    COUNT(load_id) AS number_of_trips,
    AVG(l.total_miles / NULLIF(fs.total_gallons_used, 0)) AS avg_mpg,
    AVG((fs.total_fuel_cost + ex.total_other_expenses) / l.total_miles) AS avg_cost_per_mile,
    AVG(l.revenue / l.total_miles) AS avg_revenue_per_mile,
    AVG(l.revenue - (fs.total_fuel_cost + ex.total_other_expenses)) AS avg_net_profit
FROM
    loads l
JOIN
    -- Pre-aggregate fuel data per load
    (SELECT load_id, SUM(total_cost) AS total_fuel_cost, SUM(gallons) AS total_gallons_used FROM fuel_stops GROUP BY load_id) fs
    ON l.load_id = fs.load_id
JOIN
    -- Pre-aggregate expense data per load
    (SELECT load_id, SUM(amount) AS total_other_expenses FROM expenses GROUP BY load_id) ex
    ON l.load_id = ex.load_id
GROUP BY
    pickup_location,
    dropoff_location
ORDER BY
    avg_net_profit DESC;