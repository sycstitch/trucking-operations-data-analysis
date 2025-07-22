-- =================================================================
-- SQL CREATE TABLE Statements (Updated)
-- Description: These commands create the database structure based on our schema.
--              Includes 'notes' columns for additional context.
-- Database: PostgreSQL
-- =================================================================

-- Table 1: loads
-- Stores primary information for each trucking load.
CREATE TABLE loads (
    load_id SERIAL PRIMARY KEY,
    load_date DATE NOT NULL,
    pickup_location VARCHAR(255),
    dropoff_location VARCHAR(255),
    total_miles INTEGER NOT NULL CHECK (total_miles > 0),
    revenue DECIMAL(10, 2) NOT NULL,
    is_drop_and_hook BOOLEAN DEFAULT false,
    wait_time_hours DECIMAL(4, 2) DEFAULT 0,
    notes TEXT
);

-- Table 2: fuel_stops
-- Tracks all fuel purchases, linked to a specific load.
CREATE TABLE fuel_stops (
    fuel_id SERIAL PRIMARY KEY,
    load_id INTEGER NOT NULL REFERENCES loads(load_id),
    stop_date DATE NOT NULL,
    gallons DECIMAL(6, 3) NOT NULL CHECK (gallons > 0),
    total_cost DECIMAL(10, 2) NOT NULL CHECK (total_cost > 0),
    location VARCHAR(255),
    notes TEXT -- Added for contextual information
);

-- Table 3: expenses
-- Tracks all non-fuel operational expenses, linked to a specific load.
CREATE TABLE expenses (
    expense_id SERIAL PRIMARY KEY,
    load_id INTEGER NOT NULL REFERENCES loads(load_id),
    expense_date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL CHECK (amount >= 0),
    description TEXT, -- This will store the 'Item Description'
    notes TEXT -- Added for contextual information
);