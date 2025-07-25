# 🚛 Trucking Operations Platform

<div align="center">

![Python](https://img.shields.io/badge/Python-v3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-11557c?style=for-the-badge&logo=python&logoColor=white)
![Seaborn](https://img.shields.io/badge/Seaborn-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=for-the-badge&logo=google-sheets&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Active-success?style=for-the-badge)

<br></br>

</div>

> 📖 **Click the three lines (☰) in the top-right corner to see the Table of Contents**

This project is a complete data pipeline and analysis platform built to transform a one-truck, family-run business from manual record-keeping into a data-driven operation. It automates the process of collecting, storing, and analyzing operational data to provide actionable insights on profitability, route performance, and spending habits.
<br></br>

## Project Goal

The primary goal was to replace a cumbersome, manual Excel workflow with a robust, automated system. By leveraging a centralized database and Python scripting, the platform calculates critical Key Performance Indicators (KPIs) and generates clear, visual reports to help the business make smarter, more profitable decisions.
<br></br>

## Key Features & Technology

* **User-Friendly Data Entry:** Utilizes a simple, intuitive **Google Sheet** as a front-end, allowing for easy data entry (including dropdowns and date pickers) without requiring technical knowledge.
* **Automated ETL Pipeline:** A Python script (`etl_script.py`) automatically **E**xtracts data from the Google Sheet via the **Google Sheets API**, **T**ransforms it into a clean format, and **L**oads it into a structured database.
* **Centralized Data Warehouse:** A **PostgreSQL** database serves as the single source of truth, ensuring data integrity and enabling powerful, complex queries.
* **Automated Reporting & Visualization:** A second Python script (`py_visualization.py`) connects to the database, runs a suite of analysis queries, and uses **Matplotlib & Seaborn** to generate a series of insightful charts and a summary report.
* **Secure & Organized Codebase:** Follows best practices by separating credentials into an untracked `config.py` file and organizing SQL queries and reports into their own directories.
<br></br>

## Live Demo & Insights

The analysis script generates a series of reports that uncover key business insights. Here are the findings from a sample dataset:
<br></br>

### 1. Trip Profitability Analysis

The primary analysis focuses on the net profit of each individual trip. The results immediately highlight the best and worst-performing loads.

<img width="1200" height="800" alt="trip_profitability" src="https://github.com/user-attachments/assets/21770b34-2e75-4c6d-b7e2-9e2a835d35ea" />

**Insight:** While the trips to New York City offered high revenue, their profitability was inconsistent. The trip to **Charlotte, NC was the most profitable**, while the trip to **Chicago, IL resulted in a significant loss of over $800**.
<br></br>

### 2. Route Performance

Beyond individual trips, the platform analyzes the average profit per mile for recurring routes, helping to identify which lanes are consistently profitable.

<img width="1200" height="800" alt="route_performance" src="https://github.com/user-attachments/assets/ede4345a-2fc9-4bdd-94a2-a13e8c639aaa" />

**Insight:** The route to **Charlotte, NC** is the most efficient, generating the highest average profit per mile. Conversely, the route to **Chicago, IL** is, on average, unprofitable.
<br></br>

### 3. Expense Analysis

Understanding *why* a trip was unprofitable is crucial. The platform provides both a high-level overview of expenses and a detailed breakdown for specific trips.

| Total Expense Breakdown             | Expense Breakdown for the Unprofitable Trip      |
| ----------------------------------- | ------------------------------------------------ |
| <img width="1000" height="700" alt="expense_breakdown" src="https://github.com/user-attachments/assets/f3d49403-f110-4ed9-beed-4471c3fa9f3c" /> | <img width="1000" height="600" alt="july_26_expense_breakdown" src="https://github.com/user-attachments/assets/ef32de14-96e6-4e09-a9ef-8941369003c1" /> |

**Insight:** Overall, **Maintenance** and **Fines** make up over 67% of non-fuel expenses. The unprofitable Chicago trip was almost entirely sunk by a single, **$450 emergency tire replacement**, highlighting the massive impact of unplanned maintenance.
<br></br>

### 4. Expense Comparison of Key Trips

The platform can compare the expense structures of different types of trips (e.g., high profit vs. low profit) to identify patterns.

<img width="1200" height="800" alt="trip_expense_comparison" src="https://github.com/user-attachments/assets/497d5d71-751d-40f8-96b2-5ea222a71b7c" />

**Insight:** This chart clearly shows that the **Low Profit** trip was destroyed by a massive maintenance cost, while the **Average Profit** trip had significant Toll expenses. This confirms that managing unplanned events and high-toll routes is key to profitability.
<br></br>

## System Architecture

The project follows a classic ETL architecture, separating the data entry, data storage, and data analysis layers.

1.  **Data Entry:** The user (Dad) enters all load, fuel, and receipt data into a shared Google Sheet. For future enhancements, this process could be further streamlined by using **Gemini AI** to automatically parse details from photos of receipts or load confirmation sheets directly within Google Sheets.
2.  **ETL Script:** The `etl_script.py` is run. It authenticates with the **Google Sheets API**, reads all the data, cleans it, and loads it into the PostgreSQL database, clearing old data to prevent duplicates.
3.  **Analysis Script:** The `py_visualization.py` is run. It connects to the database, executes a series of SQL queries, and uses the results to generate the charts and insights seen above.
<br></br>

## Setup & Usage

Follow these steps to get the project running on your local machine.

1.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url>
    cd trucking-analytics
    ```
2.  **Set Up Environment:**
    ```bash
    # Create and activate a virtual environment
    python -m venv venv
    source venv/bin/activate
    # Install dependencies
    pip install -r requirements.txt
    ```
3.  **Database Setup:**
    * Ensure PostgreSQL is running.
    * Create a database (e.g., `createdb trucking_db`).
    * Create the table schema: `psql -d trucking_db -f sql/create_tables.sql`.
4.  **Configure Credentials:**
    * Follow the Google API setup guide to generate a `credentials.json` file and place it in the root directory.
    * Copy `config.py.example` to `config.py` and fill in your database credentials.
5.  **Run the Pipeline:**
    ```bash
    # Load data from Google Sheets into the database
    python etl_script.py

    # Run the analysis and generate reports
    python py_visualization.py
    ```

## Development Notes

This project was built iteratively, and several bugs were identified and fixed during development. For a detailed log of the issues encountered (including `KeyError`, `TypeError`, and indexing bugs) and their solutions, please see `troubleshooting.md`.
<br></br>

## Future Enhancements

* **Tableau Integration:** The clean, structured PostgreSQL database is the perfect data source for a BI tool. The next step is to connect Tableau to the database to create an interactive, filterable dashboard for real-time data exploration.
* **Automated PDF/Email Reporting:** The analysis script can be extended to compile the generated charts and insights into a formatted PDF report, which can then be automatically emailed to stakeholders (myself and my dad) on a schedule.
* **Scheduled Execution:** The Python scripts could be scheduled to run automatically (e.g., daily) using a tool like Cron on a local machine or a cloud-based service like GitHub Actions.
