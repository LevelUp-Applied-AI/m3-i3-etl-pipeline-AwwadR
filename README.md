[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)

# ETL Pipeline — Amman Digital Market

## Overview

This project builds a Python ETL pipeline for the fictional **Amman Digital Market** database.

The pipeline:
- extracts data from PostgreSQL
- transforms raw marketplace data into analytics summaries using pandas
- validates data quality before loading
- loads results into PostgreSQL tables and CSV files

The base pipeline creates a **customer analytics** summary with:
- customer ID
- customer name
- city
- total number of orders
- total revenue
- average order value
- top product category

In the challenge extensions, the project was expanded to include:
- **Tier 1:** outlier detection and quality reporting
- **Tier 2:** incremental ETL with metadata tracking
- **Tier 3:** a reusable config-driven ETL framework with logging and support for multiple pipeline types

---

## Setup

1. Create and activate a virtual environment:
```bash
   python -m venv .venv
   source .venv/Scripts/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start PostgreSQL container:
```bash
docker run -d --name postgres-m3-int \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=amman_market \
  -p 5433:5432 \
  -v pgdata_m3_int:/var/lib/postgresql/data \
  postgres:15-alpine
```

4. Load schema and seed data:
```bash
docker cp schema.sql postgres-m3-int:/schema.sql
docker cp seed_data.sql postgres-m3-int:/seed_data.sql

MSYS_NO_PATHCONV=1 docker exec -it postgres-m3-int psql -U postgres -d amman_market -f /schema.sql
MSYS_NO_PATHCONV=1 docker exec -it postgres-m3-int psql -U postgres -d amman_market -f /seed_data.sql
```

5. Verify tables:
```bash
psql -h localhost -p 5433 -U postgres -d amman_market -c "\dt"
```

The database should contain:
- `customers`
- `products`
- `orders`
- `order_items`

---

## How to Run

### Base / Customer pipeline
```bash
python etl_pipeline.py
```

or:

```bash
python etl_pipeline.py config/customer_analytics.json
```

### Product analytics pipeline (Tier 3)
```bash
python etl_pipeline.py config/product_analytics.json
```

### Run tests
```bash
pytest tests/test_etl.py -v
```

---

## Output

### Base output
The base ETL pipeline produces:

#### PostgreSQL table
- `customer_analytics`

#### CSV file
- `output/customer_analytics.csv`

The customer analytics output contains:
- `customer_id`
- `customer_name`
- `city`
- `total_orders`
- `total_revenue`
- `avg_order_value`
- `top_category`

---

## Quality Checks

The customer validation step performs these checks:
- no null values in `customer_id`
- no null values in `customer_name`
- all `total_revenue` values are greater than 0
- no duplicate `customer_id` values
- all `total_orders` values are greater than 0

The product validation step performs similar checks for:
- `product_id`
- `product_name`
- `total_revenue`
- uniqueness of `product_id`
- positive `total_orders`

These checks help ensure the final analytics tables are valid and reliable before loading.

---

# Challenge Extensions

## Tier 1 — Advanced Quality Checks and Reporting

Tier 1 adds outlier detection and reporting to the customer analytics pipeline.

### Added features
- detects revenue outliers using:
  - `total_revenue > mean + 3 * std`
- adds an `is_outlier` column to the customer summary
- generates a JSON quality report:
  - `output/quality_report.json`

### Tier 1 outputs
The customer analytics output now also includes:
- `is_outlier`

The quality report contains:
- ETL timestamp
- total records checked
- checks passed
- checks failed
- flagged outliers

---

## Tier 2 — Incremental ETL with Change Detection

Tier 2 adds incremental ETL support and metadata tracking.

### Added features
- the pipeline checks the timestamp of the last successful ETL run
- on later runs, only orders newer than the last successful run are processed
- ETL run history is stored in a PostgreSQL metadata table:
  - `etl_metadata`

### `etl_metadata` table fields
- `run_id`
- `start_time`
- `end_time`
- `rows_processed`
- `status`

## Full vs Incremental Run Comparison

The pipeline was tested in both full-load mode and incremental-load mode.

### Full run
- Rows processed: 85
- Execution time: 0.24 seconds

### Incremental run
- Rows processed: 0
- Execution time: 0.33 seconds

### Tradeoffs
- A full load is simpler because it processes the entire dataset every time.
- A full load is useful for initial setup and for rebuilding analytics from scratch.
- An incremental load is faster because it only processes data newer than the last successful ETL run.
- Incremental ETL reduces repeated work, but it requires extra logic and metadata tracking.
- Incremental ETL can return 0 rows when no new data is available, which is expected behavior.

---

## Tier 3 — ETL Framework with Configuration and Logging

Tier 3 turns the project into a reusable ETL framework.

### Added features
- the pipeline reads settings from JSON config files
- logging is used instead of `print()` statements
- the same ETL framework can run multiple analytics pipelines
- two pipeline configurations are included:
  - `config/customer_analytics.json`
  - `config/product_analytics.json`

### Supported pipeline types
1. **Customer analytics**
   - outputs customer-level summary
   - saves:
     - `output/customer_analytics.csv`
     - `output/quality_report.json`

2. **Product analytics**
   - outputs product-level summary
   - saves:
     - `output/product_analytics.csv`
     - `output/product_quality_report.json`

### Design goal
Adding a new ETL pipeline only requires creating a new config file and reusing the same ETL framework code.

---

## Project Structure

```text
m3-i3-etl-pipeline-AwwadR/
├── etl_pipeline.py
├── schema.sql
├── seed_data.sql
├── README.md
├── requirements.txt
├── config/
│   ├── customer_analytics.json
│   └── product_analytics.json
├── tests/
│   └── test_etl.py
└── output/
    ├── customer_analytics.csv
    ├── product_analytics.csv
    ├── quality_report.json
    └── product_quality_report.json
```

---

## Notes

- The database connection defaults to:
  ```text
  postgresql+psycopg://postgres:postgres@localhost:5433/amman_market
  ```
- This can be overridden using the `DATABASE_URL` environment variable.
- For Git Bash on Windows, `MSYS_NO_PATHCONV=1` was used when loading SQL files into the Docker container.

---

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.