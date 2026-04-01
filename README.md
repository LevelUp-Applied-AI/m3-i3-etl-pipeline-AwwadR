[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market Customer Analytics

## Overview
This project builds a Python ETL pipeline for the fictional **Amman Digital Market** database.

The pipeline:
- extracts data from PostgreSQL
- transforms it into customer-level analytics using pandas
- validates the transformed data with quality checks
- loads the result into a new PostgreSQL table and a CSV file

The final output is a customer analytics summary that includes:
- customer ID
- customer name
- city
- total number of orders
- total revenue
- average order value
- top product category

---

## Setup

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd m3-i3-etl-pipeline-AwwadR
```

### 2. Create and activate a virtual environment
```bash
python -m venv .venv
source .venv/Scripts/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Start PostgreSQL with Docker
```bash
docker run -d --name postgres-m3-int \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=amman_market \
  -p 5433:5432 \
  -v pgdata_m3_int:/var/lib/postgresql/data \
  postgres:15-alpine
```

### 5. Load the schema and seed data
```bash
docker cp schema.sql postgres-m3-int:/schema.sql
docker cp seed_data.sql postgres-m3-int:/seed_data.sql

MSYS_NO_PATHCONV=1 docker exec -it postgres-m3-int psql -U postgres -d amman_market -f /schema.sql
MSYS_NO_PATHCONV=1 docker exec -it postgres-m3-int psql -U postgres -d amman_market -f /seed_data.sql
```

### 6. Verify tables
```bash
psql -h localhost -p 5433 -U postgres -d amman_market -c "\\dt"
```

The database should contain these tables:
- customers
- products
- orders
- order_items

---

## How to run

Run the ETL pipeline with:

```bash
python etl_pipeline.py
```

Run the tests with:

```bash
pytest tests/test_etl.py -v
```

---

## Output

The pipeline produces two outputs:

### 1. PostgreSQL table
A new table named:

```text
customer_analytics
```

### 2. CSV file
Saved at:

```text
output/customer_analytics.csv
```

The output contains these columns:
- `customer_id`
- `customer_name`
- `city`
- `total_orders`
- `total_revenue`
- `avg_order_value`
- `top_category`

---

## Transformation logic

The pipeline performs the following transformations:

1. Extracts all data from:
   - customers
   - products
   - orders
   - order_items

2. Joins orders with:
   - order_items
   - products
   - customers

3. Computes:
   - `line_total = quantity * unit_price`

4. Filters out:
   - cancelled orders
   - suspicious quantities greater than 100

5. Aggregates data to customer level:
   - total distinct orders
   - total revenue
   - average order value
   - top category by revenue

---

## Quality checks

The `validate()` step performs these checks:

- no null values in `customer_id`
- no null values in `customer_name`
- all `total_revenue` values are greater than 0
- no duplicate `customer_id` values
- all `total_orders` values are greater than 0

These checks help ensure the final analytics table is valid and reliable before loading.

If any critical validation fails, the pipeline raises a `ValueError`.

---

## Notes

- The ETL uses SQLAlchemy to connect to PostgreSQL.
- Pandas is used for joins, filtering, aggregation, and validation.
- The database connection defaults to:

```text
postgresql+psycopg://postgres:postgres@localhost:5433/amman_market
```

If needed, this can be overridden with the `DATABASE_URL` environment variable.

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
