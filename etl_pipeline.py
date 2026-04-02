"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
import time
from datetime import datetime, UTC
datetime.now(UTC).isoformat()

def ensure_metadata_table(engine):
    create_sql = """
    CREATE TABLE IF NOT EXISTS etl_metadata (
        run_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        rows_processed INTEGER,
        status VARCHAR(50)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def extract(engine, last_run_time=None):
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)

    if last_run_time is None:
        orders = pd.read_sql("SELECT * FROM orders", engine)
    else:
        orders_query = text("""
            SELECT * FROM orders
            WHERE order_date > :last_run_time
        """)
        orders = pd.read_sql(orders_query, engine, params={"last_run_time": last_run_time})

    if orders.empty:
        order_items = pd.DataFrame(columns=["item_id", "order_id", "product_id", "quantity"])
    else:
        order_ids = orders["order_id"].tolist()

        if len(order_ids) == 1:
            items_query = text("SELECT * FROM order_items WHERE order_id = :order_id")
            order_items = pd.read_sql(items_query, engine, params={"order_id": order_ids[0]})
        else:
            ids_str = ",".join(str(order_id) for order_id in order_ids)
            items_query = f"SELECT * FROM order_items WHERE order_id IN ({ids_str})"
            order_items = pd.read_sql(items_query, engine)

    print(f"Extracted customers: {len(customers)}")
    print(f"Extracted products: {len(products)}")
    print(f"Extracted orders: {len(orders)}")
    print(f"Extracted order_items: {len(order_items)}")

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    print(f"Orders before filtering: {len(orders)}")
    print(f"Order items before filtering: {len(order_items)}")

    orders = orders[orders["status"] != "cancelled"]
    order_items = order_items[order_items["quantity"] <= 100]

    print(f"Orders after filtering cancelled: {len(orders)}")
    print(f"Order items after filtering suspicious quantity: {len(order_items)}")

    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="inner")
    merged = merged.merge(customers, on="customer_id", how="inner")

    if merged.empty:
        print("No rows to transform after filtering and joins.")
        return pd.DataFrame(columns=[
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
            "is_outlier",
        ])
    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    summary = merged.groupby(
        ["customer_id", "customer_name", "city"],
        as_index=False
    ).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    )

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    mean_revenue = summary["total_revenue"].mean()
    std_revenue = summary["total_revenue"].std()
    
    if pd.isna(std_revenue):
        summary["is_outlier"] = False
    else:
        threshold = mean_revenue + 3 * std_revenue
        summary["is_outlier"] = summary["total_revenue"] > threshold

    category_revenue = merged.groupby(
        ["customer_id", "category"],
        as_index=False
    ).agg(
        category_revenue=("line_total", "sum")
    )

    category_revenue = category_revenue.sort_values(
        ["customer_id", "category_revenue", "category"],
        ascending=[True, False, True]
    )

    top_category = category_revenue.drop_duplicates(subset=["customer_id"])[
        ["customer_id", "category"]
    ].rename(columns={"category": "top_category"})

    summary = summary.merge(top_category, on="customer_id", how="left")

    summary = summary[
        [
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
            "is_outlier"
        ]
    ]

    print(f"Transformed customer summary rows: {len(summary)}")
    return summary

def get_last_successful_run(engine):
    query = """
    SELECT MAX(end_time) AS last_run
    FROM etl_metadata
    WHERE status = 'SUCCESS';
    """
    result = pd.read_sql(query, engine)
    return result.loc[0, "last_run"]

def log_etl_run(engine, start_time, end_time, rows_processed, status):
    insert_sql = """
    INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
    VALUES (:start_time, :end_time, :rows_processed, :status)
    """
    with engine.begin() as conn:
        conn.execute(
            text(insert_sql),
            {
                "start_time": start_time,
                "end_time": end_time,
                "rows_processed": rows_processed,
                "status": status,
            }
        )

def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    if df.empty:
        print("Validation skipped: transformed DataFrame is empty.")
        return {
            "no_null_customer_id": True,
            "no_null_customer_name": True,
            "positive_total_revenue": True,
            "unique_customer_id": True,
            "positive_total_orders": True,
        }
    
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "positive_total_revenue": (df["total_revenue"] > 0).all(),
        "unique_customer_id": not df["customer_id"].duplicated().any(),
        "positive_total_orders": (df["total_orders"] > 0).all(),
    }

    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check_name}: {status}")

    if not all(checks.values()):
        raise ValueError("Validation failed: one or more critical checks did not pass.")

    return checks


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    if df.empty:
        print("No rows to load. Writing empty output.")
    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows into customer_analytics")
    print(f"Saved CSV to {csv_path}")

def generate_quality_report(df, checks, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    required_cols = {"is_outlier", "customer_id", "total_revenue"}
    if required_cols.issubset(df.columns):
        outliers = df.loc[df["is_outlier"], ["customer_id", "total_revenue"]].to_dict(orient="records")
    else:
        outliers = []

    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "total_records_checked": len(df),
        "checks_passed": [name for name, passed in checks.items() if passed],
        "checks_failed": [name for name, passed in checks.items() if not passed],
        "flagged_outliers": outliers,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Saved quality report to {output_path}")

def main():
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5433/amman_market"
    )

    engine = create_engine(database_url)

    ensure_metadata_table(engine)

    start_time = datetime.now(UTC)
    timer_start = time.time()
    rows_processed = 0

    try:
        last_run_time = get_last_successful_run(engine)

        if last_run_time is None:
            print("No previous successful ETL run found. Running full load.")
        else:
            print(f"Last successful ETL run: {last_run_time}")
            print("Running incremental load.")

        data_dict = extract(engine, last_run_time=last_run_time)
        transformed_df = transform(data_dict)
        checks = validate(transformed_df)

        rows_processed = len(transformed_df)

        generate_quality_report(transformed_df, checks, "output/quality_report.json")
        load(transformed_df, engine, "output/customer_analytics.csv")

        end_time = datetime.now(UTC)
        elapsed = time.time() - timer_start

        log_etl_run(engine, start_time, end_time, rows_processed, "SUCCESS")

        print(f"Rows processed: {rows_processed}")
        print(f"Execution time: {elapsed:.2f} seconds")
        print("ETL pipeline completed successfully.")

    except Exception:
        end_time = datetime.now(UTC)
        log_etl_run(engine, start_time, end_time, rows_processed, "FAILED")
        raise


if __name__ == "__main__":
    main()
