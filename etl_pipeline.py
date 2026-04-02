"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
import time
import sys
import logging
from datetime import datetime, UTC


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

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


def extract(engine, config, last_run_time=None):
    customers_table = config["source_tables"]["customers"]
    products_table = config["source_tables"]["products"]
    orders_table = config["source_tables"]["orders"]
    order_items_table = config["source_tables"]["order_items"]

    customers = pd.read_sql(f"SELECT * FROM {customers_table}", engine)
    products = pd.read_sql(f"SELECT * FROM {products_table}", engine)

    if last_run_time is None:
        orders = pd.read_sql(f"SELECT * FROM {orders_table}", engine)
    else:
        orders_query = text(f"""
            SELECT * FROM {orders_table}
            WHERE order_date > :last_run_time
        """)
        orders = pd.read_sql(orders_query, engine, params={"last_run_time": last_run_time})

    if orders.empty:
        order_items = pd.DataFrame(columns=["item_id", "order_id", "product_id", "quantity"])
    else:
        order_ids = orders["order_id"].tolist()

        if len(order_ids) == 1:
            items_query = text(f"SELECT * FROM {order_items_table} WHERE order_id = :order_id")
            order_items = pd.read_sql(items_query, engine, params={"order_id": order_ids[0]})
        else:
            ids_str = ",".join(str(order_id) for order_id in order_ids)
            items_query = f"SELECT * FROM {order_items_table} WHERE order_id IN ({ids_str})"
            order_items = pd.read_sql(items_query, engine)

    logging.info(f"Extracted customers: {len(customers)}")
    logging.info(f"Extracted products: {len(products)}")
    logging.info(f"Extracted orders: {len(orders)}")
    logging.info(f"Extracted order_items: {len(order_items)}")

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

def transform_customer(data_dict, config):
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    excluded_status = config["filters"]["excluded_order_status"]
    max_quantity = config["filters"]["max_quantity"]

    logging.info(f"Orders before filtering: {len(orders)}")
    logging.info(f"Order items before filtering: {len(order_items)}")

    orders = orders[orders["status"] != excluded_status]
    order_items = order_items[order_items["quantity"] <= max_quantity]

    logging.info(f"Orders after filtering cancelled: {len(orders)}")
    logging.info(f"Order items after filtering suspicious quantity: {len(order_items)}")

    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="inner")
    merged = merged.merge(customers, on="customer_id", how="inner")

    if merged.empty:
        logging.info("No rows to transform after filtering and joins.")
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

    mean_revenue = summary["total_revenue"].mean()
    std_revenue = summary["total_revenue"].std()

    if pd.isna(std_revenue):
        summary["is_outlier"] = False
    else:
        threshold = mean_revenue + 3 * std_revenue
        summary["is_outlier"] = summary["total_revenue"] > threshold

    summary = summary[
        [
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
            "is_outlier",
        ]
    ]

    logging.info(f"Transformed customer summary rows: {len(summary)}")
    return summary

def transform_product(data_dict, config):
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    excluded_status = config["filters"]["excluded_order_status"]
    max_quantity = config["filters"]["max_quantity"]

    logging.info(f"Orders before filtering: {len(orders)}")
    logging.info(f"Order items before filtering: {len(order_items)}")

    orders = orders[orders["status"] != excluded_status]
    order_items = order_items[order_items["quantity"] <= max_quantity]

    logging.info(f"Orders after filtering cancelled: {len(orders)}")
    logging.info(f"Order items after filtering suspicious quantity: {len(order_items)}")

    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="inner")

    if merged.empty:
        logging.info("No rows to transform after filtering and joins.")
        return pd.DataFrame(columns=[
            "product_id",
            "product_name",
            "category",
            "total_orders",
            "total_quantity_sold",
            "total_revenue",
        ])

    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    summary = merged.groupby(
        ["product_id", "product_name", "category"],
        as_index=False
    ).agg(
        total_orders=("order_id", "nunique"),
        total_quantity_sold=("quantity", "sum"),
        total_revenue=("line_total", "sum")
    )

    logging.info(f"Transformed product summary rows: {len(summary)}")
    return summary

def validate_customer(df):
    if df.empty:
        logging.info("Validation skipped: transformed DataFrame is empty.")
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
        logging.info(f"{check_name}: {status}")

    if not all(checks.values()):
        raise ValueError("Validation failed: one or more critical checks did not pass.")

    return checks

def validate_product(df):
    if df.empty:
        logging.info("Validation skipped: transformed DataFrame is empty.")
        return {
            "no_null_product_id": True,
            "no_null_product_name": True,
            "positive_total_revenue": True,
            "unique_product_id": True,
            "positive_total_orders": True,
        }

    checks = {
        "no_null_product_id": df["product_id"].notna().all(),
        "no_null_product_name": df["product_name"].notna().all(),
        "positive_total_revenue": (df["total_revenue"] > 0).all(),
        "unique_product_id": not df["product_id"].duplicated().any(),
        "positive_total_orders": (df["total_orders"] > 0).all(),
    }

    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        logging.info(f"{check_name}: {status}")

    if not all(checks.values()):
        raise ValueError("Validation failed: one or more critical checks did not pass.")

    return checks

def transform(data_dict):
    config = {
        "filters": {
            "excluded_order_status": "cancelled",
            "max_quantity": 100
        }
    }
    return transform_customer(data_dict, config)


def validate(df):
    return validate_customer(df)

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


def load(df, engine, config):
    table_name = config["output"]["table_name"]
    csv_path = config["output"]["csv_path"]

    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    if df.empty:
        logging.info("No rows to load. Writing empty output.")

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    logging.info(f"Loaded {len(df)} rows into {table_name}")
    logging.info(f"Saved CSV to {csv_path}")


def generate_quality_report(df, checks, config):
    output_path = config["output"]["quality_report_path"]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    required_cols = {"is_outlier", "customer_id", "total_revenue"}
    if required_cols.issubset(df.columns):
        outliers = df.loc[
            df["is_outlier"],
            ["customer_id", "total_revenue"]
        ].to_dict(orient="records")
    else:
        outliers = []

    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "pipeline_name": config["pipeline_name"],
        "pipeline_type": config["pipeline_type"],
        "total_records_checked": len(df),
        "checks_passed": [name for name, passed in checks.items() if passed],
        "checks_failed": [name for name, passed in checks.items() if not passed],
        "flagged_outliers": outliers,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logging.info(f"Saved quality report to {output_path}")

def main(config_path="config/customer_analytics.json"):
    config = load_config(config_path)
    engine = create_engine(config["database_url"])

    ensure_metadata_table(engine)

    start_time = datetime.now(UTC)
    timer_start = time.time()
    rows_processed = 0

    try:
        last_run_time = get_last_successful_run(engine)

        if last_run_time is None:
            logging.info("No previous successful ETL run found. Running full load.")
        else:
            logging.info(f"Last successful ETL run: {last_run_time}")
            logging.info("Running incremental load.")

        data_dict = extract(engine, config, last_run_time=last_run_time)

        if config["pipeline_type"] == "customer":
            transformed_df = transform_customer(data_dict, config)
            checks = validate_customer(transformed_df)
        elif config["pipeline_type"] == "product":
            transformed_df = transform_product(data_dict, config)
            checks = validate_product(transformed_df)
        else:
            raise ValueError(f"Unsupported pipeline_type: {config['pipeline_type']}")

        rows_processed = len(transformed_df)

        generate_quality_report(transformed_df, checks, config)
        load(transformed_df, engine, config)

        end_time = datetime.now(UTC)
        elapsed = time.time() - timer_start

        log_etl_run(engine, start_time, end_time, rows_processed, "SUCCESS")

        logging.info(f"Rows processed: {rows_processed}")
        logging.info(f"Execution time: {elapsed:.2f} seconds")
        logging.info("ETL pipeline completed successfully.")

    except Exception:
        end_time = datetime.now(UTC)
        log_etl_run(engine, start_time, end_time, rows_processed, "FAILED")
        logging.exception("ETL pipeline failed.")
        raise

if __name__ == "__main__":
    config_path = "config/customer_analytics.json"

    if len(sys.argv) > 1:
        config_path = sys.argv[1]

    main(config_path)
