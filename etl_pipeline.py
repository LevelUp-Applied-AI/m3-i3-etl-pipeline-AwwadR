"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

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

    summary = summary[
        [
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
        ]
    ]

    print(f"Transformed customer summary rows: {len(summary)}")
    return summary


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
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "positive_total_revenue": (df["total_revenue"] > 0).all(),
        "unique_customer_id": ~df["customer_id"].duplicated().any(),
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
    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows into customer_analytics")
    print(f"Saved CSV to {csv_path}")


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5433/amman_market"
    )

    engine = create_engine(database_url)

    print("Starting ETL pipeline...")

    data_dict = extract(engine)
    transformed_df = transform(data_dict)
    validate(transformed_df)
    load(transformed_df, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()
