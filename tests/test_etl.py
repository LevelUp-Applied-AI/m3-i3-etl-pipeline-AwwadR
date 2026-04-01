"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    data = {
        "customers": pd.DataFrame([
            {"customer_id": 1, "customer_name": "Rand", "city": "Amman"}
        ]),
        "products": pd.DataFrame([
            {"product_id": 10, "name": "Phone", "category": "Electronics", "unit_price": 100}
        ]),
        "orders": pd.DataFrame([
            {"order_id": 1, "customer_id": 1, "order_date": "2025-01-01", "status": "completed"},
            {"order_id": 2, "customer_id": 1, "order_date": "2025-01-02", "status": "cancelled"},
        ]),
        "order_items": pd.DataFrame([
            {"item_id": 1, "order_id": 1, "product_id": 10, "quantity": 2},
            {"item_id": 2, "order_id": 2, "product_id": 10, "quantity": 3},
        ]),
    }

    result = transform(data)

    assert len(result) == 1
    assert result.iloc[0]["total_orders"] == 1
    assert result.iloc[0]["total_revenue"] == 200


def test_transform_filters_suspicious_quantity():
    data = {
        "customers": pd.DataFrame([
            {"customer_id": 1, "customer_name": "Rand", "city": "Amman"}
        ]),
        "products": pd.DataFrame([
            {"product_id": 10, "name": "Phone", "category": "Electronics", "unit_price": 100}
        ]),
        "orders": pd.DataFrame([
            {"order_id": 1, "customer_id": 1, "order_date": "2025-01-01", "status": "completed"}
        ]),
        "order_items": pd.DataFrame([
            {"item_id": 1, "order_id": 1, "product_id": 10, "quantity": 150}
        ]),
    }

    result = transform(data)

    assert result.empty


def test_validate_catches_nulls():
    df = pd.DataFrame([
        {
            "customer_id": None,
            "customer_name": "Rand",
            "city": "Amman",
            "total_orders": 1,
            "total_revenue": 100,
            "avg_order_value": 100,
            "top_category": "Electronics"
        }
    ])

    with pytest.raises(ValueError):
        validate(df)