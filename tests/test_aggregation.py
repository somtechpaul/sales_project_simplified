"""Positive and edge-case unit tests for AggregationProcessor."""

from datetime import date
from decimal import Decimal

from sample_assignment.aggregation import AggregationProcessor


# ==========================================================
# Positive test
# ==========================================================

def test_profit_aggregation(spark):
    """Profit should be grouped by year, product and customer."""

    order_sales_df = spark.createDataFrame(
        [
            (
                date(2024, 1, 10),
                "Technology",
                "Phones",
                "AB-10001",
                "Alice Brown",
                Decimal("10.13"),
            ),
            (
                date(2024, 2, 15),
                "Technology",
                "Phones",
                "AB-10001",
                "Alice Brown",
                Decimal("20.45"),
            ),
            (
                date(2025, 3, 20),
                "Office Supplies",
                "Paper",
                "CD-10002",
                "Bob Davis",
                Decimal("5.00"),
            ),
        ],
        [
            "order_date",
            "category",
            "sub_category",
            "customer_id",
            "customer_name",
            "profit",
        ],
    )

    result_df = AggregationProcessor.build_profit_aggregate(
        order_sales_df
    )

    results = {
        (row["order_year"], row["customer_id"]): row
        for row in result_df.collect()
    }

    assert result_df.count() == 2
    assert (
        results[(2024, "AB-10001")]["total_profit"]
        == Decimal("30.58")
    )
    assert (
        results[(2025, "CD-10002")]["total_profit"]
        == Decimal("5.00")
    )


# ==========================================================
# Negative-profit edge case
# ==========================================================

def test_profit_aggregation_preserves_losses(spark):
    """Negative profit is a valid loss and must remain in the aggregate."""

    order_sales_df = spark.createDataFrame(
        [
            (
                date(2024, 1, 10),
                "Technology",
                "Phones",
                "AB-10001",
                "Alice Brown",
                Decimal("10.00"),
            ),
            (
                date(2024, 1, 11),
                "Technology",
                "Phones",
                "AB-10001",
                "Alice Brown",
                Decimal("-15.00"),
            ),
        ],
        [
            "order_date",
            "category",
            "sub_category",
            "customer_id",
            "customer_name",
            "profit",
        ],
    )

    result = AggregationProcessor.build_profit_aggregate(
        order_sales_df
    ).first()

    assert result["total_profit"] == Decimal("-5.00")
