"""Unit test for AggregationProcessor."""

from datetime import date
from decimal import Decimal

from sample_assignment.aggregation import AggregationProcessor


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
                10.126,
            ),
            (
                date(2024, 2, 15),
                "Technology",
                "Phones",
                "AB-10001",
                "Alice Brown",
                20.445,
            ),
            (
                date(2025, 3, 20),
                "Office Supplies",
                "Paper",
                "CD-10002",
                "Bob Davis",
                5.00,
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

    alice_2024 = results[(2024, "AB-10001")]

    assert alice_2024["category"] == "Technology"
    assert alice_2024["sub_category"] == "Phones"
    assert alice_2024["customer_name"] == "Alice Brown"
    assert alice_2024["total_profit"] == Decimal("30.57")

    bob_2025 = results[(2025, "CD-10002")]

    assert bob_2025["category"] == "Office Supplies"
    assert bob_2025["sub_category"] == "Paper"
    assert bob_2025["total_profit"] == Decimal("5.00")