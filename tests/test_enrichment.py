"""Positive and negative unit tests for EnrichmentProcessor."""

from datetime import date
from decimal import Decimal

from sample_assignment.enrichment import EnrichmentProcessor


def _customers_df(spark):
    return spark.createDataFrame(
        [
            (
                "AB-10001",
                "Alice Brown",
                "alice@example.com",
                "202-555-0101",
                "1 First Street",
                "Consumer",
                "United States",
                "Seattle",
                "Washington",
                "98101",
                "West",
            ),
        ],
        [
            "customer_id",
            "customer_name",
            "email",
            "phone",
            "address",
            "segment",
            "country",
            "city",
            "state",
            "postal_code",
            "region",
        ],
    )


def _products_df(spark):
    return spark.createDataFrame(
        [
            (
                "TEC-PH-10000001",
                "Technology",
                "Phones",
                "Business Phone",
                "Washington",
                100.00,
            ),
        ],
        [
            "product_id",
            "category",
            "sub_category",
            "product_name",
            "state",
            "price_per_product",
        ],
    )


def _orders_df(spark, customer_id="AB-10001"):
    return spark.createDataFrame(
        [
            (
                1,
                "CA-2024-1001",
                date(2024, 1, 10),
                date(2024, 1, 12),
                "Standard Class",
                customer_id,
                "TEC-PH-10000001",
                2,
                200.00,
                0.10,
                10.126,
            ),
        ],
        [
            "row_id",
            "order_id",
            "order_date",
            "ship_date",
            "ship_mode",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "discount",
            "profit",
        ],
    )


# ==========================================================
# Positive test
# ==========================================================

def test_build_enriched_order_sales(spark):
    """An Order should receive Customer and Product attributes."""

    processor = EnrichmentProcessor(spark)
    customers_df = processor.build_customers(
        _customers_df(spark)
    )
    products_df = processor.build_products(
        _products_df(spark)
    )

    result = processor.build_order_sales(
        _orders_df(spark),
        customers_df,
        products_df,
    ).first()

    assert result["order_id"] == "CA-2024-1001"
    assert result["customer_name"] == "Alice Brown"
    assert result["country"] == "United States"
    assert result["category"] == "Technology"
    assert result["sub_category"] == "Phones"
    assert result["profit"] == Decimal("10.13")


# ==========================================================
# Negative test
# ==========================================================

def test_order_with_unknown_customer_is_not_enriched(spark):
    """An Order without a matching Customer must not enter the master table."""

    processor = EnrichmentProcessor(spark)
    customers_df = processor.build_customers(
        _customers_df(spark)
    )
    products_df = processor.build_products(
        _products_df(spark)
    )

    result_df = processor.build_order_sales(
        _orders_df(spark, customer_id="UNKNOWN"),
        customers_df,
        products_df,
    )

    assert result_df.count() == 0
