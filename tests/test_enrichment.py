"""Unit test for EnrichmentProcessor."""

from datetime import date
from decimal import Decimal

from sample_assignment.enrichment import EnrichmentProcessor


def test_build_enriched_order_sales(spark):
    """Order data should be enriched with Customer and Product details."""

    customers_source_df = spark.createDataFrame(
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

    products_source_df = spark.createDataFrame(
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

    orders_source_df = spark.createDataFrame(
        [
            (
                1,
                "CA-2024-1001",
                date(2024, 1, 10),
                date(2024, 1, 12),
                "Standard Class",
                "AB-10001",
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

    processor = EnrichmentProcessor(spark)

    customers_df = processor.build_customers(
        customers_source_df
    )
    products_df = processor.build_products(
        products_source_df
    )
    order_sales_df = processor.build_order_sales(
        orders_source_df,
        customers_df,
        products_df,
    )

    result = order_sales_df.first()

    assert customers_df.count() == 1
    assert products_df.count() == 1
    assert order_sales_df.count() == 1

    assert result["order_id"] == "CA-2024-1001"
    assert result["customer_name"] == "Alice Brown"
    assert result["country"] == "United States"
    assert result["category"] == "Technology"
    assert result["sub_category"] == "Phones"
    assert result["profit"] == Decimal("10.13")