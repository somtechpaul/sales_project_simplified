"""Unit tests for DataQualityProcessor."""

from datetime import date

from sample_assignment.data_quality import DataQualityProcessor


def test_customer_cleaning(spark):
    """Customer values should be standardized before validation."""

    source_df = spark.createDataFrame(
        [
            (
                "ab-10001",
                "  Alice   Brown  ",
                "ALICE@EXAMPLE.COM",
                "1234",
            ),
        ],
        [
            "customer_id",
            "customer_name",
            "email",
            "postal_code",
        ],
    )

    processor = DataQualityProcessor(spark)
    result = processor._clean_customers(source_df).first()

    assert result["customer_id"] == "AB-10001"
    assert result["customer_name"] == "Alice Brown"
    assert result["email"] == "alice@example.com"
    assert result["postal_code"] == "01234"


def test_missing_and_invalid_customer_names(
    spark,
    monkeypatch,
):
    """Missing and corrupted Customer names should be rejected."""

    source_df = spark.createDataFrame(
        [
            ("AB-10001", "Alice Brown"),
            ("CD-10002", None),
            ("EF-10003", "Gary567 Hansen"),
        ],
        ["customer_id", "customer_name"],
    )

    rules = [
        {
            "rule_type": "NOT_NULL",
            "column_name": "customer_name",
            "rule_parameter": None,
            "failure_reason": "CUSTOMER_NAME_MISSING",
        },
        {
            "rule_type": "VALID_CUSTOMER_NAME",
            "column_name": "customer_name",
            "rule_parameter": None,
            "failure_reason": "CUSTOMER_NAME_INVALID",
        },
    ]

    processor = DataQualityProcessor(spark)
    monkeypatch.setattr(
        processor,
        "_load_rules",
        lambda dataset_name: rules,
    )

    passed_df, rejected_df = processor._apply_rules(
        source_df,
        "customers",
    )

    assert passed_df.count() == 1
    assert rejected_df.count() == 2

    rejected = {
        row["customer_id"]: row["dq_failure_reasons"]
        for row in rejected_df.collect()
    }

    assert rejected["CD-10002"] == ["CUSTOMER_NAME_MISSING"]
    assert rejected["EF-10003"] == ["CUSTOMER_NAME_INVALID"]


def test_duplicate_product_ids_are_rejected(
    spark,
    monkeypatch,
):
    """Every row belonging to a duplicate Product ID should fail."""

    source_df = spark.createDataFrame(
        [
            ("PRODUCT-1",),
            ("PRODUCT-1",),
            ("PRODUCT-2",),
        ],
        ["product_id"],
    )

    rules = [
        {
            "rule_type": "UNIQUE",
            "column_name": "product_id",
            "rule_parameter": None,
            "failure_reason": "DUPLICATE_PRODUCT_ID",
        },
    ]

    processor = DataQualityProcessor(spark)
    monkeypatch.setattr(
        processor,
        "_load_rules",
        lambda dataset_name: rules,
    )

    passed_df, rejected_df = processor._apply_rules(
        source_df,
        "products",
    )

    assert passed_df.first()["product_id"] == "PRODUCT-2"
    assert rejected_df.count() == 2


def test_order_date_parsing(spark):
    """Valid dates should parse and invalid dates should become null."""

    source_df = spark.createDataFrame(
        [
            (
                "ca-2024-1001",
                "ab-10001",
                "product-1",
                "10/1/2024",
                "12/1/2024",
            ),
            (
                "ca-2024-1002",
                "ab-10001",
                "product-1",
                "bad-date",
                "13/1/2024",
            ),
        ],
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "ship_date",
        ],
    )

    processor = DataQualityProcessor(spark)

    results = {
        row["order_id"]: row
        for row in processor._clean_orders(source_df).collect()
    }

    assert results["CA-2024-1001"]["order_date"] == date(2024, 1, 10)
    assert results["CA-2024-1001"]["order_date_source"] == "10/1/2024"
    assert results["CA-2024-1002"]["order_date"] is None