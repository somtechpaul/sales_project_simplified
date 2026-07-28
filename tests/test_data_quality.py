"""Positive and negative unit tests for DataQualityProcessor."""

from datetime import date

from sample_assignment.data_quality import DataQualityProcessor


# ==========================================================
# Positive cleaning tests
# ==========================================================

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

    result = DataQualityProcessor(spark)._clean_customers(
        source_df
    ).first()

    assert result["customer_id"] == "AB-10001"
    assert result["customer_name"] == "Alice Brown"
    assert result["email"] == "alice@example.com"
    assert result["postal_code"] == "01234"


def test_valid_order_date_is_parsed(spark):
    """A date in d/M/yyyy format should be converted to a date."""

    source_df = spark.createDataFrame(
        [
            (
                "ca-2024-1001",
                "ab-10001",
                "tec-ph-10000001",
                "10/1/2024",
                "12/1/2024",
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

    result = DataQualityProcessor(spark)._clean_orders(
        source_df
    ).first()

    assert result["order_id"] == "CA-2024-1001"
    assert result["customer_id"] == "AB-10001"
    assert result["order_date"] == date(2024, 1, 10)
    assert result["ship_date"] == date(2024, 1, 12)
    assert result["order_date_source"] == "10/1/2024"


# ==========================================================
# Negative Customer and Product tests
# ==========================================================

def test_missing_and_invalid_customer_names_are_rejected(
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

    rejected = {
        row["customer_id"]: row["dq_failure_reasons"]
        for row in rejected_df.collect()
    }

    assert passed_df.count() == 1
    assert rejected_df.count() == 2
    assert rejected["CD-10002"] == ["CUSTOMER_NAME_MISSING"]
    assert rejected["EF-10003"] == ["CUSTOMER_NAME_INVALID"]


def test_duplicate_product_ids_are_rejected(spark, monkeypatch):
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


# ==========================================================
# Positive and negative Order-rule tests
# ==========================================================

def test_invalid_order_date_format_is_rejected(spark, monkeypatch):
    """Only the configured d/M/yyyy source format should pass."""

    source_df = spark.createDataFrame(
        [
            (1, "10/1/2024"),
            (2, "2024-01-10"),
        ],
        ["row_id", "order_date_source"],
    )

    rules = [
        {
            "rule_type": "DATE_FORMAT",
            "column_name": "order_date_source",
            "rule_parameter": "d/M/yyyy",
            "failure_reason": "ORDER_DATE_FORMAT_INVALID",
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
        "orders",
    )

    assert [row["row_id"] for row in passed_df.collect()] == [1]
    assert [row["row_id"] for row in rejected_df.collect()] == [2]


def test_invalid_order_values_collect_all_failures(
    spark,
    monkeypatch,
):
    """Quantity, discount, ship date and ship mode failures should be kept."""

    source_df = spark.createDataFrame(
        [
            (
                1,
                date(2024, 1, 10),
                date(2024, 1, 12),
                2,
                0.20,
                "Standard Class",
            ),
            (
                2,
                date(2024, 1, 10),
                date(2024, 1, 9),
                0,
                1.50,
                "Drone",
            ),
        ],
        [
            "row_id",
            "order_date",
            "ship_date",
            "quantity",
            "discount",
            "ship_mode",
        ],
    )

    rules = [
        {
            "rule_type": "POSITIVE",
            "column_name": "quantity",
            "rule_parameter": None,
            "failure_reason": "ORDER_QUANTITY_NOT_POSITIVE",
        },
        {
            "rule_type": "BETWEEN",
            "column_name": "discount",
            "rule_parameter": '{"min":0,"max":1}',
            "failure_reason": "ORDER_DISCOUNT_INVALID",
        },
        {
            "rule_type": "COLUMN_GTE",
            "column_name": "ship_date",
            "rule_parameter": "order_date",
            "failure_reason": "SHIP_DATE_BEFORE_ORDER_DATE",
        },
        {
            "rule_type": "ALLOWED_VALUES",
            "column_name": "ship_mode",
            "rule_parameter": (
                '["Standard Class","Second Class",'
                '"First Class","Same Day"]'
            ),
            "failure_reason": "SHIP_MODE_INVALID",
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
        "orders",
    )

    assert passed_df.first()["row_id"] == 1

    failures = set(
        rejected_df.first()["dq_failure_reasons"]
    )

    assert failures == {
        "ORDER_QUANTITY_NOT_POSITIVE",
        "ORDER_DISCOUNT_INVALID",
        "SHIP_DATE_BEFORE_ORDER_DATE",
        "SHIP_MODE_INVALID",
    }


def test_invalid_order_references_are_rejected(
    spark,
    monkeypatch,
):
    """Orders with unknown Customer or Product IDs should fail."""

    orders_df = spark.createDataFrame(
        [
            (1, "AB-10001", "TEC-PH-10000001"),
            (2, "UNKNOWN", "UNKNOWN"),
        ],
        ["row_id", "customer_id", "product_id"],
    )
    customers_df = spark.createDataFrame(
        [("AB-10001",)],
        ["customer_id"],
    )
    products_df = spark.createDataFrame(
        [("TEC-PH-10000001",)],
        ["product_id"],
    )

    rules = [
        {
            "rule_type": "REFERENCE",
            "column_name": "customer_id",
            "rule_parameter": "_customer_reference_valid",
            "failure_reason": "CUSTOMER_REFERENCE_INVALID",
        },
        {
            "rule_type": "REFERENCE",
            "column_name": "product_id",
            "rule_parameter": "_product_reference_valid",
            "failure_reason": "PRODUCT_REFERENCE_INVALID",
        },
    ]

    processor = DataQualityProcessor(spark)
    flagged_df = processor._add_order_reference_flags(
        orders_df,
        customers_df,
        products_df,
    )

    monkeypatch.setattr(
        processor,
        "_load_rules",
        lambda dataset_name: rules,
    )

    passed_df, rejected_df = processor._apply_rules(
        flagged_df,
        "orders",
    )

    assert passed_df.first()["row_id"] == 1
    assert set(
        rejected_df.first()["dq_failure_reasons"]
    ) == {
        "CUSTOMER_REFERENCE_INVALID",
        "PRODUCT_REFERENCE_INVALID",
    }
