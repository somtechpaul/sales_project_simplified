"""
Apply configurable data-quality rules to Raw retail-sales data.
"""

import json

from pyspark.sql import (
    Column,
    DataFrame,
    SparkSession,
    Window,
)
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ==========================================================
# Valid US states
# ==========================================================

VALID_US_STATES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District of Columbia",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]


# ==========================================================
# Data Quality Processor
# ==========================================================

class DataQualityProcessor:
    """
    Clean and validate Customer, Product and Order datasets.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "retail_sales",
    ):
        if spark is None:
            raise ValueError(
                "spark must be provided."
            )

        self.spark = spark
        self.catalog = catalog

        self.rule_table = (
            f"{catalog}.quality.data_quality_rules"
        )

    # ======================================================
    # Rule loading
    # ======================================================

    def _load_rules(
        self,
        dataset_name: str,
    ) -> list:
        """
        Load active rules for one dataset.

        Collect is acceptable because this is a small
        configuration table, not a business-data table.
        """

        rules_df = (
            self.spark.table(
                self.rule_table
            )
            .filter(
                (F.col("dataset_name") == dataset_name)
                & (F.col("is_active") == F.lit(True))
            )
            .orderBy(
                "rule_order"
            )
        )

        return rules_df.collect()

    # ======================================================
    # Common string cleaning
    # ======================================================

    @staticmethod
    def _clean_string_columns(
        df: DataFrame,
    ) -> DataFrame:
        """
        Trim strings and convert blank strings to null.
        """

        cleaned_df = df

        for field in df.schema.fields:

            if isinstance(
                field.dataType,
                StringType,
            ):

                trimmed_value = F.trim(
                    F.col(field.name)
                )

                cleaned_df = cleaned_df.withColumn(
                    field.name,
                    F.when(
                        F.length(
                            trimmed_value
                        ) == 0,
                        F.lit(None).cast("string"),
                    ).otherwise(
                        trimmed_value
                    ),
                )

        return cleaned_df

    # ======================================================
    # Customer cleaning
    # ======================================================

    def _clean_customers(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        Apply deterministic Customer corrections.
        """

        cleaned_df = (
            self._clean_string_columns(df)
            .withColumn(
                "customer_id",
                F.upper(
                    F.col("customer_id")
                ),
            )
            .withColumn(
                "customer_name",
                F.regexp_replace(
                    F.col("customer_name"),
                    r"\s+",
                    " ",
                ),
            )
            .withColumn(
                "postal_code",
                F.when(
                    F.col("postal_code").rlike(
                        r"^[0-9]{4}$"
                    ),
                    F.lpad(
                        F.col("postal_code"),
                        5,
                        "0",
                    ),
                ).otherwise(
                    F.col("postal_code")
                ),
            )
        )

        return cleaned_df

    # ======================================================
    # Product cleaning
    # ======================================================

    def _clean_products(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        Apply deterministic Product corrections.
        """

        return (
            self._clean_string_columns(df)
            .withColumn(
                "product_id",
                F.upper(
                    F.col("product_id")
                ),
            )
        )

    # ======================================================
    # Order cleaning
    # ======================================================

    def _clean_orders(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        Standardize Order identifiers and convert dates.

        Raw source date format:
            d/M/yyyy

        DateType values are displayed as:
            yyyy-MM-dd
        """

        return (
            self._clean_string_columns(df)
            .withColumn(
                "order_id",
                F.upper(
                    F.col("order_id")
                ),
            )
            .withColumn(
                "customer_id",
                F.upper(
                    F.col("customer_id")
                ),
            )
            .withColumn(
                "product_id",
                F.upper(
                    F.col("product_id")
                ),
            )
            .withColumn(
                "order_date",
                F.to_date(
                    F.col("order_date"),
                    "d/M/yyyy",
                ),
            )
            .withColumn(
                "ship_date",
                F.to_date(
                    F.col("ship_date"),
                    "d/M/yyyy",
                ),
            )
        )

    # ======================================================
    # Rule-condition builder
    # ======================================================

    @staticmethod
    def _build_condition(
        df: DataFrame,
        rule,
    ) -> Column:
        """
        Convert one configured rule into a PySpark condition.
        """

        rule_type = rule[
            "rule_type"
        ]

        column_name = rule[
            "column_name"
        ]

        rule_parameter = rule[
            "rule_parameter"
        ]

        column_value = F.col(
            column_name
        )

        # --------------------------------------------------
        # Completeness
        # --------------------------------------------------

        if rule_type == "NOT_NULL":

            return column_value.isNotNull()

        # --------------------------------------------------
        # Positive numeric value
        # --------------------------------------------------

        if rule_type == "POSITIVE":

            return (
                column_value.isNull()
                | (column_value > F.lit(0))
            )

        # --------------------------------------------------
        # Uniqueness
        # --------------------------------------------------

        if rule_type == "UNIQUE":

            duplicate_window = (
                Window.partitionBy(
                    column_name
                )
            )

            return (
                column_value.isNull()
                | (
                    F.count(
                        F.lit(1)
                    ).over(
                        duplicate_window
                    ) == 1
                )
            )

        # --------------------------------------------------
        # Allowed values
        # --------------------------------------------------

        if rule_type == "ALLOWED_VALUES":

            allowed_values = json.loads(
                rule_parameter
            )

            return (
                column_value.isNull()
                | column_value.isin(
                    allowed_values
                )
            )

        # --------------------------------------------------
        # Regular expression
        # --------------------------------------------------

        if rule_type == "REGEX":

            return (
                column_value.isNull()
                | column_value.rlike(
                    rule_parameter
                )
            )

        # --------------------------------------------------
        # Customer name
        # --------------------------------------------------

        if rule_type == "VALID_CUSTOMER_NAME":

            valid_name_pattern = (
                r"^[\p{L} .'\-]+$"
            )

            return (
                column_value.isNull()
                | column_value.rlike(
                    valid_name_pattern
                )
            )

        # --------------------------------------------------
        # US state
        # --------------------------------------------------

        if rule_type == "VALID_STATE":

            return (
                column_value.isNull()
                | column_value.isin(
                    VALID_US_STATES
                )
            )

        # --------------------------------------------------
        # Date cannot be in future
        # --------------------------------------------------

        if rule_type == "NOT_FUTURE":

            return (
                column_value.isNull()
                | (
                    column_value
                    <= F.current_date()
                )
            )

        # --------------------------------------------------
        # Column must be >= another column
        # --------------------------------------------------

        if rule_type == "COLUMN_GTE":

            comparison_column = F.col(
                rule_parameter
            )

            return (
                column_value.isNull()
                | comparison_column.isNull()
                | (
                    column_value
                    >= comparison_column
                )
            )

        # --------------------------------------------------
        # Numeric range
        # --------------------------------------------------

        if rule_type == "BETWEEN":

            boundaries = json.loads(
                rule_parameter
            )

            minimum_value = boundaries[
                "min"
            ]

            maximum_value = boundaries[
                "max"
            ]

            return (
                column_value.isNull()
                | column_value.between(
                    minimum_value,
                    maximum_value,
                )
            )

        # --------------------------------------------------
        # Referential integrity
        # --------------------------------------------------

        if rule_type == "REFERENCE":

            reference_flag = F.col(
                rule_parameter
            )

            return (
                F.coalesce(
                    reference_flag,
                    F.lit(False),
                )
                == F.lit(True)
            )

        raise ValueError(
            f"Unsupported rule type: {rule_type}"
        )

    # ======================================================
    # Apply configured rules
    # ======================================================

    def _apply_rules(
        self,
        df: DataFrame,
        dataset_name: str,
    ) -> tuple[DataFrame, DataFrame]:
        """
        Apply active rules and split passed/rejected records.
        """

        rules = self._load_rules(
            dataset_name
        )

        evaluated_df = df.withColumn(
            "dq_failure_reasons",
            F.expr(
                "CAST(array() AS ARRAY<STRING>)"
            ),
        )

        for rule in rules:

            condition = self._build_condition(
                evaluated_df,
                rule,
            )

            failure_reason = rule[
                "failure_reason"
            ]

            evaluated_df = evaluated_df.withColumn(
                "dq_failure_reasons",
                F.when(
                    condition,
                    F.col(
                        "dq_failure_reasons"
                    ),
                ).otherwise(
                    F.array_union(
                        F.col(
                            "dq_failure_reasons"
                        ),
                        F.array(
                            F.lit(
                                failure_reason
                            )
                        ),
                    )
                ),
            )

        evaluated_df = (
            evaluated_df
            .withColumn(
                "dq_status",
                F.when(
                    F.size(
                        F.col(
                            "dq_failure_reasons"
                        )
                    ) == 0,
                    F.lit("PASSED"),
                ).otherwise(
                    F.lit("REJECTED")
                ),
            )
            .withColumn(
                "dq_checked_timestamp",
                F.current_timestamp(),
            )
        )

        passed_df = evaluated_df.filter(
            F.col("dq_status") == "PASSED"
        )

        rejected_df = evaluated_df.filter(
            F.col("dq_status") == "REJECTED"
        )

        return (
            passed_df,
            rejected_df,
        )

    # ======================================================
    # Add order reference flags
    # ======================================================

    @staticmethod
    def _add_order_reference_flags(
        orders_df: DataFrame,
        customers_valid_df: DataFrame,
        products_valid_df: DataFrame,
    ) -> DataFrame:
        """
        Check Orders against passed Customer and Product keys.
        """

        customer_keys_df = (
            customers_valid_df
            .select(
                "customer_id"
            )
            .distinct()
            .withColumn(
                "_customer_reference_valid",
                F.lit(True),
            )
        )

        product_keys_df = (
            products_valid_df
            .select(
                "product_id"
            )
            .distinct()
            .withColumn(
                "_product_reference_valid",
                F.lit(True),
            )
        )

        return (
            orders_df
            .join(
                customer_keys_df,
                on="customer_id",
                how="left",
            )
            .join(
                product_keys_df,
                on="product_id",
                how="left",
            )
        )

    # ======================================================
    # Write DQ output
    # ======================================================

    @staticmethod
    def _write_table(
        df: DataFrame,
        table_name: str,
    ) -> None:
        """
        Replace one full-refresh DQ output table.
        """

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .saveAsTable(
                table_name
            )
        )

    # ======================================================
    # Run complete DQ pipeline
    # ======================================================

    def run(self) -> dict:
        """
        Validate Customers, Products and Orders.
        """

        print()
        print("=" * 70)
        print("RETAIL SALES DATA QUALITY")
        print("=" * 70)

        # --------------------------------------------------
        # Customers
        # --------------------------------------------------

        customers_raw_df = self.spark.table(
            f"{self.catalog}.raw.customers"
        )

        customers_clean_df = self._clean_customers(
            customers_raw_df
        )

        (
            customers_valid_df,
            customers_rejected_df,
        ) = self._apply_rules(
            df=customers_clean_df,
            dataset_name="customers",
        )

        # --------------------------------------------------
        # Products
        # --------------------------------------------------

        products_raw_df = self.spark.table(
            f"{self.catalog}.raw.products"
        )

        products_clean_df = self._clean_products(
            products_raw_df
        )

        (
            products_valid_df,
            products_rejected_df,
        ) = self._apply_rules(
            df=products_clean_df,
            dataset_name="products",
        )

        # --------------------------------------------------
        # Orders
        # --------------------------------------------------

        orders_raw_df = self.spark.table(
            f"{self.catalog}.raw.orders"
        )

        orders_clean_df = self._clean_orders(
            orders_raw_df
        )

        orders_with_references_df = (
            self._add_order_reference_flags(
                orders_df=orders_clean_df,
                customers_valid_df=customers_valid_df,
                products_valid_df=products_valid_df,
            )
        )

        (
            orders_valid_df,
            orders_rejected_df,
        ) = self._apply_rules(
            df=orders_with_references_df,
            dataset_name="orders",
        )

        orders_valid_df = orders_valid_df.drop(
            "_customer_reference_valid",
            "_product_reference_valid",
        )

        orders_rejected_df = orders_rejected_df.drop(
            "_customer_reference_valid",
            "_product_reference_valid",
        )

        # --------------------------------------------------
        # Counts
        # --------------------------------------------------

        customer_valid_count = (
            customers_valid_df.count()
        )

        customer_rejected_count = (
            customers_rejected_df.count()
        )

        product_valid_count = (
            products_valid_df.count()
        )

        product_rejected_count = (
            products_rejected_df.count()
        )

        order_valid_count = (
            orders_valid_df.count()
        )

        order_rejected_count = (
            orders_rejected_df.count()
        )

        # --------------------------------------------------
        # Write output tables
        # --------------------------------------------------

        output_tables = {
            "customers_valid": (
                f"{self.catalog}.quality.customers_valid"
            ),
            "customers_rejected": (
                f"{self.catalog}.quality.customers_rejected"
            ),
            "products_valid": (
                f"{self.catalog}.quality.products_valid"
            ),
            "products_rejected": (
                f"{self.catalog}.quality.products_rejected"
            ),
            "orders_valid": (
                f"{self.catalog}.quality.orders_valid"
            ),
            "orders_rejected": (
                f"{self.catalog}.quality.orders_rejected"
            ),
        }

        self._write_table(
            customers_valid_df,
            output_tables[
                "customers_valid"
            ],
        )

        self._write_table(
            customers_rejected_df,
            output_tables[
                "customers_rejected"
            ],
        )

        self._write_table(
            products_valid_df,
            output_tables[
                "products_valid"
            ],
        )

        self._write_table(
            products_rejected_df,
            output_tables[
                "products_rejected"
            ],
        )

        self._write_table(
            orders_valid_df,
            output_tables[
                "orders_valid"
            ],
        )

        self._write_table(
            orders_rejected_df,
            output_tables[
                "orders_rejected"
            ],
        )

        summary = {
            "customers": {
                "valid": customer_valid_count,
                "rejected": customer_rejected_count,
            },
            "products": {
                "valid": product_valid_count,
                "rejected": product_rejected_count,
            },
            "orders": {
                "valid": order_valid_count,
                "rejected": order_rejected_count,
            },
            "output_tables": output_tables,
            "status": "SUCCESS",
        }

        print()
        print("=" * 70)
        print("DATA QUALITY COMPLETED")
        print("=" * 70)
        print(
            f"Customers: {customer_valid_count} passed, "
            f"{customer_rejected_count} rejected"
        )
        print(
            f"Products : {product_valid_count} passed, "
            f"{product_rejected_count} rejected"
        )
        print(
            f"Orders   : {order_valid_count} passed, "
            f"{order_rejected_count} rejected"
        )
        print("=" * 70)

        return summary