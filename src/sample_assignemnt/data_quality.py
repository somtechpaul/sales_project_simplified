"""Apply metadata-driven data-quality rules to retail sales data."""

import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


VALID_US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "District of Columbia",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana",
    "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi",
    "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
    "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
    "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming",
]


class DataQualityProcessor:
    """Clean and validate Customers, Products and Orders."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "retail_sales",
    ):
        if spark is None:
            raise ValueError("spark must be provided.")

        self.spark = spark
        self.catalog = catalog
        self.rule_table = f"{catalog}.quality.data_quality_rules"

    def _load_rules(self, dataset_name: str) -> list:
        """Return active rules for one dataset."""

        return (
            self.spark.table(self.rule_table)
            .filter(
                (F.col("dataset_name") == dataset_name)
                & F.col("is_active")
            )
            .orderBy("rule_order")
            .collect()
        )

    @staticmethod
    def _clean_strings(df: DataFrame) -> DataFrame:
        """Trim strings and convert blank strings to null."""

        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                value = F.trim(F.col(field.name))
                df = df.withColumn(
                    field.name,
                    F.when(
                        F.length(value) == 0,
                        F.lit(None).cast("string"),
                    ).otherwise(value),
                )

        return df

    def _clean_customers(self, df: DataFrame) -> DataFrame:
        """Apply safe Customer standardizations before validation."""

        return (
            self._clean_strings(df)
            .withColumn("customer_id", F.upper("customer_id"))
            .withColumn("email", F.lower("email"))
            .withColumn(
                "customer_name",
                F.regexp_replace("customer_name", r"\s+", " "),
            )
            .withColumn(
                "postal_code",
                F.when(
                    F.col("postal_code").rlike(r"^[0-9]{4}$"),
                    F.lpad("postal_code", 5, "0"),
                ).otherwise(F.col("postal_code")),
            )
        )

    def _clean_products(self, df: DataFrame) -> DataFrame:
        """Apply safe Product standardizations before validation."""

        return (
            self._clean_strings(df)
            .withColumn("product_id", F.upper("product_id"))
        )

    def _clean_orders(self, df: DataFrame) -> DataFrame:
        """Standardize identifiers and safely parse source dates."""

        return (
            self._clean_strings(df)
            .withColumn("order_id", F.upper("order_id"))
            .withColumn("customer_id", F.upper("customer_id"))
            .withColumn("product_id", F.upper("product_id"))
            .withColumn("order_date_source", F.col("order_date"))
            .withColumn("ship_date_source", F.col("ship_date"))
            .withColumn(
                "order_date",
                F.try_to_timestamp(
                    F.col("order_date_source"),
                    F.lit("d/M/yyyy"),
                ).cast("date"),
            )
            .withColumn(
                "ship_date",
                F.try_to_timestamp(
                    F.col("ship_date_source"),
                    F.lit("d/M/yyyy"),
                ).cast("date"),
            )
        )

    @staticmethod
    def _condition(rule) -> Column:
        """Convert one metadata rule into a PySpark condition."""

        rule_type = rule["rule_type"]
        column_name = rule["column_name"]
        parameter = rule["rule_parameter"]
        value = F.col(column_name)

        if rule_type == "NOT_NULL":
            return value.isNotNull()

        if rule_type == "UNIQUE":
            duplicate_window = Window.partitionBy(column_name)
            return (
                value.isNull()
                | (F.count(F.lit(1)).over(duplicate_window) == 1)
            )

        if rule_type == "REGEX":
            return value.isNull() | value.rlike(parameter)

        if rule_type == "VALID_CUSTOMER_NAME":
            return (
                value.isNull()
                | value.rlike(r"^[\p{L} .'\-]+$")
            )

        if rule_type == "ALLOWED_VALUES":
            allowed_values = json.loads(parameter)
            return value.isNull() | value.isin(allowed_values)

        if rule_type == "VALID_STATE":
            return value.isNull() | value.isin(VALID_US_STATES)

        if rule_type == "POSITIVE":
            return value.isNull() | (value > 0)

        if rule_type == "BETWEEN":
            limits = json.loads(parameter)
            return (
                value.isNull()
                | value.between(limits["min"], limits["max"])
            )

        if rule_type == "DATE_FORMAT":
            return (
                value.isNull()
                | F.try_to_timestamp(
                    value,
                    F.lit(parameter),
                ).isNotNull()
            )

        if rule_type == "NOT_FUTURE":
            return value.isNull() | (value <= F.current_date())

        if rule_type == "COLUMN_GTE":
            other_value = F.col(parameter)
            return (
                value.isNull()
                | other_value.isNull()
                | (value >= other_value)
            )

        if rule_type == "REFERENCE":
            reference_is_valid = F.coalesce(
                F.col(parameter),
                F.lit(False),
            )

            # NOT_NULL reports missing keys. REFERENCE reports
            # only non-null keys that do not exist in the parent.
            return value.isNull() | reference_is_valid

        raise ValueError(f"Unsupported rule type: {rule_type}")

    def _apply_rules(
        self,
        df: DataFrame,
        dataset_name: str,
    ) -> tuple[DataFrame, DataFrame]:
        """Apply configured rules and split passed/rejected rows."""

        result_df = df.withColumn(
            "dq_failure_reasons",
            F.expr("CAST(array() AS ARRAY<STRING>)"),
        )

        for rule in self._load_rules(dataset_name):
            condition = self._condition(rule)

            result_df = result_df.withColumn(
                "dq_failure_reasons",
                F.when(
                    condition,
                    F.col("dq_failure_reasons"),
                ).otherwise(
                    F.array_union(
                        F.col("dq_failure_reasons"),
                        F.array(F.lit(rule["failure_reason"])),
                    )
                ),
            )

        result_df = (
            result_df
            .withColumn(
                "dq_status",
                F.when(
                    F.size("dq_failure_reasons") == 0,
                    F.lit("PASSED"),
                ).otherwise(F.lit("REJECTED")),
            )
            .withColumn(
                "dq_checked_timestamp",
                F.current_timestamp(),
            )
        )

        return (
            result_df.filter(F.col("dq_status") == "PASSED"),
            result_df.filter(F.col("dq_status") == "REJECTED"),
        )

    @staticmethod
    def _add_order_reference_flags(
        orders_df: DataFrame,
        customers_valid_df: DataFrame,
        products_valid_df: DataFrame,
    ) -> DataFrame:
        """Mark whether Order foreign keys exist in valid master data."""

        customer_keys_df = (
            customers_valid_df
            .select("customer_id")
            .distinct()
            .withColumn("_customer_reference_valid", F.lit(True))
        )

        product_keys_df = (
            products_valid_df
            .select("product_id")
            .distinct()
            .withColumn("_product_reference_valid", F.lit(True))
        )

        return (
            orders_df
            .join(customer_keys_df, "customer_id", "left")
            .join(product_keys_df, "product_id", "left")
        )

    @staticmethod
    def _write(df: DataFrame, table_name: str) -> None:
        """Replace one full-refresh DQ output table."""

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    def _write_results(
        self,
        dataset_name: str,
        valid_df: DataFrame,
        rejected_df: DataFrame,
    ) -> dict:
        """Write valid and rejected tables and return their counts."""

        valid_table = (
            f"{self.catalog}.quality.{dataset_name}_valid"
        )
        rejected_table = (
            f"{self.catalog}.quality.{dataset_name}_rejected"
        )

        valid_count = valid_df.count()
        rejected_count = rejected_df.count()

        self._write(valid_df, valid_table)
        self._write(rejected_df, rejected_table)

        return {
            "valid": valid_count,
            "rejected": rejected_count,
            "valid_table": valid_table,
            "rejected_table": rejected_table,
        }

    def run(self) -> dict:
        """Run Customer, Product and Order data quality."""

        customers_df = self._clean_customers(
            self.spark.table(f"{self.catalog}.raw.customers")
        )
        customers_valid_df, customers_rejected_df = (
            self._apply_rules(customers_df, "customers")
        )

        products_df = self._clean_products(
            self.spark.table(f"{self.catalog}.raw.products")
        )
        products_valid_df, products_rejected_df = (
            self._apply_rules(products_df, "products")
        )

        orders_df = self._clean_orders(
            self.spark.table(f"{self.catalog}.raw.orders")
        )
        orders_df = self._add_order_reference_flags(
            orders_df,
            customers_valid_df,
            products_valid_df,
        )
        orders_valid_df, orders_rejected_df = (
            self._apply_rules(orders_df, "orders")
        )

        reference_columns = [
            "_customer_reference_valid",
            "_product_reference_valid",
        ]
        orders_valid_df = orders_valid_df.drop(*reference_columns)
        orders_rejected_df = orders_rejected_df.drop(*reference_columns)

        summary = {
            "customers": self._write_results(
                "customers",
                customers_valid_df,
                customers_rejected_df,
            ),
            "products": self._write_results(
                "products",
                products_valid_df,
                products_rejected_df,
            ),
            "orders": self._write_results(
                "orders",
                orders_valid_df,
                orders_rejected_df,
            ),
            "status": "SUCCESS",
        }

        print("=" * 60)
        print("DATA QUALITY COMPLETED")
        for dataset_name in ["customers", "products", "orders"]:
            result = summary[dataset_name]
            print(
                f"{dataset_name.title()}: "
                f"{result['valid']} passed, "
                f"{result['rejected']} rejected"
            )
        print("=" * 60)

        return summary