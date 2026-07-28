"""Create full-refresh raw Delta tables from the three source files."""

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ==========================================================
# Source Schemas
# ==========================================================

CUSTOMER_SCHEMA = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True),
])


PRODUCT_SCHEMA = StructType([
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Price per product", DoubleType(), True),
])


ORDER_SCHEMA = StructType([
    StructField("Row ID", LongType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
])


# ==========================================================
# Raw Ingestion
# ==========================================================

class RawDataIngestion:
    """Read source files and create raw Delta tables."""

    def __init__(
        self,
        spark: SparkSession,
        volume_path: str = (
            "/Volumes/retail_sales/raw/"
        ),
        catalog: str = "retail_sales",
    ):
        if spark is None:
            raise ValueError("spark must be provided.")

        self.spark = spark
        self.volume_path = volume_path.rstrip("/")
        self.catalog = catalog

    # ======================================================
    # Column standardization
    # ======================================================

    @staticmethod
    def _snake_case(column_name: str) -> str:
        """Convert source column names to lowercase snake_case."""

        standardized = re.sub(
            r"[^a-zA-Z0-9]+",
            "_",
            column_name.strip(),
        )

        return standardized.strip("_").lower()

    # ======================================================
    # Raw metadata
    # ======================================================

    def _prepare(
        self,
        df: DataFrame,
        source_file_name: str,
    ) -> DataFrame:
        """
        Standardize column names and add minimal raw metadata.
        """

        columns = [
            self._snake_case(column_name)
            for column_name in df.columns
        ]

        if len(columns) != len(set(columns)):
            raise ValueError(
                f"Duplicate standardized columns: {columns}"
            )

        return (
            df
            .toDF(*columns)
            .withColumn(
                "source_file_name",
                lit(source_file_name),
            )
            .withColumn(
                "ingestion_timestamp",
                current_timestamp(),
            )
        )

    # ======================================================
    # Read Customer.xlsx
    # ======================================================

    def read_customers(self) -> DataFrame:
        """Read Customer.xlsx from its first worksheet."""

        file_name = "Customer.xlsx"

        file_path = (
            f"{self.volume_path}/customers_volume_new/{file_name}"
        )

        customer_df = (
            self.spark.read
            .format("dev.mauch.spark.excel")
            .option("header", "true")
            .option(
                "dataAddress",
                "'Worksheet'!A1",
            )
            .option(
                "treatEmptyValuesAsNulls",
                "true",
            )
            .option(
                "usePlainNumberFormat",
                "true",
            )
            .schema(CUSTOMER_SCHEMA)
            .load(file_path)
        )

        return self._prepare(
            df=customer_df,
            source_file_name=file_name,
        )

    # ======================================================
    # Read Products.csv
    # ======================================================

    def read_products(self) -> DataFrame:
        """Read Products.csv."""

        file_name = "Products.csv"

        file_path = (
            f"{self.volume_path}/products_volume_new/{file_name}"
        )

        product_df = (
            self.spark.read
            .format("csv")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .schema(PRODUCT_SCHEMA)
            .load(file_path)
        )

        return self._prepare(
            df=product_df,
            source_file_name=file_name,
        )

    # ======================================================
    # Read Orders.json
    # ======================================================

    def read_orders(self) -> DataFrame:
        """Read the multiline JSON array in Orders.json."""

        file_name = "Orders.json"

        file_path = (
            f"{self.volume_path}/orders_volume_new/{file_name}"
        )

        order_df = (
            self.spark.read
            .format("json")
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .schema(ORDER_SCHEMA)
            .load(file_path)
        )

        return self._prepare(
            df=order_df,
            source_file_name=file_name,
        )

    # ======================================================
    # Write Raw Table
    # ======================================================

    @staticmethod
    def _write(
        df: DataFrame,
        table_name: str,
    ) -> None:
        """
        Create or replace one full-refresh raw Delta table.
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
    # Execute Ingestion
    # ======================================================

    def run(self) -> dict:
        """Ingest customer, product and order files."""

        sources = [
            (
                "customers",
                self.read_customers,
                f"{self.catalog}.raw.customers",
            ),
            (
                "products",
                self.read_products,
                f"{self.catalog}.raw.products",
            ),
            (
                "orders",
                self.read_orders,
                f"{self.catalog}.raw.orders",
            ),
        ]

        summary = {}

        for source_name, reader, table_name in sources:

            print("=" * 70)
            print(f"Ingesting source : {source_name}")
            print(f"Target table     : {table_name}")

            try:

                source_df = reader()
                row_count = source_df.count()

                if row_count == 0:
                    raise ValueError(
                        f"{source_name} file contains no rows."
                    )

                self._write(
                    df=source_df,
                    table_name=table_name,
                )

                summary[source_name] = {
                    "table": table_name,
                    "rows_written": row_count,
                    "status": "SUCCESS",
                }

                print(
                    f"{source_name}: {row_count} rows "
                    f"written to {table_name}"
                )

            except Exception as error:

                raise RuntimeError(
                    f"Ingestion failed for {source_name}: "
                    f"{error}"
                ) from error

        return summary