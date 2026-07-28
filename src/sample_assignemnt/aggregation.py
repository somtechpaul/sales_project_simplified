"""Create the Profit aggregate table required by the assignment."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class AggregationProcessor:
    """Aggregate profit by year, product and customer."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "retail_sales",
    ):
        if spark is None:
            raise ValueError("spark must be provided.")

        self.spark = spark
        self.catalog = catalog

    @staticmethod
    def build_profit_aggregate(
        order_sales_df: DataFrame,
    ) -> DataFrame:
        """
        Create one row per:

        year + category + sub-category + customer.
        """

        return (
            order_sales_df
            .withColumn(
                "order_year",
                F.year("order_date"),
            )
            .groupBy(
                "order_year",
                "category",
                "sub_category",
                "customer_id",
                "customer_name",
            )
            .agg(
                F.round(
                    F.sum("profit"),
                    2,
                )
                .cast("decimal(18,2)")
                .alias("total_profit")
            )
            .select(
                "order_year",
                "category",
                "sub_category",
                "customer_id",
                "customer_name",
                "total_profit",
            )
        )

    @staticmethod
    def _write(
        df: DataFrame,
        table_name: str,
    ) -> None:
        """Replace the full-refresh aggregate Delta table."""

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    def run(self) -> dict:
        """Build and write the Profit aggregate table."""

        source_table = (
            f"{self.catalog}.enriched.order_sales"
        )
        target_table = (
            f"{self.catalog}.analytics."
            "profit_by_year_category_subcategory_customer"
        )

        order_sales_df = self.spark.table(
            source_table
        )

        profit_aggregate_df = self.build_profit_aggregate(
            order_sales_df
        )

        row_count = profit_aggregate_df.count()

        self._write(
            profit_aggregate_df,
            target_table,
        )

        print("=" * 60)
        print("PROFIT AGGREGATION COMPLETED")
        print(f"Source table : {source_table}")
        print(f"Target table : {target_table}")
        print(f"Rows written : {row_count}")
        print("=" * 60)

        return {
            "status": "SUCCESS",
            "source_table": source_table,
            "target_table": target_table,
            "rows_written": row_count,
        }