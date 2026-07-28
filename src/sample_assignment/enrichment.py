"""Create enriched Customer, Product and Order Sales tables."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class EnrichmentProcessor:
    """Build the enriched tables required by the assignment."""

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
    def build_customers(customers_valid_df: DataFrame) -> DataFrame:
        """Select clean Customer business columns."""

        return customers_valid_df.select(
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
        )

    @staticmethod
    def build_products(products_valid_df: DataFrame) -> DataFrame:
        """Select clean Product business columns."""

        return products_valid_df.select(
            "product_id",
            "category",
            "sub_category",
            "product_name",
            "state",
            "price_per_product",
        )

    @staticmethod
    def build_order_sales(
        orders_valid_df: DataFrame,
        customers_df: DataFrame,
        products_df: DataFrame,
    ) -> DataFrame:
        """
        Enrich valid Orders with Customer and Product information.

        Profit is rounded and stored with exactly two decimal places.
        """

        customer_lookup_df = customers_df.select(
            "customer_id",
            "customer_name",
            "country",
        )

        product_lookup_df = products_df.select(
            "product_id",
            "category",
            "sub_category",
        )

        order_columns_df = orders_valid_df.select(
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
        )

        return (
            order_columns_df
            .join(
                customer_lookup_df,
                on="customer_id",
                how="inner",
            )
            .join(
                product_lookup_df,
                on="product_id",
                how="inner",
            )
            .select(
                "row_id",
                "order_id",
                "order_date",
                "ship_date",
                "ship_mode",
                "customer_id",
                "customer_name",
                "country",
                "product_id",
                "category",
                "sub_category",
                "quantity",
                "price",
                "discount",
                F.round(
                    F.col("profit"),
                    2,
                ).cast("decimal(18,2)").alias("profit"),
            )
        )

    @staticmethod
    def _write(df: DataFrame, table_name: str) -> None:
        """Replace one full-refresh enriched Delta table."""

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    def run(self) -> dict:
        """Build and write all enriched tables."""

        customers_valid_df = self.spark.table(
            f"{self.catalog}.quality.customers_valid"
        )
        products_valid_df = self.spark.table(
            f"{self.catalog}.quality.products_valid"
        )
        orders_valid_df = self.spark.table(
            f"{self.catalog}.quality.orders_valid"
        )

        customers_df = self.build_customers(
            customers_valid_df
        )
        products_df = self.build_products(
            products_valid_df
        )
        order_sales_df = self.build_order_sales(
            orders_valid_df,
            customers_df,
            products_df,
        )

        tables = {
            "customers": (
                f"{self.catalog}.enriched.customers"
            ),
            "products": (
                f"{self.catalog}.enriched.products"
            ),
            "order_sales": (
                f"{self.catalog}.enriched.order_sales"
            ),
        }

        counts = {
            "customers": customers_df.count(),
            "products": products_df.count(),
            "order_sales": order_sales_df.count(),
        }

        expected_order_count = orders_valid_df.count()

        if counts["order_sales"] != expected_order_count:
            raise ValueError(
                "Order enrichment changed the number of valid Orders. "
                "Check Customer and Product join keys."
            )

        self._write(customers_df, tables["customers"])
        self._write(products_df, tables["products"])
        self._write(order_sales_df, tables["order_sales"])

        print("=" * 60)
        print("ENRICHMENT COMPLETED")
        for name in ["customers", "products", "order_sales"]:
            print(
                f"{tables[name]}: {counts[name]} rows"
            )
        print("=" * 60)

        return {
            "status": "SUCCESS",
            "tables": tables,
            "row_counts": counts,
        }