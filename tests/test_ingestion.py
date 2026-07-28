"""Unit tests for RawDataIngestion."""

from sample_assignment.ingestion import RawDataIngestion


def test_snake_case_column_names():
    """Column names should be converted to lowercase snake_case."""

    assert RawDataIngestion._snake_case("Customer ID") == "customer_id"
    assert RawDataIngestion._snake_case(
        "Price per product"
    ) == "price_per_product"
    assert RawDataIngestion._snake_case("Sub-Category") == "sub_category"


def test_prepare_adds_ingestion_metadata(spark):
    """Preparing a DataFrame should standardize columns and add metadata."""

    source_df = spark.createDataFrame(
        [("AB-10001", "Alice Brown")],
        ["Customer ID", "Customer Name"],
    )

    ingestion = RawDataIngestion(spark)

    result_df = ingestion._prepare(source_df, "Customer.xlsx")

    assert result_df.columns == [
        "customer_id",
        "customer_name",
        "source_file_name",
        "ingestion_timestamp",
    ]

    result = result_df.first()

    assert result["customer_id"] == "AB-10001"
    assert result["source_file_name"] == "Customer.xlsx"
    assert result["ingestion_timestamp"] is not None


def test_run_processes_all_three_sources(spark, monkeypatch):
    """The ingestion run should process Customer, Product and Order."""

    sample_df = spark.createDataFrame([(1,)], ["id"])

    ingestion = RawDataIngestion(
        spark=spark,
        catalog="retail_sales",
    )

    monkeypatch.setattr(ingestion, "read_customers", lambda: sample_df)
    monkeypatch.setattr(ingestion, "read_products", lambda: sample_df)
    monkeypatch.setattr(ingestion, "read_orders", lambda: sample_df)

    written_tables = []

    def record_write(df, table_name):
        written_tables.append(table_name)

    monkeypatch.setattr(ingestion, "_write", record_write)

    result = ingestion.run()

    assert written_tables == [
        "retail_sales.raw.customers",
        "retail_sales.raw.products",
        "retail_sales.raw.orders",
    ]

    assert result["customers"]["status"] == "SUCCESS"
    assert result["products"]["status"] == "SUCCESS"
    assert result["orders"]["status"] == "SUCCESS"
    