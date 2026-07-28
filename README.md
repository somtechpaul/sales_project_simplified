# Retail Sales Data Processing Assignment

## Pipeline Flow

1. Ingestion
2. Data Quality
3. Enrichment
4. Aggregation
5. SQL Reporting

## Source Files

- Customer.xlsx
- Products.csv
- Orders.json

## Output Tables

- retail_sales.raw.customers
- retail_sales.raw.products
- retail_sales.raw.orders
- retail_sales.quality.customers_valid
- retail_sales.quality.products_valid
- retail_sales.quality.orders_valid
- retail_sales.enriched.customers
- retail_sales.enriched.products
- retail_sales.enriched.order_sales
- retail_sales.analytics.profit_by_year_category_subcategory_customer

## Execution

1. Run notebooks/00_setup
2. Run notebooks/01_run_pipeline
3. Run notebooks/02_reporting_queries

## Tests

Run:

pytest -v tests

## Assumptions

- Source files are full snapshots.
- Tables are refreshed using overwrite.
- Spark Excel library is installed on the Databricks compute.