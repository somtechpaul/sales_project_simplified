"""Shared configuration for the retail sales assignment."""


# ==========================================================
# Catalog and schemas
# ==========================================================

CATALOG = "retail_sales"

RAW_SCHEMA = f"{CATALOG}.raw"
QUALITY_SCHEMA = f"{CATALOG}.quality"
ENRICHED_SCHEMA = f"{CATALOG}.enriched"
ANALYTICS_SCHEMA = f"{CATALOG}.analytics"


# ==========================================================
# Source files in Unity Catalog volumes
# ==========================================================

VOLUME_ROOT = f"/Volumes/{CATALOG}/raw"

CUSTOMER_FILE_PATH = (
    f"{VOLUME_ROOT}/customers_volume_new/Customer.xlsx"
)
PRODUCT_FILE_PATH = (
    f"{VOLUME_ROOT}/products_volume_new/Products.csv"
)
ORDER_FILE_PATH = (
    f"{VOLUME_ROOT}/orders_volume_new/Orders.json"
)


# ==========================================================
# Raw tables
# ==========================================================

RAW_CUSTOMERS_TABLE = f"{RAW_SCHEMA}.customers"
RAW_PRODUCTS_TABLE = f"{RAW_SCHEMA}.products"
RAW_ORDERS_TABLE = f"{RAW_SCHEMA}.orders"


# ==========================================================
# Data-quality tables
# ==========================================================

DQ_RULES_TABLE = f"{QUALITY_SCHEMA}.data_quality_rules"

CUSTOMERS_VALID_TABLE = f"{QUALITY_SCHEMA}.customers_valid"
CUSTOMERS_REJECTED_TABLE = f"{QUALITY_SCHEMA}.customers_rejected"

PRODUCTS_VALID_TABLE = f"{QUALITY_SCHEMA}.products_valid"
PRODUCTS_REJECTED_TABLE = f"{QUALITY_SCHEMA}.products_rejected"

ORDERS_VALID_TABLE = f"{QUALITY_SCHEMA}.orders_valid"
ORDERS_REJECTED_TABLE = f"{QUALITY_SCHEMA}.orders_rejected"


# ==========================================================
# Enriched tables
# ==========================================================

ENRICHED_CUSTOMERS_TABLE = f"{ENRICHED_SCHEMA}.customers"
ENRICHED_PRODUCTS_TABLE = f"{ENRICHED_SCHEMA}.products"
ENRICHED_ORDER_SALES_TABLE = f"{ENRICHED_SCHEMA}.order_sales"


# ==========================================================
# Analytics table
# ==========================================================

PROFIT_AGGREGATE_TABLE = (
    f"{ANALYTICS_SCHEMA}."
    "profit_by_year_category_subcategory_customer"
)


# The assignment processes complete source-file snapshots.
WRITE_MODE = "overwrite"