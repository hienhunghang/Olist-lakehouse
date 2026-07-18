from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="inventory_snapshot",
    comment="Standardized and validated inventory snapshot data from bronze layer"
)
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_fail("valid_snapshot_date", "snapshot_date IS NOT NULL")
@dp.expect_or_drop("valid_stock_level", "stock_level IS NOT NULL AND stock_level >= 0")
@dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
@dp.expect("has_category", "product_category_name IS NOT NULL AND LENGTH(TRIM(product_category_name)) > 0")
@dp.expect("reasonable_stock_level", "stock_level <= 10000")
@dp.expect("recent_snapshot", "snapshot_date >= '2017-01-01'")
def inventory_snapshot_silver():
    """
    Reads from bronze inventory_snapshot table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize product_id
    - Lowercase and trim product_category_name for consistency
    - Extract date components (year, month, day, day of week)
    - Add processing metadata timestamp
    - Remove duplicates based on (product_id, snapshot_date)
    - Filter out records with rescued data (malformed rows)
    
    Quality rules:
    - Require valid product_id (fail pipeline)
    - Require valid snapshot_date (fail pipeline)
    - Drop records with invalid or negative stock levels
    - Drop records with rescued data (malformed CSV rows)
    - Warn if product has no category name
    - Warn if stock level exceeds 10,000 units
    - Warn if snapshot is older than 2 years
    """
    return (
        spark.read
        .table("olist_catalog.blob_bronze.inventory_snapshot")
        .select(
            # Standardize product ID - trim whitespace
            F.trim(F.col("product_id")).alias("product_id"),
            
            # Standardize category name - lowercase and trim
            F.when(
                F.col("product_category_name").isNotNull(),
                F.lower(F.trim(F.col("product_category_name")))
            ).alias("product_category_name"),
            
            # Keep snapshot_date as-is (already date type)
            F.col("snapshot_date"),
            
            # Extract date components for easier filtering
            F.year(F.col("snapshot_date")).alias("snapshot_year"),
            F.month(F.col("snapshot_date")).alias("snapshot_month"),
            F.dayofmonth(F.col("snapshot_date")).alias("snapshot_day"),
            F.dayofweek(F.col("snapshot_date")).alias("snapshot_day_of_week"),
            
            # Keep stock level as-is
            F.col("stock_level"),
            
            # Keep rescued data column for quality check
            F.col("_rescued_data"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["product_id", "snapshot_date"])
    )