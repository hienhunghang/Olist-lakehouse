from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="order_items",
    comment="Standardized and validated order items data from bronze layer"
)
@dp.expect_or_fail("valid_order_id", "order_id IS NOT NULL AND LENGTH(TRIM(order_id)) > 0")
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_fail("valid_seller_id", "seller_id IS NOT NULL AND LENGTH(TRIM(seller_id)) > 0")
@dp.expect_or_fail("valid_order_item_id", "order_item_id IS NOT NULL AND order_item_id > 0")
@dp.expect_or_drop("valid_price", "price IS NOT NULL AND price >= 0")
@dp.expect_or_drop("valid_freight", "freight_value IS NOT NULL AND freight_value >= 0")
@dp.expect("has_shipping_limit", "shipping_limit_date IS NOT NULL")
@dp.expect("reasonable_price", "price <= 100000")
@dp.expect("reasonable_freight", "freight_value <= 10000")
def order_items_silver():
    """
    Reads from bronze order_items table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize IDs (order, product, seller)
    - Add processing metadata timestamp
    - Calculate total item cost (price + freight)
    - Remove duplicates based on (order_id, order_item_id)
    
    Quality rules:
    - Require valid order_id, product_id, seller_id (fail pipeline)
    - Require valid order_item_id (positive integer, fail pipeline)
    - Drop records with invalid or negative prices/freight values
    - Warn if shipping limit date is missing
    - Warn if price or freight values are unreasonably high
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.order_items")
        .select(
            # Standardize IDs - trim whitespace
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("order_item_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),
            
            # Keep timestamp as-is
            F.col("shipping_limit_date"),
            
            # Keep financial values as-is
            F.col("price"),
            F.col("freight_value"),
            
            # Add calculated field: total item cost
            (F.col("price") + F.col("freight_value")).alias("total_item_cost"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["order_id", "order_item_id"])
    )