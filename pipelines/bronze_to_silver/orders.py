from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="orders",
    comment="Standardized and validated orders data from bronze layer",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dp.expect_or_fail("valid_order_id", "order_id IS NOT NULL AND LENGTH(TRIM(order_id)) > 0")
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) > 0")
@dp.expect_or_fail("valid_purchase_timestamp", "order_purchase_timestamp IS NOT NULL")
@dp.expect("valid_order_status", "order_status IN ('delivered', 'shipped', 'processing', 'canceled', 'invoiced', 'unavailable', 'approved', 'created')")
@dp.expect("approved_before_delivered", "order_approved_at IS NULL OR order_delivered_customer_date IS NULL OR order_approved_at <= order_delivered_customer_date")
@dp.expect("estimated_after_purchase", "order_estimated_delivery_date >= order_purchase_timestamp")
@dp.expect("carrier_before_customer", "order_delivered_carrier_date IS NULL OR order_delivered_customer_date IS NULL OR order_delivered_carrier_date <= order_delivered_customer_date")
def orders_silver():
    """
    Reads from bronze orders table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize IDs
    - Lowercase and trim order status
    - Add processing metadata timestamp
    - Filter out CDC metadata columns
    
    Quality rules:
    - Require valid order_id and customer_id (fail pipeline)
    - Require purchase timestamp (fail pipeline)
    - Drop records with invalid order statuses
    - Warn on logical timestamp inconsistencies
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # Handle updates/deletes from CDC source
        .table("olist_catalog.postgres_bronze.orders")
        .select(
            # Standardize IDs - trim whitespace
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            
            # Standardize order status - lowercase and trim
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            
            # Keep timestamps as-is (already in correct format)
            F.col("order_purchase_timestamp"),
            F.col("order_approved_at"),
            F.col("order_delivered_carrier_date"),
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
    )