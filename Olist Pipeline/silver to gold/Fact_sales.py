from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="olist_catalog.olist_gold.fact_sales",
    comment="Sales fact table at order item grain with dimension foreign keys and measures"
)
def fact_sales():
    # Read source tables
    order_items = spark.read.table("olist_catalog.olist_silver.order_items")
    orders = spark.read.table("olist_catalog.olist_silver.orders")
    
    # Read dimension tables for foreign key lookups
    dim_customers = spark.read.table("olist_catalog.olist_gold.dim_customers")
    dim_sellers = spark.read.table("olist_catalog.olist_gold.dim_sellers")
    dim_products = spark.read.table("olist_catalog.olist_gold.dim_products")
    dim_time = spark.read.table("olist_catalog.olist_gold.dim_time")
    
    # Join order_items with orders to get order-level information
    base_fact = order_items.join(
        orders,
        "order_id",
        "inner"
    )
    
    # Create fact table with foreign keys and measures
    fact = base_fact.select(
        # Composite primary key
        F.col("order_id"),
        F.col("order_item_id"),
        
        # Foreign keys to dimension tables
        F.col("customer_id").alias("customer_key"),
        F.col("seller_id").alias("seller_key"),
        F.col("product_id").alias("product_key"),
        
        # Date foreign keys (date_key in YYYYMMDD format)
        F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.date_format(F.col("order_approved_at"), "yyyyMMdd").cast("int").alias("approved_date_key"),
        F.date_format(F.col("order_delivered_carrier_date"), "yyyyMMdd").cast("int").alias("carrier_date_key"),
        F.date_format(F.col("order_delivered_customer_date"), "yyyyMMdd").cast("int").alias("delivered_date_key"),
        F.date_format(F.col("order_estimated_delivery_date"), "yyyyMMdd").cast("int").alias("estimated_delivery_date_key"),
        
        # Degenerate dimensions (attributes kept in fact table)
        F.col("order_status"),
        F.col("order_purchase_timestamp"),
        F.col("order_approved_at"),
        F.col("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),
        F.col("shipping_limit_date"),
        
        # Measures - additive
        F.lit(1).alias("quantity"),
        F.col("price"),
        F.col("freight_value"),
        F.col("total_item_cost"),
        
        # Derived measures - delivery performance
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        ).alias("days_to_delivery"),
        
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        ).alias("delivery_vs_estimate_days"),
        
        F.when(
            F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"),
            1
        ).otherwise(0).alias("on_time_delivery_flag"),
        
        # Processing time measures
        F.datediff(
            F.col("order_approved_at"),
            F.col("order_purchase_timestamp")
        ).alias("days_to_approval"),
        
        F.datediff(
            F.col("order_delivered_carrier_date"),
            F.col("order_approved_at")
        ).alias("days_to_carrier"),
        
        # Metadata
        F.current_timestamp().alias("fact_created_at")
    )
    
    return fact
