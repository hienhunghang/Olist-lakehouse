from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="olist_catalog.olist_gold.fact_inventory",
    comment="Inventory snapshot fact table with dimension foreign keys and stock measures"
)
def fact_inventory():
    # Read source table
    inventory = spark.read.table("olist_catalog.olist_silver.inventory_snapshot")
    
    # Read dimension tables for foreign key lookups
    dim_products = spark.read.table("olist_catalog.olist_gold.dim_products")
    dim_time = spark.read.table("olist_catalog.olist_gold.dim_time")
    
    # Create fact table with foreign keys and measures
    fact = inventory.select(
        # Foreign keys to dimension tables
        F.col("product_id").alias("product_key"),
        
        # Date foreign key (date_key in YYYYMMDD format)
        F.date_format(F.col("snapshot_date"), "yyyyMMdd").cast("int").alias("snapshot_date_key"),
        
        # Degenerate dimensions (attributes kept in fact table)
        F.col("product_category_name"),

        # Measures - semi-additive (additive across products, not across time)
        F.col("stock_level"),
        
        # Derived measures - stock status categories
        F.when(F.col("stock_level") == 0, "Out of Stock")
         .when(F.col("stock_level") <= 10, "Low Stock")
         .when(F.col("stock_level") <= 50, "Medium Stock")
         .when(F.col("stock_level") > 50, "High Stock")
         .otherwise("Unknown").alias("stock_status"),
        
        # Stock availability flag
        F.when(F.col("stock_level") > 0, 1).otherwise(0).alias("in_stock_flag"),
        
        # Metadata
        F.current_timestamp().alias("fact_created_at")
    )
    
    return fact
