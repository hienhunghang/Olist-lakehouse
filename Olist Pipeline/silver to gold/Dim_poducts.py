from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="olist_catalog.olist_gold.dim_products",
    comment="Product dimension table with category and physical attributes"
)
def dim_products():
    # Read products from silver layer
    products = spark.read.table("olist_catalog.olist_silver.products")
    
    # Create product dimension with enhanced attributes
    dim_products = products.select(
        # Primary key
        F.col("product_id"),
        
        # Category attributes
        F.col("product_category_name"),
        
        # Content attributes
        F.col("product_name_length"),
        F.col("product_description_length"),
        F.col("product_photos_qty"),
        
        # Physical dimensions
        F.col("product_weight_g"),
        F.col("product_length_cm"),
        F.col("product_height_cm"),
        F.col("product_width_cm"),
        F.col("product_volume_cm3"),
        
        # Derived attributes - Weight categories
        F.when(F.col("product_weight_g") <= 500, "Light")
         .when(F.col("product_weight_g") <= 2000, "Medium")
         .when(F.col("product_weight_g") <= 5000, "Heavy")
         .when(F.col("product_weight_g") > 5000, "Very Heavy")
         .otherwise("Unknown").alias("weight_category"),
        
        # Derived attributes - Size categories based on volume
        F.when(F.col("product_volume_cm3") <= 1000, "Small")
         .when(F.col("product_volume_cm3") <= 10000, "Medium")
         .when(F.col("product_volume_cm3") <= 50000, "Large")
         .when(F.col("product_volume_cm3") > 50000, "Extra Large")
         .otherwise("Unknown").alias("size_category"),
        
        # Product information quality indicator
        F.when(
            (F.col("product_photos_qty") >= 3) & 
            (F.col("product_description_length") >= 100),
            "High"
        ).when(
            (F.col("product_photos_qty") >= 1) & 
            (F.col("product_description_length") >= 50),
            "Medium"
        ).otherwise("Low").alias("info_quality"),
        
        # Metadata
        F.col("processed_at").alias("source_processed_at"),
        F.current_timestamp().alias("dim_created_at")
    )
    
    return dim_products
