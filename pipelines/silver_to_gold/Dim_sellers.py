from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="olist_catalog.olist_gold.dim_sellers",
    comment="Seller dimension table with geographic and business attributes"
)
def dim_sellers():
    # Read sellers from silver layer
    sellers = spark.read.table("olist_catalog.olist_silver.sellers")
    
    # Create seller dimension with enhanced attributes
    dim_sellers = sellers.select(
        # Primary key
        F.col("seller_id"),
        
        # Geographic attributes
        F.col("seller_zip_code_prefix"),
        F.col("seller_city"),
        F.col("seller_state"),
        
        # Derived geographic attributes
        F.concat(
            F.col("seller_city"), 
            F.lit(", "), 
            F.col("seller_state")
        ).alias("seller_location"),
        
        # Region classification (Brazilian regions)
        F.when(F.col("seller_state").isin(["AC", "AP", "AM", "PA", "RO", "RR", "TO"]), "North")
         .when(F.col("seller_state").isin(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]), "Northeast")
         .when(F.col("seller_state").isin(["GO", "MT", "MS", "DF"]), "Central-West")
         .when(F.col("seller_state").isin(["ES", "MG", "RJ", "SP"]), "Southeast")
         .when(F.col("seller_state").isin(["PR", "RS", "SC"]), "South")
         .otherwise("Unknown").alias("seller_region"),
        
        # Metadata
        F.col("processed_at").alias("source_processed_at"),
        F.current_timestamp().alias("dim_created_at")
    )
    
    return dim_sellers