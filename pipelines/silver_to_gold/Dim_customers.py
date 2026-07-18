from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="olist_catalog.olist_gold.dim_customers",
    comment="Customer dimension table with geographic and business attributes"
)
def dim_customers():
    # Read customers from silver layer
    customers = spark.read.table("olist_catalog.olist_silver.customers")
    
    # Create customer dimension with enhanced attributes
    dim_customers = customers.select(
        # Primary keys
        F.col("customer_id"),
        F.col("customer_unique_id"),
        
        # Geographic attributes
        F.col("customer_zip_code_prefix"),
        F.col("customer_city"),
        F.col("customer_state"),
        
        # Derived geographic attributes
        F.concat(
            F.col("customer_city"), 
            F.lit(", "), 
            F.col("customer_state")
        ).alias("customer_location"),
        
        # Region classification (Brazilian regions)
        F.when(F.col("customer_state").isin(["AC", "AP", "AM", "PA", "RO", "RR", "TO"]), "North")
         .when(F.col("customer_state").isin(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]), "Northeast")
         .when(F.col("customer_state").isin(["GO", "MT", "MS", "DF"]), "Central-West")
         .when(F.col("customer_state").isin(["ES", "MG", "RJ", "SP"]), "Southeast")
         .when(F.col("customer_state").isin(["PR", "RS", "SC"]), "South")
         .otherwise("Unknown").alias("customer_region"),
        
        # Metadata
        F.col("processed_at").alias("source_processed_at"),
        F.current_timestamp().alias("dim_created_at")
    )
    
    return dim_customers
