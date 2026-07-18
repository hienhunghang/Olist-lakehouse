from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="olist_catalog.olist_gold.fact_marketing",
    comment="Marketing campaign fact table with dimension foreign keys and spend measures"
)
def fact_marketing():
    # Read source table
    marketing = spark.read.table("olist_catalog.olist_silver.marketing_campaign")
    
    # Read dimension tables for foreign key lookups
    dim_sellers = spark.read.table("olist_catalog.olist_gold.dim_sellers")
    dim_time = spark.read.table("olist_catalog.olist_gold.dim_time")
    
    # Create fact table with foreign keys and measures
    fact = marketing.select(
        # Foreign keys to dimension tables
        F.col("seller_id").alias("seller_key"),
        
        # Date foreign key (date_key in YYYYMMDD format)
        F.date_format(F.col("campaign_month"), "yyyyMMdd").cast("int").alias("campaign_date_key"),
        
        # Degenerate dimensions (attributes kept in fact table)
        F.col("campaign_month"),
        F.col("campaign_year"),
        F.col("campaign_month_num"),
        F.col("channel"),
        
        # Measures - additive
        F.lit(1).alias("quantity"),  # For counting campaigns
        F.col("ad_spend"),
        F.col("discount_percent"),
        
        # Derived measures
        F.when(F.col("ad_spend") > 1000, "High")
         .when(F.col("ad_spend") > 500, "Medium")
         .otherwise("Low").alias("spend_category"),
        
        F.when(F.col("discount_percent") >= 20, "High")
         .when(F.col("discount_percent") >= 10, "Medium")
         .otherwise("Low").alias("discount_category"),
        
        # Metadata
        F.current_timestamp().alias("fact_created_at")
    )
    
    return fact
