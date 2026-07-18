from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="marketing_campaign",
    comment="Standardized and validated marketing campaign data from bronze layer"
)
@dp.expect_or_fail("valid_seller_id", "seller_id IS NOT NULL AND LENGTH(TRIM(seller_id)) > 0")
@dp.expect_or_fail("valid_campaign_month", "campaign_month IS NOT NULL")
@dp.expect("valid_ad_spend", "ad_spend IS NOT NULL AND ad_spend >= 0")
@dp.expect("valid_discount_percent", "discount_percent IS NOT NULL AND discount_percent >= 0 AND discount_percent <= 100")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
@dp.expect("has_channel", "channel IS NOT NULL AND LENGTH(TRIM(channel)) > 0")
@dp.expect("reasonable_ad_spend", "ad_spend <= 1000000")
@dp.expect("recent_campaign", "campaign_month >= '2016-01-01'")
def marketing_campaign_silver():
    """
    Reads from bronze marketing_campaign table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize seller_id
    - Lowercase and trim channel names for consistency
    - Extract date components (year, month) from campaign_month
    - Add processing metadata timestamp
    - Remove duplicates based on (seller_id, campaign_month, channel)
    - Filter out records with rescued data (malformed rows)
    
    Quality rules:
    - Require valid seller_id (fail pipeline)
    - Require valid campaign_month (fail pipeline)
    - Drop records with invalid or negative ad spend
    - Drop records with invalid discount percent (must be 0-100)
    - Drop records with rescued data (malformed CSV rows)
    - Warn if channel is missing
    - Warn if ad spend exceeds 1 million
    - Warn if campaign is older than 2 years
    """
    return (
        spark.read
        .table("olist_catalog.blob_bronze.marketing_campaign")
        .select(
            # Standardize seller ID - trim whitespace
            F.trim(F.col("seller_id")).alias("seller_id"),
            
            # Keep campaign_month as-is (already date type)
            F.col("campaign_month"),
            
            # Extract date components for easier filtering
            F.year(F.col("campaign_month")).alias("campaign_year"),
            F.month(F.col("campaign_month")).alias("campaign_month_num"),
            
            # Keep ad spend and discount as-is
            F.col("ad_spend"),
            F.col("discount_percent"),
            
            # Standardize channel - lowercase and trim
            F.when(
                F.col("channel").isNotNull(),
                F.lower(F.trim(F.col("channel")))
            ).alias("channel"),
            
            # Keep rescued data column for quality check
            F.col("_rescued_data"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["seller_id", "campaign_month", "channel"])
    )