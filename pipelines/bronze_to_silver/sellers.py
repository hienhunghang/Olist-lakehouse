from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="sellers",
    comment="Standardized and validated seller data from bronze layer"
)
@dp.expect_or_fail("valid_seller_id", "seller_id IS NOT NULL AND LENGTH(TRIM(seller_id)) > 0")
@dp.expect_or_fail("valid_city", "seller_city IS NOT NULL AND LENGTH(TRIM(seller_city)) > 0")
@dp.expect_or_fail("valid_state", "seller_state IS NOT NULL AND LENGTH(TRIM(seller_state)) = 2")
@dp.expect_or_drop("valid_zip_code", "seller_zip_code_prefix IS NOT NULL AND seller_zip_code_prefix > 0")
@dp.expect("reasonable_zip_code", "seller_zip_code_prefix BETWEEN 1000 AND 99999")
def sellers_silver():
    """
    Reads from bronze sellers table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize seller_id
    - Trim and uppercase state codes for consistency
    - Trim and title case city names
    - Add processing metadata timestamp
    - Remove duplicates based on seller_id
    
    Quality rules:
    - Require valid seller_id (fail pipeline)
    - Require valid seller_city (fail pipeline)
    - Require valid seller_state with 2 characters (fail pipeline)
    - Drop records with invalid or null zip codes
    - Warn if zip code is outside reasonable range (1000-99999)
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.sellers")
        .select(
            # Standardize seller ID - trim whitespace
            F.trim(F.col("seller_id")).alias("seller_id"),
            
            # Keep zip code as-is (integer)
            F.col("seller_zip_code_prefix"),
            
            # Standardize city - trim and title case
            F.initcap(F.trim(F.col("seller_city"))).alias("seller_city"),
            
            # Standardize state - trim and uppercase (state codes should be uppercase)
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["seller_id"])
    )