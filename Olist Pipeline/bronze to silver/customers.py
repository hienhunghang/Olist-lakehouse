from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="customers",
    comment="Standardized and validated customer data from bronze layer"
)
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) > 0")
@dp.expect_or_fail("valid_unique_id", "customer_unique_id IS NOT NULL AND LENGTH(TRIM(customer_unique_id)) > 0")
@dp.expect_or_fail("valid_city", "customer_city IS NOT NULL AND LENGTH(TRIM(customer_city)) > 0")
@dp.expect_or_fail("valid_state", "customer_state IS NOT NULL AND LENGTH(TRIM(customer_state)) = 2")
@dp.expect_or_drop("valid_zip_code", "customer_zip_code_prefix IS NOT NULL AND customer_zip_code_prefix > 0")
@dp.expect("reasonable_zip_code", "customer_zip_code_prefix BETWEEN 1000 AND 99999")
def customers_silver():
    """
    Reads from bronze customers table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize IDs
    - Trim and uppercase state codes for consistency
    - Trim and title case city names
    - Add processing metadata timestamp
    - Remove duplicates based on customer_id
    
    Quality rules:
    - Require valid customer_id (fail pipeline)
    - Require valid customer_unique_id (fail pipeline)
    - Require valid customer_city (fail pipeline)
    - Require valid customer_state with 2 characters (fail pipeline)
    - Drop records with invalid or null zip codes
    - Warn if zip code is outside reasonable range (1000-99999)
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.customers")
        .select(
            # Standardize IDs - trim whitespace
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("customer_unique_id"),
            
            # Keep zip code as-is (integer)
            F.col("customer_zip_code_prefix"),
            
            # Standardize city - trim and title case
            F.initcap(F.trim(F.col("customer_city"))).alias("customer_city"),
            
            # Standardize state - trim and uppercase (state codes should be uppercase)
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["customer_id"])
    )