from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="brazil_holidays",
    comment="Standardized and validated Brazil holiday data from bronze layer"
)
@dp.expect_or_fail("valid_holiday_date", "holiday_date IS NOT NULL")
@dp.expect_or_fail("valid_country_code", "country_code = 'BR'")
@dp.expect_or_fail("valid_year", "year IS NOT NULL AND year > 0")
@dp.expect_or_drop("valid_date_format", "holiday_date >= '1900-01-01' AND holiday_date <= '2100-12-31'")
@dp.expect("has_holiday_name", "holiday_name IS NOT NULL AND LENGTH(TRIM(holiday_name)) > 0")
@dp.expect("has_local_name", "holiday_local_name IS NOT NULL AND LENGTH(TRIM(holiday_local_name)) > 0")
@dp.expect("reasonable_year", "year BETWEEN 2000 AND 2100")
def brazil_holidays_silver():
    """
    Reads from bronze brazil_holidays_raw table, applies standardization and quality checks.
    
    Standardization operations:
    - Parse holiday_date string to date type
    - Trim and clean text fields (holiday names)
    - Cast launch_year to integer
    - Extract year, month, day from holiday_date
    - Add processing metadata timestamp
    - Remove duplicates based on (holiday_date, holiday_name)
    
    Quality rules:
    - Require valid holiday_date (fail pipeline)
    - Require country_code = 'BR' (fail pipeline)
    - Require valid year (fail pipeline)
    - Drop records with invalid date format
    - Warn if holiday has no English name
    - Warn if holiday has no local name
    - Warn if year is outside reasonable range (2000-2100)
    """
    return (
        spark.read
        .table("olist_catalog.api_bronze.brazil_holidays_raw")
        .select(
            # Parse date string to date type
            F.to_date(F.col("holiday_date")).alias("holiday_date"),
            
            # Extract date components for easier filtering
            F.year(F.to_date(F.col("holiday_date"))).alias("holiday_year"),
            F.month(F.to_date(F.col("holiday_date"))).alias("holiday_month"),
            F.dayofmonth(F.to_date(F.col("holiday_date"))).alias("holiday_day"),
            F.dayofweek(F.to_date(F.col("holiday_date"))).alias("holiday_day_of_week"),
            
            # Standardize text fields - trim whitespace
            F.trim(F.col("holiday_local_name")).alias("holiday_local_name"),
            F.trim(F.col("holiday_name")).alias("holiday_name"),
            
            # Keep country code and boolean flags as-is
            F.col("country_code"),
            F.col("is_fixed"),
            F.col("is_global"),
            
            # Keep optional fields
            F.col("counties"),
            F.col("holiday_types"),
            
            # Cast launch_year to integer
            F.col("launch_year").cast("int").alias("launch_year"),
            
            # Keep year as-is (already bigint)
            F.col("year"),
            
            # Keep ingestion time from bronze
            F.col("ingestion_time"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["holiday_date", "holiday_name"])
    )