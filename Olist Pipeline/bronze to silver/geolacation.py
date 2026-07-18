from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="geolocation",
    comment="Standardized geolocation data with quality rules"
)
@dp.expect_or_fail("valid_zip_code_prefix", "geolocation_zip_code_prefix IS NOT NULL AND geolocation_zip_code_prefix > 0")
@dp.expect_or_fail("valid_state", "geolocation_state IS NOT NULL AND LENGTH(TRIM(geolocation_state)) = 2")
@dp.expect_or_fail("valid_city", "geolocation_city IS NOT NULL AND LENGTH(TRIM(geolocation_city)) > 0")
@dp.expect_or_drop("valid_lat_lng", "geolocation_lat IS NOT NULL AND geolocation_lng IS NOT NULL AND geolocation_lat BETWEEN -90 AND 90 AND geolocation_lng BETWEEN -180 AND 180")
@dp.expect("reasonable_zip_code", "geolocation_zip_code_prefix BETWEEN 1000 AND 99999")
def geolocation_silver():
    """
    Reads from bronze geolocation table, applies standardization and quality checks.

    Standardization operations:
    - Trim and uppercase state codes for consistency
    - Trim and title case city names
    - Coerce lat/lng to float
    - Add processing metadata timestamp
    - Remove duplicates based on (zip_code_prefix, city, state)

    Quality rules:
    - Require valid and positive zip code prefix (fail pipeline)
    - Require valid state and city (fail pipeline)
    - Drop invalid coordinates (lat/lng null or out of range)
    - Warn if zip code is outside reasonable range (1000-99999)
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.geolocation")
        .select(
            F.col("geolocation_zip_code_prefix"),
            F.col("geolocation_lat"),
            F.col("geolocation_lng"),
            F.initcap(F.trim(F.col("geolocation_city"))).alias("geolocation_city"),
            F.upper(F.trim(F.col("geolocation_state"))).alias("geolocation_state"),
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"])
    )