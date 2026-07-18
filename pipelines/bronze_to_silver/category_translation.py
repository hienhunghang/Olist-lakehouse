from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="category_translation",
    comment="Standardized product category translations from Portuguese to English"
)
@dp.expect_or_fail("valid_category_name", "product_category_name IS NOT NULL AND LENGTH(TRIM(product_category_name)) > 0")
@dp.expect_or_fail("valid_category_english", "product_category_name_english IS NOT NULL AND LENGTH(TRIM(product_category_name_english)) > 0")
@dp.expect("meaningful_translation", "LOWER(TRIM(product_category_name)) != LOWER(TRIM(product_category_name_english))")
def category_translation_silver():
    """
    Reads from bronze category_translation table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and lowercase category names for consistency
    - Remove leading/trailing whitespace
    - Add processing metadata timestamp
    
    Quality rules:
    - Require valid product_category_name (fail pipeline)
    - Require valid product_category_name_english (fail pipeline)
    - Warn if translation is same as original (likely missing translation)
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.category_translation")
        .select(
            # Standardize category names - trim and lowercase for consistency
            F.lower(F.trim(F.col("product_category_name"))).alias("product_category_name"),
            F.lower(F.trim(F.col("product_category_name_english"))).alias("product_category_name_english"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["product_category_name"])
    )