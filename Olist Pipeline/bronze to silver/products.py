from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="products",
    comment="Standardized and validated product data from bronze layer"
)
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_drop("valid_dimensions", "product_weight_g > 0 AND product_length_cm > 0 AND product_height_cm > 0 AND product_width_cm > 0")
@dp.expect_or_drop("valid_photos_qty", "product_photos_qty IS NULL OR product_photos_qty >= 0")
@dp.expect("has_category", "product_category_name IS NOT NULL AND LENGTH(TRIM(product_category_name)) > 0")
@dp.expect("reasonable_weight", "product_weight_g IS NULL OR product_weight_g <= 100000")
@dp.expect("reasonable_dimensions", "product_length_cm IS NULL OR (product_length_cm <= 300 AND product_height_cm <= 300 AND product_width_cm <= 300)")
@dp.expect("has_name_info", "product_name_length IS NOT NULL AND product_name_length > 0")
def products_silver():
    """
    Reads from bronze products table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize product_id
    - Lowercase and trim product_category_name for consistency
    - Cast numeric lengths and quantities to integers
    - Calculate product volume from dimensions
    - Fix column name typos (lenght -> length)
    - Add processing metadata timestamp
    - Remove duplicates based on product_id
    
    Quality rules:
    - Require valid product_id (fail pipeline)
    - Drop records with invalid dimensions (must be positive)
    - Drop records with negative photo quantities
    - Warn if product has no category name
    - Warn if weight exceeds 100kg (100,000g)
    - Warn if dimensions exceed 3 meters (300cm)
    - Warn if product has no name information
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.products")
        .select(
            # Standardize product ID - trim whitespace
            F.trim(F.col("product_id")).alias("product_id"),
            
            # Standardize category name - lowercase and trim
            F.when(
                F.col("product_category_name").isNotNull(),
                F.lower(F.trim(F.col("product_category_name")))
            ).alias("product_category_name"),
            
            # Cast text lengths to integers (they represent character counts) and fix typo
            F.col("product_name_lenght").cast("int").alias("product_name_length"),
            F.col("product_description_lenght").cast("int").alias("product_description_length"),
            
            # Cast photo quantity to integer
            F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
            
            # Keep physical measurements as doubles (can have decimals)
            F.col("product_weight_g"),
            F.col("product_length_cm"),
            F.col("product_height_cm"),
            F.col("product_width_cm"),
            
            # Calculate volume in cubic centimeters
            (
                F.col("product_length_cm") * 
                F.col("product_height_cm") * 
                F.col("product_width_cm")
            ).alias("product_volume_cm3"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["product_id"])
    )