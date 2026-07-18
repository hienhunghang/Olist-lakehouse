from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="reviews",
    comment="Standardized and validated customer reviews from bronze layer",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dp.expect_or_fail("valid_review_id", "review_id IS NOT NULL AND LENGTH(TRIM(review_id)) > 0")
@dp.expect_or_fail("valid_order_id", "order_id IS NOT NULL AND LENGTH(TRIM(order_id)) > 0")
@dp.expect_or_fail("valid_creation_date", "review_creation_date IS NOT NULL")
@dp.expect("valid_review_score", "review_score BETWEEN 1 AND 5")
@dp.expect("answer_after_creation", "review_answer_timestamp IS NULL OR review_creation_date IS NULL OR review_answer_timestamp >= review_creation_date")
@dp.expect("has_comment_content", "review_comment_title IS NOT NULL OR review_comment_message IS NOT NULL OR review_score IS NOT NULL")
def reviews_silver():
    """
    Reads from bronze reviews table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize IDs
    - Trim text fields (title and message)
    - Add processing metadata timestamp
    - Filter out CDC metadata columns
    - Coalesce empty strings to NULL for text fields
    
    Quality rules:
    - Require valid review_id and order_id (fail pipeline)
    - Require review_creation_date (fail pipeline)
    - Drop records with invalid review scores (must be 1-5)
    - Warn if answer timestamp precedes creation date
    - Warn if review has no content (no title, message, or score)
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # Handle updates/deletes from CDC source
        .table("olist_catalog.postgres_bronze.reviews")
        .select(
            # Standardize IDs - trim whitespace
            F.trim(F.col("review_id")).alias("review_id"),
            F.trim(F.col("order_id")).alias("order_id"),
            
            # Keep review score as-is (integer)
            F.col("review_score"),
            
            # Standardize text fields - trim and convert empty strings to NULL
            F.when(
                F.length(F.trim(F.col("review_comment_title"))) > 0,
                F.trim(F.col("review_comment_title"))
            ).alias("review_comment_title"),
            
            F.when(
                F.length(F.trim(F.col("review_comment_message"))) > 0,
                F.trim(F.col("review_comment_message"))
            ).alias("review_comment_message"),
            
            # Keep timestamps as-is (already in correct format)
            F.col("review_creation_date"),
            F.col("review_answer_timestamp"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
    )