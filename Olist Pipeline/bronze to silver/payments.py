from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="payments",
    comment="Standardized and validated payment data from bronze layer"
)
@dp.expect_or_fail("valid_order_id", "order_id IS NOT NULL AND LENGTH(TRIM(order_id)) > 0")
@dp.expect_or_fail("valid_payment_sequential", "payment_sequential IS NOT NULL AND payment_sequential > 0")
@dp.expect_or_fail("valid_payment_type", "payment_type IS NOT NULL AND LENGTH(TRIM(payment_type)) > 0")
@dp.expect_or_drop("valid_payment_types", "LOWER(TRIM(payment_type)) IN ('credit_card', 'debit_card', 'boleto', 'voucher', 'not_defined')")
@dp.expect_or_drop("valid_installments", "payment_installments IS NOT NULL AND payment_installments > 0")
@dp.expect_or_drop("valid_payment_value", "payment_value IS NOT NULL AND payment_value >= 0")
@dp.expect("reasonable_payment_value", "payment_value <= 100000")
@dp.expect("reasonable_installments", "payment_installments <= 24")
@dp.expect("installments_match_value", "payment_installments = 1 OR payment_value > 0")
def payments_silver():
    """
    Reads from bronze payments table, applies standardization and quality checks.
    
    Standardization operations:
    - Trim and normalize order_id
    - Lowercase and trim payment_type for consistency
    - Add processing metadata timestamp
    - Remove duplicates based on (order_id, payment_sequential)
    
    Quality rules:
    - Require valid order_id (fail pipeline)
    - Require valid payment_sequential (positive integer, fail pipeline)
    - Require valid payment_type (fail pipeline)
    - Drop records with invalid payment types (must be known types)
    - Drop records with invalid installments (must be positive)
    - Drop records with invalid or negative payment values
    - Warn if payment value is unreasonably high (>100,000)
    - Warn if installments exceed 24
    - Warn if multi-installment payment has zero value
    """
    return (
        spark.read
        .table("olist_catalog.postgres_bronze.payments")
        .select(
            # Standardize order ID - trim whitespace
            F.trim(F.col("order_id")).alias("order_id"),
            
            # Keep payment sequential as-is
            F.col("payment_sequential"),
            
            # Standardize payment type - lowercase and trim
            F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
            
            # Keep numeric values as-is
            F.col("payment_installments"),
            F.col("payment_value"),
            
            # Add processing metadata
            F.current_timestamp().alias("processed_at")
        )
        # Remove duplicates if any exist
        .dropDuplicates(["order_id", "payment_sequential"])
    )