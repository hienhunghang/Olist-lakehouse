from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

@dp.materialized_view(
    name="olist_catalog.olist_gold.dim_time",
    comment="Time dimension table with standard date attributes and Brazil holiday information"
)
def dim_time():
    # Generate date spine from 2016 to 2026 (adjust range as needed)
    from datetime import datetime
    start_date = "2016-01-01"
    end_date = "2026-12-31"
    
    # Calculate number of days between start and end dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    num_days = (end_dt - start_dt).days + 1
    
    # Create date spine with all dates in range
    date_spine = (
        spark.range(0, num_days)
        .select(
            F.date_add(F.lit(start_date), F.col("id").cast("int")).alias("date")
        )
    )
    
    # Add standard time dimension attributes
    time_dim = date_spine.select(
        # Primary date column
        F.col("date"),
        
        # Date key (YYYYMMDD format as integer)
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        
        # Year attributes
        F.year("date").alias("year"),
        F.date_format("date", "yyyy").alias("year_string"),
        
        # Quarter attributes
        F.quarter("date").alias("quarter"),
        F.concat(F.lit("Q"), F.quarter("date")).alias("quarter_string"),
        F.concat(F.year("date"), F.lit("-Q"), F.quarter("date")).alias("year_quarter"),
        
        # Month attributes
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "MMM").alias("month_short_name"),
        F.date_format("date", "yyyyMM").alias("year_month"),
        
        # Week attributes
        F.weekofyear("date").alias("week_of_year"),
        
        # Day attributes
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofyear("date").alias("day_of_year"),
        F.dayofweek("date").alias("day_of_week"),  # 1=Sunday, 7=Saturday
        F.date_format("date", "EEEE").alias("day_name"),
        F.date_format("date", "EEE").alias("day_short_name"),
        
        # Weekend flag
        F.when(F.dayofweek("date").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        
        # Fiscal attributes (can adjust fiscal year start if needed)
        F.when(F.month("date") >= 1, F.year("date")).otherwise(F.year("date") - 1).alias("fiscal_year")
    )
    
    # Read Brazil holidays
    holidays = spark.read.table("olist_catalog.olist_silver.brazil_holidays")
    
    # Join with holidays to add holiday information
    result = time_dim.join(
        holidays.select(
            F.col("holiday_date").alias("date"),
            F.col("holiday_name"),
            F.col("holiday_local_name"),
            F.col("holiday_types"),
            F.col("is_fixed"),
            F.col("is_global"),
            F.col("counties")
        ),
        "date",
        "left"
    ).select(
        # Date attributes
        "date_key",
        "date",
        
        # Year attributes
        "year",
        
        # Quarter attributes
        "quarter",
        "year_quarter",
        
        # Month attributes
        "month",
        "month_name",
        "year_month",
        
        # Week attributes
        "week_of_year",
        
        # Day attributes
        "day_of_month",
        "day_of_year",
        "day_of_week",
        "day_name",
        
        # Flags
        "is_weekend",
        F.col("holiday_name").isNotNull().alias("is_holiday"),
        
        # Holiday attributes from brazil_holidays
        "holiday_name",
        "holiday_local_name"
    )
    
    return result