from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
name="finguard.silver.fraud_watchlist"
,comment="Cleaned fraud watchlist"
)
def fraud_watchlist_silver() -> DataFrame:

    bronze_df=(spark.readStream.table("finguard.bronze.fraud_watchlist"))

    cleaned_df=bronze_df.select(
    F.upper(F.col("watchlist_id")).alias("watchlist_id"),
    F.col("watch_type"),
   F.upper(F.col("entity_id")).alias("entity_id"),
   F.upper(F.col("risk_level")).alias("risk_level"),
   F.upper(F.col("action")).alias("action"),
    F.col("reason_code"),
    F.col("reason_description"),
    F.col("status"),
    F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
    F.col("reported_by"),
    F.col("reported_source"),
    F.col("country"),
    F.col("city"),
    F.col("source_file"),
    F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return cleaned_df