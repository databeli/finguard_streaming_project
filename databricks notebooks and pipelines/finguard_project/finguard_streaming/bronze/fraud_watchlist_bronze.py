from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
name="finguard.bronze.fraud_watchlist"
,comment="Fraud watchlist raw stream data ingested by auto loader"
)
def fraud_watchlist_bronze() -> DataFrame:

    streaming_df=(spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.inferColumnTypes","true")
              .load("/Volumes/finguard/source/fraud_watchlist/source_data/")
    )

    parsed_streaming_df=streaming_df.select(
    F.col("watchlist_id"),
    F.col("watch_type"),
    F.col("entity_id"),
    F.col("risk_level"),
    F.col("action"),
    F.col("reason_code"),
    F.col("reason_description"),
    F.col("status"),
    F.col("effective_from"),
    F.col("reported_by"),
    F.col("reported_source"),
    F.col("country"),
    F.col("city"),
    F.col("_rescued_data"),
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df