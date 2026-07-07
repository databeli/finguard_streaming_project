# Databricks notebook source
source_path='/Volumes/finguard/source/fraud_watchlist/source_data/'

# COMMAND ----------

dbutils.fs.ls(source_path)

# COMMAND ----------

input_stream=(spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", "/Volumes/finguard/source/fraud_watchlist/schema/")
              .option("cloudFiles.inferColumnTypes","true")
              .load(source_path)
)


# COMMAND ----------

from pyspark.sql import functions as F
tranformed_df=input_stream.select(
"*",
F.col("_metadata.file_path").alias("file_path"),
F.current_timestamp().alias("ingestion_timestamp")
)

# COMMAND ----------

streaming_query=(tranformed_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation", "/Volumes/finguard/source/fraud_watchlist/checkpoint/")
.trigger(availableNow=True)
.toTable("finguard.bronze.fraud_watchlist_batch_test")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.fraud_watchlist_batch_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.fraud_watchlist