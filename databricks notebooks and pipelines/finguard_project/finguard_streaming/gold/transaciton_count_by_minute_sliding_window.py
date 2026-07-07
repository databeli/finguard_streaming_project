from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


@dp.table(
name="finguard.gold.transaciton_count_by_minute_sliding_window"
,comment="Alert details where transaciton has been performed with value higher than what is configured by customer"
)
def transaciton_count_by_minute() -> DataFrame:

    transactions=spark.readStream.table("finguard.silver.transactions")

    transactions_with_watermark=transactions.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df=transactions_with_watermark.groupBy(
        F.window("transaction_timestamp","5 minutes","1 minute")
    ).agg(

    F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df
