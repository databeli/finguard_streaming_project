from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

@dp.table(
name="finguard.bronze.transactions"
,comment="Transctions raw stream data ingested by Kafka"
)
def transactions_bronze() -> DataFrame:
    kafka_connection_json=dbutils.secrets.get(scope="finguard-scope",key="kafka_connection_details")
    kafka_config=json.loads(kafka_connection_json)
    bootstrap_servers=kafka_config['bootstrap_servers']
    api_key=kafka_config['api_key']
    api_secret=kafka_config['api_secret']
    topic=kafka_config['topic']

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

    streaming_df=(spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers",bootstrap_servers)
              .option("subscribe",topic)
            .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets","earliest")
        .load()
    )

    parsed_streaming_df=streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType"),
    F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df