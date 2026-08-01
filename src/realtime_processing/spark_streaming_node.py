from pyspark.sql import SparkSession
from pyspark.sql.functions import window, pandas_udf, PandasUDFType, col, sum as _sum, from_json, to_timestamp
from pyspark.sql.types import *
import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USERNAME = os.environ.get("KAFKA_USERNAME", "kafka_client")
PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC_INPUT = "imat3b-DOGE"
TOPIC_OUTPUT = "imat3b-DOGE-VWAP"
SYMBOL = "DOGEUSDT"


def main():
    spark = (
        SparkSession.builder.appName("CryptoVWAPCalculator")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )
        .master("local[*]")
        .getOrCreate()
    )

    
    df_kafka_input = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_INPUT)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{USERNAME}" password="{PASSWORD}";',
        )
        .load()
    )

    schema = StructType([
        StructField("symbol", StringType()),
        StructField("@timestamp", StringType()),
        StructField("close", StringType()),
        StructField("volume", StringType())
    ])


    df = (
        df_kafka_input
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
        .withColumn("close", col("close").cast(DoubleType()))
        .withColumn("volume", col("volume").cast(DoubleType()))
        .withColumn("event_time", to_timestamp(col("@timestamp")))
        .filter(col("close").isNotNull() & col("volume").isNotNull())
    )

    agg = (
        df.withWatermark("event_time", "5 minutes")
        .groupBy(
            window("event_time", "5 minutes"),
            col("symbol")
        )
        .agg(
            (_sum(col("close") * col("volume")) / _sum("volume")).alias("vwap")
        )
    )

    # 4. Salida limpia para Kafka
    output = agg.select(
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("symbol"),
        col("vwap")
    )

    df_kafka_output = output.selectExpr(
        "CAST(symbol AS STRING) as key",
        "to_json(struct(*)) as value"
    )
    
    query = (
        df_kafka_output.writeStream.format("kafka").outputMode("append")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{USERNAME}" password="{PASSWORD}";',
        )
        .option("topic", TOPIC_OUTPUT)
        .option("checkpointLocation", "/tmp/checkpoints")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
