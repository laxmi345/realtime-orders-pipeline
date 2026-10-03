"""Spark Structured Streaming: Kafka -> clean/validate -> PostgreSQL."""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
PG_URL = "jdbc:postgresql://%s:5432/%s" % (os.getenv("PG_HOST", "postgres"), os.getenv("PG_DB", "ordersdb"))
PG_USER = os.getenv("PG_USER", "analytics")
PG_PASSWORD = os.getenv("PG_PASSWORD", "analytics")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("category", StringType()),
    StructField("city", StringType()),
    StructField("payment_method", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType()),
])

spark = (
    SparkSession.builder.appName("orders-stream")
    .config("spark.sql.shuffle.partitions", "4")  # default 200 is far too many for local mode
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

clean = (
    parsed.withColumn("event_time", col("event_time").cast("timestamp"))
    .filter("order_id IS NOT NULL AND amount > 0 AND quantity > 0 AND event_time IS NOT NULL")
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["order_id"])
)


def write_batch(df, batch_id):
    """Write each micro-batch to PostgreSQL."""
    df.persist()  # compute once; count() and write() would otherwise recompute the batch
    try:
        n = df.count()
        if n == 0:
            return
        (
            df.write.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", "orders")
            .option("user", PG_USER)
            .option("password", PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        print("batch %s written: %d rows" % (batch_id, n))
    finally:
        df.unpersist()


query = (
    clean.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", "/tmp/checkpoints/orders")
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()
