"""
PySpark Structured Streaming Consumer
=====================================
Procesamiento de Datos Masivos | ITESO

Reads food delivery events from Kafka, parses them with the shared schema,
applies multiple aggregations, and persists results to MongoDB using
foreachBatch with upsert semantics.

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
      Consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    count, sum as _sum, avg, round as _round,
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType,
)
from pymongo import MongoClient, UpdateOne

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "food-delivery-events"

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB  = "food_delivery"

CHECKPOINT_BASE = "/tmp/checkpoints/food_delivery"

# ─────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FoodDeliveryConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    )
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# Schema (matches the Producer's JSON payload)
# ─────────────────────────────────────────────
event_schema = StructType([
    StructField("order_id",           IntegerType()),
    StructField("customer_id",        IntegerType()),
    StructField("restaurant_id",      IntegerType()),
    StructField("delivery_driver_id", IntegerType()),
    StructField("event_type",         StringType()),
    StructField("order_amount",       DoubleType()),
    StructField("delivery_zone",      StringType()),
    StructField("payment_method",     StringType()),
    StructField("timestamp",          StringType()),
])

# ─────────────────────────────────────────────
# Read stream from Kafka
# ─────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON value into structured columns
events = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), event_schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
)

# ─────────────────────────────────────────────
# Aggregations (the "core" of the pipeline)
# ─────────────────────────────────────────────

# 1) Delivered orders by zone — count, total revenue, avg ticket
zone_stats = (
    events
    .filter(col("event_type") == "order_delivered")
    .groupBy("delivery_zone")
    .agg(
        count("*").alias("delivered_orders"),
        _round(_sum("order_amount"), 2).alias("total_revenue"),
        _round(avg("order_amount"), 2).alias("avg_order_amount"),
    )
)

# 2) Distribution by event_type (full lifecycle, not only delivered)
event_type_stats = (
    events
    .groupBy("event_type")
    .agg(
        count("*").alias("event_count"),
        _round(_sum("order_amount"), 2).alias("total_amount"),
    )
)

# 3) Revenue and orders by payment method (delivered orders only)
payment_stats = (
    events
    .filter(col("event_type") == "order_delivered")
    .groupBy("payment_method")
    .agg(
        count("*").alias("orders"),
        _round(_sum("order_amount"), 2).alias("total_revenue"),
    )
)

# ─────────────────────────────────────────────
# MongoDB writer (foreachBatch + upsert via pymongo)
# ─────────────────────────────────────────────
def upsert_to_mongo(collection_name: str, key_field: str):
    """Return a foreachBatch function that upserts each row into MongoDB."""
    def _writer(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        # Aggregations are small — collect to the driver as plain dicts
        records = [row.asDict(recursive=True) for row in batch_df.collect()]

        client = MongoClient(MONGO_URI, w="majority")  # write concern: majority
        try:
            coll = client[MONGO_DB][collection_name]
            ops = [
                UpdateOne(
                    {"_id": rec[key_field]},
                    {"$set": {**rec, "batch_id": batch_id}},
                    upsert=True,
                )
                for rec in records
            ]
            result = coll.bulk_write(ops, ordered=False)
            print(
                f"[{collection_name}] batch={batch_id} "
                f"upserted={result.upserted_count} "
                f"modified={result.modified_count} "
                f"matched={result.matched_count}"
            )
        finally:
            client.close()
    return _writer

# ─────────────────────────────────────────────
# Start streaming queries
# ─────────────────────────────────────────────
queries = [
    zone_stats.writeStream
        .outputMode("complete")
        .foreachBatch(upsert_to_mongo("zone_stats", "delivery_zone"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/zone_stats")
        .start(),

    event_type_stats.writeStream
        .outputMode("complete")
        .foreachBatch(upsert_to_mongo("event_type_stats", "event_type"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/event_type_stats")
        .start(),

    payment_stats.writeStream
        .outputMode("complete")
        .foreachBatch(upsert_to_mongo("payment_stats", "payment_method"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/payment_stats")
        .start(),
]

print("Streaming queries started. Press Ctrl+C to stop.")
for q in queries:
    q.awaitTermination()