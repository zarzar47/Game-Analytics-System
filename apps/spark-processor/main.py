import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "GameAnalytics"

# Using JDBC URL format for Spark
POSTGRES_URL = os.getenv("DATABASE_URL", "jdbc:postgresql://localhost:5432/game_analytics")
POSTGRES_USER = os.getenv("DATABASE_USER", "rafay")
POSTGRES_PASSWORD = os.getenv("DATABASE_PASSWORD", "rafay")

# --- SCHEMA DEFINITION ---
# This must match the structure of the JSON data from the faker
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_name", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("player_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("fps", DoubleType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("level", IntegerType(), True),
    StructField("sentiment_score", DoubleType(), True)
])

# --- UTILITY FUNCTIONS ---
def get_spark_session():
    """Initializes and returns a Spark Session"""
    return (
        SparkSession.builder
        .appName("GameAnalyticsProcessor")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0")
        .config("spark.sql.shuffle.partitions", "4") # Optimize for small cluster
        .getOrCreate()
    )

def create_kafka_read_stream(spark, kafka_servers, topic):
    """Creates a streaming DataFrame reading from a Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest") # Changed from "latest"
        .load()
    )

def write_to_postgres(df, epoch_id, table_name):
    """
    A generic function to write a micro-batch DataFrame to a Postgres table.
    Uses UPSERT logic (ON CONFLICT DO UPDATE) to keep the table updated.
    """
    # Define primary keys for each table to handle conflicts
    primary_keys = {
        "realtime_revenue": "window, game_name, player_type",
        "realtime_concurrency": "window, game_name, region",
        "realtime_performance": "window, game_name, platform, region"
    }.get(table_name, "id") # Default if not specified

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .mode("append") # Use append mode for foreachBatch
        .save()
    )
    # Note: A more robust upsert would require executing raw SQL.
    # For this project, we will pre-create tables and let dashboards re-read.
    # A simple approach is to TRUNCATE and INSERT, but that causes flicker.
    # Spark's built-in JDBC source doesn't directly support ON CONFLICT.
    # We will rely on downstream dashboard to query the latest results.
    print(f"Batch {epoch_id} written to {table_name}")

# --- ANALYTICS STREAMS ---

def process_revenue(df):
    """Processes purchase events to calculate real-time revenue."""
    revenue_df = (
        df.filter(col("event_type") == "purchase")
        .withWatermark("timestamp", "1 minute")
        .groupBy(
            window("timestamp", "1 minute").alias("time_window"),
            "game_name",
            "player_type"
        )
        .agg(
            sum("purchase_amount").alias("total_revenue"),
            avg("purchase_amount").alias("avg_purchase"),
            approx_count_distinct("player_id").alias("unique_purchasers")
        )
        .select(
            col("time_window").start.alias("window_start"),
            col("time_window").end.alias("window_end"),
            "game_name",
            "player_type",
            "total_revenue",
            "avg_purchase",
            "unique_purchasers"
        )
    )
    return (
        revenue_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "realtime_revenue"))
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )

def process_concurrency(df):
    """Tracks active player counts per game and region."""
    concurrency_df = (
        df.filter(col("event_type") == "heartbeat") # Use heartbeat as sign of activity
        .withWatermark("timestamp", "5 minutes")
        .groupBy(
            window("timestamp", "5 minutes", "1 minute").alias("time_window"), # 5min window, slides every 1min
            "game_name",
            "region"
        )
        .agg(approx_count_distinct("player_id").alias("concurrent_players"))
        .select(
            col("time_window").start.alias("window_start"),
            col("time_window").end.alias("window_end"),
            "game_name",
            "region",
            "concurrent_players"
        )
    )
    return (
        concurrency_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "realtime_concurrency"))
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )

def process_performance(df):
    """Monitors technical game health (FPS, Latency)."""
    performance_df = (
        df.filter(col("event_type") == "heartbeat")
        .withWatermark("timestamp", "1 minute")
        .groupBy(
            window("timestamp", "1 minute").alias("time_window"),
            "game_name",
            "platform",
            "region"
        )
        .agg(
            avg("fps").alias("avg_fps"),
            avg("latency_ms").alias("avg_latency")
        )
        .select(
            col("time_window").start.alias("window_start"),
            col("time_window").end.alias("window_end"),
            "game_name",
            "platform",
        "region",
            "avg_fps",
            "avg_latency"
        )
    )
    return (
        performance_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "realtime_performance"))
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Starting Spark Streaming Processor...")

    # 1. Read from Kafka
    kafka_stream_df = create_kafka_read_stream(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    # 2. Deserialize JSON and select columns
    events_df = (
        kafka_stream_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # 3. Start all processing streams
    print("🔥 Firing up analytics streams...")
    query_revenue = process_revenue(events_df)
    query_concurrency = process_concurrency(events_df)
    query_performance = process_performance(events_df)

    # 4. Await termination
    print("⏳ Awaiting stream terminations...")
    spark.streams.awaitAnyTermination()
