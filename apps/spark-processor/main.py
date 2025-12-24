import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, approx_count_distinct, to_timestamp, lower
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092") # Use internal docker name
KAFKA_TOPIC = "GameAnalytics"

POSTGRES_URL = os.getenv("DATABASE_URL", "jdbc:postgresql://db:5432/game_analytics")
POSTGRES_USER = os.getenv("DATABASE_USER", "rafay")
POSTGRES_PASSWORD = os.getenv("DATABASE_PASSWORD", "rafay")

# --- SCHEMA DEFINITION ---
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", DoubleType(), True), # CHANGED to Double for Unix Epoch
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
    StructField("sentiment_score", DoubleType(), True),
    StructField("player_count", IntegerType(), True)
])

# --- UTILITY FUNCTIONS ---
def get_spark_session():
    return (
        SparkSession.builder
        .appName("GameAnalyticsProcessor")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def create_kafka_read_stream(spark, kafka_servers, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

def write_to_postgres(df, epoch_id, table_name):
    # LOGGING to see if rows are actually passing filters
    row_count = df.count()
    print(f"DEBUG: Batch {epoch_id} for {table_name} contains {row_count} rows.")
    
    if row_count > 0:
        (
            df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table_name)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .mode("append") 
            .save()
        )

# --- ANALYTICS STREAMS ---

def process_revenue(df):
    revenue_df = (
        df.filter(lower(col("event_type")) == "purchase")
        .withWatermark("timestamp", "2 minutes")
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
        .option("checkpointLocation", "/tmp/checkpoints/revenue")
        .trigger(processingTime="10 seconds")
        .start()
    )

def process_concurrency(df):
    concurrency_df = (
        df.filter(lower(col("event_type")) == "heartbeat")
        .withWatermark("timestamp", "5 minutes")
        .groupBy(
            window("timestamp", "5 minutes", "1 minute").alias("time_window"),
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
        .option("checkpointLocation", "/tmp/checkpoints/concurrency")
        .trigger(processingTime="10 seconds")
        .start()
    )

def process_performance(df):
    performance_df = (
        df.filter(lower(col("event_type")) == "heartbeat")
        .withWatermark("timestamp", "2 minutes")
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
        .option("checkpointLocation", "/tmp/checkpoints/performance")
        .trigger(processingTime="10 seconds")
        .start()
    )

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Starting Spark Streaming Processor...")

    kafka_stream_df = create_kafka_read_stream(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    events_df = (
        kafka_stream_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # CRITICAL FIX: Cast the Unix float timestamp to a Timestamp type
    events_df = events_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # DEBUG CONSOLE SINK
    debug_query = events_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='10 seconds') \
        .start()

    print("🔥 Firing up analytics streams...")
    query_revenue = process_revenue(events_df)
    query_concurrency = process_concurrency(events_df)
    query_performance = process_performance(events_df)

    spark.streams.awaitAnyTermination()