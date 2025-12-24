import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum, avg, approx_count_distinct, 
    to_timestamp, lower, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = "GameAnalytics"

POSTGRES_URL = os.getenv("DATABASE_URL", "jdbc:postgresql://db:5432/game_analytics")
POSTGRES_USER = os.getenv("DATABASE_USER", "rafay")
POSTGRES_PASSWORD = os.getenv("DATABASE_PASSWORD", "rafay")

# --- SCHEMA DEFINITION ---
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # ISO format
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

def get_spark_session():
    """Create Spark session with Kafka + JDBC support"""
    print("🔧 Creating Spark Session...")
    spark = (
        SparkSession.builder
        .appName("GameAnalyticsStreamProcessor")
        .master("local[4]")  # 4 threads for parallel processing
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session Created")
    return spark

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable
    
    print("⏳ Waiting for Kafka to be ready...")
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=5000
            )
            topics = admin.list_topics()
            if KAFKA_TOPIC in topics:
                print(f"✅ Kafka is ready. Topic '{KAFKA_TOPIC}' exists.")
                admin.close()
                return True
            else:
                print(f"⏳ Topic '{KAFKA_TOPIC}' not yet created, waiting...")
                time.sleep(2)
        except NoBrokersAvailable:
            retry_count += 1
            print(f"⏳ Kafka not ready, retrying... ({retry_count}/{max_retries})")
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Error checking Kafka: {e}")
            time.sleep(2)
    
    print("⚠️ Proceeding anyway, Kafka might be ready...")
    return False

def create_kafka_read_stream(spark, kafka_servers, topic):
    """Create a streaming DataFrame from Kafka"""
    print(f"📡 Connecting to Kafka: {kafka_servers}")
    print(f"📋 Subscribing to topic: {topic}")
    
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # Read all historical data
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "1000")  # Process in batches
        # CRITICAL: Use different consumer group from API
        .option("kafka.group.id", "spark-analytics-consumer")
        .load()
    )

def write_to_postgres(df, epoch_id, table_name):
    """Write batch to PostgreSQL with error handling"""
    try:
        row_count = df.count()
        timestamp = time.strftime('%H:%M:%S')
        
        if row_count > 0:
            print(f"✅ [{timestamp}] Batch {epoch_id} for {table_name}: {row_count} rows")
            
            # Show sample
            print(f"📊 Sample from {table_name}:")
            df.show(2, truncate=False)
            
            # Write to database
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
            print(f"✅ Successfully wrote {row_count} rows to {table_name}\n")
        else:
            print(f"⏳ [{timestamp}] Batch {epoch_id} for {table_name}: 0 rows (waiting for watermark)\n")
            
    except Exception as e:
        print(f"❌ ERROR in batch {epoch_id} for {table_name}:")
        print(f"   {str(e)}\n")
        import traceback
        traceback.print_exc()

# --- ANALYTICS STREAMS ---

def process_revenue(events_df):
    """Revenue analytics from purchase events"""
    print("💰 Setting up Revenue Stream...")
    
    revenue_df = (
        events_df
        .filter(lower(col("event_type")) == "purchase")
        .filter(col("purchase_amount").isNotNull())
        .filter(col("player_type").isNotNull())
        .filter(col("game_name").isNotNull())
        .withWatermark("timestamp", "30 seconds")
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
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            "game_name",
            "player_type",
            "total_revenue",
            "avg_purchase",
            "unique_purchasers"
        )
    )
    
    query = (
        revenue_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "realtime_revenue"))
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark-checkpoints/revenue")
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    print("✅ Revenue Stream Started")
    return query

def process_concurrency(events_df):
    """Player concurrency from heartbeat events"""
    print("👥 Setting up Concurrency Stream...")
    
    concurrency_df = (
        events_df
        .filter(lower(col("event_type")) == "heartbeat")
        .filter(col("player_id").isNotNull())
        .filter(col("region").isNotNull())
        .filter(col("game_name").isNotNull())
        .withWatermark("timestamp", "1 minute")
        .groupBy(
            window("timestamp", "1 minute", "30 seconds").alias("time_window"),
            "game_name",
            "region"
        )
        .agg(approx_count_distinct("player_id").alias("concurrent_players"))
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            "game_name",
            "region",
            "concurrent_players"
        )
    )
    
    query = (
        concurrency_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "realtime_concurrency"))
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark-checkpoints/concurrency")
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    print("✅ Concurrency Stream Started")
    return query

def process_performance(events_df):
    """Performance metrics from heartbeat events"""
    print("⚡ Setting up Performance Stream...")
    
    performance_df = (
        events_df
        .filter(lower(col("event_type")) == "heartbeat")
        .filter(col("fps").isNotNull())
        .filter(col("latency_ms").isNotNull())
        .filter(col("platform").isNotNull())
        .filter(col("region").isNotNull())
        .filter(col("game_name").isNotNull())
        .withWatermark("timestamp", "30 seconds")
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
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            "game_name",
            "platform",
            "region",
            "avg_fps",
            "avg_latency"
        )
    )
    
    query = (
        performance_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "realtime_performance"))
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark-checkpoints/performance")
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    print("✅ Performance Stream Started")
    return query

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("=" * 80)
    print("🚀 GAME ANALYTICS SPARK STREAMING PROCESSOR")
    print("=" * 80)
    print()
    
    # Wait for dependencies
    wait_for_kafka()
    
    # Create Spark session
    spark = get_spark_session()
    
    print("\n📡 CONNECTING TO KAFKA STREAM")
    print("-" * 80)
    
    # Read from Kafka
    kafka_stream_df = create_kafka_read_stream(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    
    # Parse JSON events
    print("🔧 Parsing JSON events from Kafka...")
    events_df = (
        kafka_stream_df
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )
    
    # Convert timestamp from ISO string to timestamp type
    print("⏰ Converting timestamps...")
    events_df = (
        events_df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .filter(col("timestamp").isNotNull())
    )
    
    print("✅ Event parsing configured")
    
    print("\n🔥 STARTING ANALYTICS STREAMS")
    print("-" * 80)
    
    # Start all three analytics streams
    query_revenue = process_revenue(events_df)
    query_concurrency = process_concurrency(events_df)
    query_performance = process_performance(events_df)
    
    print("\n" + "=" * 80)
    print("✅ ALL STREAMS RUNNING")
    print("=" * 80)
    print(f"📊 Kafka Topic: {KAFKA_TOPIC}")
    print(f"📊 Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📊 Database: {POSTGRES_URL}")
    print(f"📊 Consumer Group: spark-analytics-consumer")
    print("=" * 80)
    print("\n⏳ Processing events (CTRL+C to stop)...\n")
    
    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        query_revenue.stop()
        query_concurrency.stop()
        query_performance.stop()
        spark.stop()
        print("✅ Shutdown complete")