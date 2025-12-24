import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum, avg, approx_count_distinct, 
    to_timestamp, lower, current_timestamp, expr
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
    StructField("timestamp", StringType(), True),
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
    print("🔧 Creating Spark Session...")
    spark = (
        SparkSession.builder
        .appName("GameAnalyticsStreamProcessor")
        .master("local[4]")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session Created\n")
    return spark

def debug_raw_kafka(df, epoch_id):
    """Debug: Show raw Kafka messages"""
    print(f"\n🔍 DEBUG BATCH {epoch_id}: RAW KAFKA MESSAGES")
    print("=" * 80)
    count = df.count()
    print(f"Total messages in batch: {count}")
    if count > 0:
        df.show(5, truncate=False)
    else:
        print("⚠️ NO MESSAGES IN KAFKA STREAM!")
    print("=" * 80 + "\n")

def debug_parsed_events(df, epoch_id):
    """Debug: Show parsed events with timestamps"""
    print(f"\n🔍 DEBUG BATCH {epoch_id}: PARSED EVENTS")
    print("=" * 80)
    count = df.count()
    print(f"Total parsed events: {count}")
    if count > 0:
        df.select("event_type", "game_name", "timestamp", "player_id").show(5, truncate=False)
        
        # Check for NULL timestamps
        null_count = df.filter(col("timestamp").isNull()).count()
        if null_count > 0:
            print(f"⚠️ WARNING: {null_count} events have NULL timestamps!")
            df.filter(col("timestamp").isNull()).select("event_type", "timestamp").show(3)
    else:
        print("⚠️ NO EVENTS AFTER PARSING!")
    print("=" * 80 + "\n")

def debug_filtered_events(df, epoch_id, event_type):
    """Debug: Show filtered events for specific analytics"""
    print(f"\n🔍 DEBUG BATCH {epoch_id}: FILTERED {event_type.upper()} EVENTS")
    print("=" * 80)
    count = df.count()
    print(f"Total {event_type} events: {count}")
    if count > 0:
        df.show(3, truncate=False)
    else:
        print(f"⚠️ NO {event_type.upper()} EVENTS FOUND!")
    print("=" * 80 + "\n")

def write_to_postgres(df, epoch_id, table_name):
    try:
        row_count = df.count()
        timestamp = time.strftime('%H:%M:%S')
        
        if row_count > 0:
            print(f"✅ [{timestamp}] Writing {row_count} rows to {table_name}")
            df.show(2, truncate=False)
            
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
            print(f"✅ Successfully wrote to {table_name}\n")
        else:
            print(f"⏳ [{timestamp}] No rows for {table_name} (watermark delay)\n")
            
    except Exception as e:
        print(f"❌ ERROR writing to {table_name}: {e}\n")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=" * 80)
    print("🐛 SPARK STREAMING DEBUG MODE")
    print("=" * 80)
    print("This will show you EXACTLY what Spark is reading from Kafka\n")
    
    spark = get_spark_session()
    
    # Read from Kafka
    print(f"📡 Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}\n")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # STEP 1: Debug raw Kafka messages
    print("🔍 STEP 1: Checking raw Kafka stream...\n")
    raw_debug = (
        kafka_df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp", "partition", "offset")
        .writeStream
        .foreachBatch(debug_raw_kafka)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    # Wait a bit for first batch
    time.sleep(15)
    
    # STEP 2: Parse and check events
    print("\n🔍 STEP 2: Parsing JSON events...\n")
    events_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    )
    
    parsed_debug = (
        events_df
        .writeStream
        .foreachBatch(debug_parsed_events)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    # Wait for another batch
    time.sleep(15)
    
    # STEP 3: Check specific event types
    print("\n🔍 STEP 3: Checking purchase events...\n")
    purchase_df = events_df.filter(lower(col("event_type")) == "purchase")
    
    purchase_debug = (
        purchase_df
        .writeStream
        .foreachBatch(lambda df, epoch: debug_filtered_events(df, epoch, "purchase"))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    time.sleep(15)
    
    print("\n🔍 STEP 4: Checking heartbeat events...\n")
    heartbeat_df = events_df.filter(lower(col("event_type")) == "heartbeat")
    
    heartbeat_debug = (
        heartbeat_df
        .writeStream
        .foreachBatch(lambda df, epoch: debug_filtered_events(df, epoch, "heartbeat"))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    print("\n" + "=" * 80)
    print("🐛 DEBUG MODE RUNNING")
    print("=" * 80)
    print("Watch the output above to see:")
    print("  1. Are messages being read from Kafka?")
    print("  2. Are they being parsed correctly?")
    print("  3. Are timestamps NULL?")
    print("  4. Are event_type filters working?")
    print("=" * 80)
    print("\nPress CTRL+C when you have enough debug info...\n")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping debug mode...")
        raw_debug.stop()
        parsed_debug.stop()
        purchase_debug.stop()
        heartbeat_debug.stop()
        spark.stop()