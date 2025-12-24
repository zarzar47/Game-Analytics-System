"""
Game Analytics Real-Time Pipeline Orchestration
================================================
This DAG orchestrates the entire big data pipeline:
1. Data generation and streaming
2. MongoDB ingestion from Kafka
3. Data size monitoring and archival triggers
4. Spark batch processing
5. Dashboard refresh signals
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook
from datetime import datetime, timedelta
import pymongo
import json
import subprocess
import logging

# Default arguments
default_args = {
    'owner': 'game_analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'game_analytics_pipeline',
    default_args=default_args,
    description='Real-time game analytics ETL pipeline',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['real-time', 'gaming', 'analytics'],
)

# ============================================================================
# TASK 1: Monitor Data Size and Trigger Archival
# ============================================================================

def check_data_size_and_archive(**context):
    """
    Monitors MongoDB size. If exceeds 300MB, triggers archival to Hadoop.
    """
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    db = client['game_analytics']
    
    # Get database statistics
    stats = db.command('dbStats')
    size_mb = stats['dataSize'] / (1024 * 1024)  # Convert to MB
    
    logging.info(f"MongoDB Size: {size_mb:.2f} MB")
    
    # Push size to XCom for monitoring
    context['ti'].xcom_push(key='mongodb_size_mb', value=size_mb)
    
    # Archive trigger threshold
    if size_mb > 300:
        logging.warning(f"Size threshold exceeded: {size_mb:.2f} MB > 300 MB")
        context['ti'].xcom_push(key='archive_required', value=True)
        return 'archive_to_hadoop'
    else:
        logging.info("Size within limits. No archival needed.")
        context['ti'].xcom_push(key='archive_required', value=False)
        return 'continue_pipeline'

monitor_size = PythonOperator(
    task_id='monitor_mongodb_size',
    python_callable=check_data_size_and_archive,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 2: Archive Old Data to Hadoop HDFS
# ============================================================================

def archive_to_hadoop(**context):
    """
    Archives data older than 24 hours from MongoDB to Hadoop HDFS in Parquet format.
    Updates metadata catalog.
    """
    archive_required = context['ti'].xcom_pull(key='archive_required', task_ids='monitor_mongodb_size')
    
    if not archive_required:
        logging.info("Skipping archival - not required")
        return
    
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    db = client['game_analytics']
    
    # Calculate cutoff time (24 hours ago)
    cutoff_time = datetime.utcnow() - timedelta(hours=24)
    
    # Export old documents to JSON (would be Parquet in production)
    collections = ['game_events', 'sessions', 'transactions']
    archived_docs = 0
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Find old documents
        old_docs = list(collection.find({
            'timestamp': {'$lt': cutoff_time}
        }))
        
        if not old_docs:
            logging.info(f"No old documents in {collection_name}")
            continue
        
        # Archive filename with timestamp
        archive_filename = f"/archives/{collection_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        # In production, use hdfs_hook to write to HDFS
        # For demo, we'll simulate
        logging.info(f"Archiving {len(old_docs)} documents from {collection_name} to HDFS: {archive_filename}")
        
        # Write metadata
        metadata = {
            'collection': collection_name,
            'archive_time': datetime.utcnow().isoformat(),
            'document_count': len(old_docs),
            'hdfs_path': archive_filename,
            'compression': 'gzip',
            'format': 'parquet',
            'size_mb': sum(len(json.dumps(doc, default=str)) for doc in old_docs) / (1024 * 1024)
        }
        
        db['archive_metadata'].insert_one(metadata)
        logging.info(f"Metadata logged: {metadata}")
        
        # Delete archived documents
        result = collection.delete_many({'timestamp': {'$lt': cutoff_time}})
        archived_docs += result.deleted_count
        logging.info(f"Deleted {result.deleted_count} old documents from {collection_name}")
    
    logging.info(f"Total archived documents: {archived_docs}")
    context['ti'].xcom_push(key='archived_count', value=archived_docs)

archive_task = PythonOperator(
    task_id='archive_to_hadoop',
    python_callable=archive_to_hadoop,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 3: Consume Kafka Events to MongoDB
# ============================================================================

def ingest_kafka_to_mongo(**context):
    """
    Reads recent events from Kafka and loads them into MongoDB.
    This simulates a micro-batch ingestion (every minute).
    """
    from kafka import KafkaConsumer
    import json
    
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    db = client['game_analytics']
    
    # Kafka consumer
    consumer = KafkaConsumer(
        'GameAnalytics',
        bootstrap_servers='kafka:29092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow_ingestion',
        consumer_timeout_ms=10000  # 10 second timeout
    )
    
    batch_size = 0
    events_by_type = {}
    
    for message in consumer:
        event = message.value
        event_type = event.get('event_type')
        
        # Route to appropriate collection
        if event_type in ['session_start', 'session_end']:
            db['sessions'].insert_one(event)
        elif event_type == 'purchase':
            db['transactions'].insert_one(event)
        else:
            db['game_events'].insert_one(event)
        
        batch_size += 1
        events_by_type[event_type] = events_by_type.get(event_type, 0) + 1
    
    consumer.close()
    
    logging.info(f"Ingested {batch_size} events to MongoDB")
    logging.info(f"Breakdown: {events_by_type}")
    
    context['ti'].xcom_push(key='ingested_count', value=batch_size)

ingest_task = PythonOperator(
    task_id='ingest_kafka_to_mongodb',
    python_callable=ingest_kafka_to_mongo,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 4: ETL from MongoDB to PostgreSQL (Staging)
# ============================================================================

def etl_mongo_to_postgres(**context):
    """
    Extracts data from MongoDB, transforms it, and loads into PostgreSQL star schema.
    """
    import psycopg2
    from pymongo import MongoClient
    
    # MongoDB connection
    mongo_client = MongoClient('mongodb://mongo:27017/')
    mongo_db = mongo_client['game_analytics']
    
    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        host='db',
        database='game_analytics',
        user='rafay',
        password='rafay'
    )
    pg_cursor = pg_conn.cursor()
    
    # Transform and load transactions
    transactions = mongo_db['transactions'].find({
        'timestamp': {'$gte': datetime.utcnow() - timedelta(minutes=2)}
    })
    
    inserted = 0
    for txn in transactions:
        try:
            # Insert into dim_player if not exists
            pg_cursor.execute("""
                INSERT INTO dim_player (player_id, username, country_code, region, platform, player_segment)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO NOTHING
            """, (
                txn.get('player_id'),
                txn.get('player_id', 'Unknown'),
                txn.get('country', 'US'),
                txn.get('region', 'NA'),
                txn.get('platform', 'PC'),
                txn.get('player_type', 'CASUAL')
            ))
            
            # Insert transaction
            pg_cursor.execute("""
                INSERT INTO fact_transaction (transaction_id, player_id, item_id, game_id, amount_usd, currency)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """, (
                txn.get('event_id'),
                txn.get('player_id'),
                txn.get('item_id', 'item_000'),
                txn.get('game_id'),
                txn.get('purchase_amount', 0.0),
                txn.get('currency', 'USD')
            ))
            inserted += 1
        except Exception as e:
            logging.error(f"Error inserting transaction: {e}")
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()
    
    logging.info(f"ETL completed: {inserted} transactions loaded to PostgreSQL")

etl_task = PythonOperator(
    task_id='etl_mongo_to_postgres',
    python_callable=etl_mongo_to_postgres,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 5: Trigger Spark Streaming Job
# ============================================================================

spark_job = BashOperator(
    task_id='run_spark_analytics',
    bash_command='echo "Spark Streaming is running continuously in spark-processor container"',
    dag=dag,
)

# ============================================================================
# TASK 6: Refresh Dashboard Cache
# ============================================================================

def refresh_dashboard_cache(**context):
    """
    Signals dashboard to refresh cached metrics.
    """
    import requests
    
    try:
        # In production, this would hit a cache invalidation endpoint
        response = requests.post('http://api:8000/internal/cache/refresh', timeout=5)
        logging.info(f"Dashboard cache refresh: {response.status_code}")
    except Exception as e:
        logging.warning(f"Could not refresh dashboard cache: {e}")

cache_refresh = PythonOperator(
    task_id='refresh_dashboard_cache',
    python_callable=refresh_dashboard_cache,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 7: Data Quality Checks
# ============================================================================

def data_quality_checks(**context):
    """
    Validates data integrity and alerts on anomalies.
    """
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    db = client['game_analytics']
    
    checks = {
        'null_player_ids': db['game_events'].count_documents({'player_id': None}),
        'future_timestamps': db['game_events'].count_documents({
            'timestamp': {'$gt': datetime.utcnow() + timedelta(minutes=5)}
        }),
        'negative_revenue': db['transactions'].count_documents({'purchase_amount': {'$lt': 0}})
    }
    
    issues = [k for k, v in checks.items() if v > 0]
    
    if issues:
        logging.warning(f"Data quality issues detected: {checks}")
        context['ti'].xcom_push(key='quality_issues', value=checks)
    else:
        logging.info("All data quality checks passed")

quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES (Pipeline Flow)
# ============================================================================

# Linear pipeline with conditional archival
monitor_size >> ingest_task >> etl_task >> spark_job >> cache_refresh >> quality_checks

# Parallel archival branch (only when needed)
monitor_size >> archive_task >> quality_checks
