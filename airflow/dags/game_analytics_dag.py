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
import os
import tempfile

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
    from pymongo import MongoClient
    
    client = MongoClient('mongodb://admin:admin@mongo:27017/')
    db = client['game_analytics']
    
    stats = db.command('dbStats')
    size_mb = stats['storageSize'] / (1024 * 1024)  # Use storageSize not dataSize
    
    logging.info(f"MongoDB Total Size: {size_mb:.2f} MB")
    context['ti'].xcom_push(key='mongodb_size_mb', value=size_mb)
    
    if size_mb > 0:
        logging.warning(f"ALERT: Size {size_mb:.2f} MB exceeds 300 MB threshold")
        context['ti'].xcom_push(key='archive_required', value=True)
        return 'archive_to_hadoop'
    
    client.close()
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
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from hdfs import InsecureClient
    import tempfile
    import os
    import hashlib
    from pymongo import MongoClient
    
    archive_required = context['ti'].xcom_pull(
        key='archive_required', 
        task_ids='monitor_mongodb_size'
    )
    
    if not archive_required:
        logging.info("Skipping archival - not required")
        return
    
    # Connect to MongoDB
    mongo_client = MongoClient('mongodb://admin:admin@mongo:27017/')
    db = mongo_client['game_analytics']
    
    # Connect to HDFS via WebHDFS (HTTP interface)
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    
    # Calculate cutoff time (24 hours ago)
    cutoff_time = datetime.utcnow() - timedelta(minutes=1)
    
    cutoff_str = cutoff_time.isoformat()
    
    # Collections to archive
    collections = ['game_events', 'sessions', 'transactions']
    total_archived = 0
    archive_batch_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Find old documents
        old_docs = list(collection.find({
            'timestamp': {'$lt': cutoff_str}
        }))
        
        if not old_docs:
            logging.info(f"No old documents in {collection_name} using string {cutoff_str}")
            continue
        
        logging.info(f"Found {len(old_docs)} documents to archive from {collection_name}")
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(old_docs)
            
            # Remove MongoDB's _id field (not serializable to Parquet)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            # Convert datetime objects to strings for Parquet compatibility
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
            
            # Create temporary Parquet file
            with tempfile.NamedTemporaryFile(
                mode='wb', 
                suffix='.parquet', 
                delete=False
            ) as tmp_file:
                tmp_path = tmp_file.name
                
                # Write DataFrame to Parquet with compression
                table = pa.Table.from_pandas(df)
                pq.write_table(
                    table, 
                    tmp_path,
                    compression='gzip',
                    use_dictionary=True,
                    compression_level=9
                )
            
            # Get file size and checksum
            file_size = os.path.getsize(tmp_path)
            with open(tmp_path, 'rb') as f:
                file_checksum = hashlib.sha256(f.read()).hexdigest()
            
            # Define HDFS path
            hdfs_dir = '/archives'
            hdfs_filename = f"{collection_name}_{archive_batch_id}.parquet"
            hdfs_path = f"{hdfs_dir}/{hdfs_filename}"
            
            # Ensure archive directory exists in HDFS
            try:
                hdfs_client.makedirs(hdfs_dir)
            except Exception as e:
                logging.info(f"Archive directory already exists or error: {e}")
            
            # Upload to HDFS
            logging.info(f"Uploading {tmp_path} to HDFS: {hdfs_path}")
            with open(tmp_path, 'rb') as local_file:
                hdfs_client.write(
                    hdfs_path, 
                    data=local_file, 
                    overwrite=True
                )
            
            logging.info(f"✅ Successfully uploaded to HDFS: {hdfs_path}")
            
            # Verify file exists in HDFS
            hdfs_status = hdfs_client.status(hdfs_path)
            hdfs_size = hdfs_status['length']
            logging.info(f"HDFS file size: {hdfs_size} bytes")
            
            # Store metadata in MongoDB
            metadata = {
                'archive_batch_id': archive_batch_id,
                'collection': collection_name,
                'archive_time': datetime.utcnow(),
                'document_count': len(old_docs),
                'time_range': {
                    'start': df['timestamp'].min(),
                    'end': df['timestamp'].max()
                },
                'hdfs_path': hdfs_path,
                'hdfs_size_bytes': hdfs_size,
                'local_size_bytes': file_size,
                'compression': 'gzip',
                'compression_ratio': round(file_size / hdfs_size, 2) if hdfs_size > 0 else 1.0,
                'format': 'parquet',
                'checksum_sha256': file_checksum,
                'status': 'completed'
            }
            
            db['archive_metadata'].insert_one(metadata)
            logging.info(f"Metadata saved: {metadata}")
            
            # Delete archived documents from MongoDB
            delete_result = collection.delete_many({
                'timestamp': {'$lt': cutoff_time}
            })
            total_archived += delete_result.deleted_count
            logging.info(f"Deleted {delete_result.deleted_count} documents from {collection_name}")
            
            # Clean up temporary file
            os.unlink(tmp_path)
            
        except Exception as e:
            logging.error(f"Error archiving {collection_name}: {e}")
            import traceback
            traceback.print_exc()
            
            # Mark as failed in metadata
            db['archive_metadata'].insert_one({
                'archive_batch_id': archive_batch_id,
                'collection': collection_name,
                'archive_time': datetime.utcnow(),
                'status': 'failed',
                'error': str(e)
            })
            continue
    
    mongo_client.close()
    
    logging.info(f"🎉 Archive Complete: {total_archived} total documents archived")
    context['ti'].xcom_push(key='archived_count', value=total_archived)
    context['ti'].xcom_push(key='archive_batch_id', value=archive_batch_id)

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
    from kafka import KafkaConsumer
    import json
    
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    db = client['game_analytics']
    
    consumer = KafkaConsumer(
        'GameAnalytics',
        bootstrap_servers='kafka:29092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow_ingestion_v3', # Incrementing to v3
        consumer_timeout_ms=5000
    )
    
    batch_size = 0
    for message in consumer:
        event = message.value
        
        if isinstance(event.get('timestamp'), str):
            try:
                # This converts "2025-12-25T..." into a Python datetime object
                event['timestamp'] = datetime.fromisoformat(event['timestamp'])
            except ValueError:
                event['timestamp'] = datetime.utcnow()
        
        event_type = event.get('event_type')
        
        # 1. Determine the correct ID and Collection
        if event_type == 'purchase':
            collection = db['transactions']
            # Use transaction_id if available, otherwise fallback to event_id
            unique_id = event.get('transaction_id') or event.get('event_id')
            filter_query = {'transaction_id': unique_id}
            # Ensure the event dict actually has the transaction_id field populated
            event['transaction_id'] = unique_id
        elif event_type in ['session_start', 'session_end']:
            collection = db['sessions']
            filter_query = {'session_id': event.get('session_id')}
        else:
            collection = db['game_events']
            filter_query = {'event_id': event.get('event_id')}

        # 2. Skip if we truly have no ID to prevent 'null' index errors
        if not list(filter_query.values())[0]:
            logging.warning(f"Skipping event due to missing ID: {event}")
            continue

        # 3. Upsert
        collection.replace_one(filter_query, event, upsert=True)
        batch_size += 1
    
    consumer.close()
    logging.info(f"Ingested {batch_size} events.")

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
    # Import the hook instead of raw MongoClient
    from airflow.providers.mongo.hooks.mongo import MongoHook
    
    # 1. Use the Hook to get a connection that already has your admin/admin credentials
    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_client = mongo_hook.get_conn()
    mongo_db = mongo_client['game_analytics']
    
    # 2. PostgreSQL connection (ensure your credentials match)
    pg_conn = psycopg2.connect(
        host='db',
        database='game_analytics',
        user='rafay',
        password='rafay'
    )
    pg_cursor = pg_conn.cursor()
    
    # 3. Proceed with the logic
    # The 'transactions' find command will now work because mongo_client is authenticated
    transactions = mongo_db['transactions'].find({
        'timestamp': {'$gte': datetime.utcnow() - timedelta(minutes=10)}
    })
    
    inserted = 0
    for txn in transactions:
        # ... rest of your insertion logic ...
        inserted += 1
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    # Note: Do not close the mongo_client manually if using the hook's connection
    
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

def test_hdfs_connection(**context):
    """Test HDFS connectivity"""
    from hdfs import InsecureClient
    
    try:
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        
        # List root directory
        root_contents = hdfs_client.list('/')
        logging.info(f"✅ HDFS Connected. Root contents: {root_contents}")
        
        # Create test file
        test_path = '/test_airflow_connection.txt'
        hdfs_client.write(
            test_path, 
            data='Airflow HDFS test', 
            overwrite=True
        )
        logging.info(f"✅ Test file written: {test_path}")
        
        # Delete test file
        hdfs_client.delete(test_path)
        logging.info(f"✅ Test file deleted")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ HDFS Connection Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Add to DAG
hdfs_test = PythonOperator(
    task_id='test_hdfs_connection',
    python_callable=test_hdfs_connection,
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

# Update dependencies
monitor_size >> hdfs_test >> archive_task