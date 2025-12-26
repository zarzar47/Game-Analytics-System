"""
Game Analytics Star Schema ETL Pipeline - FIXED KAFKA INGESTION
========================================
This DAG orchestrates the complete ETL from MongoDB (hot storage) to PostgreSQL (star schema).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
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
    'game_analytics_star_schema_etl',
    default_args=default_args,
    description='MongoDB to Star Schema ETL Pipeline',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['star-schema', 'etl', 'gaming'],
)

# ============================================================================
# TASK 1: Ingest Kafka Events to MongoDB (FIXED)
# ============================================================================

def ingest_kafka_to_mongo(**context):
    """
    Consume Kafka events and store in MongoDB (hot storage).
    FIXED: Better consumer configuration and error handling
    """
    from kafka import KafkaConsumer
    import json
    from datetime import datetime

    # Authenticated Mongo Connection
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    db = client.get_database('game_analytics')
    
    # FIXED: Better Kafka Consumer Setup
    try:
        consumer = KafkaConsumer(
            'GameAnalytics',
            bootstrap_servers='kafka:29092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',  # FIXED: Read from beginning, not latest!
            enable_auto_commit=True,
            group_id='airflow_ingestion_star_schema',
            consumer_timeout_ms=10000,  # 10 seconds timeout
            max_poll_records=500,  # Fetch up to 500 messages per poll
            fetch_min_bytes=1,  # Don't wait for batch
            fetch_max_wait_ms=1000,  # Max 1 second wait
            session_timeout_ms=30000,  # 30s session timeout
            heartbeat_interval_ms=10000  # Heartbeat every 10s
        )
    except Exception as e:
        logging.error(f"Failed to create Kafka consumer: {e}")
        raise
    
    batch_size = 0
    errors = 0
    logging.info("🔍 Starting Kafka → MongoDB ingestion...")
    
    # FIXED: Better message processing with error handling
    try:
        for message in consumer:
            try:
                event = message.value
                
                # Parse timestamp
                if isinstance(event.get('timestamp'), str):
                    try:
                        event['timestamp'] = datetime.fromisoformat(event['timestamp'])
                    except ValueError:
                        event['timestamp'] = datetime.utcnow()
                
                event_type = event.get('event_type')
                
                # Route to appropriate collection
                if event_type == 'purchase':
                    collection = db['transactions']
                    unique_id = event.get('transaction_id') or event.get('event_id')
                    filter_query = {'transaction_id': unique_id}
                    event['transaction_id'] = unique_id
                elif event_type in ['session_start', 'session_end']:
                    collection = db['sessions']
                    filter_query = {'session_id': event.get('session_id')}
                elif event_type == 'heartbeat':
                    collection = db['telemetry']
                    filter_query = {'event_id': event.get('event_id')}
                elif event_type == 'level_up':
                    collection = db['progression']
                    filter_query = {'event_id': event.get('event_id')}
                elif event_type == 'review':
                    collection = db['feedback']
                    filter_query = {'review_id': event.get('review_id', event.get('event_id'))}
                else:
                    collection = db['game_events']
                    filter_query = {'event_id': event.get('event_id')}

                # Upsert
                if list(filter_query.values())[0]:
                    collection.replace_one(filter_query, event, upsert=True)
                    batch_size += 1
                    
                    # Log progress every 100 messages
                    if batch_size % 100 == 0:
                        logging.info(f"📊 Processed {batch_size} events so far...")
                        
            except Exception as e:
                errors += 1
                logging.error(f"❌ Failed to insert event: {e}")
                if errors > 10:
                    logging.error("Too many errors, stopping ingestion")
                    break
                continue
    except Exception as e:
        logging.error(f"Consumer loop error: {e}")
    finally:
        consumer.close()
    
    # NEW: Better logging
    if batch_size == 0:
        logging.warning("⚠️ No events consumed from Kafka! Possible issues:")
        logging.warning("   1. Faker might not be generating data")
        logging.warning("   2. Kafka topic might be empty")
        logging.warning("   3. Consumer group offset might be at end of topic")
        logging.warning("   To reset consumer group: docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group airflow_ingestion_star_schema --reset-offsets --to-earliest --execute --topic GameAnalytics")
    else:
        logging.info(f"✅ Ingested {batch_size} events into MongoDB")
    
    context['ti'].xcom_push(key='ingested_count', value=batch_size)
    context['ti'].xcom_push(key='error_count', value=errors)

ingest_task = PythonOperator(
    task_id='ingest_kafka_to_mongodb',
    python_callable=ingest_kafka_to_mongo,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 2: ETL MongoDB → Star Schema (Dimensions First)
# ============================================================================

def populate_dimensions(**context):
    """
    Extract data from MongoDB and populate dimension tables.
    This must run BEFORE fact table population to satisfy foreign keys.
    """
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    mongo_db = client.get_database('game_analytics')
    
    pg_conn = psycopg2.connect(
        host='db', 
        database='game_analytics', 
        user='rafay', 
        password='rafay'
    )
    cur = pg_conn.cursor()
    
    try:
        # Get recent events (last 15 minutes)
        lookback = datetime.utcnow() - timedelta(minutes=1440)
        all_events = []
        
        for collection_name in ['game_events', 'transactions', 'sessions', 'telemetry', 'progression', 'feedback']:
            events = list(mongo_db[collection_name].find({'timestamp': {'$gte': lookback}}))
            all_events.extend(events)
        
        if not all_events:
            logging.info("ℹ️ No new events to process (this is normal if no data generated recently)")
            return
        
        logging.info(f"📊 Processing {len(all_events)} events for dimension population")
        
        # --- 1. POPULATE dim_player ---
        player_records = {}
        for event in all_events:
            pid = event.get('player_id')
            if pid and pid not in player_records:
                player_records[pid] = {
                    'player_id': pid,
                    'username': event.get('username', f'Player_{pid[-8:]}'),
                    'country_code': event.get('country_code', 'US'),
                    'region': event.get('region', 'NA'),
                    'platform': event.get('platform', 'PC'),
                    'player_segment': event.get('player_segment', 'CASUAL'),
                    'registration_date': event.get('registration_date'),
                    'last_login': event.get('timestamp'),
                    'is_active': event.get('is_active', True)
                }
        
        for player_id, data in player_records.items():
            cur.execute("""
                INSERT INTO dim_player 
                (player_id, username, country_code, region, platform, player_segment, registration_date, last_login, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    last_login = EXCLUDED.last_login,
                    platform = EXCLUDED.platform,
                    player_segment = EXCLUDED.player_segment,
                    is_active = EXCLUDED.is_active;
            """, (
                data['player_id'], data['username'], data['country_code'],
                data['region'], data['platform'], data['player_segment'],
                data['registration_date'], data['last_login'], data['is_active']
            ))
        
        logging.info(f"✅ Upserted {len(player_records)} players into dim_player")
        
        # --- 2. POPULATE dim_game ---
        game_records = {}
        for event in all_events:
            gid = event.get('game_id')
            if gid and gid not in game_records:
                game_records[gid] = {
                    'game_id': gid,
                    'game_name': event.get('game_name', 'Unknown Game'),
                    'genre': event.get('genre', 'Unknown'),
                    'developer': event.get('developer', 'Unknown')
                }
        
        for game_id, data in game_records.items():
            cur.execute("""
                INSERT INTO dim_game (game_id, game_name, genre, developer)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (game_id) DO NOTHING;
            """, (data['game_id'], data['game_name'], data['genre'], data['developer']))
        
        logging.info(f"✅ Upserted {len(game_records)} games into dim_game")
        
        # --- 3. POPULATE dim_time ---
        time_records = {}
        for event in all_events:
            ts = event.get('timestamp')
            if ts and isinstance(ts, datetime):
                # Truncate to minute precision to avoid duplicates
                ts_minute = ts.replace(second=0, microsecond=0)
                if ts_minute not in time_records:
                    time_records[ts_minute] = {
                        'timestamp': ts_minute,
                        'hour': ts_minute.hour,
                        'day': ts_minute.day,
                        'month': ts_minute.month,
                        'year': ts_minute.year,
                        'day_of_week': ts_minute.strftime('%A'),
                        'is_weekend': ts_minute.weekday() >= 5
                    }
        
        for ts, data in time_records.items():
            cur.execute("""
                INSERT INTO dim_time (timestamp, hour, day, month, year, day_of_week, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
                RETURNING time_id;
            """, (
                data['timestamp'], data['hour'], data['day'], 
                data['month'], data['year'], data['day_of_week'], data['is_weekend']
            ))
        
        logging.info(f"✅ Upserted {len(time_records)} time entries into dim_time")
        
        # --- 4. POPULATE dim_geography ---
        geo_records = {}
        for event in all_events:
            country_code = event.get('country_code')
            region = event.get('region')
            if country_code and country_code not in geo_records:
                geo_records[country_code] = {
                    'country_code': country_code,
                    'region': region or 'Unknown'
                }
        
        for country_code, data in geo_records.items():
            cur.execute("""
                INSERT INTO dim_geography (country_code, region)
                VALUES (%s, %s)
                ON CONFLICT (country_code) DO UPDATE SET region = EXCLUDED.region;
            """, (data['country_code'], data['region']))
        
        logging.info(f"✅ Upserted {len(geo_records)} geography entries into dim_geography")
        
        # --- 5. POPULATE dim_item ---
        item_records = {}
        for event in all_events:
            item_id = event.get('item_id')
            if item_id and item_id not in item_records:
                item_records[item_id] = {
                    'item_id': item_id,
                    'item_name': event.get('item_name', 'Unknown Item'),
                    'category': event.get('item_category', 'Unknown'),
                    'base_price': event.get('purchase_amount', 0.0)
                }
        
        for item_id, data in item_records.items():
            cur.execute("""
                INSERT INTO dim_item (item_id, item_name, category, base_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (item_id) DO NOTHING;
            """, (data['item_id'], data['item_name'], data['category'], data['base_price']))
        
        logging.info(f"✅ Upserted {len(item_records)} items into dim_item")
        
        pg_conn.commit()
        logging.info("✅ All dimensions populated successfully")
        
    except Exception as e:
        pg_conn.rollback()
        logging.error(f"❌ Dimension population failed: {e}")
        raise
    finally:
        cur.close()
        pg_conn.close()

populate_dims = PythonOperator(
    task_id='populate_dimensions',
    python_callable=populate_dimensions,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 3: ETL MongoDB → Star Schema (Facts)
# ============================================================================

def populate_facts(**context):
    """
    Populate fact tables using foreign keys from dimension tables.
    """
    mongo_hook = MongoHook(conn_id='mongo_default')
    client = mongo_hook.get_conn()
    mongo_db = client.get_database('game_analytics')
    
    pg_conn = psycopg2.connect(
        host='db', 
        database='game_analytics', 
        user='rafay', 
        password='rafay'
    )
    cur = pg_conn.cursor()
    
    try:
        lookback = datetime.utcnow() - timedelta(minutes=15)
        
        # Helper function to get time_id
        def get_time_id(timestamp):
            if not timestamp or not isinstance(timestamp, datetime):
                return None
            ts_minute = timestamp.replace(second=0, microsecond=0)
            cur.execute("SELECT time_id FROM dim_time WHERE timestamp = %s", (ts_minute,))
            result = cur.fetchone()
            return result[0] if result else None
        
        # --- 1. POPULATE fact_session ---
        sessions = list(mongo_db['sessions'].find({'timestamp': {'$gte': lookback}}))
        session_rows = []
        
        for session in sessions:
            sid = session.get('session_id')
            pid = session.get('player_id')
            gid = session.get('game_id')
            ts = session.get('timestamp')
            duration = session.get('expected_duration_sec', 0)
            
            if sid and pid and gid:
                time_id = get_time_id(ts)
                session_rows.append((sid, pid, gid, time_id, duration))
        
        if session_rows:
            execute_values(cur, """
                INSERT INTO fact_session (session_id, player_id, game_id, start_time_id, duration_seconds)
                VALUES %s
                ON CONFLICT (session_id) DO UPDATE SET duration_seconds = EXCLUDED.duration_seconds;
            """, session_rows)
            logging.info(f"✅ Upserted {len(session_rows)} sessions into fact_session")
        
        # --- 2. POPULATE fact_transaction ---
        transactions = list(mongo_db['transactions'].find({'timestamp': {'$gte': lookback}}))
        transaction_rows = []
        
        for txn in transactions:
            txn_id = txn.get('transaction_id')
            sid = txn.get('session_id')
            pid = txn.get('player_id')
            gid = txn.get('game_id')
            ts = txn.get('timestamp')
            amount = txn.get('purchase_amount', 0.0)
            currency = txn.get('currency', 'USD')
            
            if txn_id and pid and gid:
                time_id = get_time_id(ts)
                transaction_rows.append((txn_id, sid, pid, gid, time_id, amount, currency))
        
        if transaction_rows:
            execute_values(cur, """
                INSERT INTO fact_transaction (transaction_id, session_id, player_id, game_id, time_id, amount_usd, currency)
                VALUES %s
                ON CONFLICT (transaction_id) DO NOTHING;
            """, transaction_rows)
            logging.info(f"✅ Inserted {len(transaction_rows)} transactions into fact_transaction")
        
        # --- 3. POPULATE fact_telemetry ---
        telemetry = list(mongo_db['telemetry'].find({'timestamp': {'$gte': lookback}}))
        telemetry_rows = []
        
        for telem in telemetry:
            sid = telem.get('session_id')
            gid = telem.get('game_id')
            ts = telem.get('timestamp')
            fps = telem.get('fps')
            latency = telem.get('latency_ms')
            platform = telem.get('platform')
            region = telem.get('region')
            
            if sid and gid:
                time_id = get_time_id(ts)
                telemetry_rows.append((sid, gid, time_id, fps, latency, platform, region))
        
        if telemetry_rows:
            execute_values(cur, """
                INSERT INTO fact_telemetry (session_id, game_id, time_id, fps, latency_ms, platform, region)
                VALUES %s;
            """, telemetry_rows)
            logging.info(f"✅ Inserted {len(telemetry_rows)} telemetry records into fact_telemetry")
        
        pg_conn.commit()
        logging.info("✅ All fact tables populated successfully")
        
        # Push stats to XCom
        context['ti'].xcom_push(key='sessions_loaded', value=len(session_rows))
        context['ti'].xcom_push(key='transactions_loaded', value=len(transaction_rows))
        context['ti'].xcom_push(key='telemetry_loaded', value=len(telemetry_rows))
        
    except Exception as e:
        pg_conn.rollback()
        logging.error(f"❌ Fact population failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cur.close()
        pg_conn.close()

populate_facts_task = PythonOperator(
    task_id='populate_facts',
    python_callable=populate_facts,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 4: Monitor MongoDB Size
# ============================================================================

def check_mongodb_size(**context):
    """Check if MongoDB needs archival"""
    from pymongo import MongoClient
    
    client = MongoClient('mongodb://admin:admin@mongo:27017/')
    db = client['game_analytics']
    
    stats = db.command('dbStats')
    size_mb = stats['storageSize'] / (1024 * 1024)
    
    logging.info(f"📊 MongoDB Size: {size_mb:.2f} MB")
    context['ti'].xcom_push(key='mongodb_size_mb', value=size_mb)
    
    if size_mb > 10:
        logging.warning(f"⚠️ MongoDB size ({size_mb:.2f} MB) exceeds 300 MB threshold")
        context['ti'].xcom_push(key='archive_required', value=True)
        return 'archive_to_hadoop'
    
    client.close()
    return 'skip_archive'

monitor_size = PythonOperator(
    task_id='monitor_mongodb_size',
    python_callable=check_mongodb_size,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 5: Archive to Hadoop
# ============================================================================

def archive_to_hadoop(**context):
    """Archive old data from MongoDB to HDFS"""
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
        logging.info("ℹ️ Skipping archival - not required")
        return
    
    mongo_client = MongoClient('mongodb://admin:admin@mongo:27017/')
    db = mongo_client['game_analytics']
    
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    
    cutoff_time = datetime.utcnow() - timedelta(hours=24)
    cutoff_str = cutoff_time.isoformat()
    
    collections = ['game_events', 'sessions', 'transactions', 'telemetry', 'progression', 'feedback']
    total_archived = 0
    archive_batch_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    for collection_name in collections:
        collection = db[collection_name]
        
        old_docs = list(collection.find({'timestamp': {'$lt': cutoff_str}}))
        
        if not old_docs:
            logging.info(f"ℹ️ No old documents in {collection_name}")
            continue
        
        logging.info(f"📦 Archiving {len(old_docs)} documents from {collection_name}")
        
        try:
            df = pd.DataFrame(old_docs)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
            
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                table = pa.Table.from_pandas(df)
                pq.write_table(table, tmp_path, compression='gzip', compression_level=9)
            
            file_size = os.path.getsize(tmp_path)
            
            with open(tmp_path, 'rb') as f:
                file_checksum = hashlib.sha256(f.read()).hexdigest()
            
            hdfs_dir = '/archives'
            hdfs_filename = f"{collection_name}_{archive_batch_id}.parquet"
            hdfs_path = f"{hdfs_dir}/{hdfs_filename}"
            
            try:
                hdfs_client.makedirs(hdfs_dir)
            except:
                pass
            
            logging.info(f"☁️ Uploading to HDFS: {hdfs_path}")
            with open(tmp_path, 'rb') as local_file:
                hdfs_client.write(hdfs_path, data=local_file, overwrite=True)
            
            hdfs_status = hdfs_client.status(hdfs_path)
            hdfs_size = hdfs_status['length']
            
            metadata = {
                'archive_batch_id': archive_batch_id,
                'collection': collection_name,
                'archive_time': datetime.utcnow(),
                'document_count': len(old_docs),
                'hdfs_path': hdfs_path,
                'hdfs_size_bytes': hdfs_size,
                'compression_ratio': round(file_size / hdfs_size, 2) if hdfs_size > 0 else 1.0,
                'checksum_sha256': file_checksum,
                'status': 'completed'
            }
            
            db['archive_metadata'].insert_one(metadata)
            
            delete_result = collection.delete_many({'timestamp': {'$lt': cutoff_time}})
            total_archived += delete_result.deleted_count
            
            os.unlink(tmp_path)
            
        except Exception as e:
            logging.error(f"❌ Error archiving {collection_name}: {e}")
            continue
    
    mongo_client.close()
    logging.info(f"✅ Archived {total_archived} documents to HDFS")

archive_task = PythonOperator(
    task_id='archive_to_hadoop',
    python_callable=archive_to_hadoop,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 6: Data Quality Checks
# ============================================================================

def data_quality_checks(**context):
    """Validate star schema integrity"""
    pg_conn = psycopg2.connect(
        host='db', 
        database='game_analytics', 
        user='rafay', 
        password='rafay'
    )
    cur = pg_conn.cursor()
    
    checks = {}
    
    try:
        # Check for orphaned records
        cur.execute("SELECT COUNT(*) FROM fact_transaction WHERE player_id NOT IN (SELECT player_id FROM dim_player)")
        checks['orphaned_transactions'] = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM fact_telemetry WHERE game_id NOT IN (SELECT game_id FROM dim_game)")
        checks['orphaned_telemetry'] = cur.fetchone()[0]
        
        # Check for NULL foreign keys
        cur.execute("SELECT COUNT(*) FROM fact_transaction WHERE player_id IS NULL")
        checks['null_player_ids'] = cur.fetchone()[0]
        
        # Count records per table
        for table in ['dim_player', 'dim_game', 'dim_time', 'fact_session', 'fact_transaction', 'fact_telemetry']:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            checks[f'{table}_count'] = cur.fetchone()[0]
        
        issues = [k for k, v in checks.items() if 'orphaned' in k and v > 0]
        
        if issues:
            logging.warning(f"⚠️ Data quality issues: {checks}")
        else:
            logging.info(f"✅ Data quality checks passed: {checks}")
        
        context['ti'].xcom_push(key='quality_checks', value=checks)
        
    finally:
        cur.close()
        pg_conn.close()

quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Main ETL flow
ingest_task >> populate_dims >> populate_facts_task >> quality_checks

# Parallel monitoring and archival
monitor_size >> archive_task >> quality_checks