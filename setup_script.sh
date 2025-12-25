#!/bin/bash

# ============================================================================
# Game Analytics Big Data System - Quick Setup Script
# ============================================================================
# This script automates the setup of all required components
# Run this from your project root directory

set -e  # Exit on any error

echo "=========================================="
echo "Game Analytics System - Quick Setup"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting."; exit 1; }
echo "✅ Docker and Docker Compose found"
echo ""

# Create directory structure
echo "Creating directory structure..."
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p mongo-init
mkdir -p sql
mkdir -p hadoop/archives
echo "✅ Directories created"
echo ""

# Create Hadoop environment file
echo "Creating Hadoop configuration..."
cat > hadoop.env << 'EOF'
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
HDFS_CONF_dfs_replication=1
HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false
YARN_CONF_yarn_log___aggregation___enable=true
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_fs_state___store_uri=/rmstate
YARN_CONF_yarn_nodemanager_remote___app___log___dir=/app-logs
YARN_CONF_yarn_log_server_url=http://historyserver:8188/applicationhistory/logs/
YARN_CONF_yarn_timeline___service_enabled=true
YARN_CONF_yarn_timeline___service_generic___application___history_enabled=true
YARN_CONF_yarn_resourcemanager_system___metrics___publisher_enabled=true
YARN_CONF_yarn_resourcemanager_hostname=resourcemanager
YARN_CONF_yarn_timeline___service_hostname=historyserver
YARN_CONF_yarn_resourcemanager_address=resourcemanager:8032
YARN_CONF_yarn_resourcemanager_scheduler_address=resourcemanager:8030
YARN_CONF_yarn_resourcemanager_resource___tracker_address=resourcemanager:8031
EOF
echo "✅ Hadoop configuration created"
echo ""

# Create MongoDB initialization script
echo "Creating MongoDB initialization..."
cat > mongo-init/init.js << 'EOF'
// Initialize game_analytics database
db = db.getSiblingDB('game_analytics');

// Create collections
db.createCollection('game_events');
db.createCollection('sessions');
db.createCollection('transactions');
db.createCollection('archive_metadata');

// Create indexes for performance
db.game_events.createIndex({ "timestamp": 1 });
db.game_events.createIndex({ "game_id": 1 });
db.game_events.createIndex({ "event_type": 1 });
db.game_events.createIndex({ "player_id": 1 });

db.sessions.createIndex({ "player_id": 1 });
db.sessions.createIndex({ "session_id": 1 }, { unique: true });
db.sessions.createIndex({ "timestamp": 1 });

db.transactions.createIndex({ "player_id": 1, "timestamp": -1 });
db.transactions.createIndex({ "transaction_id": 1 }, { unique: true });

db.archive_metadata.createIndex({ "archive_time": -1 });
db.archive_metadata.createIndex({ "collection": 1 });

print("✅ MongoDB initialized successfully for game analytics");
EOF
echo "✅ MongoDB initialization script created"
echo ""

# Create enhanced star schema
echo "Creating enhanced star schema..."
cat > sql/star_schema.sql << 'EOF'
-- Enhanced Star Schema for Game Analytics
-- This extends the basic schema with proper dimensional modeling

-- Dimension: Player
CREATE TABLE IF NOT EXISTS dim_player (
    player_id VARCHAR(255) PRIMARY KEY,
    username VARCHAR(255),
    country_code VARCHAR(10),
    region VARCHAR(50),
    platform VARCHAR(50),
    player_segment VARCHAR(50),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Dimension: Game
CREATE TABLE IF NOT EXISTS dim_game (
    game_id VARCHAR(255) PRIMARY KEY,
    game_name VARCHAR(255),
    genre VARCHAR(100),
    developer VARCHAR(255),
    release_date DATE,
    is_multiplayer BOOLEAN DEFAULT TRUE
);

-- Dimension: Time (pre-populated)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE,
    hour INT,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN,
    quarter INT
);

-- Dimension: Item
CREATE TABLE IF NOT EXISTS dim_item (
    item_id VARCHAR(255) PRIMARY KEY,
    item_name VARCHAR(255),
    category VARCHAR(100),
    rarity VARCHAR(50),
    base_price DECIMAL(10, 2),
    currency VARCHAR(10) DEFAULT 'USD'
);

-- Fact: Transaction (Enhanced)
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_id VARCHAR(255) PRIMARY KEY,
    player_id VARCHAR(255) REFERENCES dim_player(player_id),
    item_id VARCHAR(255),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    time_id INT,
    amount_usd DECIMAL(10, 2),
    currency VARCHAR(10),
    payment_method VARCHAR(50),
    is_first_purchase BOOLEAN DEFAULT FALSE
);

-- Indexes for join performance
CREATE INDEX IF NOT EXISTS idx_transaction_player ON fact_transaction(player_id);
CREATE INDEX IF NOT EXISTS idx_transaction_game ON fact_transaction(game_id);
CREATE INDEX IF NOT EXISTS idx_transaction_time ON fact_transaction(time_id);

-- Insert sample games
INSERT INTO dim_game (game_id, game_name, genre, developer, release_date) VALUES
('valorant', 'Valorant', 'FPS', 'Riot Games', '2020-06-02'),
('elden_ring', 'Elden Ring', 'RPG', 'FromSoftware', '2022-02-25'),
('minecraft', 'Minecraft', 'Sandbox', 'Mojang', '2011-11-18'),
('stardew_valley', 'Stardew Valley', 'Simulation', 'ConcernedApe', '2016-02-26')
ON CONFLICT (game_id) DO NOTHING;

-- Insert sample items
INSERT INTO dim_item (item_id, item_name, category, rarity, base_price) VALUES
('item_100', 'Gold Sword Skin', 'Cosmetic', 'Legendary', 19.99),
('item_200', 'Battle Pass', 'Subscription', 'Common', 9.99),
('item_300', 'XP Boost', 'Powerup', 'Rare', 4.99),
('item_400', 'Premium Currency 1000', 'Currency', 'Common', 9.99)
ON CONFLICT (item_id) DO NOTHING;

COMMIT;
EOF
echo "✅ Star schema SQL created"
echo ""

# Create simplified Airflow DAG
echo "Creating Airflow DAG..."
cat > airflow/dags/game_analytics_dag.py << 'EOF'
"""
Simplified Game Analytics Pipeline DAG
Runs every 1 minute to demonstrate live updates
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'game_analytics_pipeline',
    default_args=default_args,
    description='Real-time game analytics pipeline',
    schedule_interval='*/1 * * * *',  # Every minute
    catchup=False,
    tags=['gaming', 'real-time'],
)

def check_mongodb_size(**context):
    """Monitor MongoDB size"""
    logging.info("Checking MongoDB size...")
    # Simulate size check
    size_mb = 150  # Placeholder
    logging.info(f"MongoDB size: {size_mb} MB")
    context['ti'].xcom_push(key='size_mb', value=size_mb)
    
    if size_mb > 300:
        logging.warning("Size threshold exceeded - archival needed")
        return 'archive'
    return 'continue'

def process_batch(**context):
    """Process current batch of data"""
    logging.info("Processing current data batch...")
    logging.info("✅ Batch processing complete")

def trigger_dashboard_refresh(**context):
    """Signal dashboard to refresh"""
    logging.info("Dashboard refresh triggered")

monitor = PythonOperator(
    task_id='monitor_size',
    python_callable=check_mongodb_size,
    provide_context=True,
    dag=dag,
)

process = PythonOperator(
    task_id='process_batch',
    python_callable=process_batch,
    provide_context=True,
    dag=dag,
)

refresh = PythonOperator(
    task_id='refresh_dashboard',
    python_callable=trigger_dashboard_refresh,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
monitor >> process >> refresh
EOF
echo "✅ Airflow DAG created"
echo ""

# Create requirements file for Airflow
cat > airflow/requirements.txt << 'EOF'
pymongo
psycopg2-binary
kafka-python
pandas
EOF
echo "✅ Airflow requirements created"
echo ""

# Backup original docker-compose
if [ -f docker-compose.yml ]; then
    echo "Backing up existing docker-compose.yml..."
    cp docker-compose.yml docker-compose.yml.backup
    echo "✅ Backup created: docker-compose.yml.backup"
fi

echo ""
echo "=========================================="
echo "Setup Complete! 🎉"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Review the generated files in your project directory"
echo "2. Replace your docker-compose.yml with the enhanced version from the artifacts"
echo "3. Start the stack: docker-compose up --build -d"
echo "4. Wait 2-3 minutes for all services to initialize"
echo "5. Access the interfaces:"
echo "   - Dashboard: http://localhost:8501"
echo "   - Airflow: http://localhost:8080 (admin/admin)"
echo "   - API: http://localhost:8000/docs"
echo ""
echo "To monitor the system:"
echo "  docker-compose ps        # Check service status"
echo "  docker-compose logs -f   # Follow logs"
echo ""
echo "Happy analyzing! 📊"