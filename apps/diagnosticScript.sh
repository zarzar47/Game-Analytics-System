#!/bin/bash

# ==============================================================================
# Game Analytics System - Diagnostic Script
# ==============================================================================
# This script runs a series of checks to diagnose the data flow through the
# entire ETL pipeline, from data generation to the final database.
# ==============================================================================

# ANSI Color Codes
BLUE="\033[1;34m"
GREEN="\033[0;32m"
YELLOW="\033[0;93m"
RED="\033[0;31m"
NC="\033[0m" # No Color

# Helper function to print a formatted header
print_header() {
    echo -e "\n${BLUE}=======================================================================${NC}"
    echo -e "${BLUE}>>> $1${NC}"
    echo -e "${BLUE}=======================================================================${NC}"
}

# 1. Check Docker Container Status
print_header "Checking Docker Container Status"
docker-compose ps

# 2. Check Faker Logs (Data Generation)
print_header "Tailing Faker Logs (Checking for data generation)"
docker-compose logs --tail=10 faker

# 3. Check Kafka Topic (Data Streaming)
print_header "Inspecting Kafka Topic 'GameAnalytics' (Live data stream)"
echo -e "${YELLOW}Attempting to read 1 message from the 'GameAnalytics' topic...${NC}"
docker-compose exec kafka /usr/bin/kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic GameAnalytics \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 || echo -e "${RED}Failed to read from Kafka. Is the faker service running and producing data?${NC}"

# 4. Check MongoDB (Hot Storage)
print_header "Checking MongoDB - Hot Storage"
echo -e "${YELLOW}Counting documents in key MongoDB collections...${NC}"
MONGO_COMMAND="
db.getSiblingDB('game_analytics');
print('game_events: ' + db.game_events.count());
print('sessions: ' + db.sessions.count());
print('transactions: ' + db.transactions.count());
"
docker-compose exec mongo mongo --username admin --password admin --authenticationDatabase admin --quiet --eval "$MONGO_COMMAND"

# 5. Check Airflow Scheduler Logs (ETL Orchestration)
print_header "Tailing Airflow Scheduler Logs (ETL Task Scheduling)"
docker-compose logs --tail=15 airflow-scheduler

# 6. Check PostgreSQL (Warm Storage - Star Schema)
print_header "Checking PostgreSQL - Star Schema Analytics Database"
echo -e "${YELLOW}Counting rows in key Fact and Dimension tables...${NC}"

PG_COMMANDS="
SELECT 'dim_player' AS table, COUNT(*) FROM dim_player UNION ALL
SELECT 'dim_game', COUNT(*) FROM dim_game UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time UNION ALL
SELECT 'fact_session', COUNT(*) FROM fact_session UNION ALL
SELECT 'fact_transaction', COUNT(*) FROM fact_transaction UNION ALL
SELECT 'fact_telemetry', COUNT(*) FROM fact_telemetry;
"
docker-compose exec -T db psql -U rafay -d game_analytics -c "$PG_COMMANDS"


# 7. Check Spark Processor Logs
print_header "Tailing Spark Processor Logs"
docker-compose logs --tail=15 spark-processor

# 8. Check API and Web Logs
print_header "Tailing API and Web App Logs"
echo -e "${YELLOW}--- API Logs ---${NC}"
docker-compose logs --tail=10 api
echo -e "\n${YELLOW}--- Web App Logs ---${NC}"
docker-compose logs --tail=10 web


print_header "Diagnostic Complete"
echo -e "${GREEN}The script has finished. Review the output above to identify potential issues.${NC}"
echo -e "${GREEN}Key things to look for:${NC}"
echo -e "${GREEN}- Errors in the logs for any service.${NC}"
echo -e "${GREEN}- A count of 0 in Kafka, MongoDB, or PostgreSQL, which could indicate a blockage.${NC}"
echo -e "${GREEN}- Unhealthy container statuses.${NC}"
