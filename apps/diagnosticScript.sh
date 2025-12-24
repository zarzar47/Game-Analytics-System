#!/bin/bash

echo "=========================================="
echo "GAME ANALYTICS PIPELINE DIAGNOSTICS"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. CHECK KAFKA
echo "1️⃣  CHECKING KAFKA..."
echo "   Attempting to consume 5 messages from Kafka..."
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic GameAnalytics \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 10000 2>/dev/null

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Kafka is working and has messages${NC}"
else
    echo -e "${RED}❌ Kafka has no messages or is not accessible${NC}"
fi
echo ""

# 2. CHECK SPARK LOGS
echo "2️⃣  CHECKING SPARK PROCESSOR LOGS..."
echo "   Last 30 lines from Spark processor:"
docker logs --tail 30 spark-processor 2>&1 | grep -E "(Batch|DEBUG|ERROR|rows|✅|❌|⚠️)"
echo ""

# 3. CHECK DATABASE TABLES
echo "3️⃣  CHECKING DATABASE TABLES..."

# Check realtime_revenue
echo "   📊 Checking realtime_revenue table..."
REVENUE_COUNT=$(docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -t -c "SELECT count(*) FROM realtime_revenue;" 2>/dev/null | tr -d ' \r\n')
if [ ! -z "$REVENUE_COUNT" ] && [ "$REVENUE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ realtime_revenue has $REVENUE_COUNT rows${NC}"
    docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -c "SELECT * FROM realtime_revenue ORDER BY window_start DESC LIMIT 3;"
else
    echo -e "${YELLOW}⚠️  realtime_revenue is empty or doesn't exist${NC}"
fi
echo ""

# Check realtime_concurrency
echo "   📊 Checking realtime_concurrency table..."
CONCURRENCY_COUNT=$(docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -t -c "SELECT count(*) FROM realtime_concurrency;" 2>/dev/null | tr -d ' \r\n')
if [ ! -z "$CONCURRENCY_COUNT" ] && [ "$CONCURRENCY_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ realtime_concurrency has $CONCURRENCY_COUNT rows${NC}"
    docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -c "SELECT * FROM realtime_concurrency ORDER BY window_start DESC LIMIT 3;"
else
    echo -e "${YELLOW}⚠️  realtime_concurrency is empty or doesn't exist${NC}"
fi
echo ""

# Check realtime_performance
echo "   📊 Checking realtime_performance table..."
PERFORMANCE_COUNT=$(docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -t -c "SELECT count(*) FROM realtime_performance;" 2>/dev/null | tr -d ' \r\n')
if [ ! -z "$PERFORMANCE_COUNT" ] && [ "$PERFORMANCE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ realtime_performance has $PERFORMANCE_COUNT rows${NC}"
    docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -c "SELECT * FROM realtime_performance ORDER BY window_start DESC LIMIT 3;"
else
    echo -e "${YELLOW}⚠️  realtime_performance is empty or doesn't exist${NC}"
fi
echo ""

# 4. CHECK API ENDPOINTS
echo "4️⃣  CHECKING API ENDPOINTS..."
echo "   Testing /analytics/revenue..."
REVENUE_API=$(curl -s http://localhost:8000/analytics/revenue | jq length 2>/dev/null)
if [ ! -z "$REVENUE_API" ] && [ "$REVENUE_API" -gt 0 ]; then
    echo -e "${GREEN}✅ API returns $REVENUE_API revenue records${NC}"
else
    echo -e "${YELLOW}⚠️  API returns empty or error for revenue${NC}"
    curl -s http://localhost:8000/analytics/revenue
fi
echo ""

echo "5️⃣  CHECKING EVENT TYPE DISTRIBUTION..."
docker exec -it $(docker ps -qf "name=db") psql -U rafay -d game_analytics -c "SELECT event_type, COUNT(*) as count FROM game_metrics GROUP BY event_type ORDER BY count DESC LIMIT 10;"
echo ""

echo "=========================================="
echo "DIAGNOSTIC COMPLETE"
echo "=========================================="
echo ""
echo "💡 TIPS:"
echo "   - If Kafka is empty, check if 'faker' container is running"
echo "   - If tables are empty, check Spark logs for errors"
echo "   - Spark waits for watermark to pass before writing"
echo "   - With new settings, first rows should appear in 30-60 seconds"
echo ""
echo "🔧 USEFUL COMMANDS:"
echo "   Watch Spark logs:        docker logs -f spark-processor"
echo "   Watch Faker logs:        docker logs -f faker"
echo "   Restart Spark:           docker-compose restart spark-processor"
echo "   Clear checkpoints:       docker exec spark-processor rm -rf /tmp/checkpoints/*"
echo ""