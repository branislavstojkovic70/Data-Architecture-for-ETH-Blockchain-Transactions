#!/bin/bash

# ============================================================
# KT2 - Ethereum Top 100 Wallets Pipeline
# Bronze → Silver → Gold (Top 100 Wallets)
# ============================================================

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

SPARK_MASTER="spark-master"
PACKAGES="org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}   KT2 - Top 100 Wallets Pipeline${NC}"
echo -e "${BLUE}   Started: $(date)${NC}"
echo -e "${BLUE}============================================================${NC}"

start_time=$(date +%s)

# ============================================================
# STEP 1: Bronze to Silver
# ============================================================
echo -e "\n${YELLOW}[STEP 1/2] Bronze → Silver Transformation${NC}"

docker exec $SPARK_MASTER /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages $PACKAGES \
    /queries/batch/bronze_to_silver.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Bronze→Silver complete${NC}"
else
    echo -e "${RED}✗ Bronze→Silver FAILED!${NC}"
    exit 1
fi

# ============================================================
# STEP 2: Top 100 Wallets Analysis
# ============================================================
echo -e "\n${YELLOW}[STEP 2/2] Top 100 Wallets Analysis${NC}"

docker exec $SPARK_MASTER /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages $PACKAGES \
    /queries/batch/top_100_wallets.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Top 100 Wallets analysis complete${NC}"
else
    echo -e "${RED}✗ Top 100 Wallets FAILED!${NC}"
    exit 1
fi

# ============================================================
# Summary
# ============================================================
end_time=$(date +%s)
execution_time=$((end_time - start_time))

echo -e "\n${BLUE}============================================================${NC}"
echo -e "${GREEN}✓ KT2 PIPELINE COMPLETE${NC}"
echo -e "${BLUE}============================================================${NC}"
echo -e "Execution time: ${execution_time} seconds ($(($execution_time / 60)) min)"
echo -e "Completed: $(date)"
echo -e "\n${BLUE}Results:${NC}"
echo -e "  • Silver Layer: hdfs://namenode:9000/project/silver/batch/ethereum/"
echo -e "  • Top 100 Wallets: hdfs://namenode:9000/project/transform/batch/eth_top_100_wallets/"
echo -e "\n${BLUE}View results:${NC}"
echo -e "  docker exec namenode hdfs dfs -ls /project/transform/batch/eth_top_100_wallets/"
echo -e "${BLUE}============================================================${NC}"