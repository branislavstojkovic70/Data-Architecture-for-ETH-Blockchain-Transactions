#!/bin/bash

docker-compose down
docker-compose up -d

echo "=== Waiting for services to start ==="
sleep 30

echo "=== Running batch pretransform ==="
docker exec spark-master /spark/bin/spark-submit /queries/batch_pretransform.py

echo "=== System ready! ==="
echo "Kibana: http://localhost:5601"
echo "Spark UI: http://localhost:8080"
echo "HDFS: http://localhost:9870"