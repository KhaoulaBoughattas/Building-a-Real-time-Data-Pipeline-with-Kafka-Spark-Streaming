#!/bin/bash

echo "🚀 Starting Real-Time Big Data Project..."

docker-compose up -d
sleep 20

echo "📌 Creating Kafka topic..."

docker exec kafka bash -c "
/opt/kafka/bin/kafka-topics.sh \
--create \
--bootstrap-server kafka:9092 \
--replication-factor 1 \
--partitions 1 \
--topic topicA || echo 'Topic already exists'
"

echo "🔥 Starting Spark Streaming jobs..."

docker exec spark-jupyter bash -c "
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.mongodb.spark:mongo-spark-connector:10.0.0 \
/workspace/StreamAggCountry.py
" &

docker exec spark-jupyter bash -c "
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.mongodb.spark:mongo-spark-connector:10.0.0 \
/workspace/StreamAggProduct.py
" &

docker exec spark-jupyter bash -c "
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.mongodb.spark:mongo-spark-connector:10.0.0 \
/workspace/StreamAggGender.py
" &

echo "📤 Producer already running via docker-compose"

echo "✅ Project started successfully!"
