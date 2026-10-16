.PHONY: install test lint up-small up-full down-small down-full topic-small topic-full smoke producer-bench up down lakehouse-init bronze silver demo

COMPOSE = docker compose -p gamestream-full -f compose/compose.yaml --profile full --profile lakehouse
SPARK_PACKAGES = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.hadoop:hadoop-aws:3.3.4
SPARK_IVY = --conf spark.jars.ivy=/tmp/ivy

install:
	uv sync --extra test --extra spark --extra analytics

test:
	uv run --extra test --extra spark pytest -q

lint:
	uv run --extra test ruff check producer scripts bench spark dq airflow tests

up:
	GAMESTREAM_VOTERS='1@broker1:9093,2@broker2:9093,3@broker3:9093' GAMESTREAM_RF=3 GAMESTREAM_MIN_ISR=2 $(COMPOSE) up -d --build

down:
	$(COMPOSE) down

lakehouse-init:
	$(COMPOSE) run --rm minio-init
	$(COMPOSE) exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:9092 --create --if-not-exists --topic game_events --partitions 6 --replication-factor 3 --config min.insync.replicas=2

bronze:
	$(COMPOSE) exec -d spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 $(SPARK_IVY) --packages $(SPARK_PACKAGES) /opt/gamestream/spark/stream_bronze.py

silver:
	$(COMPOSE) exec -d spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 $(SPARK_IVY) --packages $(SPARK_PACKAGES) /opt/gamestream/spark/stream_silver.py

demo:
	$(MAKE) up
	$(MAKE) lakehouse-init
	$(MAKE) bronze
	$(MAKE) silver
	uv run gamestream-produce --count 1000 --brokers 127.0.0.1:19092 --registry http://127.0.0.1:18081/apis/ccompat/v7

up-small:
	docker compose -p gamestream-small -f compose/compose.yaml --profile small up -d

up-full:
	GAMESTREAM_VOTERS='1@broker1:9093,2@broker2:9093,3@broker3:9093' GAMESTREAM_RF=3 GAMESTREAM_MIN_ISR=2 docker compose -p gamestream-full -f compose/compose.yaml --profile full up -d

down-small:
	docker compose -p gamestream-small -f compose/compose.yaml --profile small down

down-full:
	docker compose -p gamestream-full -f compose/compose.yaml --profile full down

topic-small:
	uv run python scripts/create_topic.py

topic-full:
	uv run python scripts/create_topic.py --replication 3 --min-isr 2

smoke:
	uv run python scripts/smoke_kafka.py

producer-bench:
	uv run python bench/generator_bench.py
