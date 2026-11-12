COMPOSE = docker compose
SPARK   = $(COMPOSE) exec spark-streaming

.PHONY: up down clean ps logs connector-status topics inspect schema-change gold dag-trigger test psql

up:                ## build images and start the whole stack
	$(COMPOSE) up -d --build

down:              ## stop containers (keep data)
	$(COMPOSE) down

clean:             ## stop containers and DELETE all data volumes
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:              ## follow streaming + datagen logs
	$(COMPOSE) logs -f spark-streaming datagen

connector-status:
	curl -s http://localhost:8083/connectors/pg-shop-connector/status

topics:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

inspect:           ## row counts of every Bronze/Silver/Gold/quarantine table
	$(SPARK) python -m lakehouse.jobs.inspect_lakehouse

schema-change:     ## add a column at the source (schema evolution demo)
	$(COMPOSE) exec -T postgres psql -U shop -d shop < datagen/schema_evolution.sql

gold:              ## build Gold tables now (outside Airflow)
	$(SPARK) python -m lakehouse.jobs.gold_aggregate --table all

dag-trigger:       ## trigger the daily DAG manually
	$(COMPOSE) exec airflow-scheduler airflow dags trigger lakehouse_daily

test:              ## unit tests (local Spark, no MinIO/Kafka needed)
	$(COMPOSE) run --rm --no-deps spark-streaming python -m pytest -q tests

psql:
	$(COMPOSE) exec postgres psql -U shop -d shop
