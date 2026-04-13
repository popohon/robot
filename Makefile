DC := docker compose

.PHONY: up down build ps logs logs-spark logs-quality logs-reconciler logs-producer \
        restart query-smoke query-crossjoin query-linkage quality-once \
        nessie-health nessie-demo backfill dbt-run dbt-test

# Lifecycle
up: build
	$(DC) up -d

down:
	$(DC) down -v

build:
	$(DC) build

ps:
	$(DC) ps

restart:
	$(DC) restart spark-job producer quality-check reconciler

# Logs
logs:
	$(DC) logs -f

logs-spark:
	$(DC) logs -f spark-job

logs-quality:
	$(DC) logs -f quality-check

logs-reconciler:
	$(DC) logs -f reconciler

logs-producer:
	$(DC) logs -f producer

# Queries
query-smoke:
	$(DC) exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
	$(DC) exec trino trino --execute "SHOW TABLES FROM iceberg.prod"
	$(DC) exec trino trino --execute "SELECT count(*) FROM iceberg.prod.telemetry_events"
	$(DC) exec trino trino --execute "SELECT count(*) FROM iceberg.prod.blob_index"

query-crossjoin:
	$(DC) exec trino trino --execute "SELECT t.robot_id, t.event_id, b.media_type, b.blob_uri FROM iceberg.prod.telemetry_events t INNER JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id ORDER BY t.ingest_ts DESC LIMIT 10"

query-linkage:
	$(DC) exec trino trino --execute "SELECT count(*) AS total, count(b.event_id) AS linked, round(cast(count(b.event_id) AS double)/count(*)*100,2) AS pct FROM iceberg.prod.telemetry_events t LEFT JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id"

quality-once:
	$(DC) exec quality-check python quality_check.py --once

# Nessie
nessie-health:
	curl -s http://localhost:19120/api/v1/config | python3 -m json.tool

nessie-demo:
	$(DC) --profile nessie-demo run --rm nessie-demo

# Backfill
backfill:
	$(DC) --profile backfill run --rm backfill

backfill-small:
	$(DC) --profile backfill run --rm backfill python backfill.py --hours-back 1 --events-per-second 50 --robots 10

# Compaction / small-file handling
compact:
	$(DC) --profile compact run --rm compaction

compact-report:
	$(DC) --profile compact run --rm compaction python compaction.py --report-only

# Peak simulation
peak:
	$(DC) --profile peak run --rm peak-simulator

peak-short:
	$(DC) --profile peak run --rm peak-simulator python peak_simulator.py --duration 30 --batch-size 200

# dbt
dbt-run:
	$(DC) --profile dbt run --rm dbt

dbt-test:
	$(DC) --profile dbt run --rm dbt dbt test --profiles-dir /opt/dbt
