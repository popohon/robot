# Robotics Data Lakehouse — Assignment 1 Demo

A fully local, Docker-based demo of a two-layer data lakehouse for a 200-robot fleet.
Demonstrates: streaming ingestion, ACID storage, cross-layer consistency, drift correction,
data validation, medallion architecture (Bronze/Silver/Gold), and git-like data versioning.

**All open-source. All local Docker. No cloud~**

---

## Architecture

```
200 Robots (simulated by producer.py)
    │ telemetry        │ blobs             │ heartbeat
    ▼                  ▼                   ▼
┌──────────────────────────────────────────────────────┐
│            Apache Kafka (KRaft, 24 partitions)        │
│  telemetry.raw  │  blob.manifest  │  robot.heartbeat  │
│  72h retention                                        │
└────────┬─────────────────┬─────────────────┬─────────┘
         ▼                 ▼                 ▼
    Spark Streaming   Blob Validator    Drift Offset Map
    (drift-correct)   (SHA-256)         (per-robot)
         │                 │
         ▼                 ▼
┌──────────────────────────────────────────────────────┐
│              1 PB Data Lake (Nessie Catalog)          │
│                                                       │
│  Iceberg + Parquet          │  MinIO (S3-compatible)  │
│  ┌─────────────────────┐    │  ┌──────────────────┐   │
│  │ Bronze:              │    │  │ raw-blobs/        │   │
│  │  telemetry_events    │    │  │  JPEG, PCD, WAV   │   │
│  │  blob_index          │    │  └──────────────────┘   │
│  ├─────────────────────┤    │                          │
│  │ Silver (dbt views):  │    │                          │
│  │  telemetry_validated │    │                          │
│  │  blob_linked         │    │                          │
│  ├─────────────────────┤    │                          │
│  │ Gold (dbt views):    │    │                          │
│  │  robot_hourly_stats  │    │                          │
│  │  linkage_report      │    │                          │
│  └─────────────────────┘    │                          │
└──────────────────────────────────────────────────────┘
         │
         ▼
    Trino (SQL query engine)  →  Researchers
```

## Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Streaming buffer | Apache Kafka (KRaft) | Decouple production from consumption, 72h replay |
| Structured storage | Apache Iceberg + Parquet | ACID, hidden partitioning, schema evolution |
| Object storage | MinIO (S3-compatible) | Blob storage for camera/LIDAR/audio |
| Catalog | Nessie | Git-like versioning for Iceberg metadata |
| Query engine | Trino | Distributed SQL for 200 concurrent researchers |
| Stream processing | Spark Structured Streaming | Kafka → Iceberg with per-robot drift correction |
| Transforms | dbt (dbt-trino) | Silver/Gold medallion layers |
| Visibility | Kafka UI, MinIO Console, Trino UI | Operational dashboards |

---

## Prerequisites

- **Docker Desktop** with at least 8 GB RAM allocated (12 GB recommended)
- **Docker Compose** v2 (bundled with Docker Desktop)
- **make** (pre-installed on macOS/Linux)

No other dependencies. Everything runs in Docker.

---

## Quick Start

```bash
# 1. Build images and start all services
make up

# 2. Wait ~2 minutes for Spark to download packages (first run only)
#    and for tables to be created. Watch progress:
make logs-spark

# 3. Verify data is flowing
make query-smoke

# 4. Check cross-layer linkage
make query-linkage
```

### Service UIs

| Service | URL | Purpose |
|---------|-----|---------|
| Kafka UI | http://localhost:8081 | Inspect topics, messages, consumer groups |
| MinIO Console | http://localhost:9001 | Browse blob storage (user: minioadmin / minioadmin) |
| Trino UI | http://localhost:8080 | Query execution, resource usage |
| Nessie API | http://localhost:19120/api/v2/config | Catalog health and branch info |

---

## Data Flow

### 1. Producer → Kafka

`producer.py` simulates 200 robots:
- Each robot has a **stable per-session clock drift** (±500ms) + **±20ms jitter**
- Produces **telemetry** (JSON → `telemetry.raw` topic) with sensor readings
- Uploads **blobs** (binary → MinIO) and sends **manifests** (JSON → `blob.manifest` topic)
- Publishes **heartbeats** (drift estimates → `robot.heartbeat` topic) every ~50 events
- All records pass through `validate.py` (schema + domain checks) before sending

### 2. Kafka → Iceberg (Spark Structured Streaming)

`spark_stream.py`:
- Reads from `telemetry.raw` and `blob.manifest` topics
- A **background thread** consumes `robot.heartbeat` to maintain a per-robot drift offset map
- For each event: `corrected_ts = edge_ts - drift_offset[robot_id]`
- Writes to Iceberg tables via Nessie catalog (ACID snapshots)
- Tables are partitioned: `days(corrected_ts), hours(corrected_ts), bucket(128, robot_id)`

### 3. Reconciler (cross-layer consistency)

`reconciler.py` runs every 2 minutes:
- Joins `telemetry_events` ↔ `blob_index` on `event_id`
- Reports: linkage rate, orphan count, oldest unlinked event
- State machine: `PENDING_BLOB_LINK → LINKED → ORPHANED`

### 4. Quality Check (SLA enforcement)

`quality_check.py` runs every 60 seconds:
- **Freshness**: `now() - max(ingest_ts)` — alert if >120s
- **Completeness**: event count per 5-min window + linkage rate
- **Accuracy**: temperature/torque domain bounds
- Each check: OK / WARNING / CRITICAL

### 5. dbt (Silver/Gold layers)

Run `make dbt-run` after data is flowing:
- **Silver**: `telemetry_validated` (domain-filtered), `blob_linked` (cross-layer joined)
- **Gold**: `robot_hourly_stats` (per-robot aggregations), `linkage_report` (consistency health)

---

## Make Targets

### Core
| Target | Description |
|--------|-------------|
| `make up` | Build images + start all services |
| `make down` | Stop all services + remove volumes |
| `make build` | Rebuild Docker images (after code changes) |
| `make ps` | Show running services |
| `make restart` | Restart pipeline services |

### Logs
| Target | Description |
|--------|-------------|
| `make logs` | Follow all service logs |
| `make logs-spark` | Follow Spark streaming job |
| `make logs-producer` | Follow producer output |
| `make logs-quality` | Follow quality check output |
| `make logs-reconciler` | Follow reconciler output |

### Queries
| Target | Description |
|--------|-------------|
| `make query-smoke` | Row counts for both tables |
| `make query-crossjoin` | Sample cross-layer join (telemetry + blob) |
| `make query-linkage` | Linkage rate percentage |
| `make quality-once` | Run a single quality check |

### Advanced
| Target | Description |
|--------|-------------|
| `make backfill` | Generate 24h of historical data (via Kafka) |
| `make backfill-small` | Quick backfill: 1h, 10 robots, 50 evt/s |
| `make dbt-run` | Build Silver/Gold dbt models |
| `make dbt-test` | Run dbt data quality tests |
| `make nessie-demo` | Run Nessie branching workflow demo |
| `make nessie-health` | Check Nessie catalog API |

---

## Demo Walkthrough

### Basic: Data flowing end-to-end

```bash
make up                    # Start everything
sleep 120                  # Wait for Spark + tables
make query-smoke           # Should show increasing row counts
make logs-quality          # Watch quality checks (OK/WARNING/CRITICAL)
make logs-reconciler       # Watch linkage reports (linkage_pct)
```

### Backfill: Historical data

```bash
make backfill-small        # Generate 1h of history for 10 robots
make query-smoke           # Row counts jump
```

### dbt: Medallion layers

```bash
make dbt-run               # Create Silver/Gold views
# Then in Trino:
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.silver.telemetry_validated LIMIT 5"
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.gold.robot_hourly_stats LIMIT 10"
```

### Nessie: Git-like schema evolution

```bash
make nessie-demo           # Creates branch, adds column, shows audit log
```

---

## Key Design Decisions Demonstrated

1. **Two-layer storage** — structured (Iceberg+Parquet) + unstructured (S3 blobs) with `blob_index` bridge
2. **Per-robot drift correction** — heartbeat-driven offset map, not a global constant
3. **Eventual consistency** — explicit state machine (PENDING → LINKED → ORPHANED) instead of 2PC
4. **Hidden partitioning** — `bucket(128, robot_id)` + `hours(corrected_ts)` for auto-pruning
5. **Validation at source** — schema + domain checks before Kafka, not after
6. **Nessie catalog** — git-like branching for safe schema migrations
7. **Medallion architecture** — Bronze (raw) → Silver (validated) → Gold (aggregated) via dbt

---

## Known Limitations (Demo vs Production)

This is a **demonstration stack** running on a single machine. Here's what's different from production and why:

### Throughput

| Metric | This demo | Production target |
|--------|-----------|-------------------|
| Telemetry rate | ~20 events/sec | 50K-500K events/sec |
| Blob throughput | ~1 MB/sec | 500 MB - 5 GB/sec |
| Concurrent queries | 1-5 | 20-200 |
| Robot count | 200 (simulated sequentially) | 200+ (parallel) |

The demo produces events sequentially with a 50ms sleep between events. Production would have 200 parallel producers. The architecture (Kafka partitioning, Spark parallelism) is designed for the production scale; the demo just doesn't generate that volume.

### Single Points of Failure

| Component | Demo | Production |
|-----------|------|------------|
| Kafka | 1 broker, replication=1 | 3+ brokers, replication=3 |
| MinIO | 1 node | Multi-node erasure coding or managed S3 |
| Nessie | In-memory (lost on restart) | Persistent backend (JDBC/DynamoDB) |
| Trino | Coordinator only | Coordinator + N workers |
| Spark | Single executor | Cluster with auto-scaling |

**Nessie is the biggest gap**: in-memory mode means all Iceberg metadata is lost if the container restarts. Production uses a persistent backend.

### Missing Production Components

| Feature | Demo status | Production approach |
|---------|-------------|---------------------|
| Schema registry | Not included | Apicurio or Confluent Schema Registry |
| Compaction scheduler | Not automated | Event-driven (file count threshold) |
| Monitoring/alerting | JSON logs only | Prometheus + Grafana |
| Access control | None | Trino RBAC + column-level security |
| Orchestration | Manual make targets | Dagster or Airflow |
| Chaos testing | Not included | Monthly automated failure injection |
| Cost attribution | Not applicable | Per-team dataset cost tracking |

### Spark Package Downloads

The Spark job uses the official Apache Spark Docker image (`spark:3.5.8`) and downloads Maven packages on first start via `--packages`. This takes 2-5 minutes on first run but is cached in a Docker volume (`spark_ivy_cache`) for subsequent starts. In production, packages would be baked into a custom Docker image.

### dbt Models are Views

dbt models are materialized as **views**, not physical Iceberg tables. This is intentional for the demo (no data duplication, instant creation). In production, Gold models would be materialized as Iceberg tables with scheduled refreshes for query performance.

### Clock Drift Simulation

The producer simulates drift as a **per-robot constant** (±500ms) + ±20ms jitter. Real drift is:
- **Per-sensor** (camera vs joint vs LIDAR have different clocks)
- **Time-varying** (drift accumulates between NTP syncs)
- **Temperature-dependent** (crystal oscillator frequency varies with heat)

The demo demonstrates the correction mechanism (heartbeat → offset map → UDF) but simplifies the drift model.

---

## File Structure

```
robot2/
├── docker-compose.yml          # All services (core + profiles for on-demand)
├── Makefile                    # Convenience targets
├── README.md                   # This file
│
├── pipeline/                   # All Python pipeline code
│   ├── Dockerfile              # Pre-built image for pipeline services
│   ├── requirements.txt        # kafka-python-ng, boto3, trino, requests
│   ├── producer.py             # Robot fleet simulator (200 robots, runs forever)
│   ├── spark_stream.py         # Kafka → Iceberg streaming (runs forever)
│   ├── reconciler.py           # Cross-layer linkage checker (runs every 2 min)
│   ├── quality_check.py        # SLA enforcement (runs every 60s)
│   ├── validate.py             # Schema + domain validation module
│   ├── backfill.py             # Historical data generator (on-demand)
│   ├── peak_simulator.py       # 10x traffic spike simulator (on-demand)
│   └── nessie_demo.py          # Nessie branching workflow demo (on-demand)
│
├── dbt_project/                # Silver/Gold medallion transforms
│   ├── Dockerfile              # dbt-trino image
│   ├── dbt_project.yml
│   ├── profiles.yml            # Trino connection
│   └── models/
│       ├── schema.yml          # Source definitions + column tests
│       ├── silver/             # Validated + linked views
│       └── gold/               # Aggregated views
│
├── trino/catalog/
│   └── iceberg.properties      # Trino ↔ Nessie ↔ MinIO config
├── sql/
│   └── trino_queries.sql       # Reference SQL queries
│
└── docs/                       
    ├── CODE_WALKTHROUGH.md     # Step-by-step demo presentation guide
```
