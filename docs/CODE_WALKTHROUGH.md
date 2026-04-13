# Code Walkthrough

Each section tells you **what to see**, **what's interesting**, and **what questions to expect**.

---

## How This Demo Maps to Assignment Deliverables


| Deliverable                        | Where it's demonstrated                                                 | Act           |
| ---------------------------------- | ----------------------------------------------------------------------- | ------------- |
| **D1: Storage Layer Architecture** | Two Iceberg tables + MinIO blobs + blob_index bridge                    | Acts 3, 4, 5  |
| **D2: Table Format Comparison**    | Iceberg features live: hidden partitioning, snapshots, schema evolution | Acts 5, 8     |
| **D3: Consistency Model**          | Reconciler linkage reports + quality check + peak degradation           | Acts 6, 7, 10 |
| **D4: Timestamp Reconciliation**   | Drift correction UDF + heartbeat listener + proof query                 | Acts 3, 4, 5  |
| **Bonus: Attack Responses**        | Monotonic event_id (no collisions), validation                          | Acts 3, 11    |


---

## System Diagram: The Full Pipeline

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │                    200 ROBOTS (producer.py)                          │
 │  Each robot has: stable drift ±500ms + jitter ±20ms                 │
 │  Generates: telemetry JSON + blob binary + heartbeat                │
 └──────┬──────────────────┬─────────────────────┬─────────────────────┘
        │ ①                │ ②                   │ ③
        ▼                  ▼                     ▼
 ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐
 │telemetry.raw│   │blob.manifest │   │ robot.heartbeat   │
 │  (Kafka)    │   │  (Kafka)     │   │   (Kafka)         │
 └──────┬──────┘   └──────┬───────┘   └────────┬──────────┘
        │ ④               │ ⑤                   │ ⑥
        ▼                 ▼                     ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │                SPARK STRUCTURED STREAMING (spark_stream.py)          │
 │  ⑥ Heartbeat thread → per-robot drift map                          │
 │  ④⑤ corrected_ts = edge_ts - drift_offset[robot_id]                │
 │  Writes ACID micro-batches every ~10 seconds                        │
 └──────┬──────────────────┬────────────────────────────────────────────┘
        │ ⑦               │ ⑧
        ▼                 ▼
 ┌──────────────────┐  ┌───────────────────┐   ┌──────────────────┐
 │ telemetry_events │  │   blob_index      │   │   raw-blobs/     │
 │   (Iceberg)      │  │   (Iceberg)       │   │   (MinIO/S3)     │
 │ hours(ts) +      │  │ hours(ts) +       │   │ JPEG, PCD, WAV   │
 │ bucket(128,rid)  │  │ media + bucket(64)│   │ uploaded at ②    │
 └────────┬─────────┘  └────────┬──────────┘   └──────────────────┘
          │ ⑨ event_id join     │
          ▼                     ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   RECONCILER (every 2 min) → PENDING→LINKED→ORPHANED               │
 └──────────┬───────────────────────────────────────────────────────────┘
            │ ⑩
 ┌──────────▼───────────────────────────────────────────────────────────┐
 │   QUALITY CHECK (every 60s) → Freshness / Completeness / Accuracy   │
 └──────────┬───────────────────────────────────────────────────────────┘
            │ ⑪
            ▼
        TRINO → Researchers (SQL, cross-layer joins, partition pruning)
```

---

## Act 1: "The Problem"

> **Deliverable context:** Sets up why D1-D4 exist.

### Problem Statement

> "We have 200+ robots across 50 field sites. Each robot produces two kinds of data simultaneously: structured telemetry at up to 1kHz (joint angles, torques, temperatures) and unstructured blobs (camera frames, LIDAR point clouds, audio). The total fleet generates 15 TB/day sustained, with 10x bursts during multi-robot demos.
>
> The core challenge: a researcher asks 'Show me the camera frame and joint state at the exact moment robot-042's arm failed.' This requires joining data across two completely different storage systems, where every timestamp is wrong by up to 500 milliseconds."

Load profile:

- Normal: 50K msg/sec structured, 500 MB/sec unstructured
- Peak: 500K msg/sec, 5 GB/sec

50 MB/s is peak burst per robot during experiments, size the buffer for 5 GB/s bursts but plan storage costs for 15 TB/day."

---

## Act 2: "Start the Stack"

> **Deliverable context:** D1 (Storage Layer Architecture) the physical stack.

### Commands

```bash
make up          # builds images + starts all 9 services
make ps          # See what's running
```

### What to see

walk through to each service:

```
kafka            "The buffer layer — absorbs 500K msg/sec bursts. KRaft mode, no Zookeeper."
kafka-ui         "Visual inspection — open http://localhost:8081"
minio            "S3-compatible blob storage — open http://localhost:9001"
nessie           "Git-like catalog for Iceberg — open http://localhost:19120/api/v2/config"
trino            "SQL query engine — open http://localhost:8080"
spark-job        "Streaming consumer — reads Kafka, corrects drift, writes Iceberg"
producer         "Fleet simulator — 200 robots producing events"
quality-check    "SLA monitor — checks freshness, completeness, accuracy every 60s"
reconciler       "Cross-layer linker — matches telemetry ↔ blobs every 2 min"
```

Notes:
producer.py runs an infinite while True loop with a 50ms sleep between events. It produces data forever until you stop it.

Same for the consumers:
- spark-job : streams continuously (.awaitTermination())
- reconciler : runs every 2 minutes in a loop
- quality-check : runs every 60 seconds in a loop

To stop everything: make down
To stop just the producer (keep querying existing data): docker compose stop producer

### What's interesting

> Everything runs locally in Docker. In production, Kafka would be a 3-broker cluster, MinIO would be replaced by S3(or equivalent), and Nessie would have a persistent backend. The architecture is identical but the infrastructure scaling changes

### The docker-compose

Open `docker-compose.yml` briefly:

- **Kafka KRaft mode** (line ~10): No Zookeeper. KRaft embeds consensus in Kafka itself. One fewer service to manage
- **24 partitions** (line ~23): Up to 24 parallel consumers. 200 robots hash across these
- **72h retention** (line ~22): If something breaks Friday evening, we can replay Monday morning ;)
- **Health checks with dependencies**: Services wait for their dependencies

---

## Act 3: The Producer: How Robots Generate Data

> **Deliverable context:** D1 (cross-reference mechanism, event_id design), D4 (drift simulation).

### Commands

```bash
make logs-producer    # watch events being produced
```
> it only prints output when a record is rejected by validation (which is rare since the simulated data is within bounds)
> to confirm it's working, check Kafka UI → telemetry.raw topic → message count growing (http://localhost:8081)

### What can you see and inspect!

Open `pipeline/producer.py`:

**1. Drift simulation (line 51)**

```python
return {rid: random.randint(-500, 500) for rid in robot_ids}
```

> "Each robot gets a stable clock drift for the session just like a real device with an unsynchronized internal clock. Robot-042 might be +347ms fast, robot-099 might be -182ms slow. This drift persists across all events in a session."

**2. Jitter on top of drift (line 84)**

```python
edge_ts_ms = now_ms + drift_ms + random.randint(-20, 20)
```

> "On top of the stable drift, there's ±20ms of jitter, noise from the NTP sync process. This is the residual error that remains AFTER drift correction. This is why our matching tolerance is 300ms, not 500ms: the correction removes the stable component, leaving only the 20ms jitter."

**3. Monotonic event_id (line 88)**

```python
event_id = f"{robot_id}:{session_id}:{heartbeat_counter:012d}"
```

> "The event_id is the glue between the two storage layers. It's generated at the edge before the data splits into Kafka (telemetry) and MinIO (blob). Old format used random 5 digits 864 collisions per robot per day (birthday paradox). Monotonic counter: zero collisions by construction

**4. Validation before send (line 113)**

```python
valid, errors = validate_telemetry(telemetry)
if valid:
    producer.send(TELEMETRY_TOPIC, telemetry)
```

> Every record is validated before it hits Kafka. Schema checks (required fields, types) and domain checks (temperature -40 to 150°C, torque 0-300 Nm). Bad data is rejected at the source, not discovered downstream

**5. Heartbeat publishing (line 148)**

```python
producer.send(HEARTBEAT_TOPIC, {
    "robot_id": rid,
    "estimated_drift_ms": drift_map[rid],
})
```

> Every 50 events, each robot publishes its drift estimate. This is how the streaming job knows how much to correct each robot's timestamps. In production, this comes from NTP sync logs

### What can uou see in Kafka UI

Open **[http://localhost:8081](http://localhost:8081)** → Topics:

- `telemetry.raw` — click into messages, See the JSON structure with `event_id`, `event_ts_edge_ms`, sensor readings
- `blob.manifest` — See `blob_uri`, `checksum_sha256`, `quality_state: INGESTED`
- `robot.heartbeat` — See `estimated_drift_ms` values (positive = clock is fast, negative = slow)

> Three topics, three different data paths. Telemetry is small and fast (structured). Blob manifests are metadata about uploads (the actual blobs go directly to MinIO). Heartbeats are control plane data that drives drift correction

### What to see in MinIO

Open **[http://localhost:9001](http://localhost:9001)** (minioadmin/minioadmin):

- Browse `raw-blobs` bucket
- See the path structure: `blob/{media_type}/event_date=.../event_hour=.../robot_shard=XX/robot_id=.../event_id=...`

> Notice the `robot_shard` directory. This is `robot_id_number % 16`, without it, all 200 robots writing to the same date/hour prefix would hit S3's per-prefix throughput limit (3,500 PUT/sec). With 16 shards, each shard handles ~312 MB/sec

---

## Act 4: The Streaming Job: Where Drift Correction Happens

> **Deliverable context:** D4 (Timestamp Reconciliation), D1 (partitioning strategy, buffer design).

### Commands

```bash
make logs-spark    # watch micro-batch commits
```

### What to see

Open `pipeline/spark_stream.py`

**1. The heartbeat listener thread (line 44)**

```python
def _heartbeat_listener() -> None:
    consumer = KafkaConsumer("robot.heartbeat", ...)
    for msg in consumer:
        _drift_offsets[hb["robot_id"]] = int(hb.get("estimated_drift_ms", 0))
```

> This is a background thread that continuously reads heartbeats and maintains a per robot offset map in memory. When robot-042 publishes drift=+347ms, this map updates. The UDF reads from this map for every event

**2. The drift correction UDF (line 106)**

```python
drift_udf = udf(lambda rid, ts: int(ts - _get_drift(rid)) if ts else None, LongType())
```

> For every event: `corrected_ts = edge_ts - drift_offset[robot_id]`. Robot-042's events arrive with timestamps 347ms too fast. We subtract 347ms. Now the timestamp is real world time. This is the timestamp that goes into Iceberg's partition key

**3. Table creation DDL (line 112)**

```sql
PARTITIONED BY (hours(event_ts_corrected), bucket(128, robot_id))
```

> Two partition dimensions: time (hourly) and robot (128 hash buckets). A query for one robot's last hour reads 1 partition out of 128 × 24 = 3,072 per day. That's 99.97% pruning, the query engine skips everything else.
>
> Why 128 buckets and not 200 (one per robot)? Because robot count will change. Bucket count is fixed at table creation. 128 is a power of 2 for clean hash distribution

**4. The two parallel sinks (line 228-244)**

> Telemetry and blob manifests are written to separate Iceberg tables by separate streaming queries. They're independent, if blob processing is slow, telemetry keeps going. This is the architectural decision behind eventual consistency

## Act 5: Query the Data - What Researchers See

> **Deliverable context:** D1 (cross-layer joins), D2 (Iceberg features: hidden partitioning, time travel), D4 (drift proof).

### Commands

```bash
make query-smoke         # row counts
make query-crossjoin     # cross-layer join
make query-linkage       # linkage percentage
```

### What to see in Trino

Open **[http://localhost:8080](http://localhost:8080)** (Trino UI).
The Trino UI is usefulto show that queries are running and how Iceberg partition pruning reduces the amount of data scanned

Run queries directly:

**1. Basic row counts**
```
docker compose exec trino trino
```

```sql
SELECT count(*) FROM iceberg.prod.telemetry_events;
SELECT count(*) FROM iceberg.prod.blob_index;
```

> Both tables are growing. The producer is continuously inserting, and Spark is committing micro-batches every few seconds

**2. The cross-layer join — this is the money shot**

```sql
SELECT
  t.robot_id,
  t.event_id,
  t.sensor_type,
  t.joint_angle,
  t.temperature,
  t.event_ts_corrected,
  b.media_type,
  b.blob_uri,
  b.checksum_sha256
FROM iceberg.prod.telemetry_events t
INNER JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id
ORDER BY t.event_ts_corrected DESC
LIMIT 10;
```

> This is the query that answers 'Show me the camera frame for this joint reading.' The `event_id` join key connects the two layers. Every row has both the sensor data AND the blob URI — a researcher can click that URI to download the actual camera frame

**3. Drift correction in action**

```sql
SELECT
  robot_id,
  event_ts_edge_ms,
  event_ts_corrected,
  CAST(event_ts_edge_ms AS DOUBLE) / 1000 - to_unixtime(event_ts_corrected) AS drift_applied_sec
FROM iceberg.prod.telemetry_events
WHERE robot_id = 'robot-042'
ORDER BY event_ts_corrected DESC
LIMIT 5;
```

> The `drift_applied_sec` column Sees how much correction was applied. If robot-042 has +300ms drift, you'll see ~0.3 seconds difference. This is the heartbeat-based correction in action

**4. Partition pruning demo**

```sql
-- This query reads ALL partitions (slow at scale)
SELECT count(*) FROM iceberg.prod.telemetry_events;

-- This query prunes to one hour + one robot bucket (fast)
SELECT count(*) FROM iceberg.prod.telemetry_events
WHERE event_ts_corrected > current_timestamp - INTERVAL '1' HOUR
  AND robot_id = 'robot-042';
```

> Both queries return a count, but the second one is dramatically faster at scale. Iceberg's hidden partitioning automatically prunes to the correct hour and bucket

**5. Iceberg time travel**

```sql
-- See the commit history
SELECT * FROM iceberg.prod."telemetry_events$snapshots"
ORDER BY committed_at DESC LIMIT 5;
```

> Every micro-batch creates a new snapshot. You can query data as it existed at any past snapshot. This is how ML reproducibility works: 'train on the exact dataset from snapshot X'

---

## Act 6: The Reconciler - Cross-Layer Consistency

> **Deliverable context:** D3 (Consistency Model) — this is the consistency model running live.

### Commands

```bash
make logs-reconciler    # watch linkage reports
```

### What to see in deep dive

Open `pipeline/reconciler.py` and point to the key queries:

> The reconciler runs every 2 minutes. It joins telemetry and blob_index on event_id and reports: how many are linked, how many are orphaned, what's the linkage percentage

### What to look for in the logs

```json
{
  "telemetry_total": 45000,
  "blob_total": 44800,
  "linked_count": 44500,
  "linkage_pct": 98.89,
  "status": "HEALTHY"
}
```

> 98.89% linkage means 98.89% of telemetry events have a matching blob. The remaining 1.11% are either still PENDING (blob hasn't committed yet) or truly orphaned. This is the eventual consistency model in action — not 100% instant, but observable and self-healing

### The state machine

```
 telemetry arrives → PENDING_BLOB_LINK
 blob arrives     → INGESTED
 reconciler joins → both become LINKED
 10 min timeout   → ORPHANED (flagged for investigation)
```

### The thought process

> The original pain point from the handout was 'queries silently returned incomplete results with no alert.' Our state machine fixes this: every row has an explicit status (PENDING_BLOB_LINK, LINKED, ORPHANED). A researcher who queries during the consistency window sees NULLs for the blob columns, but the `ingest_status` column tells them WHY. That's strictly better than silent failure

---

## Act 7: Quality Checks — SLA Enforcement

> **Deliverable context:** D3 (SLO definitions, freshness/completeness/accuracy).

### Commands

```bash
make logs-quality    # watch quality reports
```

### What to look for

```json
{
  "overall_severity": "OK",
  "checks": {
    "freshness": {"severity": "OK", "value_sec": 12},
    "completeness_volume": {"severity": "OK", "events_last_5m": 5200},
    "completeness_linkage": {"severity": "OK", "linkage_pct": 98.5},
    "accuracy": {"severity": "OK", "bad_temp_rows": 0}
  }
}
```

### What to See

Open `pipeline/quality_check.py` and point to:

- **Three dimensions**: freshness, completeness, accuracy
- **Freshness** = `now() - max(ingest_ts)` — how stale is the newest data?
- **Completeness** = event volume + cross-layer linkage
- **Accuracy** = domain validation (temperature range, torque bounds)
- **Severity escalation**: OK → WARNING → CRITICAL

> Each check has defined thresholds. Freshness > 60s = WARNING, > 120s = CRITICAL. This is SLO-driven engineering: we define what 'healthy' means numerically

---

## Act 8: Nessie — Git-Like Data Versioning

> **Deliverable context:** D2 (Table Format Comparison — Nessie as the catalog differentiator).

### Commands

```bash
make nessie-demo    # runs the branching workflow
```

### What it does

> The demo creates a branch called `feature/add-battery-voltage`, adds a column to `telemetry_events` on that branch, verifies the change, Sees the Nessie commit log, and cleans up. In production, this is how you'd safely test schema changes: branch, alter, test, merge. just like a Git feature branch for code

### What to see

Open **[http://localhost:19120/api/v2/config](http://localhost:19120/api/v2/config)** in browser:

> Nessie is running and healthy. It tracks every Iceberg table change like Git tracks code changes

It returns JSON, that is the output. It Sees Nessie's server config: default branch, version, supported API features. This confirms Nessie is running. There's no web UI; Nessie is a REST API catalog, not a dashboard. The value is in what it enables: git-like branching, audit logging, atomic multi-table commits.

To see more useful info, try:

```bash
# List all branches/tags
curl -s http://localhost:19120/api/v1/trees | python3 -m json.tool

# See commit log on main
curl -s http://localhost:19120/api/v1/trees/tree/main/log | python3 -m json.tool
```

### Why this matters

> Nessie is a governance tool. Every DDL change is audited. You can tag datasets for ML reproducibility ('training-data-v3'). You can branch for safe schema migrations

---

## Act 9: dbt — Medallion Architecture

> **Deliverable context:** D1 (Bronze/Silver/Gold layers described in architecture).

### Commands

```bash
make dbt-run    # create Silver/Gold views
make dbt-test   # run data quality tests
```

### What to See after dbt-run

```sql
-- Silver: validated telemetry (bad readings filtered out)
SELECT * FROM iceberg.silver.telemetry_validated LIMIT 5;

-- Silver: linked blobs with latency measurement
SELECT * FROM iceberg.silver.blob_linked LIMIT 5;

-- Gold: per-robot hourly stats (pre-aggregated)
SELECT * FROM iceberg.gold.robot_hourly_stats LIMIT 10;

-- Gold: cross-layer health report
SELECT * FROM iceberg.gold.linkage_report;
```

### Medallion layer diagram

```
 BRONZE (raw)                SILVER (validated)           GOLD (aggregated)
 ────────────                ──────────────────           ──────────────────
 telemetry_events            telemetry_validated          robot_hourly_stats
 • All rows, unfiltered      • Domain rules applied       • Per-robot per-hour
 • Raw edge timestamps       • quality_flag column        • avg/max torque, temp

 blob_index                  blob_linked                  linkage_report
 • All states                • LINKED only + latency      • Hourly linkage %
```

### The thought process

> "Bronze is raw data: exactly as ingested. Silver is validated and linked, domain rules applied, cross layer joins materialized. Gold is aggregated andready for dashboards
>
> dbt models are defined in SQL, version-controlled, testable, and documented. The `schema.yml` file defines column-level tests that run automatically."

### What's interesting about the dbt models

Open `dbt_project/models/silver/telemetry_validated.sql`:

```sql
CASE
    WHEN temperature < -40 OR temperature > 150 THEN 'TEMP_OUT_OF_RANGE'
    WHEN torque < 0 OR torque > 300 THEN 'TORQUE_OUT_OF_RANGE'
    ELSE 'VALID'
END AS quality_flag
```

> Every row gets a quality flag. Gold layer aggregations only include VALID rows. This means dashboards never See physically impossible values, but the raw Bronze data is preserved for investigation

---

## Act 10: Peak Simulation — What Happens Under 10x Load

> **Deliverable context:** D3 (At-Peak Behavior: hard vs soft guarantees, spike timeline, self-healing).

### Setup: Before the spike

Take a baseline reading:

```bash
make query-smoke        # note current row counts
make query-linkage      # note linkage percentage (should be ~98%+)
```

Open three terminal tabs:

```bash
# Tab 1: the spike itself
# (will run in a moment)

# Tab 2: watch quality degrade in real-time
make logs-quality

# Tab 3: watch linkage drop
make logs-reconciler
```

Optionally open **Kafka UI** ([http://localhost:8081](http://localhost:8081)) → Consumer Groups to watch lag.

### The spike

```bash
# Tab 1: fire the spike (2 minutes, telemetry only, no blobs)
make peak

# For a shorter demo:
make peak-short    # 30 seconds
```

**T+0s — Spike starts:**

> flooding Kafka with thousands of events per second, telemetry only, no blob uploads. This simulates a multi-robot demo where 200 robots stream sensor data simultaneously but blob processing falls behind

**T+10s — Watch Tab 1 (peak simulator):**

```
[peak]     25,000 events | 4,200 evt/s | 110s remaining
```

> We're sending ~4,000 events/sec — roughly 200x our normal rate. Kafka absorbs this trivially. The question is: can the downstream pipeline keep up?

**T+30s — Watch Tab 2 (quality check):**

```json
{"freshness": {"severity": "WARNING", "value_sec": 45}}
```

> Freshness just degraded. The quality check reports WARNING because Spark can't write to Iceberg as fast as events are arriving, this is expected

**T+60s — Watch Tab 3 (reconciler):**

```json
{"linkage_pct": 72.3, "status": "DEGRADED"}
```

> Linkage dropped to 72% because we're sending telemetry WITHOUT blobs. The reconciler correctly reports DEGRADED. But no data is lost. Every event is in Kafka. Spark will eventually write all of them

**T+2min — Spike ends. Watch recovery:**

```bash
make query-smoke        # row counts jump dramatically
make query-linkage      # linkage still low (blobs catching up)
```

**T+5min — Lag drains:**

> Spark has consumed the Kafka backlog. Freshness returns to normal. The quality check flips back to OK

**T+10min — Linkage recovers:**

> The normal producer has been sending blobs this whole time. The reconciler is gradually linking the orphaned telemetry events. Linkage climbs back toward 95%+

### What changes during the spike (See this diagram)

```
 NORMAL                              PEAK (make peak)
 ──────                              ────
 ~20 evt/s telemetry                 ~4,000 evt/s telemetry
 ~20 evt/s blob manifests            0 blob manifests (deliberate)
 Freshness: ~10s                     Freshness: 30-120s (warning!)
 Linkage: ~98%                       Linkage: drops to 60-80% (warning!)

 HARD (never break):                 SOFT (degrade temporarily):
  Zero data loss                    (warning!) Freshness
  ACID snapshots                    (warning!) Linkage rate
  72h replayability                 (warning!) File size

 RECOVERY (automatic, ~5-10 min after spike ends):
 1. Spark drains Kafka lag → freshness recovers
 2. Normal producer resumes blobs → linkage recovers
 3. Quality check flips back to OK
```

### The key message (D3 Consistency Model)

> This is the spike timeline from my architecture document. At no point did we lose data, serve incorrect results, or have an inconsistent snapshot. What degraded was freshness (from seconds to minutes) and linkage rate (telemetry outpaced blobs). Both are temporary and self-healing
>
> The hard guarantees held: zero data loss (Kafka acks=all + 72h retention), ACID on snapshots (Iceberg snapshot isolation), and replayability. The soft guarantees degraded temporarily and observably, which is the entire point of the explicit state machine

### Questions you may have

**Q: "What if the spike lasts longer than 72 hours?"**

> Messages start expiring from Kafka. That's permanent data loss. This is the CRITICAL alert threshold, when lag approaches the retention boundary, humans get paged. It hasn't happened here because our spike was 2 minutes, not 72 hours

**Q: "How do you know the system self-healed?"**

> The quality check measures it automatically. When freshness drops below the WARNING threshold and linkage exceeds 95%, the system reports HEALTHY again. No human intervention needed

**Q: "Why not scale consumers automatically?"**

> In production we could auto-scale from 6 to 24 Kafka consumers when lag exceeds 100K messages. The demo runs a single Spark executor, so we can't demonstrate horizontal scaling, but the Kafka partitioning (24 partitions) is designed for it

---

## Act 10b: Small-File Problem — Why Streaming Creates Tiny Files and How to Fix It

> **Deliverable context:** D1 (Small-File Handling deep-dive, compaction strategy).
>
> This pairs naturally with Act 10 (Peak Simulation). The spike just flooded the system with events — now look at what that did to the file layout.

### Why this matters

Streaming systems commit data in micro-batches. Each micro-batch writes one file per partition. The math:

```
 Demo rate: ~20 events/sec
 Micro-batch: every ~10 seconds = 200 events per batch
 Partitions: hours(ts) × bucket(128, robot_id) = many partitions
 Result: each file contains only a few events = tiny files (KB, not MB)

 At production scale (50K events/sec):
   50,000 × 10s ÷ 128 partitions = 3,906 events per file = ~781 KB
   Per hour: 360 batches × 128 partitions = 46,080 tiny files

 After a peak spike (500K events/sec):
   Even worse — 10x more tiny files in the same time window
```

### The problem: the small-file death spiral

```
 More commits → More small files → More metadata
       │                                │
       │                                ▼
       │                        Slower query planning
       │                                │
       │                                ▼
       │                        More S3 LIST/GET calls
       │                                │
       │                                ▼
       └────────────── Higher cost + slower queries ─┘
```

> "Each file has metadata overhead that Iceberg must track. Query planning scans all file metadata before reading any data. 46K files means 10-30 seconds just to *plan* a query."

### Demo: see the problem live

```bash
# Step 1: Check current file health
make compact-report
```

Expected output:

```
  File health: iceberg.prod.telemetry_events
  ============================================================
  Snapshots:     150
  Data files:    450
  Total size:    12.3 MB
  Avg file size: 27.3 KB       ← TINY (target: 256 MB)
  Min file size: 2.1 KB
  Total records: 85,000

  SMALL FILES: avg 27.3 KB (target: 256 MB)
  → Run compaction to merge 450 files into fewer, larger files.
```

> There are 450 files averaging 27 KB each. In production at 50K events/sec, this would be 46,000+ files per hour. Query planning would collapse

### Demo: fix it with compaction

```bash
# Step 2: Run compaction (merges small files + expires old snapshots)
make compact
```

What it does (three operations):

```
 BEFORE                          AFTER
 ──────                          ─────
 450 files × 27 KB avg     →    ~5 files × 2.5 MB avg
 150 snapshots              →    fewer snapshots (old ones expired)
 Query plan: scan 450 entries →  scan 5 entries (90x faster planning)
```

The script shows before/after comparison automatically.

### In the code

Open `pipeline/compaction.py` and point to:

**1. The optimize command (line ~138)**

```sql
ALTER TABLE iceberg.prod.telemetry_events
EXECUTE optimize(file_size_threshold => '100MB')
```

> This is Trino's equivalent of Iceberg's `rewrite_data_files()`. It reads all files smaller than 100 MB, merges them into larger files targeting 256 MB, and atomically replaces the old files in Iceberg's metadata

**2. Snapshot expiry (line ~151)**

```sql
ALTER TABLE iceberg.prod.telemetry_events
EXECUTE expire_snapshots(retention_threshold => '7d')
```

> Every micro-batch creates a snapshot. After a day of streaming: 8,640 snapshots. Each snapshot tracks which files are current. Old snapshots enable time travel but consume metadata. We keep 7 days for time travel, expire the rest

**3. Orphan file removal (line ~164)**

```sql
ALTER TABLE iceberg.prod.telemetry_events
EXECUTE remove_orphan_files(retention_threshold => '7d')
```

> After compaction, the old small files are replaced but physically still on S3. Orphan removal deletes them. This reclaims storage

### Production compaction schedule

```
 Tier           Frequency       Strategy                     Why
 ────           ─────────       ────────                     ───
 Hot (0-7d)     Every 15 min    Rewrite files <64 MB         Keep query planning fast
                                Target 256 MB                for active researchers
                                Cap: 2 GB rewritten/run

 Warm (8-90d)   Weekly          Full rewrite with sort-order  Co-locate data by
                (off-peak)      optimization (robot_id, ts)   (robot, time) for
                                                              sequential read patterns

 During spike   SUSPENDED       Accept small files            Don't steal CPU from
                                Compact after spike ends      struggling consumers
```

> The key trade-off: small files degrade query performance by 3-5x temporarily. Data loss is permanent. During spikes, durability always wins over file health. We compact after the spike ends

### Connect to peak simulation

If you just ran `make peak` (Act 10), the spike created many small files:

```bash
# See the damage
make compact-report

# Fix it
make compact

# Verify the fix
make compact-report
```

> This is the full cycle: spike creates small files → compaction repairs them → query performance returns to normal. The architecture doc describes this as 'T+60min: Compaction merges the small files from the spike into 256 MB files.'

---

## Act 11: Backfill — Late-Arriving Data

> **Deliverable context:** D1 (Late-Arriving Data handling), D3 (accept-and-compact strategy).

### Commands

```bash
make backfill-small   # 1h, 10 robots, 50 evt/s
```

### What to See

Watch the progress output:

```
[backfill]      5,000 / 180,000 (2.8%) | 1200 evt/s | rejected: 0
[backfill]     10,000 / 180,000 (5.6%) | 1180 evt/s | rejected: 0
```

Then query:

```bash
make query-smoke   # row counts should jump
```

### The thought process

> "Backfill pushes historical data through the same Kafka→Spark→Iceberg path as live data. Events with timestamps from 1 hour ago land in OLD Iceberg partitions, partitions that were already committed. Iceberg handles this gracefully: ACID writes to any partition at any time. This is why we chose accept-and-compact over watermark-drop. In robotics, every reading matters for incident investigation. A dropped reading could be exactly the one that explains why robot-042's arm failed."

### What to verify

```sql
-- After backfill, check that old partitions have data
SELECT date_trunc('hour', event_ts_corrected) AS hour_bucket, count(*)
FROM iceberg.prod.telemetry_events
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;
```

> "You'll see row counts in past hour buckets — that's the backfilled data, successfully written to old partitions."

---

## Act 12: The Validation Module

> **Deliverable context:** D1 (data quality).

### What to See

Open `pipeline/validate.py`:

> Two layers: schema validation (required fields, correct types) and domain validation (physically plausible bounds). Every record — telemetry AND blob manifest — passes through validation before hitting Kafka

### Key design decision

> "Validation happens at the PRODUCER, not at the consumer, because a bad record that enters Kafka is visible to all downstream consumers. Catching it at the source prevents one broken robot from polluting the entire pipeline

### Domain bounds are from real robotics specs

```python
TEMP_MIN_C = -40.0     # industrial robot operating range
TEMP_MAX_C = 150.0
TORQUE_MIN_NM = 0.0    # unidirectional torque sensor
TORQUE_MAX_NM = 300.0
JOINT_ANGLE_MIN = -6.3  # ~2π radians (full rotation)
JOINT_ANGLE_MAX = 6.3
```

---

## FAQ~~

### "Why do producer, reconciler, and quality-check run forever?"

They're **long-running background services**, not one-shot scripts:

```
producer.py      → while True: generate event, send to Kafka, sleep 0.05s
                   WHY: simulates a fleet of robots that never stops producing
                   MONITOR: make logs-producer

spark_stream.py  → query.awaitTermination()  (blocks forever)
                   WHY: Spark Structured Streaming processes events continuously
                   MONITOR: make logs-spark (Sees batch progress every 30s)

reconciler.py    → while True: run reconciliation queries, sleep 120s
                   WHY: cross-layer linking is a periodic maintenance task
                   MONITOR: make logs-reconciler (JSON report every 2 min)

quality_check.py → while True: run SLA checks, sleep 60s
                   WHY: SLA enforcement needs continuous measurement
                   MONITOR: make logs-quality (JSON report every 60s)
```

These mirror production patterns. In production, the producer would be replaced by actual robot firmware. The reconciler and quality check would run as scheduled jobs (Dagster/Airflow) or Kubernetes CronJobs.

### "How is dbt validation different from validate.py and Nessie?"

Three layers of validation at different stages:

```
 Layer          When              What it checks               Why separate
 ─────          ────              ──────────────               ───────────
 validate.py    BEFORE Kafka      Schema + domain bounds       Prevent bad data entering
 (producer)     (at the source)   (required fields, types,     the pipeline at all. One
                                  temp range, torque bounds)   broken robot can't pollute
                                                               everything downstream.

 dbt tests      AFTER Iceberg     Column-level data quality    Catch issues that survive
 (schema.yml)   (on stored data)  (not_null, unique, accepted  producer validation — e.g.
                                  values). Also builds Silver  Spark bugs, schema drift,
                                  views that filter invalid.   multi-source inconsistency.

 Nessie         BEFORE DDL        Schema evolution safety       Prevent a bad ALTER TABLE
 (branching)    (on metadata)     (branch → test → merge).     from breaking production.
                                  Audit trail of all changes.  Rollback by discarding
                                                               the branch.
```

**validate.py** = data-level gate at ingestion time
**dbt** = data-level tests + transforms on stored data
**Nessie** = metadata-level governance for schema changes

They're complementary and not redundant. Each catches a different class of problem.

### "Can I backfill a precise time window?"

Yes. The backfill supports both relative and absolute timestamps:

```bash
# Relative: last 24 hours from now
make backfill-small

# Precise window with exact timestamps (UTC, ISO format)
docker compose --profile backfill run --rm backfill \
  python backfill.py \
  --start 2026-04-11T10:00:00 \
  --end 2026-04-11T11:00:00 \
  --events-per-second 100 \
  --robots 50

# From a specific start time to now
docker compose --profile backfill run --rm backfill \
  python backfill.py \
  --start 2026-04-11T10:00:00 \
  --events-per-second 100
```

Timestamp precision is millisecond-level (the events carry `event_ts_edge_ms`). The backfill generates events spaced at `1/events_per_second` intervals within the window.

### "How do I monitor cross layer join?"

The join works through `event_id` — a unique key generated at the robot BEFORE the data splits:

```
 Robot captures sensor + camera simultaneously
   │
   ├─ event_id = "robot-042:s-123:000038291"
   │
   ├──→ telemetry (JSON) → Kafka → Spark → telemetry_events (Iceberg)
   │    carries: event_id, sensor readings, corrected_ts
   │
   └──→ blob (JPEG) → MinIO, manifest → Kafka → Spark → blob_index (Iceberg)
        carries: event_id, blob_uri, checksum

 Both tables have the SAME event_id → SQL JOIN connects them:

   SELECT t.joint_angle, b.blob_uri
   FROM telemetry_events t JOIN blob_index b ON t.event_id = b.event_id
```

**Monitoring the join health:**

```bash
# Quick linkage percentage
make query-linkage

# Detailed: reconciler reports every 2 minutes
make logs-reconciler

# Manual investigation: find unlinked telemetry
docker compose exec trino trino --execute "
  SELECT robot_id, count(*) AS unlinked
  FROM iceberg.prod.telemetry_events t
  LEFT JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id
  WHERE b.event_id IS NULL
  GROUP BY robot_id ORDER BY unlinked DESC LIMIT 10"

# Find the oldest unlinked event (staleness)
docker compose exec trino trino --execute "
  SELECT min(t.ingest_ts) AS oldest_unlinked,
    cast(to_unixtime(current_timestamp) - to_unixtime(min(t.ingest_ts)) as integer) AS age_sec
  FROM iceberg.prod.telemetry_events t
  LEFT JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id
  WHERE b.event_id IS NULL"
```

---

## Deliverable to Demo Cheat Sheet

**D1 — Storage Layer Architecture:**

- See `make ps` (9 services = the architecture)
- See `producer.py` line 88 (event_id = the cross-reference key)
- Run the cross-layer join query (Act 5.2)
- See MinIO blob path structure (Act 3)
- See `spark_stream.py` DDL (Act 4.3 — partition strategy)

**D2 — Table Format Comparison:**

- Run partition pruning demo (Act 5.4 — hidden partitioning)
- Run time travel query (Act 5.5 — snapshots)
- Run `make nessie-demo` (Act 8 — branching = why Nessie > Hive)
- See `dbt_project/models/` (Act 9 — schema evolution support)

**D3 — Consistency Model:**

- Run `make logs-reconciler` (Act 6 — state machine live)
- Run `make logs-quality` (Act 7 — SLO enforcement)
- Run `make peak` (Act 10 — degradation envelope in action)
- See the linkage recovery after spike (Act 10 — self-healing)

**D4 — Timestamp Reconciliation:**

- See `producer.py` drift simulation (Act 3.1-3.2)
- See `spark_stream.py` heartbeat listener + UDF (Act 4.1-4.2)
- Run drift proof query (Act 5.3)

---

## Quick Reference: Make Targets for Demo


| Target                 | When to use          | Demo act |
| ---------------------- | -------------------- | -------- |
| `make up`              | Start everything     | Act 2    |
| `make ps`              | See running services | Act 2    |
| `make logs-producer`   | See data generation  | Act 3    |
| `make logs-spark`      | See streaming        | Act 4    |
| `make query-smoke`     | Verify data flowing  | Act 5    |
| `make query-crossjoin` | See cross-layer join | Act 5    |
| `make query-linkage`   | See consistency      | Act 6    |
| `make logs-reconciler` | See reconciler       | Act 6    |
| `make logs-quality`    | See SLA checks       | Act 7    |
| `make nessie-demo`     | See branching        | Act 8    |
| `make dbt-run`         | Build Silver/Gold    | Act 9    |
| `make peak`            | Simulate 10x spike   | Act 10   |
| `make peak-short`      | Quick 30s spike      | Act 10   |
| `make backfill-small`  | Historical data      | Act 11   |


## Quick Reference: URLs During Demo


| Service  | URL                                                                          | What to See                                          |
| -------- | ---------------------------------------------------------------------------- | ---------------------------------------------------- |
| Kafka UI | [http://localhost:8081](http://localhost:8081)                               | Topics, messages, consumer groups                    |
| MinIO    | [http://localhost:9001](http://localhost:9001)                               | Blob storage, path structure (minioadmin/minioadmin) |
| Trino    | [http://localhost:8080](http://localhost:8080)                               | SQL queries, execution plans                         |
| Nessie   | [http://localhost:19120/api/v2/config](http://localhost:19120/api/v2/config) | Catalog health, JSON config                          |


## Quick Reference: Key Queries

```sql
-- Row counts
SELECT count(*) FROM iceberg.prod.telemetry_events;
SELECT count(*) FROM iceberg.prod.blob_index;

-- Cross-layer join (THE money query)
SELECT t.robot_id, t.event_id, t.joint_angle, b.media_type, b.blob_uri
FROM iceberg.prod.telemetry_events t
INNER JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id
ORDER BY t.event_ts_corrected DESC LIMIT 10;

-- Drift correction proof
SELECT robot_id, event_ts_edge_ms,
  CAST(event_ts_edge_ms AS DOUBLE)/1000 - to_unixtime(event_ts_corrected) AS drift_sec
FROM iceberg.prod.telemetry_events
WHERE robot_id = 'robot-042' LIMIT 5;

-- Linkage rate
SELECT count(*) AS total, count(b.event_id) AS linked,
  round(cast(count(b.event_id) AS double)/count(*)*100, 2) AS pct
FROM iceberg.prod.telemetry_events t
LEFT JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id;

-- Iceberg snapshots (time travel)
SELECT * FROM iceberg.prod."telemetry_events$snapshots"
ORDER BY committed_at DESC LIMIT 5;

-- Partition metadata
SELECT * FROM iceberg.prod."telemetry_events$partitions" LIMIT 10;
```

