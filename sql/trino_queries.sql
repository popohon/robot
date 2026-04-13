-- ============================================================
-- Robotics Lakehouse Trino Queries
-- Run with: docker compose exec trino trino --execute "<query>"
-- ============================================================

-- ---------- Catalog / Schema discovery ----------
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.prod;

-- ---------- Row counts ----------
SELECT count(*) AS telemetry_rows FROM iceberg.prod.telemetry_events;
SELECT count(*) AS blob_rows FROM iceberg.prod.blob_index;

-- ---------- Cross-layer join (event_id linkage) ----------
SELECT
  t.robot_id,
  t.event_id,
  t.event_ts_corrected,
  t.ingest_status,
  b.media_type,
  b.blob_uri,
  b.checksum_sha256,
  b.quality_state
FROM iceberg.prod.telemetry_events t
INNER JOIN iceberg.prod.blob_index b
  ON t.event_id = b.event_id
ORDER BY t.ingest_ts DESC
LIMIT 20;

-- ---------- Unlinked telemetry (no blob match) ----------
SELECT count(*) AS unlinked_telemetry
FROM iceberg.prod.telemetry_events t
LEFT JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id
WHERE b.event_id IS NULL;

-- ---------- Orphan blobs (no telemetry match) ----------
SELECT count(*) AS orphan_blobs
FROM iceberg.prod.blob_index b
LEFT JOIN iceberg.prod.telemetry_events t ON b.event_id = t.event_id
WHERE t.event_id IS NULL;

-- ---------- Linkage rate ----------
SELECT
  count(*) AS total_telemetry,
  count(b.event_id) AS linked,
  round(cast(count(b.event_id) AS double) / count(*) * 100, 2) AS linkage_pct
FROM iceberg.prod.telemetry_events t
LEFT JOIN iceberg.prod.blob_index b ON t.event_id = b.event_id;

-- ---------- Per-robot freshness ----------
SELECT
  robot_id,
  max(ingest_ts) AS last_seen,
  cast(to_unixtime(current_timestamp) - to_unixtime(max(ingest_ts)) AS integer) AS stale_sec
FROM iceberg.prod.telemetry_events
GROUP BY robot_id
ORDER BY stale_sec DESC
LIMIT 10;

-- ---------- Per-media-type blob counts ----------
SELECT media_type, count(*) AS cnt
FROM iceberg.prod.blob_index
GROUP BY media_type
ORDER BY cnt DESC;

-- ---------- Data quality: temperature outliers ----------
SELECT count(*) AS bad_temp_rows
FROM iceberg.prod.telemetry_events
WHERE temperature < -40 OR temperature > 150;

-- ---------- Completeness per 5-min window ----------
SELECT count(*) AS events_last_5m
FROM iceberg.prod.telemetry_events
WHERE ingest_ts > current_timestamp - INTERVAL '5' MINUTE;

-- ---------- Iceberg metadata: snapshots ----------
SELECT * FROM iceberg.prod."telemetry_events$snapshots" ORDER BY committed_at DESC LIMIT 5;

-- ---------- Iceberg metadata: partitions ----------
SELECT * FROM iceberg.prod."telemetry_events$partitions" LIMIT 20;
