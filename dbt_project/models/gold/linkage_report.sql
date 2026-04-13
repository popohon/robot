-- Gold layer: cross-layer linkage health metrics.
-- Tracks the consistency model's effectiveness over time.

WITH telemetry_stats AS (
    SELECT
        date_trunc('hour', ingest_ts) AS hour_bucket,
        count(*) AS total_telemetry,
        count(CASE WHEN ingest_status = 'LINKED' THEN 1 END) AS linked_count,
        count(CASE WHEN ingest_status = 'PENDING_BLOB_LINK' THEN 1 END) AS pending_count,
        count(CASE WHEN ingest_status = 'ORPHANED' THEN 1 END) AS orphaned_count
    FROM {{ source('bronze', 'telemetry_events') }}
    GROUP BY date_trunc('hour', ingest_ts)
),
blob_stats AS (
    SELECT
        date_trunc('hour', ingest_ts) AS hour_bucket,
        count(*) AS total_blobs,
        sum(object_size) AS total_blob_bytes
    FROM {{ source('bronze', 'blob_index') }}
    GROUP BY date_trunc('hour', ingest_ts)
)

SELECT
    t.hour_bucket,
    t.total_telemetry,
    t.linked_count,
    t.pending_count,
    t.orphaned_count,
    COALESCE(b.total_blobs, 0) AS total_blobs,
    COALESCE(b.total_blob_bytes, 0) AS total_blob_bytes,
    ROUND(
        CAST(t.linked_count AS DOUBLE) / NULLIF(t.total_telemetry, 0) * 100, 2
    ) AS linkage_pct,
    ROUND(
        CAST(t.orphaned_count AS DOUBLE) / NULLIF(t.total_telemetry, 0) * 100, 2
    ) AS orphan_pct
FROM telemetry_stats t
LEFT JOIN blob_stats b ON t.hour_bucket = b.hour_bucket
ORDER BY t.hour_bucket DESC
