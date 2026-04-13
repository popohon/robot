-- Silver layer: blob index entries that have been successfully linked
-- to telemetry via event_id.  Excludes orphaned and pending blobs.

SELECT
    b.event_id,
    b.robot_id,
    b.session_id,
    b.media_type,
    b.blob_uri,
    b.blob_ts_corrected,
    b.object_size,
    b.checksum_sha256,
    b.quality_state,
    b.ingest_ts AS blob_ingest_ts,
    t.event_ts_corrected AS telemetry_ts,
    t.sensor_type,
    t.ingest_ts AS telemetry_ingest_ts,
    CAST(
        to_unixtime(t.ingest_ts) - to_unixtime(b.ingest_ts)
    AS DOUBLE) AS link_latency_sec
FROM {{ source('bronze', 'blob_index') }} b
INNER JOIN {{ source('bronze', 'telemetry_events') }} t
    ON b.event_id = t.event_id
