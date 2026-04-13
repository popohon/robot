"""Cross-layer reconciliation job.

Periodically queries Trino to:
1. Count telemetry rows still in PENDING_BLOB_LINK state (structured present,
   blob index row missing or not yet committed).
2. Count blob_index rows with quality_state='INGESTED' that have no matching
   telemetry row (blob present, metadata not yet linked).
3. Identify successfully matched rows (event_id exists in both tables).
4. Report stats as structured JSON for operational monitoring.

This is the *reconciliation job* described in the architecture docs.  In
production it would also trigger updates (e.g. SET ingest_status='LINKED'),
but for the demo it reports only — Iceberg UPDATE from Trino requires
specific connector support that varies by version.
"""

import json
import os
import time
from datetime import datetime, timezone

import trino

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "reconciler")
RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "120"))

CATALOG = "iceberg"
SCHEMA = "prod"


def _conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=CATALOG,
        schema=SCHEMA,
        http_scheme="http",
    )


def _scalar(cur, sql: str):
    cur.execute(sql)
    rows = cur.fetchall()
    return rows[0][0] if rows else None


def reconcile() -> dict:
    conn = _conn()
    cur = conn.cursor()

    # Total row counts
    telemetry_total = _scalar(cur, "SELECT count(*) FROM telemetry_events") or 0
    blob_total = _scalar(cur, "SELECT count(*) FROM blob_index") or 0

    # Cross-layer linkage (inner join on event_id)
    linked_count = _scalar(
        cur,
        """
        SELECT count(*)
        FROM telemetry_events t
        INNER JOIN blob_index b ON t.event_id = b.event_id
        """,
    ) or 0

    # Telemetry with no blob match
    telemetry_unlinked = _scalar(
        cur,
        """
        SELECT count(*)
        FROM telemetry_events t
        LEFT JOIN blob_index b ON t.event_id = b.event_id
        WHERE b.event_id IS NULL
        """,
    ) or 0

    # Blobs with no telemetry match
    blob_orphans = _scalar(
        cur,
        """
        SELECT count(*)
        FROM blob_index b
        LEFT JOIN telemetry_events t ON b.event_id = t.event_id
        WHERE t.event_id IS NULL
        """,
    ) or 0

    # Linkage rate
    linkage_pct = round(linked_count / max(telemetry_total, 1) * 100, 2)

    # Per-media-type breakdown
    cur.execute(
        """
        SELECT b.media_type, count(*) AS cnt
        FROM blob_index b
        INNER JOIN telemetry_events t ON b.event_id = t.event_id
        GROUP BY b.media_type
        ORDER BY cnt DESC
        """
    )
    media_breakdown = {row[0]: row[1] for row in cur.fetchall()}

    # Freshness gap: oldest unlinked telemetry row
    oldest_unlinked_sec = _scalar(
        cur,
        """
        SELECT max(to_unixtime(current_timestamp) - to_unixtime(t.ingest_ts))
        FROM telemetry_events t
        LEFT JOIN blob_index b ON t.event_id = b.event_id
        WHERE b.event_id IS NULL
        """,
    )

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "telemetry_total": int(telemetry_total),
        "blob_total": int(blob_total),
        "linked_count": int(linked_count),
        "telemetry_unlinked": int(telemetry_unlinked),
        "blob_orphans": int(blob_orphans),
        "linkage_pct": linkage_pct,
        "media_breakdown": media_breakdown,
        "oldest_unlinked_sec": round(float(oldest_unlinked_sec), 1) if oldest_unlinked_sec else None,
        "status": "HEALTHY" if linkage_pct >= 95.0 else "DEGRADED",
    }
    return report


def _wait_for_tables(max_retries: int = 30, delay: int = 10) -> None:
    """Block until Trino can query both Iceberg tables."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = _conn()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM telemetry_events LIMIT 1")
            cur.fetchall()
            cur.execute("SELECT 1 FROM blob_index LIMIT 1")
            cur.fetchall()
            print(f"[reconciler] tables ready after {attempt} attempts", flush=True)
            return
        except Exception as exc:
            print(f"[reconciler] waiting for tables ({attempt}/{max_retries}): {exc}", flush=True)
            time.sleep(delay)
    print("[reconciler] WARNING: tables not ready, starting anyway", flush=True)


def main() -> None:
    _wait_for_tables()
    while True:
        try:
            report = reconcile()
            print(json.dumps(report), flush=True)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "status": "ERROR",
                        "error": str(exc),
                    }
                ),
                flush=True,
            )
        time.sleep(RECONCILE_INTERVAL_SEC)


if __name__ == "__main__":
    main()
