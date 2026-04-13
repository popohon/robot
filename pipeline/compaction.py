"""Iceberg small-file compaction and table maintenance.

Streaming systems produce many tiny files. At 20 events/sec with 10-second
micro-batches across 128 bucket partitions, each file is only ~15 KB.
After 1 hour: ~360 commits x 128 partitions = 46,000+ tiny files.

This script:
  1. Reports current file health (file count, avg size, smallest files)
  2. Runs compaction (rewrite_data_files) to merge small files → 256 MB targets
  3. Expires old snapshots to clean up metadata
  4. Removes orphan files no longer referenced by any snapshot

In production, this runs on a schedule (every 15 min for hot, weekly for warm).
For the demo, run on-demand to show the before/after effect.

Usage:
    docker compose --profile compact run --rm compaction
    # or
    make compact
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import trino

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
CATALOG = "iceberg"
SCHEMA = "prod"
TABLES = ["telemetry_events", "blob_index"]


def conn():
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="compaction",
        catalog=CATALOG, schema=SCHEMA, http_scheme="http",
    )


def query(sql: str) -> list:
    c = conn()
    cur = c.cursor()
    cur.execute(sql)
    return cur.fetchall()


def query_scalar(sql: str):
    rows = query(sql)
    return rows[0][0] if rows else None


def report_file_health(table: str) -> dict:
    """Report file count, sizes, and snapshot count for a table."""
    print(f"\n{'='*60}", flush=True)
    print(f"  File health: {CATALOG}.{SCHEMA}.{table}", flush=True)
    print(f"{'='*60}", flush=True)

    # Snapshot count (= number of commits)
    snapshot_count = query_scalar(
        f'SELECT count(*) FROM {CATALOG}.{SCHEMA}."{table}$snapshots"'
    ) or 0

    # File stats from the files metadata table
    try:
        file_stats = query(
            f"""SELECT
                count(*) AS file_count,
                coalesce(sum(file_size_in_bytes), 0) AS total_bytes,
                coalesce(avg(file_size_in_bytes), 0) AS avg_bytes,
                coalesce(min(file_size_in_bytes), 0) AS min_bytes,
                coalesce(max(file_size_in_bytes), 0) AS max_bytes,
                coalesce(sum(record_count), 0) AS total_records
            FROM {CATALOG}.{SCHEMA}."{table}$files"
            """
        )
        if file_stats:
            f = file_stats[0]
            file_count, total_bytes, avg_bytes, min_bytes, max_bytes, total_records = f
        else:
            file_count = total_bytes = avg_bytes = min_bytes = max_bytes = total_records = 0
    except Exception as e:
        print(f"  Could not read file metadata: {e}", flush=True)
        file_count = total_bytes = avg_bytes = min_bytes = max_bytes = total_records = 0

    def fmt_size(b):
        if b >= 1024 * 1024:
            return f"{b / (1024*1024):.1f} MB"
        if b >= 1024:
            return f"{b / 1024:.1f} KB"
        return f"{b} B"

    report = {
        "table": table,
        "snapshots": int(snapshot_count),
        "files": int(file_count),
        "total_size": fmt_size(total_bytes),
        "avg_file_size": fmt_size(avg_bytes),
        "min_file_size": fmt_size(min_bytes),
        "max_file_size": fmt_size(max_bytes),
        "total_records": int(total_records),
    }

    print(f"  Snapshots:     {report['snapshots']}", flush=True)
    print(f"  Data files:    {report['files']}", flush=True)
    print(f"  Total size:    {report['total_size']}", flush=True)
    print(f"  Avg file size: {report['avg_file_size']}", flush=True)
    print(f"  Min file size: {report['min_file_size']}", flush=True)
    print(f"  Max file size: {report['max_file_size']}", flush=True)
    print(f"  Total records: {report['total_records']:,}", flush=True)

    # Assessment
    if file_count > 0 and avg_bytes < 64 * 1024:
        print(f"\n  SMALL FILES: avg {report['avg_file_size']} (target: 256 MB)", flush=True)
        print(f"  → Run compaction to merge {file_count} files into fewer, larger files.", flush=True)
    elif file_count > 0 and avg_bytes < 1024 * 1024:
        print(f"\n  Files below 1 MB average — compaction recommended.", flush=True)
    else:
        print(f"\n  ✓ File health OK.", flush=True)

    return report


def run_compaction(table: str) -> None:
    """Run Iceberg compaction via Spark (rewrite_data_files equivalent in Trino).

    Trino's ALTER TABLE ... EXECUTE optimize() is the Trino way to compact.
    It rewrites small files into larger ones, targeting 256 MB per file.
    """
    print(f"\n[compact] Running compaction on {table}...", flush=True)
    try:
        # Trino Iceberg optimize: merges small files
        # file_size_threshold: only rewrite files smaller than this (100 MB)
        query(f"""
            ALTER TABLE {CATALOG}.{SCHEMA}.{table}
            EXECUTE optimize(file_size_threshold => '100MB')
        """)
        print(f"[compact] ✓ Compaction complete for {table}.", flush=True)
    except Exception as e:
        print(f"[compact] ✗ Compaction failed for {table}: {e}", flush=True)


def expire_snapshots(table: str, retain_days: int = 7) -> None:
    """Expire old snapshots to reduce metadata overhead."""
    print(f"\n[compact] Expiring snapshots older than {retain_days} days on {table}...", flush=True)
    try:
        query(f"""
            ALTER TABLE {CATALOG}.{SCHEMA}.{table}
            EXECUTE expire_snapshots(retention_threshold => '{retain_days}d')
        """)
        print(f"[compact] ✓ Snapshot expiry complete for {table}.", flush=True)
    except Exception as e:
        print(f"[compact] ✗ Snapshot expiry failed for {table}: {e}", flush=True)


def remove_orphan_files(table: str, retain_days: int = 7) -> None:
    """Remove files not referenced by any snapshot."""
    print(f"\n[compact] Removing orphan files older than {retain_days} days on {table}...", flush=True)
    try:
        query(f"""
            ALTER TABLE {CATALOG}.{SCHEMA}.{table}
            EXECUTE remove_orphan_files(retention_threshold => '{retain_days}d')
        """)
        print(f"[compact] ✓ Orphan file removal complete for {table}.", flush=True)
    except Exception as e:
        print(f"[compact] ✗ Orphan removal failed for {table}: {e}", flush=True)


def wait_for_tables(max_retries: int = 20, delay: int = 10) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            query("SELECT 1 FROM telemetry_events LIMIT 1")
            return
        except Exception:
            print(f"[compact] waiting for tables ({attempt}/{max_retries})...", flush=True)
            time.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description="Iceberg compaction and table maintenance")
    parser.add_argument("--report-only", action="store_true", help="Only report file health, don't compact")
    parser.add_argument("--expire-days", type=int, default=7, help="Expire snapshots older than N days")
    args = parser.parse_args()

    wait_for_tables()

    # Report BEFORE
    print("\n" + "=" * 60, flush=True)
    print("  BEFORE compaction", flush=True)
    print("=" * 60, flush=True)
    for table in TABLES:
        report_file_health(table)

    if args.report_only:
        return

    # Compact
    for table in TABLES:
        run_compaction(table)

    # Expire old snapshots
    for table in TABLES:
        expire_snapshots(table, args.expire_days)

    # Remove orphan files
    for table in TABLES:
        remove_orphan_files(table, args.expire_days)

    # Report AFTER
    print("\n" + "=" * 60, flush=True)
    print("  AFTER compaction", flush=True)
    print("=" * 60, flush=True)
    for table in TABLES:
        report_file_health(table)

    print("\n[compact] Done.", flush=True)


if __name__ == "__main__":
    main()
