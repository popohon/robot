"""Data quality SLA enforcement.

Runs three check dimensions against Trino/Iceberg:
  1. Freshness : max lag between ingest and wall-clock.
  2. Completeness : event count per window + cross-layer linkage rate.
  3. Accuracy : domain validity (temperature range, torque bounds).

Each check produces a severity: OK | WARNING | CRITICAL.
Overall severity is the worst of the three.
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import trino

# Config
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "quality")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "prod")
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "60"))

# SLA thresholds
FRESHNESS_WARN_SEC = int(os.getenv("FRESHNESS_WARN_SEC", "60"))
FRESHNESS_CRIT_SEC = int(os.getenv("FRESHNESS_CRIT_SEC", "120"))
MIN_EVENTS_LAST_5M = int(os.getenv("MIN_EVENTS_LAST_5M", "100"))
LINKAGE_WARN_PCT = float(os.getenv("LINKAGE_WARN_PCT", "90.0"))
LINKAGE_CRIT_PCT = float(os.getenv("LINKAGE_CRIT_PCT", "80.0"))
MAX_BAD_TEMP_ROWS = int(os.getenv("MAX_BAD_TEMP_ROWS", "0"))


def _scalar(cur, sql: str):
    cur.execute(sql)
    rows = cur.fetchall()
    return rows[0][0] if rows else None


def _severity(*levels: str) -> str:
    """Return the worst severity from a list."""
    if "CRITICAL" in levels:
        return "CRITICAL"
    if "WARNING" in levels:
        return "WARNING"
    return "OK"


def run_check() -> dict:
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
    )
    cur = conn.cursor()
    now_epoch = datetime.now(timezone.utc).timestamp()

    #1. Freshness 
    latest_ingest = _scalar(
        cur, "SELECT to_unixtime(max(ingest_ts)) FROM telemetry_events"
    )
    freshness_sec = (
        None if latest_ingest is None else max(0, int(now_epoch - float(latest_ingest)))
    )
    if freshness_sec is None or freshness_sec > FRESHNESS_CRIT_SEC:
        freshness_sev = "CRITICAL"
    elif freshness_sec > FRESHNESS_WARN_SEC:
        freshness_sev = "WARNING"
    else:
        freshness_sev = "OK"

    # 2a. Completeness: event volume
    events_last_5m = int(
        _scalar(
            cur,
            "SELECT count(*) FROM telemetry_events "
            "WHERE ingest_ts > current_timestamp - INTERVAL '5' MINUTE",
        )
        or 0
    )
    volume_sev = "OK" if events_last_5m >= MIN_EVENTS_LAST_5M else "WARNING"

    # 2b. Completeness: cross-layer linkage
    telemetry_total = int(_scalar(cur, "SELECT count(*) FROM telemetry_events") or 0)
    linked_count = int(
        _scalar(
            cur,
            "SELECT count(*) FROM telemetry_events t "
            "INNER JOIN blob_index b ON t.event_id = b.event_id",
        )
        or 0
    )
    linkage_pct = round(linked_count / max(telemetry_total, 1) * 100, 2)
    if linkage_pct < LINKAGE_CRIT_PCT:
        linkage_sev = "CRITICAL"
    elif linkage_pct < LINKAGE_WARN_PCT:
        linkage_sev = "WARNING"
    else:
        linkage_sev = "OK"

    # 3. Accuracy: domain validity 
    bad_temp_rows = int(
        _scalar(
            cur,
            "SELECT count(*) FROM telemetry_events "
            "WHERE temperature < -40 OR temperature > 150",
        )
        or 0
    )
    accuracy_sev = "OK" if bad_temp_rows <= MAX_BAD_TEMP_ROWS else "CRITICAL"

    # Per-robot freshness (top 5 stale)
    cur.execute(
        "SELECT robot_id, "
        "  cast(to_unixtime(current_timestamp) - to_unixtime(max(ingest_ts)) AS integer) AS stale_sec "
        "FROM telemetry_events "
        "GROUP BY robot_id "
        "ORDER BY stale_sec DESC "
        "LIMIT 5"
    )
    stale_robots = [{"robot_id": r[0], "stale_sec": r[1]} for r in cur.fetchall()]

    # Aggregate
    overall = _severity(freshness_sev, volume_sev, linkage_sev, accuracy_sev)

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "overall_severity": overall,
        "checks": {
            "freshness": {
                "severity": freshness_sev,
                "value_sec": freshness_sec,
                "threshold_warn": FRESHNESS_WARN_SEC,
                "threshold_crit": FRESHNESS_CRIT_SEC,
            },
            "completeness_volume": {
                "severity": volume_sev,
                "events_last_5m": events_last_5m,
                "threshold_min": MIN_EVENTS_LAST_5M,
            },
            "completeness_linkage": {
                "severity": linkage_sev,
                "linkage_pct": linkage_pct,
                "linked": linked_count,
                "total_telemetry": telemetry_total,
            },
            "accuracy": {
                "severity": accuracy_sev,
                "bad_temp_rows": bad_temp_rows,
            },
        },
        "stale_robots_top5": stale_robots,
    }


def _wait_for_tables(max_retries: int = 30, delay: int = 10) -> None:
    """Block until Trino can query the telemetry table."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = trino.dbapi.connect(
                host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
                catalog=TRINO_CATALOG, schema=TRINO_SCHEMA, http_scheme="http",
            )
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM telemetry_events LIMIT 1")
            cur.fetchall()
            print(f"[quality] tables ready after {attempt} attempts", flush=True)
            return
        except Exception as exc:
            print(f"[quality] waiting for tables ({attempt}/{max_retries}): {exc}", flush=True)
            time.sleep(delay)
    print("[quality] WARNING: tables not ready, starting anyway", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run a single check and exit.")
    args = parser.parse_args()

    _wait_for_tables()

    while True:
        try:
            report = run_check()
            print(json.dumps(report), flush=True)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "overall_severity": "ERROR",
                        "error": str(exc),
                    }
                ),
                flush=True,
            )
        if args.once:
            break
        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    main()
