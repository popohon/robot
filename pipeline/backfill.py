"""Historical data backfill for the robotics lakehouse.

Generates telemetry + blob manifests for a configurable time window and
pushes them through Kafka so they flow through the normal ingestion path
(Spark → Iceberg).  This simulates late arriving or historical data.

Usage:
    # Relative: last 24 hours
    python backfill.py --hours-back 24 --events-per-second 200

    # Precise window: specific start/end timestamps (UTC, ISO format)
    python backfill.py --start 2026-04-11T10:00:00 --end 2026-04-11T11:00:00

    # Via docker compose
    docker compose --profile backfill up backfill
"""

import argparse
import hashlib
import json
import os
import random
import time
from datetime import datetime, timezone, timedelta

import boto3
from kafka import KafkaProducer

from validate import validate_telemetry, validate_manifest

# Config (overridable via env)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
TELEMETRY_TOPIC = "telemetry.raw"
BLOB_MANIFEST_TOPIC = "blob.manifest"
HEARTBEAT_TOPIC = "robot.heartbeat"
RAW_BUCKET = "raw-blobs"
BLOB_SIZE_BYTES = 4 * 1024  # 4 KB (smaller than live producer for speed)


def utc_ms_from_dt(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def make_blob_key(robot_id: str, event_id: str, media_type: str, dt: datetime) -> str:
    shard = f"{int(robot_id.split('-')[1]) % 16:02d}"
    return (
        f"blob/{media_type}/event_date={dt:%Y-%m-%d}/event_hour={dt:%H}/"
        f"robot_shard={shard}/robot_id={robot_id}/{event_id}.bin"
    )


def run_backfill(
    hours_back: int,
    events_per_second: int,
    robots: int,
    start_ts: str = None,
    end_ts: str = None,
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    robot_ids = [f"robot-{i:03d}" for i in range(1, robots + 1)]
    media_types = ["camera_rgb", "camera_depth", "lidar_pointcloud", "audio", "raw_sensor_dump"]
    sensor_types = ["joint_state", "force_torque", "imu"]

    # Stable drift per robot for the backfill session.
    drift_map = {rid: random.randint(-500, 500) for rid in robot_ids}

    now = datetime.now(timezone.utc)

    # Determine time window: precise --start/--end or relative --hours-back.
    if start_ts and end_ts:
        start_time = datetime.fromisoformat(start_ts).replace(tzinfo=timezone.utc)
        end_time = datetime.fromisoformat(end_ts).replace(tzinfo=timezone.utc)
        hours_back = max(1, int((end_time - start_time).total_seconds() / 3600))
        now = end_time  # override loop boundary
        print(f"[backfill] Precise window: {start_time.isoformat()} → {end_time.isoformat()}", flush=True)
    elif start_ts:
        start_time = datetime.fromisoformat(start_ts).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        hours_back = max(1, int((now - start_time).total_seconds() / 3600))
        print(f"[backfill] From {start_time.isoformat()} → now", flush=True)
    else:
        start_time = now - timedelta(hours=hours_back)

    session_id = f"s-backfill-{int(now.timestamp())}"

    total_events = hours_back * 3600 * events_per_second
    interval_sec = 1.0 / events_per_second
    counter = 0
    sent = 0
    rejected = 0

    print(
        f"[backfill] Generating {total_events:,} events "
        f"({hours_back}h × {events_per_second} evt/s) "
        f"for {robots} robots",
        flush=True,
    )

    current_time = start_time
    time_step = timedelta(seconds=interval_sec)
    batch_start = time.monotonic()

    while current_time < now:
        robot_id = random.choice(robot_ids)
        media_type = random.choice(media_types)
        drift_ms = drift_map[robot_id]
        edge_ts_ms = utc_ms_from_dt(current_time) + drift_ms + random.randint(-20, 20)
        counter += 1
        event_id = f"{robot_id}:{session_id}:{counter:012d}"

        telemetry = {
            "event_id": event_id,
            "robot_id": robot_id,
            "session_id": session_id,
            "event_ts_edge_ms": edge_ts_ms,
            "sensor_type": random.choice(sensor_types),
            "joint_angle": round(random.uniform(-3.14, 3.14), 3),
            "joint_velocity": round(random.uniform(-2.0, 2.0), 3),
            "force_x": round(random.uniform(-50.0, 50.0), 2),
            "force_z": round(random.uniform(-100.0, 100.0), 2),
            "torque": round(random.uniform(0.0, 220.0), 2),
            "accel_x": round(random.uniform(-10.0, 10.0), 3),
            "gyro_z": round(random.uniform(-5.0, 5.0), 3),
            "temperature": round(random.uniform(25.0, 85.0), 2),
        }

        valid, errors = validate_telemetry(telemetry)
        if not valid:
            rejected += 1
            current_time += time_step
            continue

        producer.send(TELEMETRY_TOPIC, telemetry)

        # Blob manifest (skip actual blob upload for speed — only every 10th event).
        if counter % 10 == 0:
            blob_key = make_blob_key(robot_id, event_id, media_type, current_time)
            blob_bytes = os.urandom(BLOB_SIZE_BYTES)
            blob_checksum = hashlib.sha256(blob_bytes).hexdigest()
            s3.put_object(Bucket=RAW_BUCKET, Key=blob_key, Body=blob_bytes)

            manifest = {
                "event_id": event_id,
                "robot_id": robot_id,
                "session_id": session_id,
                "media_type": media_type,
                "blob_uri": f"s3://{RAW_BUCKET}/{blob_key}",
                "blob_ts_edge_ms": edge_ts_ms,
                "object_size": len(blob_bytes),
                "checksum_sha256": blob_checksum,
                "etag": blob_checksum[:32],
                "quality_state": "INGESTED",
            }
            m_valid, _ = validate_manifest(manifest)
            if m_valid:
                producer.send(BLOB_MANIFEST_TOPIC, manifest)

        # Heartbeat every 500 events.
        if counter % 500 == 0:
            for rid in robot_ids:
                producer.send(HEARTBEAT_TOPIC, {
                    "robot_id": rid,
                    "session_id": session_id,
                    "server_ts_ms": utc_ms_from_dt(current_time),
                    "estimated_drift_ms": drift_map[rid],
                })

        sent += 1
        current_time += time_step

        # Flush + progress every 5000 events.
        if counter % 5000 == 0:
            producer.flush()
            elapsed = time.monotonic() - batch_start
            rate = counter / max(elapsed, 0.001)
            pct = min(counter / max(total_events, 1) * 100, 100)
            print(
                f"[backfill] {counter:>10,} / {total_events:,} ({pct:5.1f}%) "
                f"| {rate:.0f} evt/s | rejected: {rejected}",
                flush=True,
            )

    producer.flush()
    print(
        f"[backfill] DONE — sent {sent:,} events, rejected {rejected:,}, "
        f"covering {hours_back}h of history",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical data through Kafka")
    parser.add_argument("--hours-back", type=int, default=24, help="Hours of history (ignored if --start used)")
    parser.add_argument("--events-per-second", type=int, default=200, help="Simulated events/sec")
    parser.add_argument("--robots", type=int, default=200, help="Number of robots to simulate")
    parser.add_argument("--start", type=str, default=None, help="Start timestamp ISO UTC (e.g. 2026-04-11T10:00:00)")
    parser.add_argument("--end", type=str, default=None, help="End timestamp ISO UTC (e.g. 2026-04-11T11:00:00)")
    args = parser.parse_args()

    run_backfill(args.hours_back, args.events_per_second, args.robots, args.start, args.end)


if __name__ == "__main__":
    main()
