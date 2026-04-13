"""Robot fleet telemetry & blob producer.

Simulates 100 robots producing structured telemetry and unstructured blob
manifests.  Each robot has a **persistent per-session clock drift** (up to
±500 ms) that stays stable within a session—mirroring real NTP-unsynchronised
edge devices.  A heartbeat topic publishes estimated offsets so downstream
consumers can correct drift per-robot.
"""

import hashlib
import json
import random
import time
from datetime import datetime, timezone
from typing import Dict

import boto3
from kafka import KafkaProducer

from validate import validate_telemetry, validate_manifest

# Config
BOOTSTRAP = "kafka:9092"
TELEMETRY_TOPIC = "telemetry.raw"
BLOB_MANIFEST_TOPIC = "blob.manifest"
HEARTBEAT_TOPIC = "robot.heartbeat"
RAW_BUCKET = "raw-blobs"
NUM_ROBOTS = 200  # 200+ robot fleet per context
BLOB_SIZE_BYTES = 64 * 1024  # 64 KB demo blobs (real: MB-scale frames/clouds)


def utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def make_blob_key(robot_id: str, event_id: str, media_type: str) -> str:
    dt = datetime.now(timezone.utc)
    robot_shard = f"{int(robot_id.split('-')[1]) % 16:02d}"
    return (
        f"blob/{media_type}/event_date={dt:%Y-%m-%d}/event_hour={dt:%H}/"
        f"robot_shard={robot_shard}/robot_id={robot_id}/{event_id}.bin"
    )


def build_robot_drift_map(robot_ids: list[str]) -> Dict[str, int]:
    """Assign a stable per-robot drift for the current session.

    In production this comes from NTP sync logs.  Here we simulate it so
    each robot has a consistent offset instead of random-per-event noise.
    """
    return {rid: random.randint(-500, 500) for rid in robot_ids}


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    robot_ids = [f"robot-{i:03d}" for i in range(1, NUM_ROBOTS + 1)]
    media_types = ["camera_rgb", "camera_depth", "lidar_pointcloud", "audio", "raw_sensor_dump"]

    # Stable per-robot drift for this session (re-rolled on restart).
    drift_map = build_robot_drift_map(robot_ids)

    session_start_ms = utc_ms()
    session_id = f"s-{session_start_ms // 60000}"
    heartbeat_counter = 0

    while True:
        robot_id = random.choice(robot_ids)
        media_type = random.choice(media_types)
        now_ms = utc_ms()
        drift_ms = drift_map[robot_id]
        # Small jitter (±20 ms) on top of stable drift to simulate noise.
        edge_ts_ms = now_ms + drift_ms + random.randint(-20, 20)
        # Monotonic counter per session — collision-proof by construction.
        # Old format used random_5_digit which had ~864 collisions/robot/day.
        heartbeat_counter += 1
        event_id = f"{robot_id}:{session_id}:{heartbeat_counter:012d}"

        # Structured telemetry (mirrors real sensor rates)
        # Real: Joint States @ 1kHz, Force/Torque @ 500Hz, IMU @ 100Hz
        telemetry = {
            "event_id": event_id,
            "robot_id": robot_id,
            "session_id": session_id,
            "event_ts_edge_ms": edge_ts_ms,
            "sensor_type": random.choice(["joint_state", "force_torque", "imu"]),
            # Joint state fields
            "joint_angle": round(random.uniform(-3.14, 3.14), 3),
            "joint_velocity": round(random.uniform(-2.0, 2.0), 3),
            # Force/torque fields
            "force_x": round(random.uniform(-50.0, 50.0), 2),
            "force_z": round(random.uniform(-100.0, 100.0), 2),
            "torque": round(random.uniform(0.0, 220.0), 2),
            # IMU fields
            "accel_x": round(random.uniform(-10.0, 10.0), 3),
            "gyro_z": round(random.uniform(-5.0, 5.0), 3),
            # Common
            "temperature": round(random.uniform(25.0, 85.0), 2),
        }
        valid, errors = validate_telemetry(telemetry)
        if valid:
            producer.send(TELEMETRY_TOPIC, telemetry)
        else:
            print(f"[producer] REJECTED telemetry {event_id}: {errors}", flush=True)
            continue

        # Unstructured blob + manifest
        blob_key = make_blob_key(robot_id, event_id, media_type)
        blob_bytes = bytes(random.getrandbits(8) for _ in range(BLOB_SIZE_BYTES))
        blob_checksum = hashlib.sha256(blob_bytes).hexdigest()
        put_resp = s3.put_object(Bucket=RAW_BUCKET, Key=blob_key, Body=blob_bytes)
        etag = put_resp.get("ETag", "").strip('"')

        manifest = {
            "event_id": event_id,
            "robot_id": robot_id,
            "session_id": session_id,
            "media_type": media_type,
            "blob_uri": f"s3://{RAW_BUCKET}/{blob_key}",
            "blob_ts_edge_ms": edge_ts_ms,
            "object_size": len(blob_bytes),
            "checksum_sha256": blob_checksum,
            "etag": etag,
            "quality_state": "INGESTED",
        }
        m_valid, m_errors = validate_manifest(manifest)
        if m_valid:
            producer.send(BLOB_MANIFEST_TOPIC, manifest)
        else:
            print(f"[producer] REJECTED manifest {event_id}: {m_errors}", flush=True)

        # Heartbeat (every ~50 events) with drift estimate
        heartbeat_counter += 1
        if heartbeat_counter % 50 == 0:
            for rid in robot_ids:
                heartbeat = {
                    "robot_id": rid,
                    "session_id": session_id,
                    "server_ts_ms": utc_ms(),
                    "estimated_drift_ms": drift_map[rid],
                }
                producer.send(HEARTBEAT_TOPIC, heartbeat)

        producer.flush()
        time.sleep(0.05)


if __name__ == "__main__":
    main()
