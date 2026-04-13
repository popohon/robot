"""Peak traffic simulator — demonstrates 10x spike behavior.

Floods Kafka with telemetry at maximum rate for a configurable duration.
Deliberately skips blob uploads to simulate real peak behavior where
structured telemetry outpaces unstructured blob processing.

What to observe during the spike:
  - Kafka consumer lag grows (check Kafka UI → Consumer Groups)
  - Quality check freshness degrades (make logs-quality)
  - Reconciler linkage drops (telemetry without blobs) (make logs-reconciler)
  - After spike ends, system self-heals (lag drains, linkage recovers)

Usage:
    docker compose --profile peak run --rm peak-simulator
    # or
    make peak
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from validate import validate_telemetry

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TELEMETRY_TOPIC = "telemetry.raw"
HEARTBEAT_TOPIC = "robot.heartbeat"


def utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def run_peak(duration_sec: int, robots: int, batch_size: int) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,  # faster than acks=all for peak simulation
        linger_ms=50,  # batch messages for throughput
        batch_size=64 * 1024,
    )

    robot_ids = [f"robot-{i:03d}" for i in range(1, robots + 1)]
    sensor_types = ["joint_state", "force_torque", "imu"]
    drift_map = {rid: random.randint(-500, 500) for rid in robot_ids}
    session_id = f"s-peak-{int(datetime.now(timezone.utc).timestamp())}"

    print(f"\n{'='*60}", flush=True)
    print(f"  PEAK SIMULATOR — {duration_sec}s burst, {robots} robots", flush=True)
    print(f"  No blob uploads (simulates telemetry outpacing blobs)", flush=True)
    print(f"{'='*60}\n", flush=True)
    print(f"What to watch in other terminals:", flush=True)
    print(f"  make logs-quality      → freshness will degrade", flush=True)
    print(f"  make logs-reconciler   → linkage_pct will drop", flush=True)
    print(f"  Kafka UI (localhost:8081) → consumer lag grows\n", flush=True)

    start = time.monotonic()
    counter = 0
    last_report = start

    # Publish heartbeats first so drift correction stays active.
    for rid in robot_ids:
        producer.send(HEARTBEAT_TOPIC, {
            "robot_id": rid,
            "session_id": session_id,
            "server_ts_ms": utc_ms(),
            "estimated_drift_ms": drift_map[rid],
        })
    producer.flush()

    while time.monotonic() - start < duration_sec:
        for _ in range(batch_size):
            robot_id = random.choice(robot_ids)
            drift_ms = drift_map[robot_id]
            edge_ts_ms = utc_ms() + drift_ms + random.randint(-20, 20)
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
            valid, _ = validate_telemetry(telemetry)
            if valid:
                producer.send(TELEMETRY_TOPIC, telemetry)

        producer.flush()

        # Report every 5 seconds.
        now = time.monotonic()
        if now - last_report >= 5:
            elapsed = now - start
            rate = counter / elapsed
            remaining = max(0, duration_sec - elapsed)
            print(
                f"[peak] {counter:>10,} events | {rate:,.0f} evt/s | "
                f"{remaining:.0f}s remaining",
                flush=True,
            )
            last_report = now

    producer.flush()
    elapsed = time.monotonic() - start
    rate = counter / elapsed

    print(f"\n{'='*60}", flush=True)
    print(f"  SPIKE COMPLETE", flush=True)
    print(f"  Sent {counter:,} telemetry events in {elapsed:.1f}s ({rate:,.0f} evt/s)", flush=True)
    print(f"  NO blob manifests sent (deliberate — simulates burst)", flush=True)
    print(f"{'='*60}\n", flush=True)
    print(f"What happens next (self-healing):", flush=True)
    print(f"  1. Spark drains the Kafka lag     (~1-5 min)", flush=True)
    print(f"  2. Freshness returns to normal     (~2 min after lag drains)", flush=True)
    print(f"  3. Linkage_pct drops temporarily   (telemetry without blobs)", flush=True)
    print(f"  4. Normal producer resumes blobs   (linkage recovers)", flush=True)
    print(f"\nRun 'make query-smoke' to see the row count jump.", flush=True)
    print(f"Run 'make logs-quality' to watch freshness recover.\n", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate 10x peak traffic burst")
    parser.add_argument("--duration", type=int, default=120, help="Spike duration in seconds")
    parser.add_argument("--robots", type=int, default=200, help="Number of robots")
    parser.add_argument("--batch-size", type=int, default=500, help="Events per flush batch")
    args = parser.parse_args()

    run_peak(args.duration, args.robots, args.batch_size)


if __name__ == "__main__":
    main()
