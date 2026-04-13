"""Spark Structured Streaming: Kafka -> Iceberg (via Nessie catalog).

Reads telemetry + blob manifest streams, applies per-robot drift correction
using a broadcast offset map updated from the heartbeat topic, and writes
ACID Iceberg tables partitioned by corrected time.

Catalog: Nessie (git-like versioning for Iceberg metadata).
"""

import json
import threading
import time
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    expr,
    from_json,
    lit,
    to_timestamp,
    udf,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Per-robot drift offset map (updated from heartbeat topic)

# In production this would be a shared state store (e.g. Redis / broadcast
# variable refreshed periodically).  For the demo we use a module-level dict
# populated by a background Kafka consumer thread.
_drift_offsets: dict[str, int] = defaultdict(int)  # robot_id -> ms offset
_DEFAULT_DRIFT_MS = 0  # fallback when no heartbeat received yet


def _heartbeat_listener() -> None:
    """Background thread: read heartbeat topic and update drift map."""
    try:
        from kafka import KafkaConsumer
    except ImportError:
        # kafka-python not available in Spark image by default;
        # fall back to static default offset.
        return
    try:
        consumer = KafkaConsumer(
            "robot.heartbeat",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="spark-drift-updater",
        )
        for msg in consumer:
            hb = msg.value
            _drift_offsets[hb["robot_id"]] = int(hb.get("estimated_drift_ms", 0))
    except Exception:
        pass  # non-fatal: drift correction falls back to default


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("robotics-lakehouse-stream")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Nessie-backed Iceberg catalog
        .config("spark.sql.catalog.robotics", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.robotics.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.robotics.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.robotics.ref", "main")
        .config("spark.sql.catalog.robotics.warehouse", "s3a://lakehouse/warehouse")
        # Hadoop S3A (for Iceberg data files + checkpoint paths)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def _get_drift(robot_id: str) -> int:
    """Return drift offset for a robot (ms).  Falls back to default."""
    return _drift_offsets.get(robot_id, _DEFAULT_DRIFT_MS)


def _progress_monitor(queries: dict, interval: int = 30) -> None:
    """Background thread: print clean batch progress for streaming queries."""
    time.sleep(60)  # let first batches complete
    while True:
        for name, q in queries.items():
            try:
                if q.isActive and q.lastProgress:
                    p = q.lastProgress
                    rows = p["numInputRows"]
                    batch_id = p["batchId"]
                    rate = p.get("processedRowsPerSecond") or 0
                    print(
                        f"[spark] {name}: batch {batch_id} | "
                        f"{rows} rows | {rate:.0f} rows/sec",
                        flush=True,
                    )
            except Exception:
                pass
        time.sleep(interval)


def main() -> None:
    # Start background heartbeat listener.
    t = threading.Thread(target=_heartbeat_listener, daemon=True)
    t.start()

    spark = build_spark()
    # Suppress all WARN (including KAFKA-1894 flood), keep ERROR only.
    spark.sparkContext.setLogLevel("ERROR")

    # Register UDF for per-robot drift correction.
    drift_udf = udf(lambda rid, ts: int(ts - _get_drift(rid)) if ts else None, LongType())

    # DDL: create namespace + tables
    print("[spark] Creating namespace and tables...", flush=True)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS robotics.prod")

    # Column order must match DataFrame output: parsed JSON fields first,
    # then withColumn-added fields (corrected_ts, ingest_ts, ingest_status)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS robotics.prod.telemetry_events (
          event_id STRING,
          robot_id STRING,
          session_id STRING,
          event_ts_edge_ms BIGINT,
          sensor_type STRING,
          joint_angle DOUBLE,
          joint_velocity DOUBLE,
          force_x DOUBLE,
          force_z DOUBLE,
          torque DOUBLE,
          accel_x DOUBLE,
          gyro_z DOUBLE,
          temperature DOUBLE,
          event_ts_corrected TIMESTAMP,
          ingest_ts TIMESTAMP,
          ingest_status STRING
        )
        USING iceberg
        PARTITIONED BY (hours(event_ts_corrected), bucket(128, robot_id))
        """
    )

    # Column order matches manifest DataFrame: JSON fields, then withColumn fields.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS robotics.prod.blob_index (
          event_id STRING,
          robot_id STRING,
          session_id STRING,
          media_type STRING,
          blob_uri STRING,
          blob_ts_edge_ms BIGINT,
          object_size BIGINT,
          checksum_sha256 STRING,
          etag STRING,
          quality_state STRING,
          blob_ts_corrected TIMESTAMP,
          ingest_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(blob_ts_corrected), media_type, bucket(64, robot_id))
        """
    )

    # Schemas
    telemetry_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("robot_id", StringType()),
            StructField("session_id", StringType()),
            StructField("event_ts_edge_ms", LongType()),
            StructField("sensor_type", StringType()),
            StructField("joint_angle", DoubleType()),
            StructField("joint_velocity", DoubleType()),
            StructField("force_x", DoubleType()),
            StructField("force_z", DoubleType()),
            StructField("torque", DoubleType()),
            StructField("accel_x", DoubleType()),
            StructField("gyro_z", DoubleType()),
            StructField("temperature", DoubleType()),
        ]
    )
    manifest_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("robot_id", StringType()),
            StructField("session_id", StringType()),
            StructField("media_type", StringType()),
            StructField("blob_uri", StringType()),
            StructField("blob_ts_edge_ms", LongType()),
            StructField("object_size", LongType()),
            StructField("checksum_sha256", StringType()),
            StructField("etag", StringType()),
            StructField("quality_state", StringType()),
        ]
    )

    print("[spark] ✓ Tables ready. Starting streaming queries...", flush=True)

    # Telemetry stream-
    raw_telemetry = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "telemetry.raw")
        .option("startingOffsets", "earliest")
        .load()
    )
    telemetry = (
        raw_telemetry.selectExpr("CAST(value AS STRING) AS payload")
        .select(from_json(col("payload"), telemetry_schema).alias("j"))
        .select("j.*")
        # Per-robot drift correction via UDF (reads from heartbeat-updated map).
        .withColumn("_corrected_ms", drift_udf(col("robot_id"), col("event_ts_edge_ms")))
        .withColumn("event_ts_corrected", to_timestamp(col("_corrected_ms") / lit(1000)))
        .drop("_corrected_ms")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("ingest_status", lit("PENDING_BLOB_LINK"))
    )

    # Blob manifest stream-
    raw_manifest = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "blob.manifest")
        .option("startingOffsets", "earliest")
        .load()
    )
    manifest = (
        raw_manifest.selectExpr("CAST(value AS STRING) AS payload")
        .select(from_json(col("payload"), manifest_schema).alias("j"))
        .select("j.*")
        .withColumn("_corrected_ms", drift_udf(col("robot_id"), col("blob_ts_edge_ms")))
        .withColumn("blob_ts_corrected", to_timestamp(col("_corrected_ms") / lit(1000)))
        .drop("_corrected_ms")
        .withColumn("ingest_ts", current_timestamp())
    )

    # Sink to Iceberg
    telemetry_query = (
        telemetry.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/telemetry")
        .toTable("robotics.prod.telemetry_events")
    )
    manifest_query = (
        manifest.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/blob_index")
        .toTable("robotics.prod.blob_index")
    )

    print("[spark] ✓ Telemetry stream started.", flush=True)
    print("[spark] ✓ Blob manifest stream started.", flush=True)
    print("[spark] Both streams running. Micro-batches committing to Iceberg.", flush=True)
    print("[spark] Progress updates every 30 seconds below:", flush=True)

    # Start progress monitor.
    mon = threading.Thread(
        target=_progress_monitor,
        args=({"telemetry": telemetry_query, "blob_index": manifest_query},),
        daemon=True,
    )
    mon.start()

    telemetry_query.awaitTermination()
    manifest_query.awaitTermination()


if __name__ == "__main__":
    main()
