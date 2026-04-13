"""Data validation for robotics telemetry and blob manifests.

Two layers of validation:
1. Schema validation — required fields present, correct types.
2. Domain validation — values within physically plausible bounds.

Usage:
    from validate import validate_telemetry, validate_manifest

Each function returns (is_valid: bool, errors: list[str]).
"""

from typing import Tuple

# Domain bounds (based on robotics sensor specs)
TEMP_MIN_C = -40.0
TEMP_MAX_C = 150.0
TORQUE_MIN_NM = 0.0
TORQUE_MAX_NM = 300.0
JOINT_ANGLE_MIN = -6.3  # ~2π
JOINT_ANGLE_MAX = 6.3
FORCE_MAX_N = 500.0

VALID_SENSOR_TYPES = {"joint_state", "force_torque", "imu"}
VALID_MEDIA_TYPES = {
    "camera_rgb", "camera_depth", "lidar_pointcloud",
    "audio", "raw_sensor_dump",
}

REQUIRED_TELEMETRY_FIELDS = [
    "event_id", "robot_id", "session_id", "event_ts_edge_ms", "sensor_type",
]
REQUIRED_MANIFEST_FIELDS = [
    "event_id", "robot_id", "session_id", "media_type",
    "blob_uri", "blob_ts_edge_ms", "checksum_sha256",
]


def validate_telemetry(record: dict) -> Tuple[bool, list]:
    """Validate a single telemetry record.

    Returns (True, []) if valid, or (False, [error_strings]) if not.
    """
    errors = []

    # Schema: required fields
    for field in REQUIRED_TELEMETRY_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"missing_field:{field}")

    if errors:
        return False, errors

    # Schema: type checks
    if not isinstance(record.get("event_ts_edge_ms"), (int, float)):
        errors.append("bad_type:event_ts_edge_ms:expected_numeric")

    # Domain: sensor_type
    if record.get("sensor_type") not in VALID_SENSOR_TYPES:
        errors.append(f"invalid_sensor_type:{record.get('sensor_type')}")

    # Domain: temperature bounds
    temp = record.get("temperature")
    if temp is not None and (temp < TEMP_MIN_C or temp > TEMP_MAX_C):
        errors.append(f"temperature_out_of_range:{temp}")

    # Domain: torque bounds
    torque = record.get("torque")
    if torque is not None and (torque < TORQUE_MIN_NM or torque > TORQUE_MAX_NM):
        errors.append(f"torque_out_of_range:{torque}")

    # Domain: joint angle bounds
    angle = record.get("joint_angle")
    if angle is not None and (angle < JOINT_ANGLE_MIN or angle > JOINT_ANGLE_MAX):
        errors.append(f"joint_angle_out_of_range:{angle}")

    # Domain: force bounds
    for force_field in ("force_x", "force_z"):
        val = record.get(force_field)
        if val is not None and abs(val) > FORCE_MAX_N:
            errors.append(f"{force_field}_out_of_range:{val}")

    # Domain: event_id format
    eid = record.get("event_id", "")
    if eid.count(":") != 2:
        errors.append(f"malformed_event_id:{eid}")

    return (len(errors) == 0), errors


def validate_manifest(record: dict) -> Tuple[bool, list]:
    """Validate a single blob manifest record."""
    errors = []

    for field in REQUIRED_MANIFEST_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"missing_field:{field}")

    if errors:
        return False, errors

    if record.get("media_type") not in VALID_MEDIA_TYPES:
        errors.append(f"invalid_media_type:{record.get('media_type')}")

    if not isinstance(record.get("blob_ts_edge_ms"), (int, float)):
        errors.append("bad_type:blob_ts_edge_ms:expected_numeric")

    checksum = record.get("checksum_sha256", "")
    if len(checksum) != 64:
        errors.append(f"bad_checksum_length:{len(checksum)}")

    blob_uri = record.get("blob_uri", "")
    if not blob_uri.startswith("s3://"):
        errors.append(f"bad_blob_uri_scheme:{blob_uri[:20]}")

    return (len(errors) == 0), errors
