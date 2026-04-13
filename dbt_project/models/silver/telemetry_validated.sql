-- Silver layer: telemetry with domain validation applied.
-- Filters out physically impossible readings.
-- This is what researchers should query instead of raw bronze.

SELECT
    event_id,
    robot_id,
    session_id,
    event_ts_edge_ms,
    event_ts_corrected,
    ingest_ts,
    sensor_type,
    joint_angle,
    joint_velocity,
    force_x,
    force_z,
    torque,
    accel_x,
    gyro_z,
    temperature,
    ingest_status,
    CASE
        WHEN temperature < -40 OR temperature > 150 THEN 'TEMP_OUT_OF_RANGE'
        WHEN torque < 0 OR torque > 300 THEN 'TORQUE_OUT_OF_RANGE'
        WHEN joint_angle < -6.3 OR joint_angle > 6.3 THEN 'ANGLE_OUT_OF_RANGE'
        ELSE 'VALID'
    END AS quality_flag
FROM {{ source('bronze', 'telemetry_events') }}
WHERE
    -- Hard filters: reject physically impossible values
    temperature BETWEEN -40 AND 150
    AND torque BETWEEN 0 AND 300
    AND event_ts_corrected IS NOT NULL
