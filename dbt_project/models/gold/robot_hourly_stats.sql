-- Gold layer: per-robot hourly statistics.
-- Pre-aggregated for dashboard and researcher queries.

SELECT
    robot_id,
    date_trunc('hour', event_ts_corrected) AS hour_bucket,
    sensor_type,
    count(*)                          AS event_count,
    avg(torque)                       AS avg_torque,
    max(torque)                       AS max_torque,
    avg(temperature)                  AS avg_temperature,
    max(temperature)                  AS max_temperature,
    avg(joint_angle)                  AS avg_joint_angle,
    min(event_ts_corrected)           AS first_event,
    max(event_ts_corrected)           AS last_event
FROM {{ ref('telemetry_validated') }}
WHERE quality_flag = 'VALID'
GROUP BY
    robot_id,
    date_trunc('hour', event_ts_corrected),
    sensor_type
