{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY field_1) AS id,
    field_1 AS name,
    CAST(field_2 AS INT) AS price,
    CAST(field_3 AS DATE) AS release_date,
    CAST(field_4 AS DOUBLE) AS screen_size,
    field_5 AS gpu,
    field_6 AS front_camera,
    field_7 AS rear_camera,
    field_8 AS color,
    field_9 AS security,
    field_10 AS bluetooth,
    field_11 AS battery_capacity,
    field_12 AS communication_port,
    field_13 AS brand,
    field_14 AS ram,
    field_15 AS cpu,
    field_16 AS operating_system,
    field_17 AS memory,
    field_18 AS chip,
    field_19 AS image,
    field_20 AS date
FROM {{ ref('staging_phone_cellphones_temp') }}