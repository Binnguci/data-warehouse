{{ config(
    materialized='table',
    schema='data_staging'
) }}

SELECT
    CAST(field_1 AS STRING) AS field_1,
    CAST(field_2 AS STRING) AS field_2,
    CAST(field_3 AS STRING) AS field_3,
    CAST(field_4 AS STRING) AS field_4,
    CAST(field_5 AS STRING) AS field_5,
    CAST(field_6 AS STRING) AS field_6,
    CAST(field_7 AS STRING) AS field_7,
    CAST(field_8 AS STRING) AS field_8,
    CAST(field_9 AS STRING) AS field_9,
    CAST(field_10 AS STRING) AS field_10,
    CAST(field_11 AS STRING) AS field_11,
    CAST(field_12 AS STRING) AS field_12,
    CAST(field_13 AS STRING) AS field_13,
    CAST(field_14 AS STRING) AS field_14,
    CAST(field_15 AS STRING) AS field_15,
    CAST(field_16 AS STRING) AS field_16,
    CAST(field_17 AS STRING) AS field_17,
    CAST(field_18 AS STRING) AS field_18,
    CAST(field_19 AS STRING) AS field_19,
    CAST(field_20 AS STRING) AS field_20,
    CAST(field_21 AS STRING) AS field_21
FROM {{ source('raw', 'fptshop_data') }}
WHERE field_1 IS NOT NULL