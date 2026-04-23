{{ config(
    materialized='incremental',
    unique_key=['product_id'],
    incremental_strategy='merge',
    schema='clean'
) }}

SELECT
    product_id,
    LOWER(IFNULL(title, 'blank')) AS title,
    IFNULL(SAFE_CAST(CAST(quantity AS STRING) AS INT64), 0)   AS quantity,
    IFNULL(SAFE_CAST(CAST(price AS STRING) AS NUMERIC), 0)    AS price,
    IFNULL(
        SAFE_CAST(CAST(total AS STRING) AS NUMERIC),
        SAFE_CAST(CAST(price AS STRING) AS NUMERIC) * SAFE_CAST(CAST(quantity AS STRING) AS NUMERIC)
    ) AS total,
    IFNULL(
        SAFE_CAST(CAST(discountPercentage AS STRING) AS NUMERIC),
        ROUND(
            (1 - SAFE_CAST(CAST(product_discountedTotal AS STRING) AS NUMERIC) 
            / SAFE_CAST(CAST(total AS STRING) AS NUMERIC)) * 100
        , 2)
    ) AS discount,
    IFNULL(
    SAFE_CAST(product_discountedTotal AS NUMERIC),
    CAST(total AS NUMERIC) * (1 - IFNULL(SAFE_CAST(CAST(discountPercentage AS STRING) AS NUMERIC), 0) / 100)
    ) AS total_value
FROM
    `dbt-bigquery-493617.raw.raw_crm_clientes`