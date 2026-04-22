{{ config(
    materialized='incremental',
    unique_key=['product_id'],
    incremental_strategy='merge',
    schema='clean'
) }}


SELECT
    product_id,
    LOWER(IFNULL(title, 'blank')) AS title,
    CAST(quantity AS INT64) AS quantity,
    CAST(price AS NUMERIC) AS price,
    IFNULL(
        CAST(total AS NUMERIC), 
        CAST(price AS NUMERIC) * CAST(quantity AS NUMERIC)
    ) AS total,
    CAST(discountPercentage AS NUMERIC) AS discount,
    IFNULL(CAST(product_discountedTotal AS NUMERIC), 
        CAST(total AS NUMERIC) - CAST(discountPercentage AS NUMERIC)
    ) AS total_value

FROM
    `dbt-bigquery-493617.raw.raw_crm_clientes`