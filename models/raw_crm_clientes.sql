{{ config(
    schema='raw'
) }}


SELECT 

    JSON_VALUE(product, '$.id') AS product_id,
    JSON_VALUE(product, '$.title') AS title,
    JSON_VALUE(product, '$.price') AS price,
    JSON_VALUE(product, '$.total') AS total,
    JSON_VALUE(product, '$.quantity') AS quantity,
    JSON_VALUE(product, '$.discountPercentage') AS discountPercentage,
    JSON_VALUE(product, '$.discountedTotal') AS product_discountedTotal

FROM
    `dbt-bigquery-493617.raw.raw_sales_data`,
    UNNEST(JSON_EXTRACT_ARRAY(carts)) AS cart,
    UNNEST(JSON_EXTRACT_ARRAY(cart, '$.products')) AS product
