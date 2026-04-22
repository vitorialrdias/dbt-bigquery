CREATE OR REPLACE TABLE `dbt-bigquery-493617.dataclean_dbt.news_data_raw` AS
SELECT
    -- Extraindo campos de dentro do array 'carts'
    cart.id AS cart_id,
    cart.total AS total_value,
    cart.title AS title,
    cart.price as price,
    cart.quantity as quantity,
    cart.discountPercentage as discountPercentage,
    cart.discountedTotal as discountedTotal
FROM
    `dbt-bigquery-493617.dataclean_dbt.news_data`,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.carts')) AS cart