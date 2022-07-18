{{ config(
    materialized="table"
) }}

WITH A AS(
    SELECT main.id, flat_category.id AS tag_id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(category) AS flat_category
    UNION ALL
    SELECT main.id, flat_target.id AS tag_id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(target) AS flat_target
    UNION ALL
    SELECT main.id, flat_service.id AS tag_id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(service) AS flat_service
    UNION ALL
    SELECT main.id, flat_friendly.id AS tag_id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(friendly) AS flat_friendly
)
SELECT *
FROM A