WITH tag_list AS(
    SELECT DISTINCT 'category' AS tag_type, flat_category.id AS tag_id, flat_category.name AS tag_name
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(category) AS flat_category
    UNION ALL
    SELECT DISTINCT 'target' AS tag_type, flat_target.id AS tag_id, flat_target.name AS tag_name
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(target) AS flat_target
    UNION ALL
    SELECT DISTINCT 'service' AS tag_type, flat_service.id AS tag_id, flat_service.name AS tag_name
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(service) AS flat_service
    UNION ALL
    SELECT DISTINCT 'friendly' AS tag_type, flat_friendly.id AS tag_id, flat_friendly.name AS tag_name
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(friendly) AS flat_friendly
)
SELECT *
FROM tag_list
