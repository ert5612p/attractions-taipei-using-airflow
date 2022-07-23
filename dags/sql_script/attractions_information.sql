WITH friendly AS(
    SELECT DISTINCT main.id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(friendly) AS flat_friendly
),
category AS(
    SELECT DISTINCT main.id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(category) AS flat_category
),
target AS(
    SELECT DISTINCT main.id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(target) AS flat_target
),
service AS(
    SELECT DISTINCT main.id
    FROM `pennylab.penny_test.attractoins_taipei` main,
    UNNEST(service) AS flat_service
)
SELECT
    id,
    name,
    IF(friendly.id IS NOT NULL, TRUE, FALSE) AS is_friendly,
    IF(category.id IS NOT NULL, TRUE, FALSE) AS is_category,
    IF(target.id IS NOT NULL, TRUE, FALSE) AS is_target,
    IF(service.id IS NOT NULL, TRUE, FALSE) AS is_service,
    zipcode,
    distric,
    TRIM(REPLACE(address, zipcode, "")) AS address,
    nlat,
    elong,
    name_zh,
    open_status,
    introduction,
    open_time,
    tel,
    fax,
    email,
    months,
    official_site,
    facebook,
    ticket,
    remind,
    staytime,
    url
FROM `pennylab.penny_test.attractoins_taipei`
LEFT JOIN friendly
USING(id)
LEFT JOIN category
USING(id)
LEFT JOIN target
USING(id)
LEFT JOIN service
USING(id)
order by id
