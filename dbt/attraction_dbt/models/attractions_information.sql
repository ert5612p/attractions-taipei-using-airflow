{{ config(
    materialized="table"
) }}

WITH A AS(
    SELECT
        id,
        name,
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
)
SELECT *
FROM A
