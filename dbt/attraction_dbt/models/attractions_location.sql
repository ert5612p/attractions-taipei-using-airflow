{{ config(
    materialized="table"
) }}

WITH A AS(
    SELECT id, zipcode, distric, address, nlat, elong
    FROM `pennylab.penny_test.attractoins_taipei`
)
SELECT *
FROM A
