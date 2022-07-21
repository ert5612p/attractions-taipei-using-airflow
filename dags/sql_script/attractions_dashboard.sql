SELECT
  main.id,
  information.name,
  tag_list.tag_id,
  tag_list.tag_name,
  tag_list.tag_type,
  location.zipcode,
  location.distric,
  TRIM(REPLACE(location.address, location.zipcode, "")) AS address,
  location.nlat,
  location.elong
FROM {{ref('attractions_tag')}} main
LEFT JOIN {{ref('attractions_tag_list')}} tag_list
USING(tag_id)
LEFT JOIN {{ref('attractions_information')}} information
USING(id)
LEFT JOIN {{ref('attractions_location')}} location
USING(id)
order by id