SELECT
  main.id,
  information.name,
  information.months,
  information.zipcode,
  information.distric,
  information.address,
  tag_list.tag_id,
  tag_list.tag_name,
  tag_list.tag_type,
FROM `pennylab.penny_test.attractions_tag` main
LEFT JOIN `pennylab.penny_test.attractions_tag_list` tag_list
USING(tag_id)
LEFT JOIN `pennylab.penny_test.attractions_information` information
USING(id)
order by id
