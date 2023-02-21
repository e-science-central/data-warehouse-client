SELECT
  category.categoryid,
  category.measurementtype
FROM
  category
WHERE
  category.study = $study

