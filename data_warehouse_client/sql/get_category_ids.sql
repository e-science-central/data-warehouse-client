SELECT
  category.id,
  category.measurementtype
FROM
  category
WHERE
  category.study = $study

