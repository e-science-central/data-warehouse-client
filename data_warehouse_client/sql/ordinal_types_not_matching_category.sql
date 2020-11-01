SELECT
  measurementtype.id,
  measurementtype.description
FROM
  measurementtype
WHERE
  measurementtype.valtype IN (5, 6)
  AND measurementtype.study = $study
  AND measurementtype.id NOT IN (
    SELECT
      measurementtype.id
    FROM
      measurementtype
      INNER JOIN category ON (
        measurementtype.id = category.measurementtype
        AND measurementtype.study = category.study
      )
  )
ORDER BY
  measurementtype.id;
