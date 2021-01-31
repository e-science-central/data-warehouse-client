SELECT
  measurementtype.id,
  measurementtype.description
FROM
  measurementtype
WHERE
  measurementtype.valtype = 7
  AND measurementtype.study = $study
  AND measurementtype.id NOT IN (
    SELECT
      measurementtype.id
    FROM
      measurementtype
      INNER JOIN boundsint ON (
        measurementtype.id = boundsint.measurementtype
        AND measurementtype.study = boundsint.study
      )
  )
ORDER BY
  measurementtype.id;
