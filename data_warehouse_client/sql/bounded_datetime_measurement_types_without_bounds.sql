SELECT
  measurementtype.id,
  measurementtype.description
FROM
  measurementtype
WHERE
  measurementtype.valtype = 9
  AND measurementtype.study = $study
  AND measurementtype.id NOT IN (
    SELECT
      measurementtype.id
    FROM
      measurementtype
      INNER JOIN boundsdatetime ON (
        measurementtype.id = boundsdatetime.measurementtype
        AND measurementtype.study = boundsdatetime.study
      )
  )
ORDER BY
  measurementtype.id;