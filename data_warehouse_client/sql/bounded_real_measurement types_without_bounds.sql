SELECT
  measurementtype.id,
  measurementtype.description
FROM
  measurementtype
WHERE
  measurementtype.valtype = 8
  AND measurementtype.study = $study
  AND measurementtype.id NOT IN (
    SELECT
      measurementtype.id
    FROM
      measurementtype
      INNER JOIN boundsreal ON (
        measurementtype.id = boundsreal.measurementtype
        AND measurementtype.study = boundsreal.study
      )
  )
ORDER BY
  measurementtype.id;
