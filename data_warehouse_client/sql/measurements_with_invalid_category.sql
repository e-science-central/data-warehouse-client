SELECT
  measurement.id
FROM
  measurement
WHERE
  measurement.study = $study
  AND measurement.valtype IN (5, 6)
  AND measurement.id NOT IN (
    SELECT
      DISTINCT measurement.id
    FROM
      measurement
      JOIN category ON (
        measurement.measurementtype = category.measurementtype
        AND measurement.study = category.study
        AND measurement.valinteger = category.categoryid
      )
    WHERE
      measurement.valtype IN (5, 6)
  )
ORDER BY
  measurement.id;
