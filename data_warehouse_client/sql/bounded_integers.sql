SELECT
  DISTINCT measurement.id, measurement.valinteger
FROM
  measurement
  JOIN boundsint ON (
    measurement.measurementtype = boundsint.measurementtype
    AND measurement.study = boundsint.study
  )
WHERE
  measurement.valtype = 7
  AND measurement.study = $study
  AND (
    measurement.valinteger < boundsint.minval
    OR measurement.valinteger > boundsint.maxval
  )
ORDER BY
  measurement.id;
