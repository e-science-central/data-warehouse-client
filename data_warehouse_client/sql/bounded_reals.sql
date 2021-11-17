SELECT
  DISTINCT measurement.id, measurement.valreal, measurement.measurementtype, measurement.measurementgroup, boundsreal.minval, boundsreal.maxval, measurement.participant
FROM
  measurement
  JOIN boundsreal ON (
    measurement.measurementtype = boundsreal.measurementtype
    AND measurement.study = boundsreal.study
  )
WHERE
  measurement.valtype = 8
  AND measurement.study = $study
  AND (
    measurement.valreal < boundsreal.minval
    OR measurement.valreal > boundsreal.maxval
  )
ORDER BY
  measurement.id;
