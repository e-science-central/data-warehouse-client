SELECT
  DISTINCT measurement.id, datetimevalue.datetimeval, measurement.measurementtype, measurement.measurementgroup, boundsdatetime.minval, boundsdatetime.maxval, measurement.participant
FROM
  measurement
  LEFT OUTER JOIN datetimevalue ON datetimevalue.measurement = measurement.id
  AND datetimevalue.study = measurement.study
  JOIN boundsdatetime ON (
    measurement.measurementtype = boundsdatetime.measurementtype
    AND measurement.study = boundsdatetime.study
  )
WHERE
  measurement.valtype = 9
  AND measurement.study = $study
  AND (
    datetimevalue.datetimeval < boundsdatetime.minval
    OR datetimevalue.datetimeval > boundsdatetime.maxval
  )
ORDER BY
  measurement.id;
