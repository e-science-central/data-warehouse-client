$core_sql
WHERE
  measurement.study = $study
  AND (
    (measurement.valtype IN (0, 4, 5, 6, 7))
    AND (measurement.valinteger = NULL)
  )
  OR (
    (measurement.valtype IN (1, 8))
    AND (measurement.valreal = NULL)
  )
  OR (
    (measurement.valtype IN (2,10))
    AND (textvalue.textval = NULL)
  )
  OR (
    (measurement.valtype IN (3, 9))
    AND (datetimevalue.datetimeval = NULL)
  );
