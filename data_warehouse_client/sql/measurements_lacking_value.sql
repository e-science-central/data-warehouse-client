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
    (measurement.valtype = 2)
    AND (textvalue.textval = NULL)
  )
  OR (
    (measurement.valtype = 3)
    AND (datetimevalue.datetimeval = NULL)
  );
