$core_sql
$where_clause
$condition measurement.participant IN (
  SELECT
    measurement.participant
FROM
  measurement
WHERE
  measurement.measurementtype = 181
  AND measurement.valinteger = $cohort_id
  AND measurement.study = $study
  )
ORDER BY
  measurement.time,
  measurement.groupinstance,
  measurement.measurementtype;
