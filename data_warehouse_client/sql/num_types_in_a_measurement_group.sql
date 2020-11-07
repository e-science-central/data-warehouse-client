SELECT
  COUNT(*)
FROM
  measurementtypetogroup
WHERE
  measurementtypetogroup.measurementgroup = $measurement_group
  AND measurementtypetogroup.study = $study;
