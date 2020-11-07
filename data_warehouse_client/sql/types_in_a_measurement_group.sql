SELECT
  measurementtypetogroup.name
FROM
  measurementtypetogroup
WHERE
  measurementtypetogroup.measurementgroup = $measurement_group
  AND measurementtypetogroup.study = $study
ORDER BY
  measurementtypetogroup.measurementtype;
