SELECT
  measurementtypetogroup.measurementgroup,
  measurementtypetogroup.measurementtype,
  measurementtypetogroup.name
FROM
  measurementtypetogroup
WHERE
  measurementtypetogroup.study = $study
ORDER BY
  measurementtypetogroup.measurementgroup,
  measurementtypetogroup.measurementtype;
