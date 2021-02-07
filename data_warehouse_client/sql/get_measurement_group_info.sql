SELECT
  measurementtypetogroup.measurementgroup,
  measurementtypetogroup.measurementtype,
  measurementtypetogroup.name,
  measurementtype.valtype,
  measurementtypetogroup.optional,
  measurementtype.units
FROM measurementtype
JOIN measurementtypetogroup
  ON ((measurementtypetogroup.measurementtype = measurementtype.id)
  AND (measurementtypetogroup.study = measurementtype.study))
WHERE (measurementtypetogroup.study = $study)
ORDER BY measurementtypetogroup.measurementgroup, measurementtypetogroup.measurementtype;
