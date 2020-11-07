SELECT
  measurementtype.id,
  measurementtype.description,
  measurementtype.valtype,
  units.name
FROM
  measurementtype
  LEFT OUTER JOIN units ON measurementtype.units = units.id
  AND measurementtype.study = units.study
WHERE
  measurementtype.id = $measurement_type_id
  AND measurementtype.study = $study;
