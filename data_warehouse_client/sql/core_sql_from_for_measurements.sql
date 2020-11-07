FROM
  measurement
  INNER JOIN measurementtype ON measurement.measurementtype = measurementtype.id
  AND measurement.study = measurementtype.study
  INNER JOIN measurementtypetogroup ON measurement.measurementgroup = measurementtypetogroup.measurementgroup
  AND measurement.measurementtype = measurementtypetogroup.measurementtype
  AND measurement.study = measurementtypetogroup.study
  LEFT OUTER JOIN textvalue ON textvalue.measurement = measurement.id
  AND textvalue.study = measurement.study
  LEFT OUTER JOIN datetimevalue ON datetimevalue.measurement = measurement.id
  AND datetimevalue.study = measurement.study
  LEFT OUTER JOIN category ON measurement.valinteger = category.categoryid
  AND measurement.measurementtype = category.measurementtype
  AND measurement.study = category.study
