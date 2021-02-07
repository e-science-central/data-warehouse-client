SELECT measurementtype.id, units.name
FROM units, measurementtype
WHERE units.id = measurementtype.units and units.study = $study and measurementtype.study = $study;
