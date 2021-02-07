SELECT measurementgroup.id, measurementgroup.description
FROM measurementgroup
WHERE measurementgroup.study = $study
ORDER BY measurementgroup.id;
