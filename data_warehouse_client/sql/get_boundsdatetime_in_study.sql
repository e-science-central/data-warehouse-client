SELECT boundsdatetime.measurementtype, boundsdatetime.minval, boundsdatetime.maxval
FROM boundsdatetime
WHERE boundsdatetime.study = $study
ORDER BY boundsdatetime.measurementtype;
