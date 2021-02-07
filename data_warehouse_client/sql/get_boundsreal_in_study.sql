SELECT boundsreal.measurementtype, boundsreal.minval, boundsreal.maxval
FROM boundsreal
WHERE boundsreal.study = $study
ORDER BY boundsreal.measurementtype;
