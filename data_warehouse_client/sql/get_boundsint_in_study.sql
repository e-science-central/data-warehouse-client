SELECT boundsint.measurementtype, boundsint.minval, boundsint.maxval
FROM boundsint
WHERE boundsint.study = $study
ORDER BY boundsint.measurementtype;
