SELECT category.measurementtype, category.categoryid, category.categoryname
FROM category
WHERE category.study = $study
ORDER BY category.measurementtype, category.categoryid;
