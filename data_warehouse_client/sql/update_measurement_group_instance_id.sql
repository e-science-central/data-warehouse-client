UPDATE measurement
SET groupinstance = $group_instance_id
WHERE id = $measurement_id;
