INSERT INTO measurement (id, time, study, trial, measurementgroup, groupinstance,
                         measurementtype, participant, source, valtype, valinteger, valreal)
VALUES (DEFAULT, $time, $study, $trial, $measurementgroup, $groupinstance,
        $measurementtype, $participant, $source, $valtype, $valinteger, $valreal)
RETURNING id;
