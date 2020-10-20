# Copyright 2020 Newcastle University.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import csv
from datetime import datetime
import re


def load_85_plus_file(dw, fname, study, trial, limit):
    """

    :param dw: data warehouse handle
    :param fname: file name
    :param study: study id
    :param trial: trial id
    :param limit: limit the number of accelerometry reading read in. Set to -1 to read in all data
    :return observations: number of observations read in to the warehouse
    """
    with open(fname, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        observation_mt = 0
        study_centre_mt = 2

        time_col = 0
        x_col = 1
        y_col = 2
        z_col = 3

        study_centre_value_col = 5

        row = next(csv_reader)
        study_centre       = ( 5, 2, row[study_centre_value_col])
        row = next(csv_reader)
        study_code         = ( 6, 2, row[study_centre_value_col])
        row = next(csv_reader)
        investigator_id    = ( 7, 0, row[study_centre_value_col])
        row = next(csv_reader)
        exercise_code      = ( 8, 2, row[study_centre_value_col])
        row = next(csv_reader)
        volunteer_number   = ( 9, 0, row[study_centre_value_col])
        row = next(csv_reader)
        body_location      = (10, 2, row[study_centre_value_col])
        row = next(csv_reader)
        hz = re.findall('[0-9]+', row[study_centre_value_col])[0]
        sample_rate        = (11, 1, hz)
        row = next(csv_reader)
        identifier         = (12, 2, row[study_centre_value_col])
        row = next(csv_reader)
        config_operator_id = (13, 0, row[study_centre_value_col])
        row = next(csv_reader)
        st = datetime.strptime(row[study_centre_value_col], "%Y %m %d %H.%M.%S")
        sts = st.strftime("%Y-%m-%d %H:%M:%S")
        start_time         = (14, 3, sts)
        row = next(csv_reader)
        serial_number      = (15, 0, row[study_centre_value_col])
        row = next(csv_reader)
        battery_voltage    = (16, 1, row[study_centre_value_col])
        row = next(csv_reader)
        ut = datetime.strptime(row[study_centre_value_col], "%Y %m %d %H.%M.%S")
        uts = ut.strftime("%Y-%m-%d %H:%M:%S")
        upload_time        = (17, 3, uts)
        row = next(csv_reader)
        upload_operator_id = (18, 0, row[study_centre_value_col])
        row = next(csv_reader)
        upload_finished    = (19, 2, row[study_centre_value_col])
        row = next(csv_reader)
        firmware_version   = (20, 2, row[study_centre_value_col])

        study_centre_values = [study_centre, study_code, investigator_id, exercise_code, volunteer_number,
                               body_location, sample_rate, identifier, config_operator_id, start_time,
                               serial_number, battery_voltage, upload_time, upload_operator_id,
                               upload_finished, firmware_version]
        #  insert participant
        (new_participant_added, participant_id) = dw.add_participant_if_new(study, volunteer_number[2],
                                                                            str(volunteer_number[2]))
        # insert source here
        # TO DO later
        # print(st, volunteer_number[2])

        dw.insertMeasurementGroup(study, study_centre_mt, study_centre_values, time=st ,
                                  trial=trial, participant=volunteer_number[2])   #  ignore source for now

        row = next(csv_reader)
        while len(row) == 0:                    # read past the empty rows
            row = next(csv_reader)
        eof = False
        observation_number = 0
        while (not eof) and (observation_number < limit):  # arbitrary limit for testing (remove for "production" use)
            observation_number = observation_number + 1
            t = datetime.strptime(row[time_col]+"00", "%Y-%m-%d %H:%M:%S.%f")
            # t_obs = t.strftime("%Y-%m-%d %H:%M:%S.%f")
            # print(t)
            n_observation      = (1, 0, observation_number)
            x_acc              = (2, 1, row[x_col])
            y_acc              = (3, 1, row[y_col])
            z_acc              = (4, 1, row[z_col])
            observation_values = [n_observation, x_acc, y_acc, z_acc]
            dw.insertMeasurementGroup(study, observation_mt, observation_values,
                                            time=t, trial=trial, participant=volunteer_number[2])
            try:
                row = next(csv_reader)
                eof = False
            except StopIteration:
                eof = True

        print(f'Processed {observation_number} lines of accelerometry data.')
        return observation_number
