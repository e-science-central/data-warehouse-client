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

def form_measurements(rows):
    """
    The raw query results within getMeasurements, getMeasurementsWithValueTest and
        getMeasurementGroupInstancesWithValueTests contain a column for each possible type of value:
            integer, real, datatime, string. Each is set to null apart from the one that holds the value appropriate
            for the type of measurement. This function replaces those columns with a single field holding the actual
            value
    :param rows: list of rows returned by a query
    :return: list of rows, each representing one measurement in a list with elements:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
    """
    n_rows: int = len(rows)
    n_cols: int = 11
    row_out = [[None] * n_cols for i in range(n_rows)]
    val_type_index = 9
    value_index = 10
    for r in range(n_rows):
        # Indices: 0 = id, 1 = time, 2 = study, 3 = participant, 4 = measurementType, 5 = measurementTypeName,
        # 6 = measurementGroup, 7 = measurementGroupInstance, 8 = trial, 9 = valType, 10 = value
        for x in range(value_index):
            row_out[r][x] = rows[r][x]
        if rows[r][val_type_index] == 0:  # integer
            row_out[r][value_index] = rows[r][10]
        elif rows[r][val_type_index] == 1:  # real
            row_out[r][value_index] = rows[r][11]
        elif rows[r][val_type_index] == 2:  # text
            row_out[r][value_index] = rows[r][12]
        elif rows[r][val_type_index] == 3:  # datetime
            row_out[r][value_index] = rows[r][13]
        elif rows[r][val_type_index] == 4 and rows[r][value_index] == 0:  # boolean False
            row_out[r][value_index] = "F"
        elif rows[r][val_type_index] == 4 and rows[r][value_index] == 1:  # boolean True
            row_out[r][value_index] = "T"
        elif rows[r][val_type_index] == 5:  # nominal
            row_out[r][value_index] = rows[r][14]
        elif rows[r][val_type_index] == 6:  # ordinal
            row_out[r][value_index] = rows[r][14]
        elif rows[r][val_type_index] == 7:  # boundedint
            row_out[r][value_index] = rows[r][10]
        elif rows[r][val_type_index] == 8:  # boundedreal
            row_out[r][value_index] = rows[r][11]
        else:
            print("typeval error of ", rows[r][9], " for id", rows[r][0], " study ", rows[r][2])
    return row_out


def form_measurement_group(dw, study, rows):
    """
    Creates a result where each measurement group instance occupies one row.
    :param dw: data warehouse handle
    :param study: study id
    :param rows: list of rows returned by getMeasurementGroupInstancesWithValueTests or
                    getMeasurements (where it returns whole measurement group instances - i.e. where
                                    measurementType is not specified, but measurement group or
                                    measurement group instance is specified)
    :return: list of rows, each representing one measurement group instance, held in a list with elements:
            groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                value1, value2....
            where value n is the value for the nth measurement in the instance (ordered by measurement type)
            Null is used if a value is missing
    """
    if len(rows) > 0:
        measurement_group: int = rows[0][6]
        mts = dw.get_type_ids_in_measurement_group(study, measurement_group)
        result_values = {}  # the measurement values
        result_common = {}  # the common values returned for each instance:
        # instance,time of first measurement,study,participant,measurementGroup,trial
        for (row_id, time, study_id, participant, mt, tn, mg, mgi, trial, val_type, value) in rows:
            if not (mgi in result_common):
                result_common.update({mgi: [mgi, time, study, participant, mg, trial]})
                result_values.update({mgi: {}})
            result_values[mgi][mt] = value  # add values to the dictionary
        result = []
        for instance in result_values:
            val_dict = result_values[instance]
            values = []
            for mt in mts:
                val = val_dict.get(mt, None)
                values = values + [val]
            result = result + [(result_common[instance] + values)]
    else:
        result = []
    return result
