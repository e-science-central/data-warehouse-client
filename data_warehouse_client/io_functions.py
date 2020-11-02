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

from typing import List
from tabulate import tabulate
import csv
import matplotlib.pyplot as pyplot


def print_rows(rows, header: List[str]):
    """
    prints each row returned by a query
    :param rows: a list of rows. Each row is a list of fields
    :param header: a list of field names
    """
    print(tabulate(rows, headers=header))


def export_measurements_as_csv(rows, fname):
    """
    Stores measurements returned by queries in a CSV file
    The input rows must be in the format produced by:
        getMeasurements, getMeasurementsWithValueTest or getMeasurementGroupInstancesWithValueTests
        The output file has a header row, followed by a row for each measurement. This has the columns:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
    :param rows: a list of rows returned by getMeasurements, getMeasurementsWithValueTest or
                    getMeasurementGroupInstancesWithValueTests
    :param fname: the filename of the output CSV file
    """
    with open(fname, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Id", "Time", "Study", "Participant", "Measurement Type", "Measurement Type Name", "Measurement Group",
             "Measurement Group Instance", "Trial", "Value Type", "Value"])
        writer.writerows(rows)


def export_measurement_groups_as_csv(dw, rows, group_id, study, fname):
    """
    Stores measurements returned by formMeasurementGroups in a CSV file
    The input rows must be in the format produced by formMeasurementGroups
    The output file has a header row, followed by a row for each measurement group instance. The table has columns:
        groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                value1, value2....
            where value n is the value for the nth measurement in the instance (ordered by measurement type)
    :param dw: data warehouse handle
    :param rows: a list of rows returned by formatMeasurementGroup
    :param group_id: the measurementGroupId
    :param study: study id
    :param fname: the filename of the output CSV file
    """
    type_names: List[str] = dw.get_types_in_a_measurement_group(study, group_id)
    with open(fname, "w", newline="") as f:
        writer = csv.writer(f)
        header_row: List[str] = ["Measurement Group Instance", "Time", "Study", "Participant", "Measurement Group",
                                 "Trial"]
        for t in range(len(type_names)):
            header_row.append(type_names[t][0])
        writer.writerow(header_row)
        writer.writerows(rows)


def print_measurement_group_instances(dw, rows, group_id, study):
    """
    Prints a list of measurement group instances, converting the datetime to strings
    The input rows must be in the format produced by formMeasurementGroups
    The output file has a row for each measurement group instance. The fields are:
        groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                value1, value2....
            where value n is the value for the nth measurement in the instance (ordered by measurement type)
    :param dw: data warehouse handle
    :param rows: a list of measurement group instances in the format produced by formatMeasurementGroup
    :param group_id: the measurementGroupId
    :param study: study id
    """
    type_names: List[str] = dw.get_types_in_a_measurement_group(study, group_id)
    n_types: int = len(type_names)
    header_row: List[str] = ["Measurement Group Instance", "Time", "Study", "Participant", "Measurement Group",
                             "Trial"]
    for t in range(n_types):
        header_row.append(type_names[t][0])
    print(tabulate(rows, headers=header_row))


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


def print_measurements(rows):
    """
    Prints a list of measurements, converting the datetimes to strings
    :param rows: a list of measurements with the elements id,time,study,participant,measurementType,
                    typeName,measurementGroup,groupInstance,trial,valType,value
    """
    header_row: List[str] = ["Id", "Time", "Study", "Participant", "MeasurementType", "Type Name",
                             "Measurement Group", "Group Instance", "Trial", "Val Type", "Value"]
    print(tabulate(rows, headers=header_row))


def plot_measurement_type(dw, rows, measurement_type_id, study, plot_file):
    """
    Plot the value of a measurement over time.
    :param dw: data warehouse handle
    :param rows: a list of measurements generated by the other client functions. Each measurement is in the form:
                    id,time,study,participant,measurementType,
                    typeName,measurementGroup,groupInstance,trial,valType,value
    :param measurement_type_id: the measurement type of the measurements to be plotted
    :param study: study id
    :param plot_file: the name of the file into which the plot will be written
    """
    # https://matplotlib.org/api/pyplot_api.html
    trans = [list(i) for i in zip(*rows)]  # transpose the list of lists
    x = trans[1]  # the data and time
    y = trans[10]  # the measurement value

    mt_info = dw.get_measurement_type_info(study, measurement_type_id)
    units = mt_info[0][3]  # get the units name
    pyplot.title(rows[0][5])
    pyplot.xlabel("Time")  # Set the x-axis label
    pyplot.ylabel(units)  # Set the y-axis label to be the units of the measurement type
    pyplot.plot(x, y)
    pyplot.savefig(plot_file)
    pyplot.close()
