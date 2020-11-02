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
