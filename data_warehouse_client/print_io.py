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


def print_rows(rows, header: List[str]):
    """
    prints each row returned by a query
    :param rows: a list of rows. Each row is a list of fields
    :param header: a list of field names
    """
    print(tabulate(rows, headers=header))


def print_measurement_group_instances(header, instances):
    """
    Prints a list of measurement group instances, converting the datetime to strings
    The output file has a row for each measurement group instance. The fields are:
        groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                value1, value2....
            where value n is the value for the nth measurement in the instance (ordered by measurement type)
    :param header: a list of column names
    :param instances: a list of measurement group instances in the format produced by formatMeasurementGroup
    """
    print(tabulate(instances, headers=header))


def print_measurements(rows):
    """
    Prints a list of measurements, converting the datetimes to strings
    :param rows: a list of measurements with the elements id,time,study,participant,measurementType,
                    typeName,measurementGroup,groupInstance,trial,valType,value
    """
    header_row: List[str] = ["Id", "Time", "Study", "Participant", "MeasurementType", "Type Name",
                             "Measurement Group", "Group Instance", "Trial", "Val Type", "Value"]
    print(tabulate(rows, headers=header_row))
