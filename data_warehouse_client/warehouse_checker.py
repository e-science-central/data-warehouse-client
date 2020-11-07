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


# Warehouse Checker code - check:
# valtype field points to non-NULL value field
# The measurementType is in the measurement group for each measurement
# The valtype is correct for the measurement type for each measurement
# the measurement group is valid for the study
# there are no missing measurement types in a measurement group instance
# no ordinal, nominal, bounded integer or bounded real values are out of bounds


from data_warehouse_client import file_utils
from data_warehouse_client import data_warehouse
from data_warehouse_client import print_io
from tabulate import tabulate


def check_category_exists(dw, study):
    """
    Find measurements of nominal or ordinal type whose value does not equal that of a category
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("sql/ordinal_types_not_matching_category.sql", mappings)
    return dw.return_query_result(query)


def check_valtype_matches_values(dw, study):
    """
    Find measurements that lack a value
    :param dw: handle to data warehouse
    :param study: study id
    :return: the measurements in the study that fail the test
    """
    mappings = {"study": str(study), "core_sql": data_warehouse.core_sql_for_measurements()}
    query = file_utils.process_sql_template("sql/measurements_lacking_value.sql", mappings)
    return dw.return_query_result(query)


def check_category_in_range(dw, study):
    """
    Returns the ids of measurements that refer to a non-existent category
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study), "core_sql": data_warehouse.core_sql_for_measurements()}
    query = file_utils.process_sql_template("sql/measurements_lacking_value.sql", mappings)
    return dw.return_query_result(query)


def check_bounded_integers(dw, study):
    """
    Returns the ids of measurements that hold bounded integers that are out of range
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("sql/bounded_integers.sql", mappings)
    return dw.return_query_result(query)


def check_bounded_reals(dw, study):
    """
    Returns the ids of measurements that hold bounded reals that are out of range
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("sql/bounded_reals.sql", mappings)
    return dw.return_query_result(query)


def print_check_warhouse(dw, study):
    """
    Prints the results of all teh tests that check the warehouse for errors
    :param dw: handle to data warehouse
    :param study: study id
    :return:
    """
    print(f'Check Study {study}')
    print()
    print(f'Check for invalid entries in the measurement table')
    r1 = check_valtype_matches_values(dw, study)
    n_invalid_entries = len(r1)
    if n_invalid_entries>0:
        print_io.print_measurements(r1)
    print(f'({n_invalid_entries} invalid entries)')

    print()
    print(f'Check for measurement types declared as ordinal or nominal but without entries in the category table')
    r2 = check_category_exists(dw, study)
    n_errors = len(r2)
    if n_errors>0:
        print(tabulate(r2, headers=['Measurement Type','Category Name']))
    print(f'({n_errors} invalid entries)')

    print()
    print(f'Check for measurements declared as ordinal or nominal but without a matching entry in the category table')
    r3 = check_category_in_range(dw, study)
    n_errors = len(r3)
    if n_errors>0:
        print(tabulate(r3, headers=['Measurement Id']))
    print(f'({n_errors} measurements)')

    print()
    print(f'Check for measurements declared as bounded integers whose value is outside of the bounds')
    r4 = check_bounded_integers(dw, study)
    n_errors = len(r4)
    if n_errors>0:
        print(tabulate(r4, headers=['Id', 'Value']))
    print(f'({n_errors} measurements)')

    print()
    print(f'Check for measurements declared as bounded reals whose value is outside of the bounds')
    r5 = check_bounded_reals(dw, study)
    n_errors = len(r5)
    if n_errors>0:
        print(tabulate(r5, headers=['Id','Value']))
    print(f'({n_errors} measurements)')
    print()
