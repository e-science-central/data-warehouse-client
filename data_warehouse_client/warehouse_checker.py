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
import datetime


def check_category_exists(dw, study):
    """
    Find measurement types of nominal or ordinal value type without entry in category table
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids and names of measurement types in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("ordinal_types_not_matching_category.sql", mappings)
    return dw.return_query_result(query)


def check_integer_bounds_exist(dw, study):
    """
    Find measurement types of bounded integer value type without entry in boundsinteger table
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids and names of measurement types in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("bounded_integer_measurement_types_without_bounds.sql", mappings)
    return dw.return_query_result(query)


def check_real_bounds_exist(dw, study):
    """
    Find measurement types of bounded real value type without entry in boundsreal table
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids and names of measurement types in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("bounded_real_measurement_types_without_bounds.sql", mappings)
    return dw.return_query_result(query)


def check_valtype_matches_values(dw, study):
    """
    Find measurements that lack a value
    :param dw: handle to data warehouse
    :param study: study id
    :return: the measurements in the study that fail the test
    """
    mappings = {"study": str(study), "core_sql": data_warehouse.core_sql_for_measurements()}
    query = file_utils.process_sql_template("measurements_lacking_value.sql", mappings)
    return dw.return_query_result(query)


def check_category_in_range(dw, study):
    """
    Returns the ids of measurements that refer to a non-existent category
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study), "core_sql": data_warehouse.core_sql_for_measurements()}
    query = file_utils.process_sql_template("measurements_lacking_value.sql", mappings)
    return dw.return_query_result(query)


def check_bounded_integers(dw, study):
    """
    Returns the ids of measurements that hold bounded integers that are out of range
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("bounded_integers.sql", mappings)
    return dw.return_query_result(query)


def check_bounded_reals(dw, study):
    """
    Returns the ids of measurements that hold bounded reals that are out of range
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    mappings = {"study": str(study)}
    query = file_utils.process_sql_template("bounded_reals.sql", mappings)
    return dw.return_query_result(query)


def print_check_warehouse(dw, study):
    """
    Prints on stdout the results of all the tests that check the warehouse for errors
    :param dw: handle to data warehouse
    :param study: study id
    :return:
    """
    print(f'Check Study {study}')
    print()

    print(f'- Check Metadata')
    print()

    print(f'-- Measurement types declared as ordinal or nominal but without entries in the Category Table')
    r2 = check_category_exists(dw, study)
    n_errors = len(r2)
    if n_errors > 0:
        print(tabulate(r2, headers=['Measurement Type Id', 'Message Type Name']))
    print(f'({n_errors} invalid entries)')

    print(f'-- Measurement types declared as bounded integer but without entries in the Boundsint Table')
    rbi = check_integer_bounds_exist(dw, study)
    n_errors = len(rbi)
    if n_errors > 0:
        print(tabulate(rbi, headers=['Measurement Type Id', 'Message Type Name']))
    print(f'({n_errors} invalid entries)')

    print(f'-- Measurement types declared as bounded real but without entries in the Boundsreal Table')
    rbr = check_real_bounds_exist(dw, study)
    n_errors = len(rbr)
    if n_errors > 0:
        print(tabulate(rbr, headers=['Measurement Type Id', 'Message Type Name']))
    print(f'({n_errors} invalid entries)')

    print(f'- Check Measurements')
    print()

    print(f'-- Measurements where the Value Type does not match the values stored in the Measurement Table')
    r1 = check_valtype_matches_values(dw, study)
    n_invalid_entries = len(r1)
    if n_invalid_entries > 0:
        print_io.print_measurements(r1)
    print(f'({n_invalid_entries} invalid entries)')

    print(f'-- Measurements declared as ordinal or nominal that refer to a non-existent category')
    r3 = check_category_in_range(dw, study)
    n_errors = len(r3)
    if n_errors > 0:
        print(tabulate(r3, headers=['Measurement Id']))
    print(f'({n_errors} measurements)')

    print()
    print(f'-- Measurements declared as Bounded Integers whose value is outside of the bounds')
    r4 = check_bounded_integers(dw, study)
    n_errors = len(r4)
    if n_errors > 0:
        print(tabulate(r4, headers=['Id', 'Value', 'MeasurementType', 'Group', 'Min', 'Max', 'Participant']))
    print(f'({n_errors} measurements)')

    print()
    print(f'-- Measurements declared as Bounded Reals whose value is outside of the bounds')
    r5 = check_bounded_reals(dw, study)
    n_errors = len(r5)
    if n_errors > 0:
        print(tabulate(r5, headers=['Id', 'Value', 'MeasurementType', 'Group', 'Min', 'Max', 'Participant']))
    print(f'({n_errors} measurements)')
    print()


def mk_txt_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + time_string + ".txt"


def print_check_warehouse_to_file(dw, study):
    """
    Prints to a file the results of all the tests that check the warehouse for errors
    :param dw: handle to data warehouse
    :param study: study id
    :return:
    """

    file_dir = "reports/"
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    fname = mk_txt_report_file_name(file_dir, "warehouse-check-", time_fname_str)

    with open(fname, "w", encoding="utf-8") as f:
        print(f'Check Study {study}\n', file=f)

        print(f'- Check Metadata\n', file=f)

        print(f'-- Measurement types declared as ordinal or nominal but without entries in the Category Table', file=f)
        r2 = check_category_exists(dw, study)
        n_errors = len(r2)
        if n_errors > 0:
            print(tabulate(r2, headers=['Measurement Type Id', 'Message Type Name']), file=f)
        print(f'({n_errors} invalid entries)\n', file=f)

        print(f'-- Measurement types declared as bounded integer but without entries in the Boundsint Table', file=f)
        rbi = check_integer_bounds_exist(dw, study)
        n_errors = len(rbi)
        if n_errors > 0:
            print(tabulate(rbi, headers=['Measurement Type Id', 'Message Type Name']), file=f)
        print(f'({n_errors} invalid entries)\n', file=f)

        print(f'-- Measurement types declared as bounded real but without entries in the Boundsreal Table', file=f)
        rbr = check_real_bounds_exist(dw, study)
        n_errors = len(rbr)
        if n_errors > 0:
            print(tabulate(rbr, headers=['Measurement Type Id', 'Message Type Name']), file=f)
        print(f'({n_errors} invalid entries)\n', file=f)

        print(f'- Check Measurements\n', file=f)

        print(f'-- Measurements where the Value Type does not match the values stored in the Measurement Table', file=f)
        r1 = check_valtype_matches_values(dw, study)
        n_invalid_entries = len(r1)
        if n_invalid_entries > 0:
            print_io.print_measurements_to_file(r1, f)
        print(f'({n_invalid_entries} invalid entries)\n', file=f)

        print(f'-- Measurements declared as ordinal or nominal that refer to a non-existent category', file=f)
        r3 = check_category_in_range(dw, study)
        n_errors = len(r3)
        if n_errors > 0:
            print(tabulate(r3, headers=['Measurement Id']), file=f)
        print(f'({n_errors} measurements)\n', file=f)

        print(f'-- Measurements declared as Bounded Integers whose value is outside of the bounds', file=f)
        r4 = check_bounded_integers(dw, study)
        n_errors = len(r4)
        if n_errors > 0:
            print(tabulate(r4, headers=['Id', 'Value']), file=f)
        print(f'({n_errors} measurements)\n', file=f)

        print(f'-- Measurements declared as Bounded Reals whose value is outside of the bounds', file=f)
        r5 = check_bounded_reals(dw, study)
        n_errors = len(r5)
        if n_errors > 0:
            print(tabulate(r5, headers=['Id', 'Value', 'MeasurementType', 'Group', 'Min', 'Max', 'Participant']),
                  file=f)
        print(f'({n_errors} measurements)\n', file=f)
