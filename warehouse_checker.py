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

import data_warehouse
import study_summary

# Warehouse Checker code - check:
# valtype field points to non-NULL value field
# The measurementType is in the measurement group for each measurement
# The valtype is correct for the measurement type for each measurement
# the measurement group is valid for the study
# there are no missing measurement types in a measurement group instance
# no ordinal, nominal, bounded integer or bounded real values are out of bounds


def check_category_exists(dw, study):
    """
    Find measurements of nominal or ordinal type whose value does not equal that of a category
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    q =  " SELECT measurementtype.id "
    q += " FROM measurementtype INNER JOIN category ON "
    q += "      (measurementtype.id = category.measurementtype AND "
    q += "      measurementtype.study = category.study) "

    outerq =  " SELECT measurementtype.id, measurementtype.description "
    outerq += " FROM   measurementtype "
    outerq += " WHERE  measurementtype.valtype IN (5,6) AND "
    outerq += "        measurementtype.study = " + str(study) + " AND "
    outerq += "        measurementtype.id NOT IN (" + q + ")"
    outerq += " ORDER BY measurementtype.id;"
    return dw.returnQueryResult(outerq)


def check_valtype_matches_values(dw, study):
    """
    Find measurements that lack a value
    :param dw: handle to data warehouse
    :param study: study id
    :return: the measurements in the study that fail the test
    """
    q =  data_warehouse.coreSQLforMeasurements()
    q += " WHERE measurement.study = " + str(study)
    q += " AND ((measurement.valtype IN (0,4,5,6,7)) AND (measurement.valinteger    = NULL)) OR "
    q += "     ((measurement.valtype IN (1,8))       AND (measurement.valreal       = NULL)) OR "
    q += "     ((measurement.valtype =  2)           AND (textvalue.textval         = NULL)) OR "
    q += "     ((measurement.valtype =  3)           AND (datetimevalue.datetimeval = NULL));   "
    return dw.returnQueryResult(q)


def check_category_in_range(dw, study):
    """
    Returns the ids of measurements that refer to a non-existent category
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    q =  " SELECT DISTINCT measurement.id "
    q += " FROM   measurement JOIN category ON "
    q += "        (measurement.measurementtype = category.measurementtype AND "
    q += "        measurement.study = category.study AND "
    q += "        measurement.valinteger = category.categoryid)"
    q += " WHERE  measurement.valtype IN (5,6)"

    q1 =  " SELECT measurement.id "
    q1 += " FROM   measurement "
    q1 += " WHERE  measurement.study = " + str(study) + " AND "
    q1 += "        measurement.valtype IN (5,6) AND "
    q1 += "        measurement.id NOT IN (" + q + ")"
    q1 += " ORDER BY measurement.id;"
    return dw.returnQueryResult(q1)


def check_bounded_integers(dw, study):
    """
    Returns the ids of measurements that hold bounded integers that are out of range
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    q = " SELECT DISTINCT measurement.id "
    q += " FROM   measurement JOIN boundsint ON "
    q += "        (measurement.measurementtype = boundsint.measurementtype AND "
    q += "        measurement.study = boundsint.study) "
    q += " WHERE  measurement.valtype = 7 AND "
    q += "        measurement.study =" + str(study) + " AND "
    q += "        (measurement.valinteger < boundsint.minval OR "
    q += "         measurement.valinteger > boundsint.maxval) "
    q += " ORDER BY measurement.id;"
    return dw.returnQueryResult(q)


def check_bounded_reals(dw, study):
    """
    Returns the ids of measurements that hold bounded reals that are out of range
    :param dw: handle to data warehouse
    :param study: study id
    :return: the ids of measurements in the study that fail the test
    """
    q = " SELECT DISTINCT measurement.id "
    q += " FROM   measurement JOIN boundsreal ON "
    q += "        (measurement.measurementtype = boundsreal.measurementtype AND "
    q += "        measurement.study = boundsreal.study) "
    q += " WHERE  measurement.valtype = 8 AND"
    q += "        measurement.study =" + str(study) + " AND "
    q += "        (measurement.valreal < boundsreal.minval OR "
    q += "         measurement.valreal > boundsreal.maxval) "
    q += " ORDER BY measurement.id;"
    return dw.returnQueryResult(q)


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
    dw.printMeasurements(r1)
    n_invalid_entries = len(r1)
    print(f'({n_invalid_entries} invalid entries)')

    print()
    print(f'Check for measurement types declared as ordinal or nominal but without entries in the category table')
    r2 = check_category_exists(dw, study)
    print(*r2, sep="\n")

    n_errors = len(r2)
    print(f'({n_errors} invalid entries)')

    print()
    print(f'Check for measurements declared as ordinal or nominal but without a matching entry in the category table')
    r3 = check_category_in_range(dw, study)
    for r in r3:
        print(r[0])
    n_errors = len(r3)
    print(f'({n_errors} measurements)')

    print()
    print(f'Check for measurements declared as bounded integers whose value is outside of the bounds')
    r4 = check_bounded_integers(dw, study)
    for r in r4:
        print(r[0])
    n_errors = len(r4)
    print(f'({n_errors} measurements)')

    print()
    print(f'Check for measurements declared as bounded reals whose value is outside of the bounds')
    r5 = check_bounded_reals(dw, study)
    for r in r5:
        print(r[0])
    n_errors = len(r5)
    print(f'({n_errors} measurements)')


# Test
# Create a connection to the data warehouse

data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")
study_id = 11
study_summary.print_study_summary(data_warehouse, study_id)
print_check_warhouse(data_warehouse, study_id)
