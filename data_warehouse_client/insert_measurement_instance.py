# Copyright 2023 Newcastle University.
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

# For the experimental redesign of the measurement table

import psycopg2
from data_warehouse_client import file_utils
import type_definitions as ty
import type_checks
from typing import Tuple, List, Optional, Dict
from multiple_mg_inserts import text_valued_type, extract_fields_to_insert, datetime_valued_type


def insert_new_measurement_group_instance(cur, study, time, participant, trial, measurement_group, source):
    mappings = {'time': time, 'study': study, 'trial': trial, 'measurementgroup': measurement_group,
                'participant': participant, 'source': source}
    insert_measurement_sql = file_utils.process_sql_template("insert_measurement.sql", mappings)
    try:
        cur.execute(insert_measurement_sql)
        measurement_id = cur.fetchone()[0]  # get the id of the new entry in the measurement table
        return True, measurement_id, ""
    except psycopg2.Error as e:
        error_message = f' Error in insert_measurement_group. {e.pgcode} occurred: {e.pgerror}, ' \
                        f' Study = {study}, Participant = {participant}, Trial = {trial}, ' \
                        f' Measurement Group = {measurement_group},' \
                        f' Source = {source} '
        return False, None, error_message


def insert_one_measurement_group_instance(cur,
                                          study: ty.Study,
                                          time: ty.DateTime,
                                          participant: ty.Participant,
                                          trial: ty.Trial,
                                          measurement_group: ty.MeasurementGroup,
                                          source: ty.Source,
                                          values: List[ty.ValueTriple],
                                          check_bounds: bool,
                                          int_bounds: Dict[ty.MeasurementType, Dict[str, int]],
                                          real_bounds: Dict[ty.MeasurementType, Dict[str, float]],
                                          datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]],
                                          category_id_map: Dict[ty.MeasurementType, List[int]]
                                          ) ->\
        Tuple[bool, Optional[ty.MeasurementGroupInstance], str]:
    """
    insert one measurement group instance in the data warehouse
    :param cur: cursor to use in accessing the data warehouse
    :param study: study id
    :param time: timestamp
    :param participant: participant id
    :param trial: trial id
    :param measurement_group: measurement group id
    :param source: source id
    :param values: list of value triples to be inserted (measurement_type, val_type, value)
    :param check_bounds: bool,
    :param int_bounds: dictionary holding integer bounds
    :param real_bounds: dictionary holding real bounds
    :param datetime_bounds: dictionary holding datetime bounds
    :param category_id_map: dictionary holding valid category ids for each measurement type
    :return: success?, id of the measurement group instance, error messages
    """
    success: bool = True   # used to indicate the success or otherwise of the insert
    measurement_group_instance_id: ty.MeasurementGroupInstance = 0  # temp val for 1st measurement inserted in instance
    error_message: str = ""

    successful_mig_insert, mgi_id, mig_insert_error_msg = insert_new_measurement_group_instance(cur, study, time,
                                                                                                participant,
                                                                                                trial,
                                                                                                measurement_group,
                                                                                                source)
    if successful_mig_insert:
        for (measurement_type, val_type, value) in values:  # for each measurement to be stored in the group instance
            successful_val_check, error_mess = type_checks.check_value_type(val_type, value, measurement_type,
                                                                            check_bounds,
                                                                            int_bounds, real_bounds,
                                                                            datetime_bounds, category_id_map)
            if successful_val_check:  # problem with the type of a measurement
                # try to insert one measurement
                val_integer, val_real = extract_fields_to_insert(val_type, value)
                succesful_insert, error_msg = insert_one_measurement(cur, study, measurement_type, value,
                                                                     val_type, val_integer, val_real, mgi_id)
                if not succesful_insert:
                    error_message = error_msg
                    success = False
            else:
                error_message = error_mess
                success = False
            if not success:
                break  # ignore the remaining measurements to be inserted in the measurement group instance
    else:
        success = False
        error_message = mig_insert_error_msg
    if success:
        return True, measurement_group_instance_id, ""
    else:
        return False, None, error_message + f' Study = {study}, Participant = {participant}, Trial = {trial},' \
                                            f' Measurement Group = {measurement_group}, Source = {source}'


def insert_one_measurement(cur, study: ty.Study, measurement_type: ty.MeasurementType, value: ty.Value,
                           val_type: ty.ValType, val_integer: Optional[int], val_real: Optional[float],
                           measurement_group_instance_id: ty.MeasurementGroupInstance) ->\
        Tuple[bool, str]:
    """
    insert a single measurement in the data warehouse - it is part of a measurement group instance.
    If it fails then raise an error so the whole transaction can be rolled back.
    :param cur: the database cursor used to insert the data
    :param study: study id
    :param measurement_type: measurement type id
    :param value: value to insert
    :param val_type: type of value
    :param val_integer: value to store in integer field
    :param val_real: value to store in real field
    :param measurement_group_instance_id:  measurement_group_instance_id for this measurement
    :return: Success?, List of ids of Measurement Groups Inserted, Error
    """
    try:  # try to insert the measurement
        mappings = {'study': study, 'groupinstance': measurement_group_instance_id, 'measurementype': measurement_type,
                    'valtype': val_type, 'valinteger': val_integer, 'valreal': val_real}
        insert_measurement_sql = file_utils.process_sql_template("insert_short_measurement.sql", mappings)
        cur.execute(insert_measurement_sql)
        measurement_id = cur.fetchone()[0]  # get the id of the new entry in the measurement table

        if text_valued_type(val_type):  # it's a string or external so make entry in textvalue table
            text_insert_map = {'measurement': measurement_id, 'textval': value, 'study': study}
            insert_text_sql = file_utils.process_sql_template("insert_text.sql", text_insert_map)
            cur.execute(insert_text_sql)

        elif datetime_valued_type(val_type):  # it's a datetime so make entry in datetimevalue table
            datetime_insert_map = {'measurement': measurement_id, 'datetimeval': value, 'study': study}
            insert_datetime_sql = file_utils.process_sql_template("insert_datetime.sql", datetime_insert_map)
            cur.execute(insert_datetime_sql)

        return True, ""   # successful insert
    except psycopg2.Error as e:  # an error has occurred when inserting into the warehouse
        error_message = f' Error in insert_measurement_group. {e.pgcode} occurred: {e.pgerror}, ' \
                        f' Measurement Type = {measurement_type},' \
                        f' value = {value}'
        return False, error_message   # insert has failed
