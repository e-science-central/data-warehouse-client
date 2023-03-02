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

import datetime
import psycopg2
import file_utils
import type_definitions as ty
import type_checks
import check_bounded_values
from typing import Tuple, List, Optional, Dict


def insert_measurement_group_instances(data_warehouse_handle,
                                       study: ty.Study,
                                       measurement_group_vals: List[Tuple[ty.MeasurementGroup, List[ty.ValueTriple]]],
                                       int_bounds: Dict[ty.MeasurementType, Dict[str, int]] = None,
                                       real_bounds: Dict[ty.MeasurementType, Dict[str, float]] = None,
                                       datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]] = None,
                                       category_id_map: Dict[ty.MeasurementType, List[int]] = None,
                                       time: Optional[ty.DateTime] = None,
                                       trial: Optional[ty.Trial] = None,
                                       participant: Optional[ty.Participant] = None,
                                       source: Optional[ty.Source] = None,
                                       cursor=None) -> Tuple[bool, List[int], str]:
    """
     Insert multiple measurement groups resulting from one input of data: all are enclosed in a single transaction, so
     if any insert fails, nothing is added to the data warehouse.
     :param data_warehouse_handle:
     :param study: the study id
     :param measurement_group_vals: a list of the values for each mg in the form: [(mg, [(measType,valType,value)])]
     :param int_bounds: dictionary holding integer bounds
     :param real_bounds: dictionary holding real bounds
     :param datetime_bounds: dictionary holding datetime bounds
     :param category_id_map: dictionary holding mapping from category ids to names for each measurement type
     :param time: the time the measurement was taken. It defaults to the current time
     :param trial: optional trial id
     :param participant: optional participant id
     :param source: optional source
     :param cursor: database cursor
     :return success boolean, the measurement groups' instanceids, error message
    """
    if time is None:  # use the current date and time if none is specified
        time = datetime.datetime.now()  # use the current date and time if none is specified

    if cursor is None:  # no cursor has been passed into the function, so create one
        cur = data_warehouse_handle.dbConnection.cursor()
    else:  # used the cursor passed to this function
        cur = cursor

    if int_bounds is None:
        int_bounds = check_bounded_values.get_bounded_int_bounds(data_warehouse_handle, study)
    if real_bounds is None:
        real_bounds = check_bounded_values.get_bounded_real_bounds(data_warehouse_handle, study)
    if datetime_bounds is None:
        datetime_bounds = check_bounded_values.get_bounded_datetime_bounds(data_warehouse_handle, study)
    if category_id_map is None:
        category_id_map = check_bounded_values.get_category_ids(data_warehouse_handle, study)

    success: bool = True  # used to indicate if all the inserts succeeded
    error_message: str = ''

    if len(measurement_group_vals) == 0:   # Catch the edge case where a loader returns nothing to insert
        success = False
        error_message = f'[Error in in insert_measurement_groups - no instances to insert.]'

    message_group_instance_ids = []   # used to hold the list of message_group_ids inserted in the data warehouse
    for (measurement_group, values) in measurement_group_vals:
        #  try to insert one measurement group instance
        success, measurement_group_instance_id, error_message = insert_one_measurement_group_instance(
            cur, study, time, participant, trial, measurement_group, source, values,
            int_bounds, real_bounds, datetime_bounds, category_id_map)
        if success:  # if successfully inserted add id to list of message group instances inserted
            message_group_instance_ids = [measurement_group_instance_id] + message_group_instance_ids
        else:  # the whole set of inserts should fail if one fails, so stop trying if this is the case
            break
    #  All measurements in all measurement groups have been inserted, or an error has been found
    if success:  # no inserts in the measurement group raised an error
        data_warehouse_handle.dbConnection.commit()  # commit the whole measurement group insert
    else:
        data_warehouse_handle.dbConnection.rollback()  # rollback the whole measurement group insert
    if cursor is None:  # if the cursor was created in this function then close it
        cur.close()
    if success:
        return True, message_group_instance_ids, ''
    else:
        return False, [], error_message


def insert_one_measurement(cur, study: ty.Study, participant: ty.Participant, time: ty.DateTime, trial: ty.Trial,
                           measurement_group: ty.MeasurementGroup,
                           measurement_type: ty.MeasurementType, source: ty.Source, value: ty.Value,
                           val_type: ty.ValType, val_integer: Optional[int], val_real: Optional[float],
                           measurement_group_instance_id: ty.MeasurementGroupInstance,
                           first_measurement_in_group: bool) -> Tuple[bool, Optional[ty.MeasurementGroupInstance], str]:
    """
    insert a single measurement in the data warehouse - it is part of a measurement group instance.
    If it fails then raise an error so the whole transaction can be rolled back.
    :param cur: the database cursor used to insert the data
    :param study: study id
    :param participant: participant id
    :param time: timestamp of measurement
    :param trial: trial id
    :param measurement_group: measurement group id
    :param measurement_type: measurement type id
    :param source: source id
    :param value: value to insert
    :param val_type: type of value
    :param val_integer: value to store in integer field
    :param val_real: value to store in real field
    :param measurement_group_instance_id:  measurement_group_instance_id for this measurement
    :param first_measurement_in_group: is this the first measurement in the measurement group instance to be inserted?
    :return: Success?, List of ids of Measurement Groups Inserted, Error
    """
    try:  # try to insert the measurement
        # mappings = {'time': time, 'study': study, 'trial': trial, 'measurementgroup': measurement_group,
        #             'groupinstance': measurement_group_instance_id, 'measurementtype': measurement_type,
        #             'participant': participant, 'source': source, 'valtype': val_type,
        #             'valinteger': val_integer, 'valreal': val_real}
        # insert_measurement_sql = file_utils.process_sql_template("insert_measurement.sql", mappings)
        #  cur.execute(insert_measurement_sql)

        # cur.mogrify("SELECT %s, %s, %s;", (dt, dt.date(), dt.time()))
        sql_template = "INSERT INTO measurement" + \
                       " (id,time,study,trial,measurementgroup,groupinstance,measurementtype" + \
                       ",participant,source,valtype,valinteger,valreal) " + \
                       "VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;"
        insert_sql = cur.mogrify(sql_template, (time, study, trial, measurement_group, measurement_group_instance_id,
                                                measurement_type, participant, source, val_type, val_integer, val_real))
        cur.execute(insert_sql)

        measurement_id = cur.fetchone()[0]  # get the id of the new entry in the measurement table
        if first_measurement_in_group:  # this is the first measurement in the group to be inserted
            group_instance_id = measurement_id  # set the group instance id field to this value
            update_group_instance_id_map = {'group_instance_id': group_instance_id, 'measurement_id': measurement_id}
            update_group_instance_id_sql = file_utils.process_sql_template("update_measurement_group_instance_id.sql",
                                                                           update_group_instance_id_map)
            cur.execute(update_group_instance_id_sql)  # set the groupinstance id for 1st measurement
        else:  # keep using the measurement_group_instance_id passed to the function
            group_instance_id = measurement_group_instance_id

        if text_valued_type(val_type):  # it's a string or external (URI) so make entry in textvalue table
            # text_insert_map = {'measurement': measurement_id, 'textval': value, 'study': study}
            # insert_text_sql = file_utils.process_sql_template("insert_text.sql", text_insert_map)
            # print(insert_text_sql)
            # cur.execute(insert_text_sql)
            cur.execute("INSERT INTO textvalue(measurement,textval,study) VALUES (%s, %s, %s);",
                        (measurement_id, value, study))

        elif datetime_valued_type(val_type):  # it's a datetime value so make entry in datetimevalue table
            # datetime_insert_map = {'measurement': measurement_id, 'datetimeval': value, 'study': study}
            # insert_datetime_sql = file_utils.process_sql_template("insert_datetime.sql", datetime_insert_map)
            insert_datetime_template = "INSERT INTO datetimevalue(measurement,datetimeval,study) VALUES (%s, %s, %s);"
            insert_datetime = cur.mogrify(insert_datetime_template, (measurement_id, value, study))
            cur.execute(insert_datetime)

        return True, group_instance_id, ""   # successful insert
    except psycopg2.Error as e:  # an error has occurred when inserting into the warehouse
        error_message = f'[Error in insert_one_measurement. {e.pgcode} occurred: {e.pgerror}, ' \
                        f'Study = {study}, Participant = {participant}, Trial = {trial}, ' \
                        f'Measurement Group = {measurement_group}, Measurement Type = {measurement_type},' \
                        f' value = {value}, Source = {source}]'
        return False, None, error_message   # insert has failed


def text_valued_type(val_type: ty.ValType) -> bool:
    """
    is this a text type that will be stored in the text table?
    :param val_type: value type
    :return: true if string value
    """
    string_type: ty.ValType = 2
    external_type: ty.ValType = 10
    return val_type in [string_type, external_type]


def datetime_valued_type(val_type: ty.ValType) -> bool:
    """
    is this a datetime type that will be stored in the datetime table?
    :param val_type: value type
    :return: true if datetime value
    """
    datetime_type: ty.ValType = 3
    bounded_datetime_type: ty.ValType = 9
    return val_type in [datetime_type, bounded_datetime_type]


def integer_valued_type(val_type: ty.ValType) -> bool:
    """
    is this an integer valued type that will be stored in the measurement table's integer field?
    :param val_type: value type
    :return: true if integer value
    """
    integer_type: ty.ValType = 0
    boolean_type: ty.ValType = 4
    nominal_type: ty.ValType = 5
    ordinal_type: ty.ValType = 6
    bounded_int_type: ty.ValType = 7
    return val_type in [integer_type, nominal_type, ordinal_type, bounded_int_type, boolean_type]


def real_valued_type(val_type: ty.ValType) -> bool:
    """
    is this a real valued type that will be stored in the measurement table's real field?
    :param val_type: value type
    :return: true if real value
    """
    real_type: ty.ValType = 1
    bounded_real_type: ty.ValType = 8
    return val_type in [real_type, bounded_real_type]


def extract_fields_to_insert(val_type: ty.ValType, value: ty.Value) -> Tuple[Optional[int], Optional[float]]:
    """
    set the entries in the measurement table's integer and real fields
    :param val_type: type of the value
    :param value: value to be inserted in the measurement table
    :return: value to be stored in the integer field, value to be stored in the real field
    """
    if integer_valued_type(val_type):
        return value, None   # all stored in integer field
    elif real_valued_type(val_type):  # the value must be stored in real field
        return None, value
    elif text_valued_type(val_type) or datetime_valued_type(val_type):
        return None, None  # the value must be stored in the text or datetime tables
    else:  # error in valType
        return None, None


def insert_one_measurement_group_instance(cur,
                                          study: ty.Study,
                                          time: ty.DateTime,
                                          participant: ty.Participant,
                                          trial: ty.Trial,
                                          measurement_group: ty.MeasurementGroup,
                                          source: ty.Source,
                                          values: List[ty.ValueTriple],
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
    :param int_bounds: dictionary holding integer bounds
    :param real_bounds: dictionary holding real bounds
    :param datetime_bounds: dictionary holding datetime bounds
    :param category_id_map: dictionary holding valid category ids for each measurement type
    :return: success?, id of the measurement group instance, error messages
    """
    success: bool = True   # used to indicate the success or otherwise of the insertion
    first_measurement_in_group: bool = True  # used to ensure the same instance id is used for every measurement
    measurement_group_instance_id: ty.MeasurementGroupInstance = 0  # temp val for 1st measurement inserted in instance
    error_message: str = ""

    for (measurement_type, val_type, value) in values:  # for each measurement to be stored in the group instance
        success, error_mess = type_checks.check_value_type(val_type, value, measurement_type,
                                                           int_bounds, real_bounds, datetime_bounds, category_id_map)
        if not success:  # problem with the type of a measurement
            error_message: str = error_mess +\
                                 f' Study = {study}, Participant = {participant}, Trial = {trial}, ' \
                                 f'Measurement Group = {measurement_group}, Measurement Type = {measurement_type},' \
                                 f' value = {value}, Source = {source}]'
            success = False
        else:
            # try to insert one measurement
            val_integer, val_real = extract_fields_to_insert(val_type, value)
            succesful_insert, mgi, error_msg = insert_one_measurement(
                cur, study, participant, time, trial, measurement_group, measurement_type, source, value,
                val_type, val_integer, val_real, measurement_group_instance_id, first_measurement_in_group)
            if succesful_insert:
                if first_measurement_in_group:
                    measurement_group_instance_id = mgi  # use this as the instance id for every measurement
                    first_measurement_in_group = False
            else:
                error_message = error_msg
                success = False
        if not success:
            break  # ignore the remaining measurements to be inserted in the measurement group instance
    if success:
        return True, measurement_group_instance_id, ""
    else:
        return False, None, error_message
