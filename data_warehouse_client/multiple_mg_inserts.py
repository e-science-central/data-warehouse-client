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


def insert_measurement_group_instances(data_warehouse_handle, study, measurement_group_vals,
                                       time=None, trial=None, participant=None, source=None, cursor=None):
    """
     Insert multiple measurement groups
     :param data_warehouse_handle:
     :param study: the study id
     :param measurement_group_vals: a list of the values for each mg in the form: [(mg, [(measType,valType,value)])]
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
    else:
        cur = cursor

    success = True
    error_message = ""

    if len(measurement_group_vals) == 0:   # Catch the edge case where a loader returns nothing to insert
        success = False
        error_message = f'[Error in in insert_measurement_groups - no instances to insert.]'

    message_group_instance_ids = []
    for (measurement_group, values) in measurement_group_vals:
        success, measurement_group_instance_id, error_message = insert_a_measurement_group_instance(
            cur, study, time, participant, trial, measurement_group, source, values)
        if success:
            message_group_instance_ids = [measurement_group_instance_id] + message_group_instance_ids
        else:
            break
    #  All measurements in all measurement groups have been inserted, or an error has been found
    if success:  # no inserts in the measurement group raised an error
        data_warehouse_handle.dbConnection.commit()  # commit the whole measurement group insert
    else:
        data_warehouse_handle.dbConnection.rollback()  # rollback the whole measurement group insert
    if cursor is None:  # if the cursor was created in this function then close it
        cur.close()
    return success, message_group_instance_ids, error_message


def insert_one_measurement(cur, study, participant, time, trial, measurement_group, measurement_type, source, value,
                           val_type, val_integer, val_real, measurement_group_instance_id, first_measurement_in_group):

    try:
        cur.execute("""
                     INSERT INTO measurement (id,time,study,trial,measurementgroup,groupinstance,
                                              measurementtype,participant,source,valtype,valinteger,valreal)
                     VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                     """,
                    (time, study, trial, measurement_group, measurement_group_instance_id,
                     measurement_type, participant, source, val_type, val_integer, val_real))
        measurement_id = cur.fetchone()[0]  # get the id of the new entry in the measurement table
        if first_measurement_in_group:  # this is the first measurement in the group to be inserted
            # Now we know the id of the first measurement, set the groupinstance field to this value
            group_instance_id = measurement_id
        else:
            group_instance_id = measurement_group_instance_id
            cur.execute("""
                         UPDATE measurement SET groupinstance = %s
                         WHERE id = %s;
                         """,
                        (group_instance_id, group_instance_id))  # set the groupinstance id for 1st measurement
        if string_valued_type(val_type):
            # it's a string or external (URI) so make entry in textvalue table
            cur.execute("""
                         INSERT INTO textvalue(measurement,textval,study)
                         VALUES (%s, %s, %s);
                         """,
                        (measurement_id, value, study))
        if datetime_valued_type(val_type):
            # it's a DateTime value so make entry in datetimevalue table
            cur.execute("""
                         INSERT INTO datetimevalue(measurement,datetimeval,study)
                         VALUES (%s, %s, %s);
                         """,
                        (measurement_id, value, study))
        return True, group_instance_id, ""
    except psycopg2.Error as e:  # an error has occurred when inserting into the warehouse
        error_message = f'[Error in insert_measurement_group. {e.pgcode} occurred: {e.pgerror}, ' \
                        f'Study = {study}, Participant = {participant}, Trial = {trial}, ' \
                        f'Measurement Group = {measurement_group}, Measurement Type = {measurement_type},' \
                        f' value = {value}, Source = {source}]'
        return False, None, error_message


def string_valued_type(val_type):
    string_type = 2
    external_type = 10
    return val_type in [string_type, external_type]


def datetime_valued_type(val_type):
    datetime_type = 3
    bounded_datetime_type = 9
    return val_type in [datetime_type, bounded_datetime_type]


def check_val_type(val_type, value):
    # define the value type ids
    integer_type = 0
    real_type = 1,
    string_type = 2
    datetime_type = 3
    boolean_type = 4
    nominal_type = 5
    ordinal_type = 6
    bounded_int_type = 7
    bounded_real_type = 8
    bounded_datetime_type = 9
    external_type = 10

    if val_type in [integer_type, nominal_type, ordinal_type, bounded_int_type]:
        return True, value, None, ""   # all stored in valInteger
    elif val_type in [real_type, bounded_real_type]:  # the value must be stored in valReal
        return True, None, value, ""
    elif val_type in [string_type, datetime_type, bounded_datetime_type, external_type]:
        return True, None, None, ""  # the value must be stored in the text or datetime tables
    elif val_type == boolean_type:
        if value in ['0', '1']:
            return True, value, None, ""
        else:  # Error in boolean value
            return False, None, None, '[Error in boolean value in insert_measurement_group'
    else:  # error in valType
        return False, None, None, f'[Error in valType ({val_type}) in insert_measurement_group.'


def insert_a_measurement_group_instance(cur, study, time, participant, trial, measurement_group, source, values):

    success = True
    first_measurement_in_group = True
    measurement_group_instance_id = 0  # used temporarily for the first measurement inserted in the group instance
    error_message = ""

    for (measurement_type, val_type, value) in values:
        val_type_ok, val_integer, val_real, val_check_error_message = check_val_type(val_type, value)
        if not val_type_ok:
            error_message = val_check_error_message +\
                            f'Study = {study}, Participant = {participant}, Trial = {trial}, ' \
                            f'Measurement Group = {measurement_group}, Measurement Type = {measurement_type},' \
                            f' value = {value}, Source = {source}]'
            success = False
        else:
            try:
                succesful_insert, mgi, error_msg = insert_one_measurement(
                    cur, study, participant, time, trial, measurement_group, measurement_type, source, value,
                    val_type, val_integer, val_real, measurement_group_instance_id, first_measurement_in_group)
                if succesful_insert:
                    if first_measurement_in_group:
                        measurement_group_instance_id = mgi
                        first_measurement_in_group = False
                else:
                    error_message = error_msg
            except psycopg2.Error as e:  # an error has occurred when inserting into the warehouse
                error_message = f'[Error in insert_measurement_group. {e.pgcode} occurred: {e.pgerror}, ' \
                                f'Study = {study}, Participant = {participant}, Trial = {trial}, ' \
                                f'Measurement Group = {measurement_group}, Measurement Type = {measurement_type},' \
                                f' value = {value}, Source = {source}]'
                success = False
        if not success:
            break  # ignore the remaining measurements to be inserted in the measurement group
    if success:
        return True, measurement_group_instance_id, ""
    else:
        return False, None, error_message
