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

import datetime
import sys
import psycopg2
import json
from more_itertools import intersperse
from data_warehouse_client import transform_result_format

from data_warehouse_client import file_utils


def get_participants_in_result(results):
    """
    find all participants in a set of results
    :param results: a list of measurements. Each measurement is held in a list with the following fields:
                    id,time,study,participant,measurementType,typeName,measurementGroup,
                    groupInstance,trial,valType,value
    :return: a list of unique participants from the measurements
    """
    participants = map(lambda r: r[3], results)  # pick out participant
    return list(set(participants))


def field_holding_value(val_type):
    """
    A helper function that returns the data warehouse field that holds measurement values of the type
    specified in the parameter
    :param val_type: the type of measurement
    :return: the database field holding the measurement value
    """
    val_types = {0: "measurement.valinteger", 1: "measurement.valreal", 2: "textvalue.textval",
                 3: "datetimevalue.datetimeval", 4: "measurement.valinteger", 5: "measurement.valinteger",
                 6: "measurement.valinteger", 7: "measurement.valinteger", 8: "measurement.valreal"}
    try:
        return val_types[val_type]
    except KeyError:
        print("Error: valType out of range: ", val_type)
        return None


def mk_where_condition(first_condition, column, test, value):
    """
    :param first_condition: true if this is the first condition in a where clause
    :param column: column name in measurement table
    :param test: comparator for value
    :param value: value to be tested in the where clause
    :return: (where clause, first_condition)
    """
    if value != -1:
        condition = " WHERE " if first_condition else " AND "
        q = f'{condition} measurement.{column}{test}{str(value)}'
        first_condition = False
    else:
        q = ""
    return q, first_condition


def core_sql_from_for_measurements():
    """
    Creates the from clause used by many of the functions that query the data warehouse
    :return: the from clause used by several of the functions that query the data warehouse
    """
    return file_utils.process_sql_template("sql/core_sql_from_for_measurements.sql")


def core_sql_for_where_clauses(study: int, participant: int, measurement_type: int, measurement_group: int,
                               group_instance: int, trial: int, start_time, end_time):
    """
    Returns the where clauses used by many of the functions that query the data warehouse to filter out rows
    according to the criteria passed as parameters. A value of -1 for any parameter means that no filter is
    created for it
    :param study: a study id
    :param participant: a participant id
    :param measurement_type: a measurementType
    :param measurement_group: a measurementGroup
    :param group_instance: a groupInstance
    :param trial: a trial id
    :param start_time: the start of a time period of interest
    :param end_time: the end of a time period of interest
    :return: a tuple containing the SQL for the where clauses, and a boolean that is true if there are >0 clauses
    """
    first_condition = True
    (qs, first_condition) = mk_where_condition(first_condition, "study", "=", study)
    (qp, first_condition) = mk_where_condition(first_condition, "participant", "=", participant)
    (qmt, first_condition) = mk_where_condition(first_condition, "measurementtype", "=", measurement_type)
    (qmg, first_condition) = mk_where_condition(first_condition, "measurementgroup", "=", measurement_group)
    (qgi, first_condition) = mk_where_condition(first_condition, "groupinstance", "=", group_instance)
    (qst, first_condition) = mk_where_condition(first_condition, "trial", "=", trial)
    (qet, first_condition) = mk_where_condition(first_condition, "time", ">=", start_time)
    (qt, first_condition) = mk_where_condition(first_condition, "time", "<=", end_time)
    return f' {qs}{qp}{qmt}{qmg}{qgi}{qst}{qet}{qt} ', first_condition


def core_sql_for_where_clauses_for_cohort(study, participants, measurement_type: int,
                                          measurement_group: int, group_instance: int, trial: int, start_time,
                                          end_time):
    """
    Returns the where clauses used by functions that query the data warehouse to filter out rows
    according to the criteria passed as parameters. A value of -1 for any parameter means that no filter is
    created for it
    :param study: a study id
    :param participants: a list of participant ids
    :param measurement_type: a measurementType
    :param measurement_group: a measurementGroup
    :param group_instance: a groupInstance
    :param trial: a trial id
    :param start_time: the start of a time period of interest
    :param end_time: the end of a time period of interest
    :return: the SQL for the where clauses
    """
    participants_str = map(lambda p: str(p), participants)
    q = " WHERE measurement.participant IN (" + ' '.join(
        [elem for elem in intersperse(",", participants_str)]) + ") "
    first_condition = False
    (qs, first_condition) = mk_where_condition(first_condition, "study", "=", study)
    (qmt, first_condition) = mk_where_condition(first_condition, "measurementtype", "=", measurement_type)
    (qmg, first_condition) = mk_where_condition(first_condition, "measurementgroup", "=", measurement_group)
    (qgi, first_condition) = mk_where_condition(first_condition, "groupinstance", "=", group_instance)
    (qst, first_condition) = mk_where_condition(first_condition, "trial", "=", trial)
    (qet, first_condition) = mk_where_condition(first_condition, "time", ">=", start_time)
    (qt, first_condition) = mk_where_condition(first_condition, "time", "<=", end_time)
    return f' {q}{qs}{qmt}{qmg}{qgi}{qst}{qet}{qt} '


def make_value_test(val_type, value_test_condition):
    """
    creates a condition for the where clause of a query
    :param val_type: the type of the field being tested
    :param value_test_condition: the test of that field
    :return: a fragment of SQL that can be included in the where clause of a query
    """
    return f' ({field_holding_value(val_type)}{value_test_condition}) '


def core_sql_select_for_measurements():
    """
     Creates the select clause used by many of the functions that query the data warehouse
     :return: the select clause used by several of the functions that query the data warehouse
     """
    return file_utils.process_sql_template("sql/core_sql_select_for_measurements.sql")


def core_sql_for_measurements():
    """
    Creates the select and from clauses used by many of the functions that query the data warehouse
    :return: the select and from clauses used by several of the functions that query the data warehouse
    """
    return f'{core_sql_select_for_measurements()} {core_sql_from_for_measurements()}'


class DataWarehouse:
    def __init__(self, credentials_file, db_name):
        # construct a connection to the warehouse
        self.credentialsFile = credentials_file
        self.dbName = db_name
        # load credentials
        print("Loading credentials..")
        try:
            with open(self.credentialsFile, 'r') as fIn:
                creds = json.load(fIn)
        except Exception as e:
            sys.exit("Unable to load the credential's file! Exiting.\n" + str(e))

        print("Connecting to the database..")
        # establish connection
        conn_string = f"dbname={self.dbName} user={creds['user']} host={creds['IP']} password={creds['pass']}"
        try:
            self.dbConnection = psycopg2.connect(conn_string)
        except Exception as e:
            sys.exit("Unable to connect to the database! Exiting.\n" + str(e))
        print("Init successful! Running queries.\n")

    def get_measurements(self, study, participant=-1, measurement_type=-1, measurement_group=-1, group_instance=-1,
                         trial=-1, start_time=-1, end_time=-1):
        """
        This function returns all measurements in the data warehouse that meet the optional criteria specified
        in the keyword arguments.
        The result is a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        :param study: a study id
        :param participant: a participant id
        :param measurement_type: a measurementType
        :param measurement_group: a measurementGroup
        :param group_instance: a groupInstance
        :param trial: a trial id
        :param start_time: the start of a time period of interest
        :param end_time: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        """
        (where_clause, first_condition) = core_sql_for_where_clauses(study, participant, measurement_type,
                                                                     measurement_group,
                                                                     group_instance, trial, start_time, end_time)
        mappings = {"core_sql": core_sql_for_measurements(), "where_clause": where_clause}
        query = file_utils.process_sql_template("sql/get_measurements.sql", mappings)
        raw_results = self.return_query_result(query)
        return transform_result_format.form_measurements(raw_results)

    def aggregate_measurements(self, study, measurement_type, aggregation, participant=-1, measurement_group=-1,
                               group_instance=-1, trial=-1, start_time=-1, end_time=-1):
        """

        :param measurement_type: the type of the measurements to be aggregated
        :param study: a study id
        :param aggregation: the aggregation function: this can be any of the postgres SQL aggregation functions,
                                  e.g."avg", "count", "max", "min", "sum"
        :param participant: a participant id
        :param measurement_group: a measurementGroup
        :param group_instance: a groupInstance
        :param trial: a trial id
        :param start_time: the start of a time period of interest
        :param end_time: the end of a time period of interest
        :return: the result of the aggregation
        """
        mt_info = self.get_measurement_type_info(study, measurement_type)
        val_type = mt_info[0][2]
        (w, first_condition) = core_sql_for_where_clauses(study, participant, measurement_type, measurement_group,
                                                          group_instance, trial, start_time, end_time)
        q = f'SELECT {aggregation} ( {field_holding_value(val_type)} ) {core_sql_from_for_measurements()} {w}'
        raw_result = self.return_query_result(q)
        return raw_result[0][0]

    def get_measurements_with_value_test(self, study, measurement_type, value_test_condition, participant=-1,
                                         measurement_group=-1, group_instance=-1, trial=-1, start_time=-1, end_time=-1):
        """
        Find all measurement of a particular type whose value meets some criteria.
        :param measurement_type: the measurement type of the measurements to be tested
        :param study: a study id
        :param value_test_condition: a string holding the condition
                  against which the value in each measurement is compared.
        :param participant: a participant id
        :param measurement_group: a measurementGroup
        :param group_instance: a groupInstance
        :param trial: a trial id
        :param start_time: the start of a time period of interest
        :param end_time: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        """
        (where_clause, first_condition) = core_sql_for_where_clauses(study, participant, measurement_type,
                                                                     measurement_group,
                                                                     group_instance, trial, start_time, end_time)
        mt_info = self.get_measurement_type_info(study, measurement_type)
        val_type = mt_info[0][2]  # find the value type of the measurement
        # Add a clause to test the field that is relevant to the type of the measurement
        condition = " WHERE " if first_condition else " AND "
        cond = make_value_test(val_type, value_test_condition)
        mappings = {"core_sql": core_sql_for_measurements(), "where_clause": where_clause, "condition": condition,
                    "cond": cond}
        query = file_utils.process_sql_template("sql/get_measurements_with_value.sql", mappings)
        raw_results = self.return_query_result(query)
        return transform_result_format.form_measurements(raw_results)

    def get_measurements_by_cohort(self, study, cohort_id, participant=-1, measurement_type=-1,
                                   measurement_group=-1, group_instance=-1, trial=-1, start_time=-1, end_time=-1):
        """
        Find all measurements in a cohort that meet the criteria.
        :param cohort_id: the value of the category in measurementType 181 that represents the condition
        :param study: a study id
        :param participant: a participant id
        :param measurement_type: a measurement type id
        :param measurement_group: a measurement group id
        :param group_instance: a groupInstance
        :param trial: a trial id
        :param start_time: the start of a time period of interest
        :param end_time: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        """
        (where_clause, first_condition) = core_sql_for_where_clauses(study, participant, measurement_type,
                                                                     measurement_group,
                                                                     group_instance, trial, start_time, end_time)
        condition = " WHERE " if first_condition else " AND "
        mappings = {"core_sql": core_sql_for_measurements(), "where_clause": where_clause, "condition": condition,
                    "cohort_id": str(cohort_id), "study": str(study)}
        query = file_utils.process_sql_template("sql/get_measurements_by_cohort.sql", mappings)
        raw_results = self.return_query_result(query)
        return transform_result_format.form_measurements(raw_results)

    def num_types_in_a_measurement_group(self, study, measurement_group):
        """
        A helper function that returns the number of measurement types in a measurement group
        :param study: study id
        :param measurement_group: measurement group id
        :return: number of measurement types in the measurement group
        """
        mappings = {"measurement_group": str(measurement_group), "study": str(study)}
        query = file_utils.process_sql_template("sql/num_types_in_a_measurement_group.sql", mappings)
        num_types = self.return_query_result(query)
        return num_types[0][0]

    def get_types_in_a_measurement_group(self, study, measurement_group):
        """
        A helper function that returns the names of the measurement types in a measurement group
        :param study: study id
        :param measurement_group: measurement group id
        :return: list of names of the measurement types in the measurement group
        """
        mappings = {"measurement_group": str(measurement_group), "study": str(study)}
        query = file_utils.process_sql_template("sql/types_in_a_measurement_group.sql", mappings)
        type_names = self.return_query_result(query)
        return type_names

    def mk_value_tests(self, value_test_conditions, study):
        """
        Helper function used to creat a Where clause to find measurements that fail the conditions
        :param value_test_conditions:   a list where each element is takes the following form:
                                        (measurementType,condition)
                                        where condition is a string holding the condition
                                        against which the value in each measurement is compared.
        :param study: study id
        :return:
        """
        all_conditions = []
        for (measurement_type, condition) in value_test_conditions:
            mt_info = self.get_measurement_type_info(study, measurement_type)
            val_type = mt_info[0][2]
            cond = f'((measurement.measurementtype = {str(measurement_type)}) AND NOT ' \
                   f'{make_value_test(val_type, condition)})'
            all_conditions = all_conditions + [cond]
        return ' '.join([elem for elem in intersperse(" OR ", all_conditions)])

    def get_measurement_group_instances(self, study, measurement_group, value_test_conditions,
                                        participant=-1, trial=-1, start_time=-1, end_time=-1):
        """
        Return all instances of a measurement group in which one or more of the measurements within the
            instance meet some specified criteria
        :param measurement_group: a measurement group
        :param study: a study id
        :param value_test_conditions: a list where each element is takes the following form:
                                    (measurementType,condition)
                                       where condition is a string holding the condition
                                       against which the value in each measurement is compared.
        :param participant: a participant id
        :param trial: a trial id
        :param start_time: the start of a time period of interest
        :param end_time: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
                    id,time,study,participant,measurementType,typeName,measurementGroup,
                    groupInstance,trial,valType,value
        """
        problem_q = ""  # returns the instance ids of all instances that fail the criteria
        problem_q += " SELECT measurement.groupinstance "
        problem_q += core_sql_from_for_measurements()
        (w, first_condition) = core_sql_for_where_clauses(study, participant, -1, measurement_group, -1, trial,
                                                          start_time, end_time)
        problem_q += w
        if len(value_test_conditions) > 0:
            problem_q += " AND (" + self.mk_value_tests(value_test_conditions, study) + ")"

        outer_query = core_sql_for_measurements()
        outer_query += " " + w
        if len(value_test_conditions) > 0:
            outer_query += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
        outer_query += " ORDER BY groupinstance, measurementtype"
        outer_query += ";"
        raw_results = self.return_query_result(outer_query)
        formed_measurements = transform_result_format.form_measurements(raw_results)
        return transform_result_format.form_measurement_group(self, study, measurement_group, formed_measurements)

    def get_measurement_group_instances_for_cohort(self, study, measurement_group, participants, value_test_conditions,
                                                   trial=-1, start_time=-1, end_time=-1):
        """
        Return all instances of a measurement group in which one or more of the measurements within the
            instance meet some specified criteria for the specified cohort of participants
        :param measurement_group: a measurement group
        :param study: a study id
        :param participants: a list of participant ids
        :param value_test_conditions: a list where each element is takes the following form:
                                    (measurementType,condition)
                                       where condition is a string holding the condition
                                       against which the value in each measurement is compared.
        :param trial: a trial id
        :param start_time: the start of a time period of interest
        :param end_time: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
                    id,time,study,participant,measurementType,typeName,measurementGroup,
                    groupInstance,trial,valType,value
        """
        problem_q = ""  # returns the instance ids of all instances that fail the criteria
        problem_q += " SELECT measurement.groupinstance "
        problem_q += core_sql_from_for_measurements()
        where_clause = core_sql_for_where_clauses_for_cohort(study, participants, -1, measurement_group,
                                                             -1, trial, start_time, end_time)
        problem_q += where_clause
        if len(value_test_conditions) > 0:
            problem_q += " AND (" + self.mk_value_tests(value_test_conditions, study) + ")"

        outer_query = core_sql_for_measurements()
        outer_query += where_clause
        if len(value_test_conditions) > 0:
            outer_query += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
        outer_query += " ORDER BY groupinstance, measurementtype"
        outer_query += ";"
        raw_results = self.return_query_result(outer_query)
        formed_measurements = transform_result_format.form_measurements(raw_results)
        return transform_result_format.form_measurement_group(self, study, measurement_group, formed_measurements)

    def get_measurement_type_info(self, study, measurement_type_id):
        """
        Returns information on a measurement type
        :param study: the study id
        :param measurement_type_id: the id of a measurement type
        :return: a list containing the elements: id, description, value type, name
        """
        mappings = {"measurement_type_id": str(measurement_type_id), "study": str(study)}
        query = file_utils.process_sql_template("sql/get_measurement_type_info.sql", mappings)
        return self.return_query_result(query)

    def return_query_result(self, query_text):
        """
        executes an SQL query. It is used for SELECT queries.
        :param query_text: the SQL
        :return: the result as a list of rows.
        """
        cur = self.dbConnection.cursor()
        cur.execute(query_text)
        return cur.fetchall()

    def exec_insert_with_return(self, query_text):
        """
        Executes INSERT, commits the outcome and returns the result from the RETURNING clause.
        :param query_text: the SQL
        :return the result from the RETURNING clause
        """
        cur = self.dbConnection.cursor()
        cur.execute(query_text)
        self.dbConnection.commit()
        return cur.fetchone()

    def exec_sql_with_no_return(self, query_text):
        """
        executes SQL and commits the outcome. Used to execute INSERT, UPDATE and DELETE statements with no RETURNING.
        :param query_text: the SQL
        """
        cur = self.dbConnection.cursor()
        cur.execute(query_text)
        self.dbConnection.commit()

    def get_all_measurement_groups(self, study):
        """
        A helper function that returns information on all the measurement groups in a study
        :param study: the study id
        :return: a list of [measurement group id, measurement group description]
        """
        mappings = {"study": str(study)}
        query = file_utils.process_sql_template("sql/get_all_measurement_groups.sql", mappings)
        return self.return_query_result(query)

    def get_all_measurement_groups_and_types_in_a_study(self, study):
        """
        A helper function that returns information on all the measurement groups and types in a study
        :param study: the study id
        :return: a list of rows. Each row is a list whose elements are: measurement group id, measurement type id
                   and the name of the measurement type
        """
        # Return all measurement groups and measurement types in a study
        mappings = {"study": str(study)}
        query = file_utils.process_sql_template("sql/get_all_measurement_groups_and_types_in_a_study.sql", mappings)
        return self.return_query_result(query)

    def insert_measurement_group(self, study, measurement_group, values,
                                 time=-1, trial=None, participant=None, source=None):  # None maps to SQL NULL
        """
         Insert one measurement group
         :param study: the study id
         :param measurement_group: the measurement group
         :param values: a list of the values from the measurement group in the form (measurementType,valType,value)
         :param time: the time the measurement was taken. It defaults to the current time
         :param trial: optional trial id
         :param participant: optional participant id
         :param source: optional source
         :return the measurement group instance
         """
        if time == -1:  # use the current date and time if none is specified
            time = datetime.datetime.now()  # use the current date and time if none is specified

        group_instance = 0
        cur = self.dbConnection.cursor()
        for (measurementType, val_type, value) in values:
            if val_type in [0, 4, 5, 6, 7]:  # the value must be stored in valInteger
                val_integer = value
                val_real = None
            elif val_type in [1, 8]:  # the value must be stored in valReal
                val_integer = None
                val_real = value
            elif val_type in [2, 3]:  # the value must be stored in the text or datetime tables
                val_integer = None
                val_real = None
            else:
                print("Error in valType in insertMeasurementGroup")
                val_integer = None
                val_real = None
            try:
                cur.execute("""
                            INSERT INTO measurement (id,time,study,trial,measurementgroup,groupinstance,
                                                     measurementtype,participant,source,valtype,valinteger,valreal)
                            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                            """,
                            (time, study, trial, measurement_group, group_instance, measurementType, participant,
                             source, val_type, val_integer, val_real))
            except psycopg2.Error as e:
                print("Error in insertMeasurementGroup: ", e.pgcode, "occurred.")
                print("See https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE")
                print(f'Study = {study},Trial = {trial}, Measurement Group = {measurement_group},'
                      f'Measurement Type = {measurementType},Source = {source}')
            gid = cur.fetchone()[0]
            if group_instance == 0:
                group_instance = gid
                # Now we know the id of the new measurement we can set the groupinstance field to be the same value for
                # all measurements in the group
                cur.execute("""
                            UPDATE measurement SET groupinstance = %s
                            WHERE id = %s;
                            """,
                            (group_instance, group_instance))  # set the groupinstance for the first measurement
            if val_type == 2:  # it's a Text Value so make entry in textvalue table
                cur.execute("""
                            INSERT INTO textvalue(measurement,textval,study)
                            VALUES (%s, %s, %s);
                            """,
                            (gid, value, study))
            if val_type == 3:  # it's a DateTime value so make entry in datetimevalue table
                cur.execute("""
                            INSERT INTO datetimevalue(measurement,datetimeval,study)
                            VALUES (%s, %s, %s);
                            """,
                            (gid, value, study))
        self.dbConnection.commit()
        return group_instance

    def get_participant_by_id(self, study, participant):
        """
         maps from unique participant.id to the local; id stored with measurements in the warehouse
         :param study: the study id
         :param participant: the id of the participant in the study
         :return The participantid of the participant
         """
        q = " SELECT participantid FROM participant " \
            " WHERE participant.study       = " + str(study) + \
            " AND participant.id = '" + participant + "';"  # error? should be turned into a string?
        res = self.return_query_result(q)
        found = len(res) == 1
        if found:
            return found, res[0][0]
        else:
            print("Participant", participant, " not found in participant.id")
            return found, res

    def get_participant(self, study_id, local_participant_id):
        """
        maps from a participantid that is local to the study, to the unique id stored with measurements in the warehouse
        :param study_id: the study id
        :param local_participant_id: the local participant id in the study
        :return The id of the participant
        """
        q = " SELECT id FROM participant " \
            " WHERE participant.study       = " + str(study_id) + \
            " AND participant.participantid = '" + local_participant_id + "';"
        res = self.return_query_result(q)
        found = len(res) == 1
        if found:
            return found, res[0][0]
        else:
            print("Participant", local_participant_id, " not found in participant.particpantid")
            return found, res

    def get_measurement_group(self, study_id, measurementgroup_description):
        """
        maps from the measurementgroup_description to the measurement group id used within the warehouse
        :param study_id: the study id
        :param measurementgroup_description: the description field of the measurement group
        :return (whether the measurement group exists, the measurement group)
        """

        q = " SELECT id FROM measurementgroup " \
            " WHERE measurementgroup.study       = " + str(study_id) + \
            " AND   measurementgroup.description = '" + measurementgroup_description + "';"
        res = self.return_query_result(q)
        found = len(res) == 1
        if found:
            return found, res[0][0]
        else:
            print("Event_type", measurementgroup_description, " not found in measurementgroup.description")
            return found, res

    def get_participants(self, study_id):
        """
        Get all participants in a study
        :param study_id: the study id
        :return: list of all the participants (id and participantid)
        """
        q = " SELECT id,participantid FROM participant " \
            " WHERE participant.study       = " + str(study_id) + ";"
        res = self.return_query_result(q)
        return res

    def add_participant(self, study_id, local_participant_id):
        """
        add a participant into the data warehouse
        :param study_id: the study id
        :param local_participant_id: the local name for the participant
        :res the new participant id
        """
        cur = self.dbConnection.cursor()
        q = " SELECT MAX(id) FROM participant " \
            " WHERE participant.study = " + str(study_id) + ";"
        res = self.return_query_result(q)  # find the biggest id
        max_id = res[0][0]
        if max_id is None:
            free_id = 0
        else:
            free_id = max_id + 1  # the next free id
        cur.execute("""
                    INSERT INTO participant(id,participantid,study)
                    VALUES (%s, %s, %s);
                    """,
                    (free_id, local_participant_id, study_id))  # insert the new entry
        self.dbConnection.commit()
        return free_id

    def add_participant_if_new(self, study_id, participant_id, local_participant_id):
        """
        add a participant into the data warehouse unless they already exist
        :param study_id: the study id
        :param participant_id: the participant_id
        :param local_participant_id: the local name for the participant
        :res (participant_added, new participant id)
        """
        cur = self.dbConnection.cursor()

        q = " SELECT id, participantid FROM participant " \
            " WHERE participant.study = " + str(study_id) + \
            " AND participant.id =  " + str(participant_id) + ";"
        res = self.return_query_result(q)
        participant_already_exists = len(res) > 0
        if participant_already_exists:
            return False, participant_id
        else:
            cur.execute("""
                        INSERT INTO participant(id,participantid,study)
                        VALUES (%s, %s, %s);
                        """,
                        (participant_id, local_participant_id, study_id))  # insert the new entry
            self.dbConnection.commit()
            return True, participant_id

    def n_mg_instances(self, mg_id, study):
        """
        Return the number of instances of a measurement group in a study
        :param mg_id:
        :param study:
        :return: number of instances
        """
        q = " SELECT COUNT(DISTINCT measurement.groupinstance) FROM measurement "
        q += " WHERE measurement.study       = " + str(study)
        q += " AND measurement.measurementgroup = " + str(mg_id)
        q += " ;"
        res = self.return_query_result(q)
        return res[0][0]

    def mg_instances(self, mg_id, study):
        """
        Return the ids of instances of a measurement group in a study
        :param mg_id:
        :param study:
        :return: ids
        """
        q = " SELECT DISTINCT measurement.groupinstance FROM measurement "
        q += " WHERE measurement.study       = " + str(study)
        q += " AND measurement.measurementgroup = " + str(mg_id)
        q += " ORDER BY measurement.groupinstance;"
        res = self.return_query_result(q)
        return res

    def get_type_ids_in_measurement_group(self, study, measurement_group):
        """
        A helper function that returns the ids of the measurement types in a measurement group
        :param study: study id
        :param measurement_group: measurement group id
        :return: list of ids of the measurement types in the measurement group
        """
        q = ""
        q += " SELECT "
        q += "    measurementtypetogroup.measurementtype  "
        q += " FROM "
        q += "    measurementtypetogroup "
        q += " WHERE "
        q += "    measurementtypetogroup.measurementgroup = "
        q += str(measurement_group)
        q += " AND "
        q += "    measurementtypetogroup.study = "
        q += str(study)
        q += " ORDER BY measurementtypetogroup.measurementtype"
        q += ";"
        type_ids = self.return_query_result(q)
        result = []
        for r in type_ids:
            result = result + [r[0]]
        return result
