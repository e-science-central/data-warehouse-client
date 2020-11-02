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

import csv
import datetime
import json
import sys
from typing import List

import matplotlib.pyplot as pyplot
import psycopg2
from more_itertools import intersperse
from tabulate import tabulate

from data_warehouse_client import file_utils


class DataWarehouse:
    def __init__(self, credentialsFile, dbName):
        # construct a connection to the warehouse
        self.credentialsFile = credentialsFile
        self.dbName = dbName
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


    def print_rows(self, rows, header: List[str]):
        """
        prints each row returned by a query
        :param rows: a list of rows. Each row is a list of fields
        :param header: a list of field names
        """
        print(tabulate(rows, headers=header))

    def export_measurements_as_csv(self, rows, fname):
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

    def form_measurements(self, rows):
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
        nRows: int = len(rows)
        nCols: int = 11
        rowOut = [[None] * nCols for i in range(nRows)]
        for r in range(nRows):
            # Keys: 0 = id, 1 = time, 2 = study, 3 = participant, 4 = measurementType, 5 = measurementTypeName,
            # 6 = measurementGroup, 7 = measurementGroupInstance, 8 = trial, 9 = valType
            for x in range(10):
                rowOut[r][x] = rows[r][x]
            if (rows[r][9] == 0):  # integer
                rowOut[r][10] = rows[r][10]
            elif (rows[r][9] == 1):  # real
                rowOut[r][10] = rows[r][11]
            elif (rows[r][9] == 2):  # text
                rowOut[r][10] = rows[r][12]
            elif (rows[r][9] == 3):  # datetime
                rowOut[r][10] = rows[r][13]
            elif (rows[r][9] == 4 and rows[r][10] == 0):  # boolean False
                rowOut[r][10] = "F"
            elif (rows[r][9] == 4 and rows[r][10] == 1):  # boolean True
                rowOut[r][10] = "T"
            elif (rows[r][9] == 5):  # nominal
                rowOut[r][10] = rows[r][14]
            elif (rows[r][9] == 6):  # ordinal
                rowOut[r][10] = rows[r][14]
            elif (rows[r][9] == 7):  # boundedint
                rowOut[r][10] = rows[r][10]
            elif (rows[r][9] == 8):  # boundedreal
                rowOut[r][10] = rows[r][11]
            else:
                print("typeval error of ", rows[r][9], " for id", rows[r][0], " study ", rows[r][2])
        return rowOut

    def print_measurement_group_instances(self, rows, groupId, study):
        """
        Prints a list of measurement group instances, converting the datetime to strings
        The input rows must be in the format produced by formMeasurementGroups
        The output file has a row for each measurement group instance. The fields are:
            groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                   value1, value2....
               where value n is the value for the nth measurement in the instance (ordered by measurement type)
        :param rows: a list of measurement group instances in the format produced by formatMeasurementGroup
        :param groupId: the measurementGroupId
        :param study: study id
        """
        typeNames: List[str] = self.get_types_in_a_measurement_group(study, groupId)
        nTypes: int = len(typeNames)
        headerRow: List[str] = ["Measurement Group Instance", "Time", "Study", "Participant", "Measurement Group",
                                "Trial"]
        for t in range(nTypes):
            headerRow.append(typeNames[t][0])
        print(tabulate(rows, headers=headerRow))

    def export_measurement_groups_as_csv(self, rows, groupId, study, fname):
        """
        Stores measurements returned by formMeasurementGroups in a CSV file
        The input rows must be in the format produced by formMeasurementGroups
        The output file has a header row, followed by a row for each measurement group instance. The table has columns:
            groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                   value1, value2....
               where value n is the value for the nth measurement in the instance (ordered by measurement type)
        :param rows: a list of rows returned by formatMeasurementGroup
        :param groupId: the measurementGroupId
        :param study: study id
        :param fname: the filename of the output CSV file
        """
        typeNames: List[str] = self.get_types_in_a_measurement_group(study, groupId)
        with open(fname, "w", newline="") as f:
            writer = csv.writer(f)
            headerRow: List[str] = ["Measurement Group Instance", "Time", "Study", "Participant", "Measurement Group",
                                    "Trial"]
            for t in range(len(typeNames)):
                headerRow.append(typeNames[t][0])
            writer.writerow(headerRow)
            writer.writerows(rows)

    def core_sql_for_measurements(self):
        """
        Creates the select and from clauses used by many of the functions that query the data warehouse
        :return: the select and from clauses used by several of the functions that query the data warehouse
        """
        return f'{self.core_sql_select_for_measurements()} {self.core_sql_from_for_measurements()}'

    def core_sql_select_for_measurements(self):
        """
         Creates the select clause used by many of the functions that query the data warehouse
         :return: the select clause used by several of the functions that query the data warehouse
         """
        return file_utils.process_sql_template("sql/core_sql_select_for_measurements.sql")

    def core_sql_from_for_measurements(self):
        """
        Creates the from clause used by many of the functions that query the data warehouse
        :return: the from clause used by several of the functions that query the data warehouse
        """
        return file_utils.process_sql_template("sql/core_sql_from_for_measurements.sql")

    def mk_where_condition(self, first_condition, column, test, value):
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
        return (q, first_condition)

    def core_sql_for_where_clauses(self, study: int, participant: int, measurement_type: int, measurement_group: int,
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
        :return: a tuple containing the SQL for the where clauses, and a count of how many there are
        """
        first_condition = True
        (qs, first_condition) = self.mk_where_condition(first_condition, "study", "=", study)
        (qp, first_condition) = self.mk_where_condition(first_condition, "participant", "=", participant)
        (qmt, first_condition) = self.mk_where_condition(first_condition, "measurementtype", "=", measurement_type)
        (qmg, first_condition) = self.mk_where_condition(first_condition, "measurementgroup", "=", measurement_group)
        (qgi, first_condition) = self.mk_where_condition(first_condition, "groupinstance", "=", group_instance)
        (qst, first_condition) = self.mk_where_condition(first_condition, "trial", "=", trial)
        (qet, first_condition) = self.mk_where_condition(first_condition, "time", ">=", start_time)
        (qt, first_condition) = self.mk_where_condition(first_condition, "time", "<=", end_time)
        return f' {qs}{qp}{qmt}{qmg}{qgi}{qst}{qet}{qt} ', first_condition

    def core_sql_for_where_clauses_for_cohort(self, study, participants, measurementType: int,
                                              measurementGroup: int, groupInstance: int, trial: int, startTime,
                                              endTime):
        """
        Returns the where clauses used by functions that query the data warehouse to filter out rows
        according to the criteria passed as parameters. A value of -1 for any parameter means that no filter is
        created for it
        :param study: a study id
        :param participants: a list of participant ids
        :param measurementType: a measurementType
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: the SQL for the where clauses
        """
        participants_str = map(lambda p: str(p), participants)
        q = " WHERE measurement.participant IN (" + ' '.join(
            [elem for elem in intersperse(",", participants_str)]) + ") "
        if study != -1:
            q += " AND measurement.study = " + str(study)
        if measurementType != -1:
            q += " AND measurement.measurementtype = " + str(measurementType)
        if measurementGroup != -1:
            q += " AND measurement.measurementgroup = " + str(measurementGroup)
        if groupInstance != -1:
            q += " AND measurement.groupinstance = " + str(groupInstance)
        if trial != -1:
            q += " AND measurement.trial = " + str(trial)
        if startTime != -1:
            q += " AND measurement.time >= " + str(startTime)
        if endTime != -1:
            q += " AND measurement.time <= " + str(endTime)
        return q

    def get_measurements(self, study=-1, participant=-1, measurementType=-1, measurementGroup=-1, groupInstance=-1,
                         trial=-1, startTime=-1, endTime=-1):
        """
        This function returns all measurements in the data warehouse that meet the optional criteria specified
        in the keyword arguments.
        The result is a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        :param study: a study id
        :param participant: a participant id
        :param measurementType: a measurementType
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        """
        (where_clause, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType,
                                                                          measurementGroup,
                                                                          groupInstance, trial, startTime, endTime)
        mappings = {"core_sql": self.core_sql_for_measurements(), "where_clause": where_clause}
        query = file_utils.process_sql_template("sql/get_measurements.sql", mappings)
        rawResults = self.return_query_result(query)
        return self.form_measurements(rawResults)

    def field_holding_value(self, val_type):
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

    def aggregate_measurements(self, measurementType, study, aggregation, participant=-1, measurementGroup=-1,
                               groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        """

        :param measurementType: the type of the measurements to be aggregated
        :param study: a study id
        :param aggregation: the aggregation function: this can be any of the postgres SQL aggregation functions,
                                  e.g."avg", "count", "max", "min", "sum"
        :param participant: a participant id
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: the result of the aggregation
        """
        mtInfo = self.get_measurement_type_info(study, measurementType)
        valType = mtInfo[0][2]
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType, measurementGroup,
                                                               groupInstance, trial, startTime, endTime)
        query = f'SELECT {aggregation} ( {self.field_holding_value(valType)} ) {self.core_sql_from_for_measurements()} {w}'
        rawResult = self.return_query_result(query)
        return rawResult[0][0]

    def make_value_test(self, valType, valueTestCondition):
        """
        creates a condition for the where clause of a query
        :param valType: the type of the field being tested
        :param valueTestCondition: the test of that field
        :return: a fragment of SQL that can be included in the where clause of a query
        """
        return f' ({self.field_holding_value(valType)}{valueTestCondition}) '

    def get_measurements_with_value_test(self, measurementType, study, valueTestCondition, participant=-1,
                                         measurementGroup=-1, groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        """
        Find all measurement of a particular type whose value meets some criteria.
        :param measurementType: the measurement type of the measurements to be tested
        :param study: a study id
        :param valueTestCondition: a string holding the condition
                  against which the value in each measurement is compared.
        :param participant: a participant id
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        """
        (where_clause, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType,
                                                                          measurementGroup,
                                                                          groupInstance, trial, startTime, endTime)
        mtInfo = self.get_measurement_type_info(study, measurementType)
        valType = mtInfo[0][2]  # find the value type of the measurement
        # Add a clause to test the field that is relevant to the type of the measurement
        condition = " WHERE " if first_condition else " AND "
        cond = self.make_value_test(valType, valueTestCondition)
        mappings = {"core_sql": self.core_sql_for_measurements(), "where_clause": where_clause, "condition": condition,
                    "cond": cond}
        query = file_utils.process_sql_template("sql/get_measurements_with_value.sql", mappings)
        rawResults = self.return_query_result(query)
        return self.form_measurements(rawResults)

    def get_measurements_by_cohort(self, cohortId, study, participant=-1, measurementType=-1,
                                   measurementGroup=-1, groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        """
        Find all measurements in a cohort that meet the criteria.
        :param cohortId: the value of the category in measurementType 181 that represents the condition
        :param study: a study id
        :param participant: a participant id
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        """
        (where_clause, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType,
                                                                          measurementGroup,
                                                                          groupInstance, trial, startTime, endTime)
        condition = " WHERE " if first_condition else " AND "
        mappings = {"core_sql": self.core_sql_for_measurements(), "where_clause": where_clause, "condition": condition,
                    "cohort_id": str(cohortId), "study": str(study)}
        query = file_utils.process_sql_template("sql/get_measurements_by_cohort.sql", mappings)
        rawResults = self.return_query_result(query)
        return self.form_measurements(rawResults)

    def num_types_in_a_measurement_group(self, study, measurementGroup):
        """
        A helper function that returns the number of measurement types in a measurement group
        :param measurementGroup: measurement group id
        :return: number of measurement types in the measurement group
        """
        mappings = {"measurement_group": str(measurementGroup), "study": str(study)}
        query = file_utils.process_sql_template("sql/num_types_in_a_measurement_group.sql", mappings)
        numTypes = self.return_query_result(query)
        return numTypes[0][0]

    def get_types_in_a_measurement_group(self, study, measurementGroup):
        """
        A helper function that returns the names of the measurement types in a measurement group
        :param measurementGroup: measurement group id
        :return: list of names of the measurement types in the measurement group
        """
        mappings = {"measurement_group": str(measurementGroup), "study": str(study)}
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
                   f'{self.make_value_test(val_type, condition)})'
            all_conditions = all_conditions + [cond]
        return ' '.join([elem for elem in intersperse(" OR ", all_conditions)])

    def get_participants_in_result(self, results):
        """

        :param results: a list of measurements. Each measurement is held in a list with the following fields:
                        id,time,study,participant,measurementType,typeName,measurementGroup,
                        groupInstance,trial,valType,value
        :return: a list of unique participants from the measurements
        """
        participants = map(lambda r: r[3], results)  # pick out participant
        return list(set(participants))

    def get_measurement_group_instances_with_value_tests(self, measurementGroup, study, valueTestConditions,
                                                         participant=-1, trial=-1, startTime=-1, endTime=-1):
        """
        Return all instances of a measurement group in which one or more of the measurements within the
            instance meet some specified criteria
        :param measurementGroup: a measurement group
        :param study: a study id
        :param valueTestConditions: a list where each element is takes the following form:
                                    (measurementType,condition)
                                       where condition is a string holding the condition
                                       against which the value in each measurement is compared.
        :param participant: a participant id
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
                    id,time,study,participant,measurementType,typeName,measurementGroup,
                    groupInstance,trial,valType,value
        """
        problem_q = ""  # returns the instance ids of all instances that fail the criteria
        problem_q += " SELECT measurement.groupinstance "
        problem_q += self.core_sql_from_for_measurements()
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, -1, measurementGroup, -1, trial,
                                                               startTime, endTime)
        problem_q += w
        if len(valueTestConditions) > 0:
            problem_q += " AND (" + self.mk_value_tests(valueTestConditions, study) + ")"

        outerQuery = self.core_sql_for_measurements()
        outerQuery += " " + w
        if len(valueTestConditions) > 0:
            outerQuery += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
        outerQuery += " ORDER BY groupinstance, measurementtype"
        outerQuery += ";"
        rawResults = self.return_query_result(outerQuery)
        return self.form_measurements(rawResults)

    def get_measurement_group_instances_for_cohort(self, measurement_group, study, participants, value_test_conditions,
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
        problem_q += self.core_sql_from_for_measurements()
        where_clause = self.core_sql_for_where_clauses_for_cohort(study, participants, -1, measurement_group,
                                                                  -1, trial, start_time, end_time)
        problem_q += where_clause
        if len(value_test_conditions) > 0:
            problem_q += " AND (" + self.mk_value_tests(value_test_conditions, study) + ")"

        outerQuery = self.core_sql_for_measurements()
        outerQuery += where_clause
        if len(value_test_conditions) > 0:
            outerQuery += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
        outerQuery += " ORDER BY groupinstance, measurementtype"
        outerQuery += ";"
        rawResults = self.return_query_result(outerQuery)
        return self.form_measurements(rawResults)

    def print_measurements(self, rows):
        """
        Prints a list of measurements, converting the datetimes to strings
        :param rows: a list of measurements with the elements id,time,study,participant,measurementType,
                        typeName,measurementGroup,groupInstance,trial,valType,value
        """
        headerRow: List[str] = ["Id", "Time", "Study", "Participant", "MeasurementType", "Type Name",
                                "Measurement Group", "Group Instance", "Trial", "Val Type", "Value"]
        print(tabulate(rows, headers=headerRow))

    def get_measurement_type_info(self, study, measurementTypeId):
        """
        Returns information on a measurement type
        :param study: the study id
        :param measurementTypeId: the id of a measurement type
        :return: a list containing the elements: id, description, value type, name
        """
        mappings = {"measurement_type_id": str(measurementTypeId), "study": str(study)}
        query = file_utils.process_sql_template("sql/get_measurement_type_info.sql", mappings)
        return self.return_query_result(query)

    def plot_measurement_type(self, rows, measurementTypeId, study, plotFile):
        """
        Plot the value of a measurement over time.
        :param rows: a list of measurements generated by the other client functions. Each measurement is in the form:
                        id,time,study,participant,measurementType,
                        typeName,measurementGroup,groupInstance,trial,valType,value
        :param measurementTypeId: the measurement type of the measurements to be plotted
        :param study: study id
        :param plotFile: the name of the file into which the plot will be written
        """
        # https://matplotlib.org/api/pyplot_api.html
        trans = [list(i) for i in zip(*rows)]  # transpose the list of lists
        x = trans[1]  # the data and time
        y = trans[10]  # the measurement value

        mtInfo = self.get_measurement_type_info(study, measurementTypeId)
        units = mtInfo[0][3]  # get the units name
        pyplot.title(rows[0][5])
        pyplot.xlabel("Time")  # Set the x-axis label
        pyplot.ylabel(units)  # Set the y-axis label to be the units of the measurement type
        pyplot.plot(x, y)
        pyplot.savefig(plotFile)
        pyplot.close()

    def return_query_result(self, queryText):
        """
        executes an SQL query. It is used for SELECT queries.
        :param queryText: the SQL
        :return: the result as a list of rows.
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        return cur.fetchall()

    def exec_insert_with_return(self, queryText):
        """
        Executes INSERT, commits the outcome and returns the result from the RETURNING clause.
        :param queryText: the SQL
        :return the result from the RETURNING clause
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        self.dbConnection.commit()
        return cur.fetchone()

    def exec_sql_with_no_return(self, queryText):
        """
        executes SQL and commits the outcome. Used to execute INSERT, UPDATE and DELETE statements with no RETURNING.
        :param queryText: the SQL
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
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

    def insert_one_measurement(self, study, measurementGroup, measurementType, valType, value,
                               time=-1, trial=None, participant=None, source=None):  # None maps to SQL NULL
        """
        Insert one measurement
        :param study: the study id
        :param measurementGroup: the measurement group
        :param measurementType: the measurement type
        :param valType: the value type
        :param value: the measurement value
        :param time: the time the measurement was taken. It defaults to the current time
        :param trial: optional trial id
        :param participant: optional participant id
        :param source: optional source
        :return the id of the measurement
        """
        if time == -1:  # use the current date and time if none is specified
            time = datetime.datetime.now()  # use the current date and time if none is specified

        if valType in [0, 4, 5, 6, 7]:  # the value must be stored in valInteger
            valInteger = value
            valReal = None
        elif valType in [1, 8]:  # the value must be stored in valReal
            valInteger = None
            valReal = value
        elif valType in [2, 3]:  # the value must be stored in the text or datetime tables
            valInteger = None
            valReal = None
        else:
            print("Error in valType in insertOneMeasurement")
        groupInstance = 0
        cur = self.dbConnection.cursor()
        cur.execute("""
                    INSERT INTO measurement (id,time,study,trial,measurementgroup,groupinstance,
                                             measurementtype,participant,source,valtype,valinteger,valreal)
                    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                    """,
                    (time, study, trial, measurementGroup, groupInstance, measurementType, participant,
                     source, valType, valInteger, valReal))
        id = cur.fetchone()[0]
        groupInstance = id
        # Now we know the id of the new measurement we can set the groupinstance field to be the same value.
        cur.execute("""
                    UPDATE measurement SET groupinstance = %s
                    WHERE id = %s;
                    """,
                    (groupInstance, id))

        if valType == 2:  # it's a Text Value so make entry in textvalue table
            cur.execute("""
                        INSERT INTO textvalue(measurement,textval,study)
                        VALUES (%s, %s, %s);
                        """,
                        (id, value, study))
        if valType == 3:  # it's a DateTime value so make entry in datetimevalue table
            cur.execute("""
                        INSERT INTO datetimevalue(measurement,datetimeval,study)
                        VALUES (%s, %s, %s);
                        """,
                        (id, value, study))
        self.dbConnection.commit()
        return id

    def insert_measurement_group(self, study, measurementGroup, values,
                                 time=-1, trial=None, participant=None, source=None):  # None maps to SQL NULL
        """
         Insert one measurement group
         :param study: the study id
         :param measurementGroup: the measurement group
         :param values: a list of the values from the measurement group in the form (measurementType,valType,value)
         :param time: the time the measurement was taken. It defaults to the current time
         :param trial: optional trial id
         :param participant: optional participant id
         :param source: optional source
         :return the measurement group instance
         """
        if time == -1:  # use the current date and time if none is specified
            time = datetime.datetime.now()  # use the current date and time if none is specified

        groupInstance = 0
        cur = self.dbConnection.cursor()
        for (measurementType, valType, value) in values:
            if valType in [0, 4, 5, 6, 7]:  # the value must be stored in valInteger
                valInteger = value
                valReal = None
            elif valType in [1, 8]:  # the value must be stored in valReal
                valInteger = None
                valReal = value
            elif valType in [2, 3]:  # the value must be stored in the text or datetime tables
                valInteger = None
                valReal = None
            else:
                print("Error in valType in insertMeasurementGroup")
            try:
                cur.execute("""
                            INSERT INTO measurement (id,time,study,trial,measurementgroup,groupinstance,
                                                     measurementtype,participant,source,valtype,valinteger,valreal)
                            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                            """,
                            (time, study, trial, measurementGroup, groupInstance, measurementType, participant,
                             source, valType, valInteger, valReal))
            except psycopg2.Error as e:
                print("Error in insertMeasurementGroup: ", e.pgcode, "occurred.")
                print("See https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE")
                print(f'Study = {study},Trial = {trial}, Measurement Group = {measurementGroup},'
                      f'Measurement Type = {measurementType},Source = {source}')
            id = cur.fetchone()[0]
            if groupInstance == 0:
                groupInstance = id
                # Now we know the id of the new measurement we can set the groupinstance field to be the same value for
                # all measurements in the group
                cur.execute("""
                            UPDATE measurement SET groupinstance = %s
                            WHERE id = %s;
                            """,
                            (groupInstance, groupInstance))  # set the groupinstance for the first measurement
            if valType == 2:  # it's a Text Value so make entry in textvalue table
                cur.execute("""
                            INSERT INTO textvalue(measurement,textval,study)
                            VALUES (%s, %s, %s);
                            """,
                            (id, value, study))
            if valType == 3:  # it's a DateTime value so make entry in datetimevalue table
                cur.execute("""
                            INSERT INTO datetimevalue(measurement,datetimeval,study)
                            VALUES (%s, %s, %s);
                            """,
                            (id, value, study))
        self.dbConnection.commit()
        return groupInstance

    def get_participant_by_id(self, study, participant):
        """
         maps from unique participant.id to the local; id stored with measurements in the warehouse
         :param study: the study id
         :param participant: the id of the participant in the study
         :return The participantid of the participant
         """
        q = " SELECT participantid FROM participant " \
            " WHERE participant.study       = " + str(study) + \
            " AND participant.id = '" + participant + "';"
        res = self.return_query_result(q)
        found = len(res) == 1
        if found:
            return (found, res[0][0])
        else:
            print("Participant", participant, " not found in participant.id")
            return (found, res)

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
            return (found, res[0][0])
        else:
            print("Participant", local_participant_id, " not found in participant.particpantid")
            return (found, res)

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
            return (found, res[0][0])
        else:
            print("Event_type", measurementgroup_description, " not found in measurementgroup.description")
            return (found, res)

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
            return (False, participant_id)
        else:
            cur.execute("""
                        INSERT INTO participant(id,participantid,study)
                        VALUES (%s, %s, %s);
                        """,
                        (participant_id, local_participant_id, study_id))  # insert the new entry
            self.dbConnection.commit()
            return (True, participant_id)

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

    def form_measurement_group(self, study, rows):
        """
        Creates a result where each measurement group instance occupies one row.
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
            mts = self.get_type_ids_in_measurement_group(study, measurement_group)
        result_values = {}  # the measurement values
        result_common = {}  # the common values returned for each instance:
        # instance,time of first measurement,study,participant,measurementGroup,trial
        for (id, time, study_id, participant, mt, tn, mg, mgi, trial, val_type, value) in rows:
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
        return result
