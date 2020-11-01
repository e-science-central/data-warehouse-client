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

    def printRows(self, rows, header: List[str]):
        """
        prints each row returned by a query
        :param rows: a list of rows. Each row is a list of fields
        :param header: a list of field names
        """
        print(tabulate(rows, headers=header))

    def exportMeasurementAsCSV(self, rows, fname):
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

    def formMeasurements(self, rows):
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
            rowOut[r][0] = rows[r][0]  # id
            rowOut[r][1] = rows[r][1]  # time
            rowOut[r][2] = rows[r][2]  # study
            rowOut[r][3] = rows[r][3]  # participant
            rowOut[r][4] = rows[r][4]  # measurementType
            rowOut[r][5] = rows[r][5]  # measurementTypeName
            rowOut[r][6] = rows[r][6]  # measurementGroup
            rowOut[r][7] = rows[r][7]  # measurementGroupInstance
            rowOut[r][8] = rows[r][8]  # trial
            rowOut[r][9] = rows[r][9]  # valType
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

    def printMeasurementGroupInstances(self, rows, groupId, study):
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
        typeNames: List[str] = self.getTypesInAMeasurementGroup(study, groupId)
        nTypes: int = len(typeNames)
        headerRow: List[str] = ["Measurement Group Instance", "Time", "Study", "Participant", "Measurement Group",
                                "Trial"]
        for t in range(nTypes):
            headerRow.append(typeNames[t][0])
        print(tabulate(rows, headers=headerRow))

    def exportMeasurementGroupsAsCSV(self, rows, groupId, study, fname):
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
        typeNames: List[str] = self.getTypesInAMeasurementGroup(study, groupId)
        with open(fname, "w", newline="") as f:
            writer = csv.writer(f)
            headerRow: List[str] = ["Measurement Group Instance", "Time", "Study", "Participant", "Measurement Group",
                                    "Trial"]
            for t in range(len(typeNames)):
                headerRow.append(typeNames[t][0])
            writer.writerow(headerRow)
            writer.writerows(rows)

    def coreSQLforMeasurements(self):
        """
        Creates the select and from clauses used by many of the functions that query the data warehouse
        :return: the select and from clauses used by several of the functions that query the data warehouse
        """
        return self.coreSQLSelectForMeasurements() + self.coreSQLFromForMeasurements()

    def coreSQLSelectForMeasurements(self):
        """
         Creates the select clause used by many of the functions that query the data warehouse
         :return: the select clause used by several of the functions that query the data warehouse
         """
        q: str
        q = ""
        q += " SELECT "
        q += "    measurement.id,measurement.time, measurement.study,measurement.participant,"
        q += "    measurement.measurementtype, measurementtypetogroup.name,"
        q += "    measurement.measurementgroup,measurement.groupinstance,measurement.trial,"
        q += "    measurement.valtype, measurement.valinteger,"
        q += "    measurement.valreal , textvalue.textval, datetimevalue.datetimeval,category.categoryname "
        return q

    def coreSQLFromForMeasurements(self):
        """
        Creates the from clause used by many of the functions that query the data warehouse
        :return: the from clause used by several of the functions that query the data warehouse
        """
        q: str
        q = ""
        q += " FROM "
        q += "    measurement INNER JOIN measurementtype ON  measurement.measurementtype = measurementtype.id "
        q += "                                           AND measurement.study           = measurementtype.study "
        q += "    INNER JOIN measurementtypetogroup ON measurement.measurementgroup  = measurementtypetogroup.measurementgroup "
        q += "                                         AND measurement.measurementtype   = measurementtypetogroup.measurementtype "
        q += "                                         AND measurement.study             = measurementtypetogroup.study "
        q += "    LEFT OUTER JOIN textvalue ON  textvalue.measurement = measurement.id "
        q += "                              AND textvalue.study       = measurement.study "
        q += "    LEFT OUTER JOIN datetimevalue ON  datetimevalue.measurement = measurement.id "
        q += "                                  AND datetimevalue.study       = measurement.study "
        q += "    LEFT OUTER JOIN category  ON  measurement.valinteger      = category.categoryid "
        q += "                              AND measurement.measurementtype = category.measurementtype "
        q += "                              AND measurement.study           = category.study "
        return q

    def mk_where_condition(self, first_condition, column, test, value):
        """
        :param first_condition: true if this is the first condition in a where clause
        :param column: column name in measurement table
        :param test: comparator for value
        :param value: value to be tested in the where clause
        :return: (where clause, first_condition)
        """
        if value != -1:
            if first_condition:
                q = " WHERE "
            else:
                q = " AND "
            q += "   measurement." + column + test + str(value)
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
        return (" " + qs + qp + qmt + qmg + qgi + qst + qet + qt + " ", first_condition)

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

    def getMeasurements(self, study=-1, participant=-1, measurementType=-1, measurementGroup=-1, groupInstance=-1,
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
        q = self.coreSQLforMeasurements()
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType, measurementGroup,
                                                               groupInstance, trial, startTime, endTime)
        q += w
        q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
        rawResults = self.returnQueryResult(q)
        return self.formMeasurements(rawResults)

    def fieldHoldingValue(self, valType):
        """
        A helper function that returns the data warehouse field that holds measurement values of the type
        specified in the parameter
        :param valType: the type of measurement
        :return: the database field holding the measurement value
        """
        # Use the valType to return the field that holds the value in the measurement
        if valType == 0:  # integer
            field = "measurement.valinteger"
        elif valType == 1:  # real
            field = "measurement.valreal"
        elif valType == 2:  # text
            field = "textvalue.textval"
        elif valType == 3:  # datetime
            field = "datetimevalue.datetimeval"
        elif valType == 4:  # boolean
            field = "measurement.valinteger"  # note we use 0 & 1 for booleans
        elif valType == 5:  # nominal
            field = "measurement.valinteger"  # note we use the integer encoding for nominals
        elif valType == 6:  # ordinal
            field = "measurement.valinteger"  # note we use the integer encoding for ordinals
        elif valType == 7:  # bounded integer
            field = "measurement.valinteger"
        elif valType == 8:  # bounded real
            field = "measurement.valreal"
        else:
            print("Error: valType out of range: ", valType)
        return field

    def aggregateMeasurements(self, measurementType, study, aggregation, participant=-1, measurementGroup=-1,
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
        mtInfo = self.getMeasurementTypeInfo(study, measurementType)
        valType = mtInfo[0][2]
        q = "SELECT " + aggregation + "(" + self.fieldHoldingValue(valType) + ") "
        q += self.coreSQLFromForMeasurements()
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType, measurementGroup,
                                                               groupInstance, trial, startTime, endTime)
        q += w
        rawResult = self.returnQueryResult(q)
        return rawResult[0][0]

    def makeValueTest(self, valType, valueTestCondition):
        """
        creates a condition for the where clause of a query
        :param valType: the type of the field being tested
        :param valueTestCondition: the test of that field
        :return: a fragment of SQL that can be included in the where clause of a query
        """
        cond = " (" + self.fieldHoldingValue(valType) + valueTestCondition + ") "
        return cond

    def getMeasurementsWithValueTest(self, measurementType, study, valueTestCondition, participant=-1,
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
        q = self.coreSQLforMeasurements()
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType, measurementGroup,
                                                               groupInstance, trial, startTime, endTime)
        q += w

        mtInfo = self.getMeasurementTypeInfo(study, measurementType)
        valType = mtInfo[0][2]  # find the value type of the measurement
        # Add a clause to test the field that is relevant to the type of the measurement
        if first_condition:
            q += " WHERE "  # if this is the first condition in the where clause
        else:
            q += " AND "  # if there have already been conditions
        cond = self.makeValueTest(valType, valueTestCondition)
        q += cond
        q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
        rawResults = self.returnQueryResult(q)
        return self.formMeasurements(rawResults)

    def getMeasurementsByCohort(self, cohortId, study, participant=-1, measurementType=-1,
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
        c: str
        c = ""  # get all participants in a cohort
        c += " SELECT measurement.participant "
        c += " FROM   measurement "
        c += " WHERE  measurement.measurementtype = 181 "  # the measurementType holding the condition
        c += " AND    measurement.valinteger      = " + str(cohortId)
        c += " AND    measurement.study           = " + str(study)

        q: str = self.coreSQLforMeasurements()
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, measurementType, measurementGroup,
                                                               groupInstance, trial, startTime, endTime)
        q += w
        if first_condition:
            q += " WHERE "  # if this is the first condition in the where clause
        else:
            q += " AND "  # if there have already been conditions
        q += " measurement.participant IN (" + c + ") "
        q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
        rawResults = self.returnQueryResult(q)
        return self.formMeasurements(rawResults)

    def numTypesInAMeasurementGroup(self, study, measurementGroup):
        """
        A helper function that returns the number of measurement types in a measurement group
        :param measurementGroup: measurement group id
        :return: number of measurement types in the measurement group
        """
        q = ""
        q += " SELECT "
        q += "    COUNT(*)  "
        q += " FROM "
        q += "    measurementtypetogroup "
        q += " WHERE "
        q += "    measurementtypetogroup.measurementgroup = "
        q += str(measurementGroup)
        q += " AND "
        q += "    measurementtypetogroup.study = "
        q += str(study)
        q += ";"
        numTypes = self.returnQueryResult(q)
        return numTypes[0][0]

    def getTypesInAMeasurementGroup(self, study, measurementGroup):
        """
        A helper function that returns the names of the measurement types in a measurement group
        :param measurementGroup: measurement group id
        :return: list of names of the measurement types in the measurement group
        """
        q = ""
        q += " SELECT "
        q += "    measurementtypetogroup.name  "
        q += " FROM "
        q += "    measurementtypetogroup "
        q += " WHERE "
        q += "    measurementtypetogroup.measurementgroup = "
        q += str(measurementGroup)
        q += " AND "
        q += "    measurementtypetogroup.study = "
        q += str(study)
        q += " ORDER BY measurementtypetogroup.measurementtype"
        q += ";"
        type_names = self.returnQueryResult(q)
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
            mt_info = self.getMeasurementTypeInfo(study, measurement_type)
            val_type = mt_info[0][2]
            cond = "((measurement.measurementtype = " + str(measurement_type) + ") AND NOT " + \
                   self.makeValueTest(val_type, condition) + ")"
            all_conditions = all_conditions + [cond]
        result = ' '.join([elem for elem in intersperse(" OR ", all_conditions)])
        return result

    def get_participants_in_result(self, results):
        """

        :param results: a list of measurements. Each measurement is held in a list with the following fields:
                        id,time,study,participant,measurementType,typeName,measurementGroup,
                        groupInstance,trial,valType,value
        :return: a list of unique participants from the measurements
        """
        participants = map(lambda r: r[3], results)  # pick out participant
        return list(set(participants))

    def getMeasurementGroupInstancesWithValueTests(self, measurementGroup, study, valueTestConditions,
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
        problem_q += self.coreSQLFromForMeasurements()
        (w, first_condition) = self.core_sql_for_where_clauses(study, participant, -1, measurementGroup, -1, trial,
                                                               startTime, endTime)
        problem_q += w
        if len(valueTestConditions) > 0:
            problem_q += " AND (" + self.mk_value_tests(valueTestConditions, study) + ")"

        outerQuery = self.coreSQLforMeasurements()
        outerQuery += w
        if len(valueTestConditions) > 0:
            outerQuery += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
        outerQuery += " ORDER BY groupinstance, measurementtype"
        outerQuery += ";"
        rawResults = self.returnQueryResult(outerQuery)
        return self.formMeasurements(rawResults)

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
        problem_q += self.coreSQLFromForMeasurements()
        where_clause = self.core_sql_for_where_clauses_for_cohort(study, participants, -1, measurement_group,
                                                                  -1, trial, start_time, end_time)
        problem_q += where_clause
        if len(value_test_conditions) > 0:
            problem_q += " AND (" + self.mk_value_tests(value_test_conditions, study) + ")"

        outerQuery = self.coreSQLforMeasurements()
        outerQuery += where_clause
        if len(value_test_conditions) > 0:
            outerQuery += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
        outerQuery += " ORDER BY groupinstance, measurementtype"
        outerQuery += ";"
        rawResults = self.returnQueryResult(outerQuery)
        return self.formMeasurements(rawResults)

    def printMeasurements(self, rows):
        """
        Prints a list of measurements, converting the datetimes to strings
        :param rows: a list of measurements with the elements id,time,study,participant,measurementType,
                        typeName,measurementGroup,groupInstance,trial,valType,value
        """
        headerRow: List[str] = ["Id", "Time", "Study", "Participant", "MeasurementType", "Type Name",
                                "Measurement Group", "Group Instance", "Trial", "Val Type", "Value"]
        print(tabulate(rows, headers=headerRow))

    def getMeasurementTypeInfo(self, study, measurementTypeId):
        """
        Returns information on a measurement type
        :param study: the study id
        :param measurementTypeId: the id of a measurement type
        :return: a list containing the elements: id, description, value type, name
        """
        q = ""
        q += " SELECT "
        q += "    measurementtype.id,measurementtype.description,measurementtype.valtype,units.name "
        q += " FROM "
        q += "    measurementtype LEFT OUTER JOIN units ON  measurementtype.units = units.id "
        q += "                                          AND measurementtype.study = units.study "
        q += " WHERE "
        q += "    measurementtype.id = "
        q += str(measurementTypeId)
        q += " AND "
        q += "    measurementtype.study = "
        q += str(study)
        q += ";"
        mtinfo = self.returnQueryResult(q)
        return mtinfo

    def plotMeasurementType(self, rows, measurementTypeId, study, plotFile):
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

        mtInfo = self.getMeasurementTypeInfo(study, measurementTypeId)
        units = mtInfo[0][3]  # get the units name
        pyplot.title(rows[0][5])
        pyplot.xlabel("Time")  # Set the x-axis label
        pyplot.ylabel(units)  # Set the y-axis label to be the units of the measurement type
        pyplot.plot(x, y)
        pyplot.savefig(plotFile)
        pyplot.close()

    def returnQueryResult(self, queryText):
        """
        executes an SQL query. It is used for SELECT queries.
        :param queryText: the SQL
        :return: the result as a list of rows.
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        rowsq = cur.fetchall()
        return rowsq

    def execInsertWithReturn(self, queryText):
        """
        Executes INSERT, commits the outcome and returns the result from the RETURNING clause.
        :param queryText: the SQL
        :return the result from the RETURNING clause
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        self.dbConnection.commit()
        return cur.fetchone()

    def execSQLWithNoReturn(self, queryText):
        """
        executes SQL and commits the outcome. Used to execute INSERT, UPDATE and DELETE statements with no RETURNING.
        :param queryText: the SQL
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        self.dbConnection.commit()

    def get_all_measurement_groups(self, study_id):
        """
        A helper function that returns information on all the measurement groups in a study
        :param study_id: the study id
        :return: a list of [measurement group id, measurement group description]
        """
        q = ""
        q += " SELECT"
        q += "    measurementgroup.id,"
        q += "    measurementgroup.description "
        q += " FROM "
        q += "    measurementgroup "
        q += " WHERE "
        q += "    measurementgroup.study = "
        q += str(study_id)
        q += " ORDER BY measurementgroup.id; "
        return self.returnQueryResult(q)

    def getAllMeasurementGroupsAndTypesInAStudy(self, studyId):
        """
        A helper function that returns information on all the measurement groups and types in a study
        :param studyId: the study id
        :return: a list of rows. Each row is a list whose elements are: measurement group id, measurement type id
                   and the name of the measurement type
        """
        # Return all measurement groups and measurement types in a study
        q = ""
        q += " SELECT"
        q += "    measurementtypetogroup.measurementgroup,"
        q += "    measurementtypetogroup.measurementtype, "
        q += "    measurementtypetogroup.name            "
        q += " FROM "
        q += "    measurementtypetogroup "
        q += " WHERE "
        q += "    measurementtypetogroup.study = "
        q += str(studyId)
        q += " ORDER BY measurementtypetogroup.measurementgroup, measurementtypetogroup.measurementtype; "
        return self.returnQueryResult(q)

    def insertOneMeasurement(self, study, measurementGroup, measurementType, valType, value,
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

    def insertMeasurementGroup(self, study, measurementGroup, values,
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
        res = self.returnQueryResult(q)
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
        res = self.returnQueryResult(q)
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
        res = self.returnQueryResult(q)
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
        res = self.returnQueryResult(q)
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
        res = self.returnQueryResult(q)  # find the biggest id
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
        res = self.returnQueryResult(q)
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
        res = self.returnQueryResult(q)
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
        res = self.returnQueryResult(q)
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
        type_ids = self.returnQueryResult(q)
        result = []
        for r in type_ids:
            result = result + [r[0]]
        return result

    def formMeasurementGroup(self, study, rows):
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
