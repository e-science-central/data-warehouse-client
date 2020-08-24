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
import json
import sys
from typing import List

import matplotlib.pyplot as pyplot
import psycopg2


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
        print("Init successfull! Running queries.\n")

    def printQueryResult(self, queryText):
        """
        executes a query and prints each row of the result
        :param queryText: the SQL query to be executed
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        rows = cur.fetchall()
        self.printRows(rows)

    def printRows(self, rows):
        """
        prints each row returned by a query
        :param rows: a list of rows. Each row is a list of fields
        """
        for row in rows:
            print(row)

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
                print("typeval error of ", rows[r][9])
        return rowOut

    def formMeasurementGroup(self,rows):
        """
        Takes the output of getMeasurementGroupInstancesWithValueTests and creates a result where each
           measurement group instance occupies one row.
        :param rows: list of rows returned by getMeasurementGroupInstancesWithValueTests or
                     getMeasurements (where it returns whole measurement group instances - i.e. where
                                      measurementType is not specified, but measurement group or
                                      measurement group instance is specified)
        :return: list of rows, each representing one measurement group instance, held in a list with elements:
               groupInstance,time of first measurement in instance,study,participant,measurementGroup,trial,
                   value1, value2....
               where value n is the value for the nth measurement in the instance (ordered by measurement type)
        """
        measurementGroup:int = rows[0][6]
        nMeasurementsPerInstance: int = self.numTypesInAMeasurementGroup(measurementGroup)
        nRows: int = len(rows) // nMeasurementsPerInstance # integer division in Python 3
        nCols: int = 6 + nMeasurementsPerInstance
        rowOut = [[None] * nCols for i in range(nRows)]
        firstMeasurementInInstance:int = 0
        for i in range(nRows):
            rowOut[i][0] = rows[firstMeasurementInInstance][7]  # groupInstance
            rowOut[i][1] = rows[firstMeasurementInInstance][1]  # time
            rowOut[i][2] = rows[firstMeasurementInInstance][2]  # study
            rowOut[i][3] = rows[firstMeasurementInInstance][3]  # participant
            rowOut[i][4] = rows[firstMeasurementInInstance][6]  # measurementGroup
            rowOut[i][5] = rows[firstMeasurementInInstance][8]  # trial
            for m in range(nMeasurementsPerInstance):
                rowOut[i][6+m] = rows[firstMeasurementInInstance+m][10]
            firstMeasurementInInstance = firstMeasurementInInstance + nMeasurementsPerInstance
        return rowOut

    def printMeasurementGroupInstances(self, rows):
        """
        Prints a list of measurement group instances, converting the datetime to strings
        :param rows: a list of measurement group instances in the format produced by formatMeasurementGroup
        """
        if len(rows)>0:
           nTypes:int = len(rows[0])-6
        for row in rows:
            print(row[0], ",", str(row[1]),end='')
            for i in range(nTypes+4):
               print(",", row[2+i],end='')
            print("")

    def exportMeasurementGroupsAsCSV(self, rows, groupId, fname):
        """
        Stores measurements returned by queries in a CSV file
        The input rows must be in the format produced by formMeasurementGroups
        The output file has a header row, followed by a row for each measurement group instance. This has the columns:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
        :param rows: a list of rows returned by formatMeasurementGroup
        :param groupId: the measurementGroupId
        :param fname: the filename of the output CSV file
        """
        typeNames:List[str] = self.getTypesInAMeasurementGroup(groupId)
        with open(fname, "w", newline="") as f:
            writer = csv.writer(f)
            headerRow:List[str] = ["Measurement Group Instance","Time","Study","Participant","Measurement Group","Trial"]
            for t in range(len(typeNames)):
                headerRow.append(typeNames[t][0])
            writer.writerow(headerRow)
            writer.writerows(rows)

    def coreSQLforMeasurements(self):
        """
        Creates the select and from clauses used by many of the functions that query the data warehouse
        :return: the select and from clauses used by several of the functions that query the data warehouse
        """
        q: str
        q = ""
        q += " SELECT "
        q += "    measurement.id,measurement.time, measurement.study,measurement.participant,"
        q += "    measurement.measurementtype, measurementtypetogroup.name,"
        q += "    measurement.measurementgroup,measurement.groupinstance,measurement.trial,"
        q += "    measurement.valtype, measurement.valinteger,"
        q += "    measurement.valreal , textvalue.textval, datetimevalue.datetimeval,category.categoryname "
        q += self.coreSQLFromForMesaurements()
        return q

    def coreSQLFromForMesaurements(self):
        """
        Creates the select and from clauses used by many of the functions that query the data warehouse
        :return: the from clause used by several of the functions that query the data warehouse
        """
        q: str
        q = ""
        q += " FROM "
        q += "    measurement INNER JOIN measurementtype ON measurement.measurementtype = measurementtype.id "
        q += "    INNER JOIN measurementtypetogroup ON measurement.measurementgroup = measurementtypetogroup.measurementgroup AND measurement.measurementtype = measurementtypetogroup.measurementtype"
        q += "    LEFT OUTER JOIN textvalue ON textvalue.measurement = measurement.id "
        q += "    LEFT OUTER JOIN datetimevalue ON datetimevalue.measurement = measurement.id "
        q += "    LEFT OUTER JOIN category  ON measurement.valinteger = category.categoryid AND measurement.measurementtype = category.measurementtype"
        return q

    def coreSQLforWhereClauses(self, study: int, participant: int, measurementType: int, measurementGroup: int,
                               groupInstance: int, trial: int, startTime, endTime):
        """
        Returns the where clauses used by many of the functions that query the data warehouse to filter out rows
        according to the criteria passed as parameters. A value of -1 for any parameter means that no filter is
        created for it
        :param study: a study id
        :param participant: a participant id
        :param measurementType: a measurementType
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: a tuple containing the SQL for the where clauses, and a count of how many there are
        """
        conditionCount: int = 0
        q: str = " "
        if (study != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.study = " + str(study)
        if (participant != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.participant = " + str(participant)
        if (measurementType != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.measurementtype = " + str(measurementType)
        if (measurementGroup != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.measurementgroup = " + str(measurementGroup)
        if (groupInstance != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.groupinstance = " + str(groupInstance)
        if (trial != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.trial = " + str(trial)
        if (startTime != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.time >= " + str(startTime)
        if (endTime != -1):
            conditionCount = conditionCount + 1
            if (conditionCount == 1):
                q += " WHERE "
            else:
                q += " AND "
            q += "   measurement.time <= " + str(endTime)
        return (q, conditionCount)

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
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, measurementType, measurementGroup,
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
        if valType == 0:                        # integer
            field = "measurement.valinteger"
        elif valType == 1:                      # real
            field = "measurement.valreal"
        elif valType == 2:                      # text
            field = "textvalue.textval"
        elif valType == 3:                      # datetime
            field = "datetimevalue.datetimeval"
        elif valType == 4:                      # boolean
            field = "measurement.valinteger"    # note we use 0 & 1 for booleans
        elif valType == 5:                      # nominal
            field = "measurement.valinteger"    # note we use the integer encoding for nominals
        elif valType == 6:                      #ordinal
            field = "measurement.valinteger"    # note we use the integer encoding for ordinals
        elif valType == 7:                      # bounded integer
            field = "measurement.valinteger"
        elif valType == 8:                      # bounded real
            field = "measurement.valreal"
        else:
            print("Error: valType out of range: ", valType)
        return field

    def aggregateMeasurements(self, measurementType, aggregation, study=-1, participant=-1, measurementGroup=-1,
                              groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        """

        :param measurementType: the type of the measurements to be aggregated
        :param aggregation: the aggregation function: this can be any of the postgres SQL aggregation functions,
                                  e.g."avg", "count", "max", "min", "sum"
        :param study: a study id
        :param participant: a participant id
        :param measurementGroup: a measurementGroup
        :param groupInstance: a groupInstance
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: the result of the aggregation
        """
        mtInfo = self.getMeasurementTypeInfo(measurementType)
        valType = mtInfo[0][2]
        q = "SELECT " + aggregation + "(" + self.fieldHoldingValue(valType) + ") "
        q += self.coreSQLFromForMesaurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, measurementType, measurementGroup,
                                                          groupInstance, trial, startTime, endTime)
        q += w
        rawResult = self.returnQueryResult(q)
        return rawResult[0]

    def makeValueTest(self, valType, valueTestCondition):
        """
        creates a condition for the where clause of a query
        :param valType: the type of the field being tested
        :param valueTestCondition: the test of that field
        :return: a fragment of SQL that can be included in the where clause of a query
        """
        cond = " (" + self.fieldHoldingValue(valType) + valueTestCondition + ") "
        return cond

    def getMeasurementsWithValueTest(self, measurementType, valueTestCondition, study=-1, participant=-1,
                                     measurementGroup=-1, groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        """
        Find all measurement of a particular type whose value meets some criteria.
        :param measurementType: the measurement type of the measurements to be tested
        :param valueTestCondition: a string holding the condition
                  against which the value in each measurement is compared.
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
        # Returns all the measurements of type measurementType that match the valueTestCondition (expressed using SQL syntax) and meet the conditions from the other parameters
        q = self.coreSQLforMeasurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, measurementType, measurementGroup,
                                                          groupInstance, trial, startTime, endTime)
        q += w

        mtInfo = self.getMeasurementTypeInfo(measurementType)
        valType = mtInfo[0][2] # find the vlaue type of the measurement
        # Add a clause to test the field that is relevant to the type of the measurement
        if (conditionCount == 0):
            q += " WHERE " # if this is the first condition in the where clause
        else:
            q += " AND " # if there have already been conditions
        cond = self.makeValueTest(valType, valueTestCondition)
        q += cond
        q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
        rawResults = self.returnQueryResult(q)
        return self.formMeasurements(rawResults)

    def numTypesInAMeasurementGroup(self, measurementGroup):
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
        q += ";"
        numTypes = self.returnQueryResult(q)
        return numTypes[0][0]

    def getTypesInAMeasurementGroup(self, measurementGroup):
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
        q += " ORDER BY measurementtypetogroup.measurementtype"
        q += ";"
        numTypes = self.returnQueryResult(q)
        return numTypes

    def getMeasurementGroupInstancesWithValueTests(self, measurementGroup, valueTestConditions, study=-1,
                                                   participant=-1, trial=-1, startTime=-1, endTime=-1):
        """
        Return all instances of a measurement group in which one or more of the measurements within the
            instance meet some specified criteria
        :param measurementGroup: a measurement group
        :param valueTestConditions: a list where each element is takes the following form:
                                    (measurementType,condition)
                                       where condition is a string holding the condition
                                       against which the value in each measurement is compared.
        :param study: a study id
        :param participant: a participant id
        :param trial: a trial id
        :param startTime: the start of a time period of interest
        :param endTime: the end of a time period of interest
        :return: a list of measurements. Each measurement is held in a list with the following fields:
                    id,time,study,participant,measurementType,typeName,measurementGroup,
                    groupInstance,trial,valType,value
        """
        q = ""  # q returns the instance ids of all instances that meet meet the criteria
        q += " SELECT restable.groupinstance "
        q += " FROM ( "
        q += " SELECT groupinstance " + self.coreSQLFromForMesaurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, -1, measurementGroup, -1, trial,
                                                          startTime, endTime)
        q += w
        if (conditionCount == 0):
            q += " WHERE (" # first condition in a where clause
        else:
            q += " AND ("
        totalConditions = len(valueTestConditions)
        c = 1  # the current condition being processed
        for (measurementType, condition) in valueTestConditions:
            mtInfo = self.getMeasurementTypeInfo(measurementType)
            valType = mtInfo[0][2]
            cond = "((measurement.measurementtype = " + str(measurementType) + ") AND " +\
                   self.makeValueTest(valType,condition) + ")"
            q += cond
            if (c < totalConditions):
                q += " OR "
            c = c + 1
        q += " OR "
        q += " (measurement.measurementtype NOT IN ("
        # make list of MeasurementTypesFrom valueTestConditions
        c = 1
        for (measurementType, condition) in valueTestConditions:
            q += str(measurementType)
            if (c < totalConditions):
                q += ","
            c = c + 1
        q += ") "
        q += "))) AS restable"
        q += " GROUP BY restable.groupinstance "
        q += " HAVING   COUNT(*) = "
        groupSize = self.numTypesInAMeasurementGroup(measurementGroup)
        q += str(groupSize)

        outerQuery = self.coreSQLforMeasurements()
        outerQuery += " WHERE "
        outerQuery += "    measurement.groupinstance IN (" + q + ")"
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
        for row in rows:
            print(row[0], ",", str(row[1]), ",", row[2], ",", row[3], ",", row[4], ",", row[5], ",", row[6], ",",
                  row[7], ",", row[8], ",", row[9], ",", row[10])

    def getMeasurementTypeInfo(self, measurementTypeId):
        """
        Returns information on a measurement type
        :param measurementTypeId: the id of a measurement type
        :return: a list containing the elements: id, description, value type, name
        """
        q = ""
        q += " SELECT "
        q += "    measurementtype.id,measurementtype.description,measurementtype.valtype,units.name "
        q += " FROM "
        q += "    measurementtype LEFT OUTER JOIN units ON measurementtype.units = units.id"
        q += " WHERE "
        q += "    measurementtype.id = "
        q += str(measurementTypeId)
        q += ";"
        mtinfo = self.returnQueryResult(q)
        return mtinfo

    def plotMeasurementType(self, rows, measurementTypeId, plotFile):
        """
        Plot the value of a measurement over time.
        :param rows: a list of measurements generated by the other client functions. Each measurement is in the form:
                        id,time,study,participant,measurementType,
                        typeName,measurementGroup,groupInstance,trial,valType,value
        :param measurementTypeId: the measurement type of the measurements to be plotted
        :param plotFile: the name of the file into which the plot will be written
        """
        # https://matplotlib.org/api/pyplot_api.html
        trans = [list(i) for i in zip(*rows)]  # transpose the list of lists
        x = trans[1]  # the data and time
        y = trans[10]  # the measurement value

        mtInfo = self.getMeasurementTypeInfo(measurementTypeId)
        units = mtInfo[0][3]  # get the units name
        pyplot.title(rows[0][5])
        pyplot.xlabel("Time") # Set the x-axis label
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

    def execQuery(self, queryText):
        """
        executes SQL and commits the outcome. It is used to execute INSERT, UPDATE and DELETE.
        :param queryText: the SQL
        """
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        cur.commit()

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
