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

import matplotlib.pyplot as pyplot
import psycopg2


class DataWarehouse:
    def __init__(self, credentialsFile, dbName):
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
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        rows = cur.fetchall()
        for row in rows:
            print(row)

    def printRows(self, rows):
        for row in rows:
            print(row)

    def exportMeasurementAsCSV(self, rows, fname):
        with open(fname, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Id", "Time", "Study", "Participant", "MeasurementType", "MeasurementTypeName", "MeasurementGroup",
                 "MeasurementGroupInstance", "Trial", "ValType", "Value"])
            writer.writerows(rows)

    def formMeasurements(self, rows):
        nRows: int = len(rows)
        nCols: int = 11
        #  rowOut = [[0]*nCols]*nRows
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
            else:
                print("typeval error of ", rows[r][9])
        return rowOut

    def coreSQLforMeasurements(self):
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
        q = self.coreSQLforMeasurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, measurementType, measurementGroup,
                                                          groupInstance, trial, startTime, endTime)
        q += w
        q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
        rawResults = self.returnQueryResult(q)
        return self.formMeasurements(rawResults)

    def fieldHoldingValue(self, valType):
        # Use the valType to return the field that holds the value in the measurement
        if valType == 0:
            field = "measurement.valinteger"
        elif valType == 1:
            field = "measurement.valreal"
        elif valType == 2:
            field = "textvalue.textval"
        elif valType == 3:
            field = "datetimevalue.datetimeval"
        elif valType == 4:
            field = "measurement.valinteger"  # note we use 0 & 1 for booleans
        elif valType == 5:
            field = "measurement.valinteger"  # note we use the integer encoding for nominals
        elif valType == 6:
            field = "measurement.valinteger"  # note we use the integer encoding for ordinals
        elif valType == 7:
            field = "measurement.valinteger"  # bounded integer
        else:
            print("Error: valType out of range: ", valType)
        return field

    def aggregateMeasurements(self, measurementType, aggregation, study=-1, participant=-1, measurementGroup=-1,
                              groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        mtInfo = self.getMeasurementTypeInfo(measurementType)
        valType = mtInfo[0][2]
        q = "SELECT " + aggregation + "(" + self.fieldHoldingValue(valType) + ") "
        q += self.coreSQLFromForMesaurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, measurementType, measurementGroup,
                                                          groupInstance, trial, startTime, endTime)
        q += w
        rawResult = self.returnQueryResult(q)
        return rawResult

    def makeValueTest(self, valType, valueTestCondition):
        cond = " (" + self.fieldHoldingValue(valType) + valueTestCondition + ") "
        return cond

    def getMeasurementsWithValueTest(self, measurementType, valueTestCondition, study=-1, participant=-1,
                                     measurementGroup=-1, groupInstance=-1, trial=-1, startTime=-1, endTime=-1):
        # Returns all the measurements of type measurementType that match the valueTestCondition (expressed using SQL syntax) and meet the conditions from the other parameters
        q = self.coreSQLforMeasurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, measurementType, measurementGroup,
                                                          groupInstance, trial, startTime, endTime)
        q += w

        mtInfo = self.getMeasurementTypeInfo(measurementType)
        valType = mtInfo[0][2]
        # Add a clause to test the measurement that is relevant to the type of the measurement
        if (conditionCount == 0):
            q += " WHERE "
        else:
            q += " AND "
        cond = self.makeValueTest(valType, valueTestCondition)
        q += cond
        q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
        rawResults = self.returnQueryResult(q)
        return self.formMeasurements(rawResults)

    def numTypesInAMeasurementGroup(self, measurementGroup):
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

    def getMeasurementGroupInstancesWithValueTests(self, measurementGroup, valueTestConditions, study=-1,
                                                   participant=-1, trial=-1, startTime=-1, endTime=-1):
        q = ""  # q returns the instance ids of all instances that meet meet the criteria
        q += " SELECT restable.groupinstance "
        q += " FROM ( "
        q += " SELECT groupinstance " + self.coreSQLFromForMesaurements()
        (w, conditionCount) = self.coreSQLforWhereClauses(study, participant, -1, measurementGroup, -1, trial,
                                                          startTime, endTime)
        q += w
        if (conditionCount == 0):
            q += " WHERE ("
        else:
            q += " AND ("
        totalConditions = len(valueTestConditions)
        c = 1  # the current condition being processed
        for (measurementType, condition) in valueTestConditions:
            mtInfo = self.getMeasurementTypeInfo(measurementType)
            valType = mtInfo[0][2]
            cond = "((measurement.measurementtype = " + str(measurementType) + ") AND " + self.makeValueTest(valType,
                                                                                                             condition) + ")"
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
        for row in rows:
            print(row[0], ",", str(row[1]), ",", row[2], ",", row[3], ",", row[4], ",", row[5], ",", row[6], ",",
                  row[7], ",", row[8], ",", row[9], ",", row[10])

    def getMeasurementTypeInfo(self, measurementTypeId):
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
        # https://matplotlib.org/api/pyplot_api.html
        trans = [list(i) for i in zip(*rows)]  # transpose the list of lists
        x = trans[1]  # the data and time
        y = trans[10]  # the measurement value

        mtInfo = self.getMeasurementTypeInfo(measurementTypeId)
        units = mtInfo[0][3]  # get the units name
        pyplot.title(rows[0][5])
        pyplot.xlabel("Time")
        pyplot.ylabel(units)
        pyplot.plot(x, y)
        pyplot.savefig(plotFile)
        pyplot.close()

    def returnQueryResult(self, queryText):
        cur = self.dbConnection.cursor()
        cur.execute(queryText)
        rowsq = cur.fetchall()
        return rowsq

    def execQuery(self, dbConnection, queryText):
        cur = dbConnection.cursor()
        cur.execute(queryText)
        cur.commit()

    def getAllMeasurementGroupsAndTypesInAStudy(self, studyId):
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
