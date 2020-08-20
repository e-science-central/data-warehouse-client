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

import psycopg2
import sys
import json
import matplotlib.pyplot as pyplot
import csv

def printQueryResult(dbConnection,queryText):
  cur= dbConnection.cursor()
  cur.execute(queryText)
  rows = cur.fetchall()
  for row in rows:
     print(row)
     
def printRows(rows):
    for row in rows:
        print(row)

def exportMeasurementAsCSV(rows,fname):
    with open(fname,"w",newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Id","Time","Study","Participant","MeasurementType","MeasurementTypeName","MeasurementGroup","MeasurementGroupInstance","Trial","ValType","Value"])
        writer.writerows(rows)

def formMeasurements(rows):
  nRows:int  = len(rows)
  nCols:int  = 11 
#  rowOut = [[0]*nCols]*nRows 
  rowOut = [[None] * nCols for i in range(nRows)]
  for r in range(nRows):
     rowOut[r][0]=rows[r][0] #id
     rowOut[r][1]=rows[r][1] #time
     rowOut[r][2]=rows[r][2] #study
     rowOut[r][3]=rows[r][3] #participant
     rowOut[r][4]=rows[r][4] #measurementType
     rowOut[r][5]=rows[r][5] #measurementTypeName
     rowOut[r][6]=rows[r][6] #measurementGroup
     rowOut[r][7]=rows[r][7] #measurementGroupInstance
     rowOut[r][8]=rows[r][8] #trial
     rowOut[r][9]=rows[r][9] #valType
     if(rows[r][9]  ==0):    # integer
        rowOut[r][10]=rows[r][10]
     elif(rows[r][9]==1):    # real
       rowOut[r][10]=rows[r][11]
     elif(rows[r][9]==2):    # text
       rowOut[r][10]=rows[r][12]
     elif(rows[r][9]==3):    # datetime
       rowOut[r][10]=rows[r][13]
     elif(rows[r][9]==4 and rows[r][10]==0): # boolean False
       rowOut[r][10]="F"
     elif(rows[r][9]==4 and rows[r][10]==1): # boolean True
       rowOut[r][10]="T"
     elif(rows[r][9]==5):   # nominal
       rowOut[r][10]=rows[r][14] 
     elif(rows[r][9]==6):   # ordinal
       rowOut[r][10]=rows[r][14] 
     elif(rows[r][9]==7):   # boundedint
       rowOut[r][10]=rows[r][10] 
     else:
       print("typeval error of ",rows[r][9])
  return rowOut

def coreSQLforMeasurements():
   q:str
   q =  ""
   q += " SELECT "
   q += "    measurement.id,measurement.time, measurement.study,measurement.participant,"
   q += "    measurement.measurementtype, measurementtypetogroup.name,"
   q += "    measurement.measurementgroup,measurement.groupinstance,measurement.trial,"
   q += "    measurement.valtype, measurement.valinteger,"
   q += "    measurement.valreal , textvalue.textval, datetimevalue.datetimeval,category.categoryname "
   q += coreSQLFromForMesaurements()
   return q

def coreSQLFromForMesaurements():
   q:str
   q =  ""
   q += " FROM "
   q += "    measurement INNER JOIN measurementtype ON measurement.measurementtype = measurementtype.id "
   q += "    INNER JOIN measurementtypetogroup ON measurement.measurementgroup = measurementtypetogroup.measurementgroup AND measurement.measurementtype = measurementtypetogroup.measurementtype"
   q += "    LEFT OUTER JOIN textvalue ON textvalue.measurement = measurement.id "
   q += "    LEFT OUTER JOIN datetimevalue ON datetimevalue.measurement = measurement.id "
   q += "    LEFT OUTER JOIN category  ON measurement.valinteger = category.categoryid AND measurement.measurementtype = category.measurementtype"
   return q   
   
def coreSQLforWhereClauses(study:int,participant:int,measurementType:int,measurementGroup:int,groupInstance:int,trial:int,startTime,endTime):
   conditionCount:int = 0 
   q:str = " "
   if (study != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.study = " + str(study)
   if (participant != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.participant = " + str(participant)
   if (measurementType != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.measurementtype = " + str(measurementType)
   if (measurementGroup != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.measurementgroup = " + str(measurementGroup)
   if (groupInstance != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.groupinstance = " + str(groupInstance)
   if (trial != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.trial = " + str(trial)
   if (startTime != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.time >= " + str(startTime)
   if (endTime != -1):
      conditionCount = conditionCount+1
      if (conditionCount==1):
         q+= " WHERE "
      else:
         q+= " AND "
      q += "   measurement.time <= " + str(endTime)
   return (q,conditionCount)

def getMeasurements(dbConnection,study=-1,participant=-1,measurementType=-1,measurementGroup=-1,groupInstance=-1,trial=-1,startTime=-1,endTime=-1):
   q                  = coreSQLforMeasurements()
   (w,conditionCount) = coreSQLforWhereClauses(study,participant,measurementType,measurementGroup,groupInstance,trial,startTime,endTime)
   q                  += w
   q                  += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
   rawResults         =  returnQueryResult(dbConnection,q)   
   return formMeasurements(rawResults)

def fieldHoldingValue(valType):
   # Use the valType to return the field that holds the value in the measurement
   if (valType == 0):
       field = "measurement.valinteger"
   elif (valType == 1):
       field = "measurement.valreal"
   elif (valType == 2):
       field = "textvalue.textval"
   elif (valType == 3):
       field = "datetimevalue.datetimeval"
   elif (valType == 4):
       field+= "measurement.valinteger" # note we use 0 & 1 for booleans
   elif (valType == 5):
       field = "measurement.valinteger" # note we use the integer encoding for nominals
   elif (valType == 6):
       field = "measurement.valinteger" # note we use the integer encoding for ordinals
   elif (valType == 7):
       field = "measurement.valinteger" # bounded integer
   else:
       print("Error: valType out of range: ",valType)
   return field

def aggregateMeasurements(dbConnection,measurementType,aggregation,study=-1,participant=-1,measurementGroup=-1,groupInstance=-1,trial=-1,startTime=-1,endTime=-1):
   mtInfo=getMeasurementTypeInfo(dbConnection,measurementType)
   valType = mtInfo[0][2]   
   q                  =  "SELECT " + aggregation + "(" + fieldHoldingValue(valType) + ") "
   q                  += coreSQLFromForMesaurements()
   (w,conditionCount) =  coreSQLforWhereClauses(study,participant,measurementType,measurementGroup,groupInstance,trial,startTime,endTime)
   q                  += w
   rawResult          =  returnQueryResult(dbConnection,q) 
   return rawResult

def makeValueTest(valType,valueTestCondition):
   cond = " (" + fieldHoldingValue(valType)  + valueTestCondition + ") "
   return cond

def getMeasurementsWithValueTest(dbConnection,measurementType,valueTestCondition,study=-1,participant=-1,measurementGroup=-1,groupInstance=-1,trial=-1,startTime=-1,endTime=-1):
   # Returns all the measurements of type measurementType that match the valueTestCondition (expressed using SQL syntax) and meet the conditions from the other parameters
   q                  =  coreSQLforMeasurements()
   (w,conditionCount) =  coreSQLforWhereClauses(study,participant,measurementType,measurementGroup,groupInstance,trial,startTime,endTime)
   q                  += w 

   mtInfo=getMeasurementTypeInfo(dbConnection,measurementType)
   valType = mtInfo[0][2]
   # Add a clause to test the measurement that is relevant to the type of the measurement
   if (conditionCount==0):
         q+= " WHERE "
   else:
         q+= " AND "
   cond = makeValueTest(valType,valueTestCondition)
   q += cond
   q += " ORDER BY measurement.time, measurement.groupinstance, measurement.measurementtype;"
   rawResults = returnQueryResult(dbConnection,q)   
   return formMeasurements(rawResults)

def numTypesInAMeasurementGroup(dbConnection,measurementGroup):
   q =  ""
   q += " SELECT "
   q += "    COUNT(*)  "
   q += " FROM "
   q += "    measurementtypetogroup "
   q += " WHERE "
   q += "    measurementtypetogroup.measurementgroup = "
   q += str(measurementGroup)
   q += ";"
   numTypes = returnQueryResult(dbConnection,q)
   return numTypes[0][0]

def getMeasurementGroupInstancesWithValueTests(dbConnection,measurementGroup,valueTestConditions,study=-1,participant=-1,trial=-1,startTime=-1,endTime=-1):
   q                  =  "" # q returns the instance ids of all instances that meet meet the criteria
   q                  += " SELECT restable.groupinstance "
   q                  += " FROM ( "
   q                  += " SELECT groupinstance " + coreSQLFromForMesaurements()
   (w,conditionCount) = coreSQLforWhereClauses(study,participant,-1,measurementGroup,-1,trial,startTime,endTime)
   q += w
   if (conditionCount==0):
      q+= " WHERE ("
   else:
      q+= " AND ("
   totalConditions = len(valueTestConditions)
   c = 1 # the current condition being processed
   for (measurementType,condition) in valueTestConditions:
      mtInfo=getMeasurementTypeInfo(dbConnection,measurementType)
      valType = mtInfo[0][2]
      cond = "((measurement.measurementtype = " + str(measurementType) + ") AND " + makeValueTest(valType,condition) +")"
      q += cond
      if (c<totalConditions):
          q += " OR "
      c = c+1
   q += " OR "
   q += " (measurement.measurementtype NOT IN (" 
   # make list of MeasurementTypesFrom valueTestConditions
   c = 1
   for (measurementType,condition) in valueTestConditions:
       q+= str(measurementType)
       if (c<totalConditions):
          q += ","
       c = c+1
   q += ") "
   q += "))) AS restable"
   q += " GROUP BY restable.groupinstance "
   q += " HAVING   COUNT(*) = "
   groupSize = numTypesInAMeasurementGroup(dbConnection,measurementGroup)
   q += str(groupSize)
   
   outerQuery =  coreSQLforMeasurements()
   outerQuery += " WHERE "
   outerQuery += "    measurement.groupinstance IN (" + q + ")"
   outerQuery += " ORDER BY groupinstance, measurementtype"
   outerQuery += ";"
   rawResults = returnQueryResult(dbConnection,outerQuery)   
   return formMeasurements(rawResults)   
   
def printMeasurements(rows):
       for row in rows:
           print(row[0],",",str(row[1]),",",row[2],",",row[3],",",row[4],",",row[5],",",row[6],",",row[7],",",row[8],",",row[9],",",row[10])

def getMeasurementTypeInfo(dbConnection,measurementTypeId):
   q =  ""
   q += " SELECT "
   q += "    measurementtype.id,measurementtype.description,measurementtype.valtype,units.name "
   q += " FROM "
   q += "    measurementtype LEFT OUTER JOIN units ON measurementtype.units = units.id"
   q += " WHERE "
   q += "    measurementtype.id = "
   q += str(measurementTypeId)
   q += ";"
   mtinfo = returnQueryResult(dbConnection,q)
   return mtinfo

def plotMeasurementType(dbConnection,rows,measurementTypeId,plotFile):
   #https://matplotlib.org/api/pyplot_api.html
   trans = [list(i) for i in zip(*rows)] # transpose the list of lists
   x = trans[1] # the data and time
   y = trans[10] # the measurement value

   mtInfo=getMeasurementTypeInfo(dbConnection,measurementTypeId)
   units = mtInfo[0][3] # get the units name
   pyplot.title(rows[0][5])
   pyplot.xlabel("Time")
   pyplot.ylabel(units)
   pyplot.plot(x,y) 
   pyplot.savefig(plotFile) 
   pyplot.close()

def returnQueryResult(dbConnection,queryText):
  cur = dbConnection.cursor()
  cur.execute(queryText)
  rowsq = cur.fetchall()
  return rowsq

def execQuery(dbConnection,queryText):
  cur = dbConnection.cursor()
  cur.execute(queryText)
  cur.commit()

def getAllMeasurementGroupsAndTypesInAStudy(dbConnection,studyId):
   #Return all measurement groups and measurement types in a study
   q =  ""
   q += " SELECT"
   q += "    measurementtypetogroup.measurementgroup,"
   q += "    measurementtypetogroup.measurementtype, "
   q += "    measurementtypetogroup.name            "
   q += " FROM "
   q += "    measurementtypetogroup "
   q += " WHERE "
   q += "    measurementtypetogroup.study = "
   q +=      str(studyId)
   q += " ORDER BY measurementtypetogroup.measurementgroup, measurementtypetogroup.measurementtype; "
   return returnQueryResult(dbConnection,q)

# Main program starts here

# load credentials
print("Loading credentials..")
CREDS_FILE = 'db-credentials.json'
try:
    with open(CREDS_FILE, 'r') as fIn:
        creds = json.load(fIn)
except Exception as e: 
    sys.exit("Unable to load the credential's file! Exiting.\n" + str(e))

print("Connecting to the database..")
# establish connection
db = "datawarehouse"
conn_string = f"dbname={db} user={creds['user']} host={creds['IP']} password={creds['pass']}"
try:
    conn1 = psycopg2.connect(conn_string)
except Exception as e:
    sys.exit("Unable to connect to the database! Exiting.\n" + str(e))
print("Init successfull! Running queries.\n")

# define cursor to work with
cur1 = conn1.cursor()

#Sample queries...
print("\nQ1\n")
q1res = getMeasurements(conn1,groupInstance=1)
printMeasurements(q1res)

print("\nQ2\n")
q2res = getMeasurements(conn1,participant=1)
printMeasurements(q2res)

# Q3: 6.3	Q3: Find all the measurements of one Type ($mt) in a study
#     If the measurementtype is 1 then 
print("\nQ3\n")
q3res = getMeasurements(conn1,measurementType=1)
printMeasurements(q3res)

# Q4: Find all the measurements of one Measurement Group ($gd) within a Study ($s)
#     If the Measurement Group is 1  and the study is 1 then:

print("\nQ4\n")
q4res = getMeasurements(conn1,measurementGroup=1,study=1)
printMeasurements(q4res)

print("\nQ5: an example of aggregating across a measurement\n")
q5 =  ""
q5 += "SELECT"
q5 += "   avg(measurement.valreal)"
q5 += "   FROM measurement"
q5 += "   WHERE measurement.measurementtype = 13;"
printQueryResult(conn1,q5)

"""
# Try self-join for selections based on criteria in more than one measurement within a group
 
q6 =  ""
q6 += "SELECT"
q6 += "   m1.time, m1.participant, m1.valinteger, m2.valreal"
q6 += "   FROM  measurement AS m1 INNER JOIN measurement AS m2 ON m1.groupinstance = m2.groupinstance  "
q6 += "   WHERE m1.measurementgroup = 15 AND m1.measurementtype = 151 AND m2.measurementtype = 153"
q6 += "   ORDER BY m1.participant;"

print("\nQ6\n")
printQueryResult(conn1,q6)

q7 =  ""
q7 += "SELECT"
q7 += "   m1.time, m1.participant, m1.source,  m1.valinteger, m2.valinteger, m3.valreal, m4.valreal "
q7 += "   FROM  measurement AS m1 INNER JOIN measurement AS m2 ON m1.groupinstance = m2.groupinstance "
q7 += "                           INNER JOIN measurement AS m3 ON m1.groupinstance = m3.groupinstance "
q7 += "                           INNER JOIN measurement AS m4 ON m1.groupinstance = m4.groupinstance "
q7 += "   WHERE m1.measurementgroup = 15 AND m1.measurementtype = 151 AND m2.measurementtype = 152 AND m3.measurementtype = 153 AND m4.measurementtype = 154"
q7 += "   ORDER BY m1.participant;"

print("\nQ7\n")
printQueryResult(conn1,q7)

print("\nQ9: test view\n")
q9 =  ""
q9 += "SELECT * FROM anthropometry; "
printQueryResult(conn1,q9)
"""

# return all results on a participant
print("\nReturn all results on one participant: 1\n")
resultRows = getMeasurements(conn1,participant=1)
printMeasurements(resultRows)

print("\nPrint all measurementgroups and types in a study\n")
printRows(getAllMeasurementGroupsAndTypesInAStudy(conn1,4))

#print("\nPrint all results in a measurement group for a study\n")
#mgis = getMeasurements(conn1,measurementGroup=16, study=3)
#printMeasurements(mgis)

print("\nPlot all results in a measurement group for a study\n")
mts = getMeasurements(conn1,measurementType=155,study=3) # these are continuous glucose monitor measurements
plotMeasurementType(conn1,mts,155,'example155.png')
exportMeasurementAsCSV(mts,'example155.csv')

print("\nprint all measurements with type = 152")
ms2 = getMeasurements(conn1,measurementType=152)
printMeasurements(ms2)

print("\nprint all measurements with measurementgroup = 15")
ms3 = getMeasurements(conn1,measurementGroup=15)
printMeasurements(ms3)

print("\nprint all measurements with type = 155 from study 3 where the value is greater that 9")
ms4 = getMeasurementsWithValueTest(conn1,155,"> 9",study=3)
printMeasurements(ms4)
plotMeasurementType(conn1,ms4,155,'example2.png')
exportMeasurementAsCSV(ms4,'example2.csv')

print("\nprint all measurements in group 15")
ms5 = getMeasurements(conn1,measurementGroup=15)
printMeasurements(ms5)

print ("\nReturn all instances of measurement group 15 where the participant's age is greater than 22 and body mass is less than 55kgs")
ms6 = getMeasurementGroupInstancesWithValueTests(conn1,15,[(151,">22"),(154,"<55.0")])
printMeasurements(ms6)

print("\nFind average of all measurements with type = 155 from study 3")
ms7 = aggregateMeasurements(conn1,155,"avg",study=3)
printRows(ms7)

