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

data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

# Sample queries...
print("\nQ1\n")
q1res = data_warehouse.getMeasurements(groupInstance=1)
data_warehouse.printMeasurements(q1res)

print("\nQ2\n")
q2res = data_warehouse.getMeasurements(participant=1)
data_warehouse.printMeasurements(q2res)

# Q3: 6.3	Q3: Find all the measurements of one Type ($mt) in a study
#     If the measurementtype is 1 then 
print("\nQ3\n")
q3res = data_warehouse.getMeasurements(measurementType=1)
data_warehouse.printMeasurements(q3res)

# Q4: Find all the measurements of one Measurement Group ($gd) within a Study ($s)
#     If the Measurement Group is 1  and the study is 1 then:

print("\nQ4\n")
q4res = data_warehouse.getMeasurements(measurementGroup=1, study=1)
data_warehouse.printMeasurements(q4res)

# return all results on a participant
print("\nReturn all results on one participant: 1\n")
resultRows = data_warehouse.getMeasurements(participant=1)
data_warehouse.printMeasurements(resultRows)

print("\nPrint all measurementgroups and types in a study\n")
data_warehouse.printRows(data_warehouse.getAllMeasurementGroupsAndTypesInAStudy(4))

print("\nPrint all results in a measurement group for a study\n")
mgis = data_warehouse.getMeasurements(measurementGroup=16, study=3)
data_warehouse.printMeasurements(mgis)

print("\nPlot all results in a measurement group for a study\n")
mts = data_warehouse.getMeasurements(measurementType=155, study=3)  # these are continuous glucose monitor measurements
data_warehouse.plotMeasurementType(mts, 155, 'output/example155.png')
data_warehouse.exportMeasurementAsCSV(mts, 'output/example155.csv')

print("\nprint all measurements with type = 152")
ms2 = data_warehouse.getMeasurements(measurementType=152)
data_warehouse.printMeasurements(ms2)

print("\nprint all measurements with measurementgroup = 15")
ms3 = data_warehouse.getMeasurements(measurementGroup=15)
data_warehouse.printMeasurements(ms3)

print("\nprint all measurements with type = 155 from study 3 where the value is greater that 9")
ms4 = data_warehouse.getMeasurementsWithValueTest(155, "> 9", study=3)
data_warehouse.printMeasurements(ms4)
data_warehouse.plotMeasurementType(ms4, 155, 'output/example2.png')
data_warehouse.exportMeasurementAsCSV(ms4, 'output/example2.csv')

print("\nprint all measurements in group 15")
ms5 = data_warehouse.getMeasurements(measurementGroup=15)
data_warehouse.printMeasurements(ms5)

print(
    "\nReturn all instances of measurement group 15 where the participant's age is greater than 22 and body mass is less than 55kgs")
ms6 = data_warehouse.getMeasurementGroupInstancesWithValueTests(15, [(151, ">22"), (154, "<55.0")])
data_warehouse.printMeasurements(ms6)

print("\nFind average of all measurements with type = 155 from study 3")
ms7 = data_warehouse.aggregateMeasurements(155, "avg", study=3)
data_warehouse.printRows(ms7)
