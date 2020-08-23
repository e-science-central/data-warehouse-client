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

#Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

# Sample queries...
print("\nQ1: All measurements in group instance 1\n")
q1res = data_warehouse.getMeasurements(groupInstance=1)
data_warehouse.printMeasurements(q1res)

print("\nQ2: All measurements for participant 1\n")
q2res = data_warehouse.getMeasurements(participant=1)
data_warehouse.printMeasurements(q2res)

print("\nQ3: All the measurements of type 1\n")
q3res = data_warehouse.getMeasurements(measurementType=1)
data_warehouse.printMeasurements(q3res)

print("\nQ4: All measurements in Measurement Group 1, within Study 1(\n")
q4res = data_warehouse.getMeasurements(measurementGroup=1, study=1)
data_warehouse.printMeasurements(q4res)

print("\nQ5: All results on participant 1\n")
resultRows = data_warehouse.getMeasurements(participant=1)
data_warehouse.printMeasurements(resultRows)

print("\nQ6: All measurementgroups and types in study 1\n")
data_warehouse.printRows(data_warehouse.getAllMeasurementGroupsAndTypesInAStudy(4))

print("\nQ7: All results in measurement group 16 for study 3\n")
mgis = data_warehouse.getMeasurements(measurementGroup=16, study=3)
data_warehouse.printMeasurements(mgis)

print("\nQ8: Plot all results of type 155 in study 3\n")
mts = data_warehouse.getMeasurements(measurementType=155, study=3)  # these are continuous glucose monitor measurements
data_warehouse.plotMeasurementType(mts, 155, 'output/example155.png')

print("\nQ9: Create a CSV file for all results of type 155 in study 3\n")
data_warehouse.exportMeasurementAsCSV(mts, 'output/example155.csv')

print("\nQ10: All measurements with type = 152")
ms2 = data_warehouse.getMeasurements(measurementType=152)
data_warehouse.printMeasurements(ms2)

print("\nQ11: All measurements with measurementgroup = 15")
ms3 = data_warehouse.getMeasurements(measurementGroup=15)
data_warehouse.printMeasurements(ms3)

print("\nQ12: Plot all measurements in study 3 with type = 155 where the value is greater that 9")
ms4 = data_warehouse.getMeasurementsWithValueTest(155, "> 9", study=3)
data_warehouse.printMeasurements(ms4)
data_warehouse.plotMeasurementType(ms4, 155, 'output/example2.png')

print("\nQ13: Create a CSV file for all measurements in study 3 with type = 155 where the value is greater that 9")
data_warehouse.exportMeasurementAsCSV(ms4, 'output/example2.csv')

print("\nQ14: All measurements in group 15")
ms5 = data_warehouse.getMeasurements(measurementGroup=15)
data_warehouse.printMeasurements(ms5)

print("\nQ15: All instances of measurement group 15 where the participant's age is greater than 22")
print("          and body mass is less than 55kgs")
ms6 = data_warehouse.getMeasurementGroupInstancesWithValueTests(15, [(151, ">22"), (154, "<55.0")])
data_warehouse.printMeasurements(ms6)

print("\nQ16: The average of all measurements of type 155 from study 3")
ms7 = data_warehouse.aggregateMeasurements(155, "avg", study=3)
data_warehouse.printRows(ms7)

print("\nQ17: The output of Q15 with one Measurement Group instance per row")
ms8 = data_warehouse.formMeasurementGroup(ms6)
data_warehouse.printMeasurementGroupInstances(ms8)
data_warehouse.exportMeasurementGroupsAsCSV(ms8,15,'output/example17.csv')

