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
data_warehouse.printRows(data_warehouse.getAllMeasurementGroupsAndTypesInAStudy(4),
                         ["Measurement Group","Measurement Type","Type Name"])

print("\nQ7: All results in measurement group 16 for study 3\n")
mgis = data_warehouse.getMeasurements(measurementGroup=16, study=3)
data_warehouse.printMeasurements(mgis)

print("\nQ8: Plot all results of type 155 in study 3\n")
mts = data_warehouse.getMeasurements(measurementType=155, study=3)  # these are continuous glucose monitor measurements
data_warehouse.plotMeasurementType(mts, 155, 'output/example155.png')

print("\nQ9: Create a CSV file for all results of type 155 in study 3\n")
data_warehouse.exportMeasurementAsCSV(mts, 'output/example155.csv')

print("\nQ10: All measurements with type = 152\n")
ms2 = data_warehouse.getMeasurements(measurementType=152)
data_warehouse.printMeasurements(ms2)

print("\nQ11: All measurements with measurementgroup = 15\n")
ms3 = data_warehouse.getMeasurements(measurementGroup=15)
data_warehouse.printMeasurements(ms3)

print("\nQ12: Plot all measurements in study 3 with type = 155 where the value is greater that 9\n")
ms4 = data_warehouse.getMeasurementsWithValueTest(155, "> 9", study=3)
data_warehouse.printMeasurements(ms4)
data_warehouse.plotMeasurementType(ms4, 155, 'output/example2.png')

print("\nQ13: Create a CSV file for all measurements in study 3 with type = 155 where the value is greater that 9\n")
data_warehouse.exportMeasurementAsCSV(ms4, 'output/example2.csv')

print("\nQ14: All measurements in group 15")
ms5 = data_warehouse.getMeasurements(measurementGroup=15)
data_warehouse.printMeasurements(ms5)

print("\nQ15: All instances of measurement group 15 where the participant's age is greater than 22\n")
print("          and body mass is less than 55kgs")
ms6 = data_warehouse.getMeasurementGroupInstancesWithValueTests(15, [(151, ">22"), (154, "<55.0")])
data_warehouse.printMeasurements(ms6)

print("\nQ16: The average of all measurements of type 155 from study 3\n")
ms7 = data_warehouse.aggregateMeasurements(155, "avg", study=3)
print(ms7)

print("\nQ17: The output of Q15 with one Measurement Group instance per row\n")
ms8 = data_warehouse.formMeasurementGroup(ms6)
data_warehouse.printMeasurementGroupInstances(ms8,15)
data_warehouse.exportMeasurementGroupsAsCSV(ms8,15,'output/example17.csv')

print("\nQ18: All measurements for Study 5\n")
ms18 = data_warehouse.getMeasurements(study=5)
data_warehouse.printMeasurements(ms18)

print("\nQ19: All measurements in group 20 for Study 5 in tabular form\n")
ms19 = data_warehouse.getMeasurements(study=5,measurementGroup=20)
mgs19 = data_warehouse.formMeasurementGroup(ms19)
data_warehouse.printMeasurementGroupInstances(mgs19,20)
data_warehouse.exportMeasurementGroupsAsCSV(mgs19,20,'output/example19.csv')

print("\nQ20: All measurements for Study 5, trial 2 in tabular form\n")
ms20 = data_warehouse.getMeasurements(study=5,trial=2, measurementGroup=20)
mgs20 = data_warehouse.formMeasurementGroup(ms20)
data_warehouse.printMeasurementGroupInstances(mgs20,20)

print("\nQ21: All measurements for Study 5, trial 3 in tabular form\n")
ms21 = data_warehouse.getMeasurements(study=5,trial=3,measurementGroup=20)
mgs21 = data_warehouse.formMeasurementGroup(ms21)
data_warehouse.printMeasurementGroupInstances(mgs21,20)

print("\nQ22: All measurements for Study 5, measurement Group 20, from 2018\n")
yearQuery:str = "SELECT measurement.id,measurement.time,measurement.measurementtype,measurement.valreal" +\
                " FROM  measurement " + \
                " WHERE  EXTRACT(YEAR FROM measurement.time) = '2018' AND " +\
                "        measurement.measurementgroup = 20;"

ms22 = data_warehouse.returnQueryResult(yearQuery)
ms22Header = ["Id", "Time", "Measurement Type", "Value"]
data_warehouse.printRows(ms22,ms22Header)

print("\nQ23: Cohort-based analysis: all measurements from all participants with Multiple Sclerosis")
ms23 = data_warehouse.getMeasurementsByCohort(2,study=5)
data_warehouse.printMeasurements(ms23)

