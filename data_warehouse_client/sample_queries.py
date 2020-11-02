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

from data_warehouse_client import data_warehouse
from data_warehouse_client import study_summary
#from data_warehouse_client import mobilise_cohort_selection
#import warehouse_checker

#Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials-read-only.json", "datawarehouse")

# Sample queries...
print("\nQ1: All measurements in group instance 1\n")
q1res = data_warehouse.get_measurements(group_instance=1)
data_warehouse.print_measurements(q1res)

print("\nQ2: All measurements for participant 1\n")
q2res = data_warehouse.get_measurements(participant=1)
data_warehouse.print_measurements(q2res)

print("\nQ3: All the measurements of type 1\n")
q3res = data_warehouse.get_measurements(measurement_type=1)
data_warehouse.print_measurements(q3res)

print("\nQ4: All measurements in Measurement Group 1, within Study 1\n")
q4res = data_warehouse.get_measurements(measurement_group=1, study=1)
data_warehouse.print_measurements(q4res)

print("\nQ5: All results for participant 1\n")
resultRows = data_warehouse.get_measurements(participant=1)
data_warehouse.print_measurements(resultRows)

print("\nQ6: All measurementgroups and types in study 1\n")
data_warehouse.print_rows(data_warehouse.get_all_measurement_groups_and_types_in_a_study(4),
                          ["Measurement Group","Measurement Type","Type Name"])

print("\nQ7: All results in measurement group 16 for study 3\n")
mgis = data_warehouse.get_measurements(measurement_group=16, study=3)
data_warehouse.print_measurements(mgis)

print("\nQ8: Plot all results of type 155 in study 3\n")
mts = data_warehouse.get_measurements(measurement_type=155, study=3)  # these are continuous glucose monitor measurements
data_warehouse.plot_measurement_type(mts, 155, 3, 'output/example155.png')

print("\nQ9: Create a CSV file for all results of type 155 in study 3\n")
data_warehouse.export_measurements_as_csv(mts, 'output/example155.csv')

print("\nQ10: All measurements with type = 152\n")
ms2 = data_warehouse.get_measurements(measurement_type=152)
data_warehouse.print_measurements(ms2)

print("\nQ11: All measurements with measurementgroup = 15\n")
ms3 = data_warehouse.get_measurements(measurement_group=15)
data_warehouse.print_measurements(ms3)

print("\nQ12: Plot all measurements in study 3 with type = 155 where the value is greater that 9\n")
ms4 = data_warehouse.get_measurements_with_value_test(155, 3, "> 9")
data_warehouse.print_measurements(ms4)
data_warehouse.plot_measurement_type(ms4, 155, 3, 'output/example2.png')

print("\nQ13: Create a CSV file for all measurements in study 3 with type = 155 where the value is greater that 9\n")
data_warehouse.export_measurements_as_csv(ms4, 'output/example2.csv')

print("\nQ14: All measurements in group 15")
ms5 = data_warehouse.get_measurements(measurement_group=15)
data_warehouse.print_measurements(ms5)

print("\nQ15: All instances of measurement group 15 where the participant's age is greater than 22")
print("          and body mass is less than 55kgs in study 2")
ms6 = data_warehouse.get_measurement_group_instances_with_value_tests(15, 2, [(151, ">22"), (154, "<55.0")])
#data_warehouse.printMeasurements(ms6)
mgs6 = data_warehouse.form_measurement_group(2, ms6)
data_warehouse.print_measurement_group_instances(mgs6, 15, 2)
print("\nParticipants: ")
parts = data_warehouse.get_participants_in_result(ms6)
print(*parts,sep = ',')
print()

ms6a = data_warehouse.get_measurement_group_instances_for_cohort(15,2,parts,[])
data_warehouse.print_measurements(ms6a)

ms6b = data_warehouse.get_measurement_group_instances_for_cohort(15,2,[4],[])
data_warehouse.print_measurements(ms6b)

print("\nQ16: The average of all measurements of type 155 from study 3\n")
ms7 = data_warehouse.aggregate_measurements(155, 3, "avg")
print(ms7)

print("\nQ17: The output of Q15 with one Measurement Group instance per row\n")
ms8 = data_warehouse.form_measurement_group(2, ms6)
data_warehouse.print_measurement_group_instances(ms8, 15, 2)
data_warehouse.export_measurement_groups_as_csv(ms8, 15, 2, 'output/example17.csv')

print("\nQ18: All measurements for Study 5\n")
ms18 = data_warehouse.get_measurements(study=5)
data_warehouse.print_measurements(ms18)

print("\nQ19: All measurements in group 20 for Study 5 measurement group 20 in tabular form\n")
ms19 = data_warehouse.get_measurements(study=5, measurement_group=20)
mgs19 = data_warehouse.form_measurement_group(5, ms19)
data_warehouse.print_measurement_group_instances(mgs19, 20, 5)

print("\n   : store in CSV file\n")
data_warehouse.export_measurement_groups_as_csv(mgs19, 20, 5, 'output/example19.csv')

print("\nQ20: All measurements for Study 5, trial 2, measurement group 20 in tabular form\n")
ms20 = data_warehouse.get_measurements(study=5, trial=2, measurement_group=20)
mgs20 = data_warehouse.form_measurement_group(5, ms20)
data_warehouse.print_measurement_group_instances(mgs20, 20, 5)

print("\nQ21: All measurements for Study 5, trial 4 in tabular form\n")
ms21 = data_warehouse.get_measurements(study=5, trial=4, measurement_group=20)
mgs21 = data_warehouse.form_measurement_group(5, ms21)
data_warehouse.print_measurement_group_instances(mgs21, 20, 5)

print("\nQ22: All measurements for Study 5, measurement Group 20, from 2018\n")
yearQuery:str = "SELECT measurement.id,measurement.time,measurement.measurementtype,measurement.valreal" +\
                " FROM  measurement " + \
                " WHERE  EXTRACT(YEAR FROM measurement.time) = '2018' AND " +\
                "        measurement.measurementgroup = 20;"

ms22 = data_warehouse.return_query_result(yearQuery)
ms22Header = ["Id", "Time", "Measurement Type", "Value"]
data_warehouse.print_rows(ms22, ms22Header)

print("\nQ23: Cohort-based analysis: all measurements from all participants with Multiple Sclerosis in study 5")
ms23 = data_warehouse.get_measurements_by_cohort(2, 5)
data_warehouse.print_measurements(ms23)

# Comment out inserts
# print("\nQ24: Test Inserts: insert an integer value")
# newrowid = data_warehouse.insertOneMeasurement(6,23,182,0,333,participant=36)
# print(newrowid)
#
# print("\nQ25: Test Inserts: insert a measurement group ")
# newrowid = data_warehouse.insertMeasurementGroup(6,22,[(182,0,58),(183,1,99.94),(184,2,"The quick brown fox"),
#                                                        (185,3,'2020-03-08 14:05:06'),
#                                                        (186,4,1),(187,5,1),(188,6,2),
#                                                        (189,7,4),(190,8,3.142)],participant=36)
# ms25 = data_warehouse.getMeasurements(groupInstance=newrowid)
# mgs25 = data_warehouse.formMeasurementGroup(5,ms25)
# data_warehouse.printMeasurementGroupInstances(mgs25,22)

print("\nQ26: All measurements in group 6 for Study 10")
ms26 = data_warehouse.get_measurements(measurement_group=6, study=10)
data_warehouse.print_measurements(ms26)
print("\n")
mgs26 = data_warehouse.form_measurement_group(10, ms26)
data_warehouse.print_measurement_group_instances(mgs26, 6, 10)

print("\nQ27: All measurements in Study 14")
study_summary.print_all_instances_in_a_study(data_warehouse,14)

print("\nQ28: All measurement group 14 measurements for all participants in UNEW and USFD with HA or CHF in study 26")
# Retrieve the cohort
#cohort = mobilise_cohort_selection.get_mobilise_cohort(data_warehouse, 26, ["UNEW","USFD"], ["HA","CHF"])
# Use the cohort in a query
# print(*cohort, sep=',')
#ms28 = data_warehouse.get_measurement_group_instances_for_cohort(14, 26, cohort, [])
#mgs28 = data_warehouse.formMeasurementGroup(14, ms28)
#data_warehouse.printMeasurementGroupInstances(mgs28, 14, 26)

print("\nQ29: All instances of measurement group 15 where the participant's age is greater than 22")
print("          and body mass is less than 55kgs in study 24")
ms6 = data_warehouse.get_measurement_group_instances_with_value_tests(15, 2, [(151, ">22"), (154, "<55.0")])
#data_warehouse.printMeasurements(ms6)
mgs6 = data_warehouse.form_measurement_group(2, ms6)
data_warehouse.print_measurement_group_instances(mgs6, 15, 2)
