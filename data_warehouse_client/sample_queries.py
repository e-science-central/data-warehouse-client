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
from data_warehouse_client import print_io
from data_warehouse_client import study_summary
from data_warehouse_client import plot
from data_warehouse_client import csv_io
import datetime


# Create a connection to the data warehouse
dw = data_warehouse.DataWarehouse("db-credentials-read-only.json", "datawarehouse")

# Sample queries...
print("\nQ1: All measurements in measurement group 6 in study 4\n")
q1res = dw.get_measurements(4, measurement_group=6)
print_io.print_measurements(q1res)

print("\nQ2: All measurements for participant 1 in study 4\n")
q2res = dw.get_measurements(4, participant=1)
print_io.print_measurements(q2res)

print("\nQ3: All the measurements of type 1 in study 4\n")
q3res = dw.get_measurements(4, measurement_type=220)
print_io.print_measurements(q3res)

print("\nQ4: All measurements in Measurement Group 1, in Study 1\n")
(header, instances) = dw.get_measurement_group_instances(1, 1, [])
print_io.print_measurement_group_instances(header, instances)

print("\nQ5: All results for participant 1 in study 4\n")
resultRows = dw.get_measurements(4, participant=1)
print_io.print_measurements(resultRows)

print("\nQ6: All measurementgroups and types in study 1\n")
print_io.print_rows(dw.get_all_measurement_groups_and_types_in_a_study(4),
                    ["Measurement Group", "Measurement Type", "Type Name"])

print("\nQ7: All results in measurement group 16 for study 3\n")
mgis = dw.get_measurements(3, measurement_group=16)
print_io.print_measurements(mgis)

print("\nQ8: Plot all results of type 155 in study 3\n")
mts = dw.get_measurements(3, measurement_type=155)  # these are continuous glucose monitor measurements
plot.plot_measurements(dw, mts, 3, 155, 'output/example155.png')

print("\nQ9: Create a CSV file for all results of type 155 in study 3\n")
csv_io.export_measurements_as_csv(mts, 'output/example155.csv')

print("\nQ10: All measurements with type = 152 in study 4\n")
ms2 = dw.get_measurements(4, measurement_type=152)
print_io.print_measurements(ms2)
plot.plot_measurements(dw, ms2, 4, 152, 'output/example2.png')

print("\nQ11: All measurements with measurementgroup = 15 in study 4\n")
(header, instances) = dw.get_measurement_group_instances(4, 15, [])
print_io.print_measurement_group_instances(header, instances)

print("\nQ12: Plot all measurements in study 3 with type = 155 where the value is greater that 9\n")
res = dw.get_measurements_with_value_test(3, 155, "> 9")
print_io.print_measurements(res)

print("\nQ13: Create a CSV file for all measurements in study 3 with type = 155 where the value is greater that 9\n")
csv_io.export_measurements_as_csv(res, 'output/example2.csv')

print("\nQ14: All measurements in group 15 in study 4")
(header, instances) = dw.get_measurement_group_instances(4, 15, [])
print_io.print_measurement_group_instances(header, instances)

print("\nQ15: All instances of measurement group 15 where the participant's age is greater than 22")
print("          and body mass is less than 55kgs in study 2")
(header, instances) = dw.get_measurement_group_instances(2, 15, [(151, ">22"), (154, "<55.0")])
print_io.print_measurement_group_instances(header, instances)

print("\nParticipants: ")
parts = data_warehouse.get_participants_in_result(instances)
print(*parts, sep=',')
print()

(header, instances) = dw.get_measurement_group_instances_for_cohort(2, 15, parts, [])
print_io.print_measurement_group_instances(header, instances)

(header, instances) = dw.get_measurement_group_instances_for_cohort(15, 2, [4], [])
print_io.print_measurement_group_instances(header, instances)

print("\nQ16: The output of Q15 stored as a CSV\n")
csv_io.export_measurement_groups_as_csv(header, instances, 'output/example17.csv')

print("\nQ17: The average of all measurements of type 155 from study 3\n")
ms7 = dw.aggregate_measurements(3, 155, "avg")
print(ms7)

print("\nQ18: All measurements for Study 5\n")
ms18 = dw.get_measurements(study=5)
print_io.print_measurements(ms18)

print("\nQ19: All measurements in group 20 for Study 5 measurement group 20 in tabular form\n")
(header, instances) = dw.get_measurement_group_instances(5, 20, [])
print_io.print_measurement_group_instances(header, instances)

print("\n   : store in CSV file\n")
csv_io.export_measurement_groups_as_csv(header, instances, 'output/example19.csv')

print("\nQ20: All measurements for Study 5, trial 2, measurement group 20 in tabular form\n")
(header, instances) = dw.get_measurement_group_instances(5, 20, [], trial=2)
print_io.print_measurement_group_instances(header, instances)

print("\nQ21: All measurements for Study 5, measurement group 20, trial 4 in tabular form\n")
(header, instances) = dw.get_measurement_group_instances(5, 20, [], trial=4)
print_io.print_measurement_group_instances(header, instances)

print("\nQ22: All measurements for Study 5, measurement Group 20, from 2018\n")
year_query = "SELECT measurement.id,measurement.time,measurement.measurementtype,measurement.valreal" +\
             " FROM  measurement " + \
             " WHERE  EXTRACT(YEAR FROM measurement.time) = '2018' AND " +\
             "        measurement.measurementgroup = 20;"

ms22 = dw.return_query_result(year_query)
ms22Header = ["Id", "Time", "Measurement Type", "Value"]
print_io.print_rows(ms22, ms22Header)

print("\nQ23: Cohort-based analysis: all measurements from all participants with Multiple Sclerosis in study 5")
ms23 = dw.get_measurements_by_cohort(5, 2)
print_io.print_measurements(ms23)

print("\nQ26: All measurements in group 6 for Study 27")
(header, instances) = dw.get_measurement_group_instances(27, 6, [])
print_io.print_measurement_group_instances(header, instances)
print("\n")

print("\nQ27: All measurements in Study 5")
study_summary.print_all_instances_in_a_study(dw, 5)

print("\nQ28: All measurement group 14 measurements for all participants in UNEW and USFD with HA or CHF in study 26")
# Retrieve the cohort
# cohort = mobilise_cohort_selection.get_mobilise_cohort(data_warehouse, 26, ["UNEW","USFD"], ["HA","CHF"])
# Use the cohort in a query
# print(*cohort, sep=',')
# ms28 = data_warehouse.get_measurement_group_instances_for_cohort(14, 26, cohort, [])
# mgs28 = data_warehouse.formMeasurementGroup(14, ms28)
# data_warehouse.printMeasurementGroupInstances(mgs28, 14, 26)

print("\nQ29: All instances of measurement group 15 where the participant's age is greater than 22")
print("          and body mass is less than 55kgs in study 2")
(header, instances) = dw.get_measurement_group_instances(2, 15, [(151, ">22"), (154, "<55.0")])
# data_warehouse.printMeasurements(ms6)
print_io.print_measurement_group_instances(header, instances)

# print()
# print("Run Warehouse Checker\n")
# warehouse_checker.print_check_warhouse(dw, 4)

print("\nQ30: All instances of the x_accelerometer in measurement group 85 between 10:00 and 11:00 on 16-6-2020")
ms = dw.get_measurements(85, measurement_group=0, measurement_type=2,
                         start_time=datetime.datetime(2009, 6, 16, hour=10),
                         end_time=datetime.datetime(2009, 6, 16, hour=11))
plot.plot_measurements(dw, ms, 85, 2, 'output/example85.png')
# study_to_save = 4
# file_name = "c:/Temp/study4metadata.json"
# print(f'Save Metadata from Study {study_to_save}')
# table_writer_json.data_warehouse_metadata_tables_to_file(dw, study_to_save, file_name)
