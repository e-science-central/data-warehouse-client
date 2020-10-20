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

# test of load 85+

import data_warehouse
import clone_study_metadata
import load_85_plus_study_file
import study_summary

# Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

# example 1
# source_study_id = 85
dest_study_id   = 85
trial = 1
input_file_name = "input/NE____NE85+______0005___7D_________RW_00014__2009 06 16 10.08.54_12__.csv"

# print("Copy Metadata Table rows from one study to another\n")
# n_rows_inserted = clone_study_metadata.clone_study_metadata(data_warehouse, source_study_id, dest_study_id)
# print(n_rows_inserted, "New Entries in Study Metadata tables")

load_85_plus_study_file.load_85_plus_file(data_warehouse, input_file_name, dest_study_id, trial, 5000)

study_summary.print_study_summary(data_warehouse,dest_study_id)
study_summary.print_all_instances_in_a_study(data_warehouse,dest_study_id)
