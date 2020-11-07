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

from data_warehouse_client import clone_study_metadata
from data_warehouse_client import data_warehouse

# Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

# example 1
source_study_id = 4
dest_study_id = 11

print("Copy Metadata Table rows from one study to another\n")
n_rows_inserted = clone_study_metadata.clone_study_metadata(data_warehouse, source_study_id, dest_study_id)
print(n_rows_inserted, "New Entries in Study Metadata tables")
