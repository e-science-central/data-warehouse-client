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

# only run this if you're certain that you want to remove all metdata and measurements from a study!
from data_warehouse_client import delete_study_contents
from data_warehouse_client import table_writer_json
from data_warehouse_client import data_warehouse

# Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

study = 86
file_name = "output/metadatatables" + str(study) + ".json"
# store everything important just in case!
# table_writer_json.data_warehouse_metadata_tables_to_file(data_warehouse, study, file_name)


# delete_study_contents.delete_study_contents(data_warehouse, study)

# for study in range(8,26):
# delete_study_contents.delete_study_completely(data_warehouse, ???)
