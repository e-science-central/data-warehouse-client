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

from mobiliseclient import *
import data_warehouse
import mobilise_load_fns
import clone_and_load_new_study
import study_summary
import delete_study_contents

print(f'Clone and Load New Study\n')

# Configure connection to e-Science Central
#   e-Science Central Networking info
hostname = "mobilised.di-projects.net"
port = 443
ssl = True

#   Log onto the system and obtain a JWT
mc = EscClient(hostname, port, ssl)
#   print('Issuing')
#   jwt = tc.issueToken("username", "password", "Python Test")
#   The actual token is a field of the returned jwt object
#   token = jwt.token

#   Use an existing JWT
token = 'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1NSIsInJvbGUiOiJ1c2VyIiwiZXhwIjoxNjI2MzQ5ODU4LCJpYXQiOjE1OTQ4MTM4NTgsImp0aSI6IjJjOWZhZmU1NzMwZjZkZGMwMTczNTI1MDQwZmYzYjBhIn0.YLOQhgHrcKuGBiCLUEiDHNA74666-dDwgNSD6FHdD03UkW2tUoMwJAsgAaYKa6PBZfBy95vHB4XrPcXDhyfCtA'
mc.jwt = token

#   Use this to validate a token that you already have
print('Validating Token')
print(mc.validateToken(token))

# Create a connection to the Data Warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")


# Example:
old_study = 4
new_study = 27 # NB this must have already been created
esc_project = "HA01"
unique_id_measurement_type = 220

delete_study_contents.delete_study_contents(data_warehouse, new_study)
clone_and_load_new_study.create_and_load_new_study(data_warehouse, old_study, new_study, mc, esc_project,
                              unique_id_measurement_type, mobilise_load_fns.fn_mapper())

study_summary.print_all_instances_in_a_study(data_warehouse,new_study)
