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


import accelerometry_analytics
import data_warehouse
from tabulate import tabulate

# Create a connection to the Data Warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

res_second = accelerometry_analytics.enmo_aggregations(data_warehouse, 85, 0, 2, 3, 4, 'second')
print(tabulate(res_second, headers=["Time","Average ENMO"]))
print()

res_minute = accelerometry_analytics.enmo_aggregations(data_warehouse, 85, 0, 2, 3, 4, 'minute')
print(tabulate(res_minute, headers=["Time","Average ENMO"]))

