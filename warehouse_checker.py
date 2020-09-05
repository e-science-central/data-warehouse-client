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

# Warehouse Checker code - check:
# valtype field points to non-NULL value field
# The measurementType is in the measurement group for each measurement
# The valtype is correct for the measurement type for each measurement
# the measurement group is valid for the study
# there are no missing measurement types in a measurement group instance
# no ordinal, nominal, bounded integer or bounded real values are out of bounds

#Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

def checkvaltype_matches_values():
    q =  data_warehouse.coreSQLforMeasurements()
    q += " WHERE ((measurement.valtype IN (0,4,5,6,7)) AND (measurement.valinteger    = NULL)) OR "
    q += "       ((measurement.valtype IN (1,8))       AND (measurement.valreal       = NULL)) OR "
    q += "       ((measurement.valtype =  2)           AND (textvalue.textval         = NULL)) OR "
    q += "       ((measurement.valtype =  3)           AND (datetimevalue.datetimeval = NULL));   "
    return data_warehouse.returnQueryResult(q)

print("\nQ26 Check for invalid entries in the measurement table")
ms26 = checkvaltype_matches_values()
data_warehouse.printMeasurements(ms26)
