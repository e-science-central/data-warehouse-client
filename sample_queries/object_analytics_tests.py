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


# def aggregate_instance(study, mg, mts, aggregation_fn):
#    """
#    applies a function to the the values of the message types in each message group instance
#    :param mts: list of message types
#    :param aggregation_fn: a function operating on the list of message types
#    :return: a list of the results of applying the function to the values in each message group instance
#    """

from tabulate import tabulate

import data_warehouse
from sample_queries import object_analytics

data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

study_id = 100

q1 = object_analytics.head_count_sql(study_id)
r1 = data_warehouse.returnQueryResult(q1)
print(tabulate(r1, headers=['Frame', 'Head Count']))
print()

closeness_limit = 50
q2 = object_analytics.social_distancing_violations_sql(study_id, object_analytics.pythagorus(), closeness_limit)
r2 = data_warehouse.returnQueryResult(q2)
print(tabulate(r2, headers=['Frame', 'Time', 'Obj 1 Id', 'Obj 2 Id', 'Distance']))
