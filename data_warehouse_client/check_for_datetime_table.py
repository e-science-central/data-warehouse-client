# Copyright 2023 Newcastle University.
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

# determine if there's a a table called 'boundsdatetime' (this was added in later versions of the warehouse design)

from file_utils import process_sql_template


def datetimebounds_table_in_dw(dw_handle):
    """
    :param dw_handle: data warehouse handle
    :type dw_handle: database handle (only needs to be read-only)
    :return: True if the database has a table called 'boundsdatetime'
    :rtype: Boolean
    """
    mappings = {}  # parameter mappings for query
    query = process_sql_template("get_all_table_names.sql", mappings)  # query to retrieve the names of all tables
    tabs = dw_handle.return_query_result(query)  # execute the query
    return ('boundsdatetime',) in tabs
