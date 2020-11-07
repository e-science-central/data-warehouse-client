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


from data_warehouse_client import table_writer_json


def clone_study_metadata(dw, source_study_id, dest_study_id):
    """
    Copies all the metadata from one study to another
    :param dw: data warehouse end point
    :param source_study_id: original study
    :param dest_study_id: destination study
    :return total rows inserted
    """
    # extract existing metadata
    tbls = table_writer_json.data_warehouse_metadata_tables_to_dictionary(dw, source_study_id)
    for table in tbls:  # for each table
        for row in tbls[table]:  # for each row
            row.update({'study': dest_study_id})  # update all occurrences of study to the destination study
    return table_writer_json.write_tables_in_dw_from_dictionary(dw, tbls)  # write the result to the warehouse
