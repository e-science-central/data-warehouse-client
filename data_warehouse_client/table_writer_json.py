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

import json
import os


def table_to_dictionary(dw, table_name, study_id):
    """
    Read a table and turn it into a dictionary
    Based on code in: https://stackoverflow.com/questions/3286525/return-sql-table-as-json-in-python
    :param dw: data warehouse handle
    :param table_name: Data Warehouse table name
    :param study_id: the study id
    :result a list of dictionaries - one for each row of the table
    """
    cur = dw.dbConnection.cursor()
    q = "select * from " + table_name + " where study = " + str(study_id) + ";"
    cur.execute(q)
    r = [dict((cur.description[i][0], value)
              for i, value in enumerate(row)) for row in cur.fetchall()]
    return r


def tables_to_dictionary(dw, study_id, tables):
    """
    Transform a set of tables into a dictionary
    :param dw: data warehouse handle
    :param study_id: the study id
    :param tables: a list of tables to convert to a dictionary
    :result a dictionary of tables, each a list of dictionary - one for each row of the table
    """
    table_dict = {}
    for table_name in tables:
        td = table_to_dictionary(dw, table_name, study_id)
        table_dict[table_name] = td
    return table_dict


def data_warehouse_metadata_tables_to_dictionary(dw, study_id):
    """
    Writes the data warehouse metadata tables into a dictionary
    :param dw: data warehouse handle
    :param study_id: the study id
    :return a dictionary of tables, each a list of dictionaries - one for each row of the table
    """
    return tables_to_dictionary(dw, study_id, metadata_table_names())


def metadata_table_names():
    """
    :return: the names of all the metadata tables in the data warehouse
    """
    return ["trial", "units", "measurementtype", "measurementgroup", "sourcetype", "boundsreal",
            "boundsint", "category", "measurementtypetogroup"]


def dictionary_to_json_file(d, file_name):
    """
    write dictionary to file in json format
    :param d: dictionary
    :param file_name: output filename
    """
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    with open(file_name, 'w') as json_file:
        json.dump(d, json_file)


def data_warehouse_metadata_tables_to_file(dw, study_id, file_name):
    """
    Write a set of data warehouse tables into a file
    :param dw: data warehouse handle
    :param study_id: the study id
    :param file_name: output filename
    """
    d = data_warehouse_metadata_tables_to_dictionary(dw, study_id)
    dictionary_to_json_file(d, file_name)


def write_tables_in_dw_from_dictionary(dw, input_dict):
    """
    writes a set of tables (in a dictionary) into the data warehouse
    :param dw: data warehouse handle
    :param input_dict: the dictionary containing the tables
    :return the total number of rows inserted
    """
    total_rows_inserted = 0
    for table in input_dict.keys():
        total_rows_inserted += insert_rows_in_dw_from_dictionary(dw, table, input_dict[table])
    return total_rows_inserted


def insert_rows_in_dw_from_dictionary(dw, table, rows):
    """
    writes a table, in the form of a list of dictionaries (one per row), into the data warehouse
    :param dw: data warehouse handle
    :param table: table name
    :param rows: a list of dictionaries - one per row
    :return number of rows inserted
    """
    # https://blog.softhints.com/python-3-convert-dictionary-to-sql-insert/
    cur = dw.dbConnection.cursor()
    n_rows_inserted = 0
    for row in rows:
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.keys())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % (table, columns, placeholders)
        cur.execute(sql, list(row.values()))
        dw.dbConnection.commit()
        n_rows_inserted += 1
    return n_rows_inserted
