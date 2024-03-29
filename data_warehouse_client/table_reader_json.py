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

from json import load


def read_json_tables(file_name):
    """
    Reads a table stored in a json file
    :param file_name: filename in which to store the output (in directory output)
    :return a list of dictionaries - one for each row of the table
    """
    try:
        with open(file_name, 'r') as jIn:
            j = load(jIn)
            return j
    except Exception as e:
        print("Unable to load the json file! Exiting: \n" + str(e))
    return j
