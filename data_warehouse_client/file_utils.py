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

from string import Template


def process_sql_template(filename, mappings=None):
    """
    Reads a templated SQL file and substitutes any variables
    :param filename: the SQL file
    :param mappings: the variables to be substituted
    :return: the text of the SQL query with any variables substituted
    """
    # https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package Option 1
    with open(f"sql/{filename}") as f:
        file_contents = f.read()
    data = ' '.join(file_contents.replace('\r\n', ' ').split())
    res = Template(data).substitute(mappings)
    return res
