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
import importlib.resources
import pkg_resources


def process_sql_template1(filename, mappings=None):
    """
    Reads a templated SQL file and substitutes any variables
    :param filename: the SQL file
    :param mappings: the variables to be substituted
    :return: the text of the SQL query with any variables substituted
    """
    with open(filename, 'r') as file:
        data = ' '.join(file.read().replace('\n', ' ').split())
    return Template(data).substitute(mappings)


def process_sql_template2(filename, mappings=None):
    """
    Reads a templated SQL file and substitutes any variables
    :param filename: the SQL file
    :param mappings: the variables to be substituted
    :return: the text of the SQL query with any variables substituted
    """
    # https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package
    # https://importlib-resources.readthedocs.io/en/latest/using.html
    from . import sql     # relative-import the *package* containing the templates
    sql_template = importlib.resources.read_text(sql, filename)
    data = ' '.join(sql_template.replace('\n', ' ').split())
    return Template(data).substitute(mappings)


def process_sql_template(filename, mappings=None):
    """
    Reads a templated SQL file and substitutes any variables
    :param filename: the SQL file
    :param mappings: the variables to be substituted
    :return: the text of the SQL query with any variables substituted
    """
    # https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package Option 1
    resource_package = 'data_warehouse_client'
    resource_path = '/'.join(('sql', filename))
    sql_template = pkg_resources.resource_string(resource_package, resource_path)
    sql_template_bytes = sql_template.decode('utf8')
    data = ' '.join(sql_template_bytes.replace('\r\n', ' ').split())
    res = Template(data).substitute(mappings)
    return res
