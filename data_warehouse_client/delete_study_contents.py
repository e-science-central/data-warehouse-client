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

# only run this if you're certain that you want to remove all metadata and measurements from a study!


def delete_study_contents(dw, study):
    """
    Deletes all measurements and metadata from a study. Only leave entry in the study table
    Designed to clear out a study before a test that will re-populate it
    :param dw: data warehouse handle
    :param study: study id
    """
    for tab in table_names_to_delete():
        dw.exec_sql_with_no_return("DELETE FROM " + tab + " WHERE study = " + str(study))


def delete_study_measurements(dw, study):
    """
    Deletes all measurements from a study. Only leave entries in the study and trial tables
    Designed to clear out a study before a test that will re-populate it
    :param dw: data warehouse handle
    :param study: study id
    """
    for tab in measurement_table_names():
        dw.exec_sql_with_no_return("DELETE FROM " + tab + " WHERE study = " + str(study))


def table_names_to_delete():
    """
    :return: the names of all the tables in the data warehouse in a study to be deleted (all but study)
    """
    return measurement_table_names() + metadata_table_names()


def measurement_table_names():
    """
    :return: the names of all the tables in the data warehouse that hold measurements
    """
    return ['textvalue', 'datetimevalue', 'measurement']


def metadata_table_names():
    """
    :return: the names of all the tables in the data warehouse that hold metadata
    """
    return ['measurementtypetogroup', 'boundsreal', 'boundsint', 'category', 'measurementtype', 'units',
            'measurementgroup', 'source', 'sourcetype', 'participant', 'trial']


def delete_study_completely(dw, study):
    """
    Deletes everything in a study: use with care
    :param dw: data warehouse handle
    :param study: study id
    """
    delete_study_contents(dw, study)
    dw.exec_sql_with_no_return("DELETE FROM study WHERE study.id = " + str(study))
