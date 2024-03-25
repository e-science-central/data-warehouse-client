# Copyright 2024 Newcastle University.
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

from datetime import datetime
from data_warehouse import core_sql_from_for_measurements, core_sql_for_where_clauses, core_sql_for_measurements
from transform_result_format import form_measurement_group
from study_summary import mk_participants_dictionary, mk_csv_report_file_name, export_measurement_groups_as_csv


def get_measurement_group_instances_for_analysis(dw, study, measurement_group, value_test_conditions,
                                                 participant=-1, trial=-1, start_time=-1, end_time=-1):
    """
    Return all instances of a measurement group in which one or more of the measurements within the
        instance meet some specified criteria
    :param dw: data warehouse handle
    :param study: a study id
    :param measurement_group: a measurement group
    :param value_test_conditions: a list where each element is takes the following form:
                                (measurementType,condition)
                                   where condition is a string holding the condition
                                   against which the value in each measurement is compared.
    :param participant: a participant id
    :param trial: a trial id
    :param start_time: the start of a time period of interest
    :param end_time: the end of a time period of interest
    :return: a list of measurements. Each measurement is held in a list with the following fields:
                id,time,study,participant,measurementType,typeName,measurementGroup,
                groupInstance,trial,valType,value
    """
    problem_q = ""  # returns the instance ids of all instances that fail the criteria
    problem_q += " SELECT measurement.groupinstance "
    problem_q += core_sql_from_for_measurements()
    (w, first_condition) = core_sql_for_where_clauses(study, participant, -1, measurement_group, -1, trial,
                                                      start_time, end_time)
    problem_q += w
    if len(value_test_conditions) > 0:
        problem_q += " AND (" + dw.mk_value_tests(value_test_conditions, study) + ")"

    outer_query = core_sql_for_measurements()
    outer_query += " " + w
    if len(value_test_conditions) > 0:
        outer_query += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
    outer_query += " ORDER BY groupinstance, measurementtype"
    outer_query += ";"
    raw_results = dw.return_query_result(outer_query)
    formed_measurements = form_measurements_for_analysis(raw_results)
    return form_measurement_group(dw, study, measurement_group, formed_measurements)


def get_measurement_group_instances_for_cohort_for_analysis(dw, study, measurement_group, participants,
                                                            value_test_conditions, trial=-1,
                                                            start_time=-1, end_time=-1):
    """
    Return all instances of a measurement group in which one or more of the measurements within the
        instance meet some specified criteria for the specified cohort of participants
    :param dw: data warehouse handle
    :param study: a study id
    :param measurement_group: a measurement group
    :param participants: a list of participant ids
    :param value_test_conditions: a list where each element is takes the following form:
                                 (measurementType,condition)
                                  where condition is a string holding the condition
                                  against which the value in each measurement is compared.
    :param trial: a trial id
    :param start_time: the start of a time period of interest
    :param end_time: the end of a time period of interest
    :return: a list of measurements. Each measurement is held in a list with the following fields:
                id,time,study,participant,measurementType,typeName,measurementGroup,
                groupInstance,trial,valType,value
    """
    problem_q = ""  # returns the instance ids of all instances that fail the criteria
    problem_q += " SELECT measurement.groupinstance "
    problem_q += core_sql_from_for_measurements()
    where_clause = dw.core_sql_for_where_clauses_for_cohort(study, participants, -1, measurement_group,
                                                            -1, trial, start_time, end_time)
    problem_q += where_clause
    if len(value_test_conditions) > 0:
        problem_q += " AND (" + dw.mk_value_tests(value_test_conditions, study) + ")"

    outer_query = core_sql_for_measurements()
    outer_query += where_clause
    if len(value_test_conditions) > 0:
        outer_query += " AND measurement.groupinstance NOT IN (" + problem_q + ")"
    outer_query += " ORDER BY groupinstance, measurementtype"
    outer_query += ";"
    raw_results = dw.return_query_result(outer_query)
    formed_measurements = form_measurements_for_analysis(raw_results)
    return form_measurement_group(dw, study, measurement_group, formed_measurements)


def form_measurements_for_analysis(rows):
    """
    The raw query results within getMeasurements, getMeasurementsWithValueTest
        contain a column for each possible type of value:
            integer, real, datatime, string. Each is set to null apart from the one that holds the value appropriate
            for the type of measurement. This function replaces those columns with a single field holding the actual
            value. For catagoricals and bools, the integer value is used
    :param rows: list of rows returned by a query
    :return: list of rows, each representing one measurement in a list with elements:
            id,time,study,participant,measurementType,typeName,measurementGroup, groupInstance,trial,valType,value
    """
    n_rows: int = len(rows)
    n_cols: int = 11
    row_out = [[None] * n_cols for i in range(n_rows)]
    val_type_index = 9
    value_index = 10
    for r in range(n_rows):
        # Indices: 0 = id, 1 = time, 2 = study, 3 = participant, 4 = measurementType, 5 = measurementTypeName,
        # 6 = measurementGroup, 7 = measurementGroupInstance, 8 = trial, 9 = valType, 10 = value
        for x in range(value_index):
            row_out[r][x] = rows[r][x]
        if rows[r][val_type_index] == 0:  # integer
            row_out[r][value_index] = rows[r][10]
        elif rows[r][val_type_index] == 1:  # real
            row_out[r][value_index] = rows[r][11]
        elif rows[r][val_type_index] == 2:  # text
            row_out[r][value_index] = rows[r][12]
        elif rows[r][val_type_index] == 3:  # datetime
            row_out[r][value_index] = rows[r][13]
        elif rows[r][val_type_index] == 4:  # boolean
            row_out[r][value_index] = rows[r][value_index]  # 0 for False, 1 for True
        elif rows[r][val_type_index] == 5:  # nominal
            row_out[r][value_index] = rows[r][value_index]  # use the key, not the text
        elif rows[r][val_type_index] == 6:  # ordinal
            row_out[r][value_index] = rows[r][value_index]  # use the key, not the text
        elif rows[r][val_type_index] == 7:  # boundedint
            row_out[r][value_index] = rows[r][10]
        elif rows[r][val_type_index] == 8:  # boundedreal
            row_out[r][value_index] = rows[r][11]
        elif rows[r][val_type_index] == 9:  # bounded datetime
            row_out[r][value_index] = rows[r][13]
        elif rows[r][val_type_index] == 10:  # external
            row_out[r][value_index] = rows[r][12]
        else:
            print("typeval error of ", rows[r][9], " for id", rows[r][0], " study ", rows[r][2])
    return row_out


def print_instances_in_a_study_for_analysis_to_csv_files(dw, study, report_dir, select_participants=False,
                                                         participants=[], local_participant_id=True, filename_prefix='',
                                                         print_empty_files=False):
    """
    Print instances in a study to a set of csvs - one per measurement group,
    but only for those participants in the participants list
    :param dw: data warehouse handle
    :param study: study id
    :param report_dir: the directory in which the profiles will be written
    :param select_participants: select a subset of participants to be included in the profile
    :param participants: list of participants to be included in the profile if select_participants is true
    :param local_participant_id: include the local participant id if this boolean is True
    :param filename_prefix: optional string to add to front of filename
    :param print_empty_files: optional boolean to print csv files with no instances in them
    """
    if local_participant_id:
        participant_local_id = mk_participants_dictionary(dw, study)   # create a dictionary mapping from id to local id
    timestamp = datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    participant_index = 3  # the participant_id is in position 3 of the list
    measurement_groups = dw.get_all_measurement_groups(study)
    for [mg_id, mg_name] in measurement_groups:
        (header, all_instances) = dw.get_measurement_group_instances_for_analysis(study, mg_id, [])
        if select_participants:  # select participants
            instances = list(filter(lambda inst: inst[participant_index] in participants, all_instances))
        else:
            instances = all_instances
        if (len(instances) > 0) or print_empty_files:   # if there are some instances in the measurement group
                                                        # or if files should be created even if there are no instances
            fname = mk_csv_report_file_name(report_dir, filename_prefix +
                                            "study-instances-" + mg_name + "-", time_fname_str)
            if local_participant_id:
                instances_with_local_participant_id = []
                extended_header = ['Local Participant'] + header
                for instance in instances:  # add the local participant id to the start of each row
                    participant_id = instance[participant_index]  # get unique participant id
                    local_participant = participant_local_id[participant_id]  # get the local participant id
                    instances_with_local_participant_id = instances_with_local_participant_id +\
                        [[local_participant] + instance]
                    export_measurement_groups_as_csv(extended_header, instances_with_local_participant_id, fname)
            else:  # don't include local id
                export_measurement_groups_as_csv(header, instances, fname)
