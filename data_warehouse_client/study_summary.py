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


# Summarise a study

from tabulate import tabulate
from data_warehouse_client import print_io
from data_warehouse_client import csv_io
import datetime


def get_instances_per_measurement_group(dw, study):
    """
    Returns the number of instances in all measurement groups in a study
    :param dw: data warehouse handle
    :param study: study id
    :return: number of instances in each measurement group
    """
    measurement_groups = dw.get_all_measurement_groups(study)
    summary_table = []
    for [mg_id, mg_name] in measurement_groups:
        n_mgi = dw.n_mg_instances(mg_id, study)
        summary_table = summary_table + [(mg_id, mg_name, n_mgi)]
    return summary_table


def print_all_instances_in_a_study(dw, study):
    """
    Print all instances in a study - don't print for measurement groups that have no measurements
    Now depreciated. Use print_instances_in_a_study_to_csv_files
    :param dw: data warehouse handle
    :param study: study id
    """
    print(f'All Measurement Group Instances in Study {study}')
    measurement_groups = dw.get_all_measurement_groups(study)
    print()
    for [mg_id, mg_name] in measurement_groups:
        (header, instances) = dw.get_measurement_group_instances(study, mg_id, [])
        if len(instances) > 0:
            print(f'All measurements in group {mg_id} ({mg_name}) for Study {study} \n')
            print_io.print_measurement_group_instances(header, instances)
            print()


def print_study_summary(dw, study):
    """
    Print a summary of the number of participants and the total instances in each measgeurement group in a study
    :param dw: data warehouse handle
    :param study: study id
    :return:
    """
    print(f'Summary of Study {study}')

    # Get Number of Participants
    n_participants = len(dw.get_participants(study))
    print(f'Number of Participants in Study {study}: {n_participants}')

    # Measurement Groups per Measurement Group Type
    print(f'Total Instances in each Measurement Group in Study {study} that contain measurements:')
    print("\n")

    summary = get_instances_per_measurement_group(dw, study)
    print(tabulate(summary, headers=['Id', 'Name', '#Instances']))

    n_measurement_group_instances = 0
    for (mg_id, mg_name, n_mgi) in summary:
        n_measurement_group_instances = n_measurement_group_instances + n_mgi

    print()
    print(f'Total Measurement Group Instances in Study {study}: {n_measurement_group_instances}')
    print()


def mk_txt_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + time_string + ".txt"


def mk_csv_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + time_string + ".csv"


def print_all_instances_in_a_study_to_file(dw, study):
    """
    Print to a file all instances in a study - don't print for measurement groups that have no measurements
    :param dw: data warehouse handle
    :param study: study id
    """
    file_dir = "reports/"
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    fname = mk_txt_report_file_name(file_dir, "study-instances-", time_fname_str)
    with open(fname, "w", encoding="utf-8") as f:
        print(f'All Measurement Group Instances in Study {study}\n', file=f)
        measurement_groups = dw.get_all_measurement_groups(study)
        for [mg_id, mg_name] in measurement_groups:
            (header, instances) = dw.get_measurement_group_instances(study, mg_id, [])
            if len(instances) > 0:
                print(f'All measurements in group {mg_id} ({mg_name}) for Study {study} \n', file=f)
                print_io.print_measurement_group_instances_to_file(header, instances, f)
                print('\n', file=f)


def print_all_instances_in_a_study_to_csv_files(dw, study):
    """
    Print all instances in a study to a set of csvs - one per measurement group
    Now depreciated. Use print_instances_in_a_study_to_csv_files
    :param dw: data warehouse handle
    :param study: study id
    """
    file_dir = "reports/"
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    measurement_groups = dw.get_all_measurement_groups(study)
    for [mg_id, mg_name] in measurement_groups:
        (header, instances) = dw.get_measurement_group_instances(study, mg_id, [])
        if len(instances) > 0:
            fname = mk_csv_report_file_name(file_dir, "study-instances-" + mg_name + "-", time_fname_str)
            csv_io.export_measurement_groups_as_csv(header, instances, fname)


def mk_participants_dictionary(dw, study):
    """
    Create a dictionary containing mapping from participant_id to local_id for all the participants in a study
    :param dw: data warehouse handle
    :param study: study_id
    :return: the dictionary
    """
    participants = dw.get_participants(study)  # return list of ids and local ids
    participant_local_id = {}
    for p in participants:
        participant_local_id[p[0]] = p[1]
    return participant_local_id


def print_instances_in_a_study_to_csv_files(dw, study, report_dir, select_participants=False, participants=[],
                                            local_participant_id=True, filename_prefix='', print_empty_files=False):
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
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    participant_index = 3  # the participant_id is in position 3 of the list
    measurement_groups = dw.get_all_measurement_groups(study)
    for [mg_id, mg_name] in measurement_groups:
        (header, all_instances) = dw.get_measurement_group_instances(study, mg_id, [])
        if select_participants:  # select participants
            instances = list(filter(lambda instance: instance[participant_index] in participants, all_instances))
        else:
            instances = all_instances
        if (len(instances) > 0) or print_empty_files:   # if there are some instances in the measurement group
                                                        #  or if files should be created even if there are no instances
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
                    csv_io.export_measurement_groups_as_csv(extended_header, instances_with_local_participant_id, fname)
            else:  # don't include local id
                csv_io.export_measurement_groups_as_csv(header, instances, fname)


def print_all_instances_in_a_study_with_local_participant_id_to_csv_files(dw, study):
    """
    Print all instances in a study to a set of csvs - one per measurement group - including participants
    This is now depreciated. Use print_instances_in_a_study_to_csv_files
    :param dw: data warehouse handle
    :param study: study id
    """
    file_dir = "reports/"
    participant_id_index = 3
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    measurement_groups = dw.get_all_measurement_groups(study)
    for [mg_id, mg_name] in measurement_groups:
        (header, instances) = dw.get_measurement_group_instances(study, mg_id, [])
        extended_header = ['Local Participant'] + header
        if len(instances) > 0:  # if there are some instances in the measurement group
            fname = mk_csv_report_file_name(file_dir, "study-instances-" + mg_name + "-", time_fname_str)
            instances_with_local_participant_id = []
            for instance in instances:  # add the local participant id to the start of each row
                participant_id = instance[participant_id_index]  # get unique participant id
                (success, part) = dw.get_participant_by_id(study, participant_id)  # get local participant id
                instances_with_local_participant_id = instances_with_local_participant_id + [[part] + instance]
            csv_io.export_measurement_groups_as_csv(extended_header, instances_with_local_participant_id, fname)


def print_study_summary_to_file(dw, study):
    """
    Print to a file a summary of the number of participants and the total instances in each measurement group in a study
    :param dw: data warehouse handle
    :param study: study id
    :return:
    """
    file_dir = "reports/"
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    fname = mk_txt_report_file_name(file_dir, "study-summary-", time_fname_str)
    with open(fname, "w", encoding="utf-8") as f:
        print(f'Summary of Study {study}', file=f)
        # Get Number of Participants
        n_participants = len(dw.get_participants(study))
        print(f'Number of Participants in Study {study}: {n_participants}', file=f)
        # Measurement Groups per Measurement Group Type
        print(f'Total Instances in each Measurement Group in Study {study} that contain measurements:\n', file=f)
        summary = get_instances_per_measurement_group(dw, study)
        print(tabulate(summary, headers=['Id', 'Name', '#Instances']), file=f)

        n_measurement_group_instances = 0
        for (mg_id, mg_name, n_mgi) in summary:
            n_measurement_group_instances = n_measurement_group_instances + n_mgi
        print(f'Total Measurement Group Instances in Study {study}: {n_measurement_group_instances}\n', file=f)
