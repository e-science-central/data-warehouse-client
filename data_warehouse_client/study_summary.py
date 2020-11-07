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
