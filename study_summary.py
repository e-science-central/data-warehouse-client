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

def get_measurement_group_summary(dw, study, mg_id):
    """
    Returns the number of instances in a measurement group,
     and the number of instances that don't have every measurement
    :param dw: data warehouse handle
    :param study: study id
    :param mg_id: measurement group
    :return: number of instances in the measurement group, and the number of instances that don't have every measurement
    """
    n_mgi = dw.n_mg_instances(mg_id, study)
    # mgis = dw.mg_instances(mg_id, study)
    msgs = dw.formMeasurementGroup(study,dw.getMeasurements(study,measurementGroup=mg_id))
    # print(mg_id, ":", n_mgi,"vs",len(msgs))
    n_partial_mgi = 0
    if len(msgs) > 0:
        message_length = len(msgs[0])
    for m in msgs:
        incomplete = False
        for x in range(6,message_length):
            if m[x] is None:
                incomplete = True
        if incomplete:
            n_partial_mgi = n_partial_mgi + 1
    return (n_mgi, n_partial_mgi)


def get_instances_per_measurement_group(dw, study):
    """
    Returns the number of instances in all measurement groups in a study,
    and the number of instances that don't have every measurement
    :param dw: data warehouse handle
    :param study: study id
    :param mg_id: measurement group
    :return: number of instances in each measurement group, and the number of instances that don't have every measurement
    """
    measurement_groups = dw.get_all_measurement_groups(study)
    summary_table = []
    for [mg_id, mg_name] in measurement_groups:
        (n_mgi, n_partial_mgi) = get_measurement_group_summary(dw, study, mg_id)
        summary_table = summary_table + [(mg_id, mg_name,n_mgi, n_partial_mgi)]
    return summary_table


def print_all_instances_in_a_study(dw, study):

    print(f'All Measurement Group Instances in Study {study}')
    measurement_groups = dw.get_all_measurement_groups(study)
    print()
    for [mg_id, mg_name] in measurement_groups:
        print(f'All measurements in group {mg_id} for Study {study} \n')
        ms = dw.getMeasurements(study=study, measurementGroup=mg_id)
        mgs = dw.formMeasurementGroup(study, ms)
        dw.printMeasurementGroupInstances(mgs, mg_id, study)
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
    print(f'Total Instances in each Measurement Group in Study {study}:')
    print("\n")

    summary = get_instances_per_measurement_group(dw, study)
    print(tabulate(summary, headers=['Id', 'Name', '#Instances', '#Partially Formed Instances']))

    n_measurement_group_instances = 0
    for (mg_id, mg_name, n_mgi, n_partial_mgi) in summary:
        n_measurement_group_instances = n_measurement_group_instances + n_mgi

    print("\n")
    print(f'Total Measurement Group Instances in Study {study}: {n_measurement_group_instances}')
