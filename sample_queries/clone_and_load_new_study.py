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


from tabulate import tabulate

import clone_study_metadata
import study_summary
import warehouse_checker
from sample_queries import mobilise_cohort_selection, participant_loader_from_esc, warehouse_loader_from_esc


# Create a new study, clone it from the metatdata in an existing study, check for errors and summarise


def create_and_load_new_study(dw_handle, old_study, new_study, esc_handle, esc_project,
                              unique_id_measurement_type, load_fns):
    """
    Create a new study, clone it from the metatdata in an existing study, check for errors and summarise
    :param dw_handle: data warehouse handle
    :param old_study: study from which teh new study is to be cloned
    :param new_study: new study id (must already exist)
    :param esc_handle: eScience Central handle
    :param esc_project: eScience Central project
    :param unique_id_measurement_type: the measurement type that holds the unique id in each measurement instance
    :param load_fns: the functions that trasnform the data from their json format to data warehouse format
    :return:
    """

    clone_study_metadata.clone_study_metadata(dw_handle, old_study, new_study)

    print(f'Insert new participants from e-Science Central project {esc_project} into Data Warehouse study {new_study}')
    project = esc_handle.getProjectByStudyCode(esc_project)
    new_participants = participant_loader_from_esc.insert_new_participants_in_warehouse(esc_handle, dw_handle,
                                                                                        project.id, new_study)
    n_new_participants = len(new_participants)
    print(f'There were {n_new_participants} added to the Data Warehouse:')
    print(tabulate(new_participants, headers=['Id', 'Local Id']))
    print()

    print(f'Write Condition and Site Information for new participants')
    for p in new_participants:
        mobilise_cohort_selection.write_condition_and_site(dw_handle, new_study, p[0], esc_project)

    # print(f'Participants now in the DW for Study {study_id}')
    # dw_participants = participant_loader_from_esc.get_all_participants_in_dw_study(data_warehouse, study_id)
    # print(*dw_participants, sep='\n')

    print(f'Copy events from e-Science Central project {esc_project} into Data Warehouse study {new_study}')
    print(f'Events in e-Science Central Project {esc_project}:')
    events = warehouse_loader_from_esc.extract_events_from_esc(esc_handle, esc_project)
    print(*events, sep='\n')

    print(f'\nLoad Data from e-Science Central Project {esc_project} into Data Warehouse Study {new_study}')
    new_instances = warehouse_loader_from_esc.load_dw_from_esc(esc_handle, dw_handle, new_study, esc_project,
                                                               unique_id_measurement_type,
                                                               load_fns)
    n_instances_added = len(new_instances)
    print(f'There were {n_instances_added} New Instances added:')
    print(*new_instances, sep=',')
    print()

    warehouse_checker.print_check_warhouse(dw_handle, new_study)
    study_summary.print_study_summary(dw_handle, new_study)
