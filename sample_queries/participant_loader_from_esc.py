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

def insert_new_participants_in_warehouse(mc, dw, esc_project, study_id):
    """
    load a list of participants from e-SC into the data warehouse
    :param mc: e-Science Central endpoint
    :param dw: Data Warehouse endpoint
    :param esc_project: the e-SC projectId
    :param study_id: the data warehouse study id
    :res a list of ids of new participants
    """

    participants_in_esc = get_all_participants_in_esc_study(mc, esc_project)
    participants_in_dw = get_all_participants_in_dw_study(dw, study_id)
    new_participants = list_difference(participants_in_esc, participants_in_dw)
    new_entries = insert_participants_by_local_id(dw, study_id, new_participants)
    return new_entries


def list_difference(l1, l2):
    """
    return the difference between 2 lists
    :param l1:
    :param l2:
    :return: list l1 - list l2
    """
    return list(set(l1) - set(l2))


def get_all_participants_in_esc_study(mc, esc_project):
    """
    return all participants in a study, from e-Science Central
    :param mc: e-Science Central endpoint
    :param esc_project: the e-SC project Id
    :res   the list of the participants found in e-Science Central
    """
    # Get all of the people in a study
    person_count = mc.getNumberOfPeopleInStudy(esc_project)
    people = mc.getPeople(esc_project, 0, person_count)
    participants = []
    for i in range(0, len(people)):
        participants = [people[i].externalId] + participants
    return participants


def get_all_participants_in_dw_study(dw, study_id):
    """
    return all local participant ids for a study
    :param dw: data warehouse end point
    :param study_id: study id in dw
    :return: list of local participant ids
    """
    participants = dw.get_participants(study_id)
    local_participant_ids = []
    for p in participants:
        local_participant_ids = [p[1]] + local_participant_ids
    return local_participant_ids


def insert_participants_by_local_id(dw, study_id, participants):
    """
    insert a set of participants (by local id) in the warehouse
    :param dw: data warehouse end point
    :param study_id: study id in dw
    :param participants: list of participants to be inserted
    :return: list of participants inserted
    """
    new_participants = []
    for participant in participants:
        new_id = dw.add_participant(study_id, participant)  # insert new participant with study id
        new_participants = [(new_id, participant)] + new_participants
    return new_participants
