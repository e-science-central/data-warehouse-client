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

import load_warehouse_helpers


def load_dw_from_esc(esc, dw, study, esc_project_id, unique_id_measurement_type, mapper_dict):
    """
    Retrieve all of the events from a project from e-SC and store in the data warehouse
    :param esc: e-Science Central endpoint
    :param dw: data warehouse endpoint
    :param study: data warehouse study id
    :param esc_project_id: the project id used by e-Science Central
    :param unique_id_measurement_type, the measurement type that holds the unique id for each measurement group
    :param mapper_dict: a dictionary mapping from the event_types in the data to the functions that load the warehouse
    :return: the instance group ids of the inserted measurement groups
    """
    events = extract_events_from_esc(esc, esc_project_id)
    instance_group_ids = load_data_into_dw(dw, study, unique_id_measurement_type, mapper_dict, events)
    return instance_group_ids


def extract_events_from_esc(esc, esc_project_id):
    """
    Retrieve all of the events from a project from e-SC
    :param esc: e-Science Central endpoint
    :param esc_project_id: the project id used by e-Science Central
    :return: a list of events
    """
    # get the project end point from e-Science Central
    project = esc.getProjectByStudyCode(esc_project_id)
    # get the total number of data items in the project
    event_count = esc.getEventCount(project.externalId)
    # retrieve all data items
    events = esc.queryEventsFromStudy(project.externalId, 0, event_count)

    event_data = []
    for i in range(0, len(events)):  # for each data item
        evt = events[i]  # get a data item
        # extract the relevant parts of the data from the structure returned by e-Science Central
        data_id = evt.metadata['_id']  # extract the unique data id
        event_type = evt.metadata['_eventType']
        object_id = evt.metadata['_objectId']
        data = evt.data
        timestamp = load_warehouse_helpers.get_timestamp(evt.timestamp)  # convert to data warehouse format
        event_data = [(data_id, event_type, object_id, data, timestamp)] + event_data
    return event_data


def load_data_into_dw(dw, study, unique_id_measurement_type, mapper_dict, event_data):
    """
    Store events retrieved from a project in e-SC in the data warehouse
    :param dw: data warehouse endpoint
    :param study: data warehouse study id
    :param unique_id_measurement_type, the measurement type that holds the unique id for each measurement group
    :param mapper_dict: a dictionary mapping from the event_types in the data to the functions that load the warehouse
    :param event_data: the events extracted from e-Science Central
    :return: the instance group ids of the inserted measurement groups
    """
    new_ids_in_dw = []
    for (data_id, event_type, object_id, data, timestamp) in event_data:
        not_already_in_warehouse = len(dw.getMeasurementsWithValueTest(unique_id_measurement_type, study,
                                                                       "='" + data_id + "'")) == 0
        # check if there's a converter function and message group for this type of data
        (event_found, load_fn, measurement_group) = load_warehouse_helpers.get_converter_fn(event_type, mapper_dict)
        if not event_found:
            print(f'Error: no load function entry for {event_type} in: {data_id}')
        # check if the participant exists
        (p_found, part) = dw.get_participant(study, object_id)
        if not p_found:
            print(f'Error: participant {object_id} not found in {data_id}')
        # if all information is available, and the data is not already in the warehouse then insert it
        if event_found and p_found and not_already_in_warehouse:
            try:
                mg_to_insert = load_fn(data_id, data)
                new_id = dw.insertMeasurementGroup(study, measurement_group, mg_to_insert,
                                                   time=timestamp, participant=part)
                new_ids_in_dw = [new_id] + new_ids_in_dw  # add instance group id to the list of inserted message groups
            except KeyError as ke:
                print(f'Error: Missing field: {ke}, in {event_type} in: {data_id}')
            except ValueError as ve:
                print(f'Error: Unknown category: {ve} in: {data_id}')
            except Exception as e:
                print("Error: ", e.__class__, "occurred.")
            # else:
                #  print("Error loading:", data_id)
    return new_ids_in_dw  # return all the inserted instance group ids


def get_data_types_from_esc(esc, esc_project):
    """
    return sorted list of data types from e-Science Central
    :param esc: e-Science Central endpoint
    :param esc_project: the project id used by e-Science Central
    :return: sorted list of data types
    """
    events = extract_events_from_esc(esc, esc_project)
    # fund unique types of data:
    unique_data_types = set()
    for e in events:
        unique_data_types.add(e[1])
    return sorted(unique_data_types)
