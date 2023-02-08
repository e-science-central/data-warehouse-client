# Copyright 2023 Newcastle University.
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

import multiple_mg_inserts
import load_warehouse_helpers
import type_definitions as typ
from typing import Tuple, List, Optional, Dict, Callable


def get_converter_fn_from_data_name(data_name: str,
                                    mapper_dict: Dict[str, Callable]) -> Tuple[bool, Optional[Callable]]:
    """
    map from the data_name to the mapper function
    :param data_name: the measurement_type_in_the_json
    :param mapper_dict: the dictionary that maps from the event_type to the mapper function
    :return: (boolean indicating if the data_name is found, mapper function
    """
    if data_name in mapper_dict:
        return True, mapper_dict[data_name]
    else:
        return False, None


def load_data(data_warehouse_handle, data, data_name: str, mapper_dict: Dict[str, Callable], study: int,
              trial: Optional[typ.Trial] = None, participant: Optional[typ.Participant] = None,
              source: Optional[typ.Source] = None,
              cursor=None) -> Tuple[bool, List[typ.MeasurementGroupInstance], List[str]]:
    """
    Load one item of data into the datawarehouse.
    :param data_warehouse_handle: handle to access the data warehouse
    :param data: the data item containing the fields to be loaded
    :param data_name: the name of the type of data: this is used to find the correct loader
    :param mapper_dict: a dictionary mapping from data_name to the loader function
    :param study: study id in the data warehouse
    :param trial: optional trial id
    :param participant: optional participant id (must already be in the warehouse)
    :param source: optional source id
    :param cursor: an optional cursor for accessing the datawarehouse
    :return: Success?, list of measurement group instance ids for the measurement groups inserted, error messages
    """
    loader_found, loader = get_converter_fn_from_data_name(data_name, mapper_dict)  # find the loader function
    if loader_found:
        # get the (message group id, value triples) and other (optional values) to use in the loading
        vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = loader(data)
        # check for errors in the values
        successful, all_mgs_and_triples, combined_error_messages = process_measurement_groups(vals_to_load_in_msgs)
        if successful:  # all values are ready to be inserted into the data warehouse
            if trial_from_data is None:  # if the loader function has returned a trial id then use it
                trial_to_insert = trial
            else:
                trial_to_insert = trial_from_data
            if participant_from_data is None:  # if the loader function has returned a participant id then use it
                participant_to_insert = participant
            else:
                participant_to_insert = participant_from_data
            if source_from_data is None:  # if the loader function has returned a source id then use it
                source_to_insert = source
            else:
                source_to_insert = source_from_data
            # try to insert the measurement group instances
            successful_inserts, message_group_instance_ids, error_message = \
                multiple_mg_inserts.insert_measurement_group_instances(
                    data_warehouse_handle, study, all_mgs_and_triples,
                    time_from_data, trial_to_insert, participant_to_insert, source_to_insert, cursor)
            return successful_inserts, message_group_instance_ids, [error_message]
        else:
            return False, [], combined_error_messages   # error(s) detected in the data to be loaded
    else:
        return False, [], [f'Loader Not Found for data named {data_name}']   # loader not found


def process_measurement_groups(
        vals_to_load_in_mgs: List[Tuple[typ.MeasurementGroup, List[Tuple[bool, List[typ.ValueTriple]]], List[str]]]) ->\
        Tuple[bool, List[Tuple[typ.MeasurementGroup, List[typ.ValueTriple]]], List[str]]:
    """
    takes the result of attempting to load each field in a message group and processes it
    :param vals_to_load_in_mgs: [(measurement_group_id, [(Success, [(measurement_type, valtype, val)], [Error Mess])])]
    :return: (Success, [(measurement_group_id, [(measurement_type, valtype, value)])], [Error Mess])
    """
    successful: bool = True
    all_mgs_and_triples: List[Tuple[int, List[typ.ValueTriple]]] = []
    all_error_messages: List[str] = []
    for (measurement_group_id, vals_to_load_in_mg) in vals_to_load_in_mgs:
        success, triples, error_messages = load_warehouse_helpers.process_message_group(vals_to_load_in_mg)
        successful = successful and success
        all_mgs_and_triples = [(measurement_group_id, triples)] + all_mgs_and_triples
        all_error_messages = error_messages + all_error_messages

    combined_error_messages = list(filter(lambda s: s != "", all_error_messages))
    return successful, all_mgs_and_triples, combined_error_messages
