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


def get_converter_fn_from_data_name(data_name, mapper_dict):
    """
    map from the data_name to the mapper function
    :param data_name: the measurement_type_in_the_json
    :param mapper_dict: the dictionary that maps from the event_type to the mapper function
    :return: (boolean indicating if the data_name is found, mapper function
    """
    found = data_name in mapper_dict
    if found:
        info = mapper_dict[data_name]
        return found, info["fn"]
    else:
        return found, None


def load_data(data_warehouse_handle, data, data_name, mapper_dict, study,
              trial=None, participant=None, source=None, cursor=None):
    loader_found, loader = get_converter_fn_from_data_name(data_name, mapper_dict)
    if loader_found:
        vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = loader(data)
        successful, all_mgs_and_triples, combined_error_messages = process_measurement_groups(vals_to_load_in_msgs)
        if successful:
            if trial_from_data is None:
                trial_to_insert = trial
            else:
                trial_to_insert = trial_from_data
            if participant_from_data is None:
                participant_to_insert = participant
            else:
                participant_to_insert = participant_from_data
            if source_from_data is None:
                source_to_insert = source
            else:
                source_to_insert = source_from_data
            successful_inserts, message_group_instance_ids, error_message = \
                multiple_mg_inserts.insert_measurement_group_instances(
                    data_warehouse_handle, study, all_mgs_and_triples,
                    time_from_data, trial_to_insert, participant_to_insert, source_to_insert, cursor)
            return successful_inserts, message_group_instance_ids, error_message
        else:
            return False, [], combined_error_messages
    else:
        return False, [], [f'Loader Not Found for data named {data_name}']


def process_measurement_groups(vals_to_load_in_mgs):
    """
    takes the result of attempting to load each field in a message group and processes it
    :param vals_to_load_in_mgs: [(measurement_group_id, [(Success, [(measurement_type, valtype, value)], Error Mess)])]
    :return: (Success, [(measurement_group_id, [(measurement_type, valtype, value)])], [Error Mess])
    """
    successful = True
    all_mgs_and_triples = []
    all_error_messages = []
    for (measurement_group_id, vals_to_load_in_mg) in vals_to_load_in_mgs:
        success, triples, error_messages = load_warehouse_helpers.process_message_group(vals_to_load_in_mg)
        successful = successful and success
        all_mgs_and_triples = [(measurement_group_id, triples)] + all_mgs_and_triples
        all_error_messages = [error_messages] + all_error_messages

    combined_error_messages = list(filter(lambda s: s != "", all_error_messages))
    return successful, all_mgs_and_triples, combined_error_messages
