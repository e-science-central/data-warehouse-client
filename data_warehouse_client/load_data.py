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
import load_warehouse_helpers as lwh
import type_definitions as ty
from typing import Tuple, List, Optional, Dict, Callable


def load_data(data_warehouse_handle, data: ty.DataToLoad, data_name: str,
              mapper: Dict[str, Callable[[ty.DataToLoad], ty.LoaderResult]], study: int,
              trial: Optional[ty.Trial] = None, participant: Optional[ty.Participant] = None,
              source: Optional[ty.Source] = None,
              cursor=None) -> Tuple[bool, List[ty.MeasurementGroupInstance], List[str]]:
    """
    Load one item of data into the datawarehouse.
    :param data_warehouse_handle: handle to access the data warehouse
    :param data: the data item containing the fields to be loaded
    :param data_name: the name of the type of data: this is used to find the correct loader
    :param mapper: a dictionary mapping from data_name to the loader function
    :param study: study id in the data warehouse
    :param trial: optional trial id
    :param participant: optional participant id (must already be in the warehouse)
    :param source: optional source id
    :param cursor: an optional cursor for accessing the datawarehouse
    :return: Success?, list of measurement group instance ids for the measurement groups inserted, error messages
    """
    loader_found, loader = lwh.get_loader_from_data_name(data_name, mapper)  # find the loader function
    if loader_found:
        # get the (message group id, value triples) and other (optional values) to use in the loading
        vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = loader(data)
        # check for errors in the values
        successful, all_mgs_and_triples, combined_error_messages = lwh.process_measurement_groups(vals_to_load_in_msgs)
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
