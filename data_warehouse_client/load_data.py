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

from multiple_mg_inserts import insert_measurement_group_instances
from load_warehouse_helpers import process_measurement_groups
import check_bounded_values
from type_definitions import MeasurementGroupInstance, DataToLoad, Source, Participant, Study, Trial, Loader, Bounds
from typing import Tuple, List, Optional, Dict


def get_loader_from_data_name(data_name: str, mapper: Dict[str, Loader]) -> Tuple[bool, Optional[Loader]]:
    """
    map from the data_name to the mapper function - used when process_measurement_groups is the way to ingest data
    :param data_name: the measurement_type_in_the_json
    :param mapper: the dictionary that maps from the event_type to the mapper function
    :return: (boolean indicating if the data_name is found, mapper function
    """
    if data_name in mapper:
        return True, mapper[data_name]
    else:
        return False, None


def load_data(data_warehouse_handle,
              data: DataToLoad,
              data_name: str,
              mapper: Dict[str, Loader],
              study: Study,
              bounds: Bounds = None,
              trial: Optional[Trial] = None,
              participant: Optional[Participant] = None,
              source: Optional[Source] = None,
              cursor=None) -> Tuple[bool, List[MeasurementGroupInstance], List[str]]:
    """
    Load one item of data into the datawarehouse.
    :param data_warehouse_handle: handle to access the data warehouse
    :param data: the data item containing the fields to be loaded
    :param data_name: the name of the type of data: this is used to find the correct loader
    :param mapper: a dictionary mapping from data_name to the loader function
    :param study: study id in the data warehouse
    :param bounds: tuple holding bounds used to check data
    :param trial: optional trial id
    :param participant: optional participant id (must already be in the warehouse)
    :param source: optional source id
    :param cursor: an optional cursor for accessing the datawarehouse
    :return: Success?, list of measurement group instance ids for the measurement groups inserted, error messages
    """

    if bounds is None:
        bounds = check_bounded_values.get_bounds(data_warehouse_handle, study)
    loader_found, loader = get_loader_from_data_name(data_name, mapper)  # find the loader function
    if loader_found:
        # get the (message group id, value triples) and other (optional values) to use in the loading
        vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = \
            loader(data, bounds)
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
                insert_measurement_group_instances(
                    data_warehouse_handle, study, all_mgs_and_triples, bounds=bounds,
                    time=time_from_data, trial=trial_to_insert,
                    participant=participant_to_insert, source=source_to_insert,
                    cursor=cursor)
            return successful_inserts, message_group_instance_ids, error_message
        else:
            return False, [], combined_error_messages   # error(s) detected in the data to be loaded
    else:
        return False, [], [f'Loader Not Found for data named {data_name}']   # loader not found
