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
import check_bounded_values


def load_data(data_warehouse_handle,
              data: ty.DataToLoad,
              data_name: str,
              mapper: Dict[str, Callable[[ty.DataToLoad], ty.LoaderResult]],
              study: ty.Study,
              int_bounds: Dict[ty.MeasurementType, Dict[str, int]] = None,
              real_bounds: Dict[ty.MeasurementType, Dict[str, float]] = None,
              datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]] = None,
              category_id_map: Dict[ty.MeasurementType, List[int]] = None,
              category_value_map: Dict[ty.MeasurementType, Dict[str, int]] = None,
              trial: Optional[ty.Trial] = None,
              participant: Optional[ty.Participant] = None,
              source: Optional[ty.Source] = None,
              cursor=None) -> Tuple[bool, List[ty.MeasurementGroupInstance], List[str]]:
    """
    Load one item of data into the datawarehouse.
    :param data_warehouse_handle: handle to access the data warehouse
    :param data: the data item containing the fields to be loaded
    :param data_name: the name of the type of data: this is used to find the correct loader
    :param mapper: a dictionary mapping from data_name to the loader function
    :param study: study id in the data warehouse
    :param int_bounds: dictionary holding integer bounds
    :param real_bounds: dictionary holding real bounds
    :param datetime_bounds: dictionary holding datetime bounds
    :param category_id_map: dictionary holding mapping from category ids to names for each measurement type
    :param category_value_map: dictionary holding mapping from category names to ids
    :param trial: optional trial id
    :param participant: optional participant id (must already be in the warehouse)
    :param source: optional source id
    :param cursor: an optional cursor for accessing the datawarehouse
    :return: Success?, list of measurement group instance ids for the measurement groups inserted, error messages
    """

    if int_bounds is None:
        int_bounds = check_bounded_values.get_bounded_int_bounds(data_warehouse_handle, study)
    if real_bounds is None:
        real_bounds = check_bounded_values.get_bounded_real_bounds(data_warehouse_handle, study)
    if datetime_bounds is None:
        datetime_bounds = check_bounded_values.get_bounded_datetime_bounds(data_warehouse_handle, study)
    if category_id_map is None:
        category_id_map = check_bounded_values.get_category_ids(data_warehouse_handle, study)
    if category_value_map is None:
        category_value_map = check_bounded_values.get_inverse_category_ids_map(data_warehouse_handle, study)
    loader_found, loader = lwh.get_loader_from_data_name(data_name, mapper)  # find the loader function
    if loader_found:
        # get the (message group id, value triples) and other (optional values) to use in the loading
        vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = \
            loader(data, int_bounds, real_bounds, datetime_bounds, category_id_map, category_value_map)
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
