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

import file_utils
from type_definitions import Study, MeasurementType, Bounds, DateTime
from typing import List, Dict


def get_category_ids(dw, study: Study) -> Dict[MeasurementType, List[int]]:
    """
    Create a dirctory to map from Measurement Type Id to Category Id
    :param dw: data warehouse handle
    :param study: Study id
    :return: Dictionary mapping from Measurement Type to a list of the Category Ids in that Measurement Type
    """
    query = file_utils.process_sql_template("get_category_ids.sql", {"study": study})  # query to get mts and cat ids
    res = dw.return_query_result(query)  # execture query
    categories: Dict[MeasurementType, List[int]] = {}  # a dictionary to hold the mt -> category id mapping
    for [category_id, mt_id] in res:
        if mt_id in categories:  # the mt is already in the dictionary
            categories[mt_id] = [category_id] + categories[mt_id]  # add the category id to the existing list of ids
        else:   # make a new entry
            categories[mt_id] = [category_id]  # make the first entry in the dictionary
    return categories


def get_inverse_category_ids_map(dw, study: Study) -> Dict[MeasurementType, Dict[str, int]]:
    """
     Create a directory to map from Measurement Type Id to a Dictionary mapping from Category Name to Category Id
     :param dw: data warehouse handle
     :param study: Study id
     :return: Dictionary mapping from Measurement Type to a Dictionary mapping from Category Name to Category Id
     """
    query = file_utils.process_sql_template("get_categories_in_study.sql", {"study": study})
    res = dw.return_query_result(query)  # execute query - returns list of [mt_id, name, id]
    categories: Dict[MeasurementType, Dict[str, id]] = {}  # a dictionary to hold the mt -> category id mapping
    for [mt_id, category_id, category_name] in res:
        if mt_id in categories:  # the mt is already in the dictionary
            categories[mt_id][category_name] = category_id  # add the category id to the existing list of ids
        else:   # make a new entry
            categories[mt_id] = {category_name: category_id}  # make the first entry in the dictionary
    return categories


def get_bounded_int_bounds(dw, study: Study) -> Dict[MeasurementType, Dict[str, int]]:
    """
    Create a dictionary to map from Measurement Type Id to lower and upper bounds for bounded integers
    :param dw: data warehouse handle
    :param study: Study id
    :return: Dictionary mapping from Measurement Type to the min and max bounds
    """
    query = file_utils.process_sql_template("get_boundsint_in_study.sql", {"study": study})  # get mts and bounds
    res = dw.return_query_result(query)  # execute query
    int_bounds: Dict[MeasurementType, Dict[str, int]] = {}  # a dictionary to hold the mt -> bounds mapping
    for [mt_id, minval, maxval] in res:
        int_bounds[mt_id] = {'minval': minval, 'maxval': maxval}  # add new entry in dictionary
    return int_bounds


def get_bounded_real_bounds(dw, study: Study) -> Dict[MeasurementType, Dict[str, float]]:
    """
     Create a dictionary to map from Measurement Type Id to lower and upper bounds for bounded reals
     :param dw: data warehouse handle
     :param study: Study id
     :return: Dictionary mapping from Measurement Type to the min and max bounds
     """
    query = file_utils.process_sql_template("get_boundsreal_in_study.sql", {"study": study})  # get mts and bounds
    res = dw.return_query_result(query)  # execute query
    real_bounds: Dict[MeasurementType, Dict[str, float]] = {}
    for [mt_id, minval, maxval] in res:
        real_bounds[mt_id] = {'minval': minval, 'maxval': maxval}  # add new entry in dictionary
    return real_bounds


def get_bounded_datetime_bounds(dw, study: Study) -> Dict[MeasurementType, Dict[str, DateTime]]:
    """
     Create a dictionary to map from Measurement Type Id to lower and upper bounds for bounded datetimes
     :param dw: data warehouse handle
     :param study: Study id
     :return: Dictionary mapping from Measurement Type to the min and max bounds
     """
    query = file_utils.process_sql_template("get_boundsdatetime_in_study.sql", {"study": study})
    res = dw.return_query_result(query)
    datetime_bounds: Dict[MeasurementType, Dict[str, DateTime]] = {}
    for [mt_id, minval, maxval] in res:
        datetime_bounds[mt_id] = {'minval': minval, 'maxval': maxval}  # add new entry in dictionary
    return datetime_bounds


def get_bounds(dw, study: Study) -> Bounds:
    """
    Create a tuple holding all the bounds needed to check values to be inserted into the warehouse
    :param dw: data warehouse handle
    :param study: Study id
    :return: Tuple holding: int bounds, real bounds, datetime bounds, category ids, category value to id mapper
    """
    bounded_int_bounds = get_bounded_int_bounds(dw, study)
    bounded_real_bounds = get_bounded_real_bounds(dw, study)
    bounded_datetime_bounds = get_bounded_datetime_bounds(dw, study)
    category_ids = get_category_ids(dw, study)
    inverse_category_ids_map = get_inverse_category_ids_map(dw, study)
    return bounded_int_bounds, bounded_real_bounds, bounded_datetime_bounds, category_ids, inverse_category_ids_map


def check_bounded_int_in_bounds(int_bounds: Dict[MeasurementType, Dict[str, int]],
                                mt: MeasurementType, value: int) -> bool:
    """
    Check if a bounded int is in bounds
    :param int_bounds: dictionary mapping from Measurement Type to the min and max bounds
    :param mt: measurement type id
    :param value: value to check
    :return: True if within bounds
    """
    bounds_mt = int_bounds[mt]   # get the bounds for this measurement type
    return bounds_mt['minval'] <= value <= bounds_mt['maxval']


def check_bounded_real_in_bounds(real_bounds: Dict[MeasurementType, Dict[str, float]],
                                 mt: MeasurementType, value: float) -> bool:
    """
    Check if a bounded real is in bounds
    :param real_bounds: dictionary mapping from Measurement Type to the min and max bounds
    :param mt: measurement type id
    :param value: value to check
    :return: True if within bounds
    """
    bounds_mt = real_bounds[mt]  # get the bounds for this measurement type
    return bounds_mt['minval'] <= value <= bounds_mt['maxval']


def check_bounded_datetime_in_bounds(datetime_bounds: Dict[MeasurementType, Dict[str, DateTime]],
                                     mt: MeasurementType, value: DateTime) -> bool:
    """
    Check if a bounded datetime is in bounds
    :param datetime_bounds: dictionary mapping from Measurement Type to the min and max bounds
    :param mt: measurement type id
    :param value: value to check
    :return: True if within bounds
    """
    bounds_mt = datetime_bounds[mt]  # get the bounds for this measurement type
    return bounds_mt['minval'] <= value <= bounds_mt['maxval']


def check_category_id(categories: Dict[MeasurementType, List[int]], mt: MeasurementType, cat_id: int) -> bool:
    """
    check if category id exists for a measurement type
    :param categories:
    :param mt: measurement type
    :param cat_id: category id
    :return: True if category id exists
    """
    return cat_id in categories[mt]
