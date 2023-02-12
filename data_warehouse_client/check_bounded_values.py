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

from data_warehouse_client import file_utils
import type_definitions as ty
from typing import Union, Tuple, List, Dict, Optional


def get_bounded_int_bounds(dw, study):
    query = file_utils.process_sql_template("get_boundsint_in_study.sql", {"study": study})
    res = dw.return_query_result(query)
    int_bounds = {}
    for [mt_id, minval, maxval] in res:
        int_bounds[mt_id] = {'minval': minval, 'maxval': maxval}
    return int_bounds


def get_bounded_real_bounds(dw, study):
    query = file_utils.process_sql_template("get_boundsreal_in_study.sql", {"study": study})
    res = dw.return_query_result(query)
    real_bounds = {}
    for [mt_id, minval, maxval] in res:
        real_bounds[mt_id] = {'minval': minval, 'maxval': maxval}
    return real_bounds


def get_bounded_datetime_bounds(dw, study):
    query = file_utils.process_sql_template("get_boundsdatetime_in_study.sql", {"study": study})
    res = dw.return_query_result(query)
    datetime_bounds = {}
    for [mt_id, minval, maxval] in res:
        datetime_bounds[mt_id] = {'minval': minval, 'maxval': maxval}
    return datetime_bounds


def check_bounded_int_in_bounds(int_bounds: Dict[ty.MeasurementType, Dict[str, int]],
                                mt: ty.MeasurementType, value: int) -> bool:
    bounds_mt = int_bounds[mt]
    return bounds_mt['minval'] <= value <= bounds_mt['maxval']


def check_bounded_real_in_bounds(real_bounds: Dict[ty.MeasurementType, Dict[str, float]],
                                 mt: ty.MeasurementType, value: float) -> bool:
    bounds_mt = real_bounds[mt]
    return bounds_mt['minval'] <= value <= bounds_mt['maxval']


def check_bounded_datetime_in_bounds(datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]],
                                     mt: ty.MeasurementType, value: ty.DateTime) -> bool:
    bounds_mt = datetime_bounds[mt]
    return bounds_mt['minval'] <= value <= bounds_mt['maxval']
