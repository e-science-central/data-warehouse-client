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

from datetime import datetime
import type_definitions as ty
from typing import Tuple, Any
import check_bounded_values as cbv
from type_definitions import Bounds as Bounds


def int_bounds(bounds: Bounds):
    return bounds[0]


def real_bounds(bounds: Bounds):
    return bounds[1]


def datetime_bounds(bounds: Bounds):
    return bounds[2]


def category_ids(bounds: Bounds):
    return bounds[3]


def category_values(bounds: Bounds):
    return bounds[4]


def check_int(val: Any) -> bool:
    """
    Check if a value represents an integer
    :param val: value
    :return: True if the value represents an integer
    """
    return isinstance(val, int)


def check_real(val: Any) -> bool:
    """
    Check if a value represents a real. Note that
    :param val: value
    :return: True if the value represents a real; note that reals must contain a decimal point (e.g. 1 is not a real)
    """
    return isinstance(val, float)


def check_datetime(val: Any) -> bool:
    """
    Check if a value represents a datetime
    :param val: value
    :return: True if the value represents a datetime
    """
    return isinstance(val, datetime)


def check_string(val: Any) -> bool:
    """
    Check if a value represents a string
    :param val: value
    :return: True if the value represents a string
    """
    return isinstance(val, str)


def check_boolean(val: Any) -> bool:
    """
    Check if a value represents a boolean
    :param val:
    :return: True if the val represents a boolean
    """
    return val in ['T', 'Y', 'F', 'N', '0', '1', 0, 1]


def ok_bool_val(value: ty.Value) -> bool:
    """
    acceptable boolean value?
    :param value: value to be tested
    :return: true if acceptable boolean value
    """
    return value in [0, 1]


def type_check(val: Any, val_type: ty.ValType):
    """
    Check the type of a value retrieved from a field. Used by the load warehouse helpers
    :param val: value
    :param val_type: the type is should be
    :return: True if the value has the right type, False otherwise
    """
    integer_type: ty.ValType = 0
    real_type: ty.ValType = 1
    string_type: ty.ValType = 2
    datetime_type: ty.ValType = 3
    boolean_type: ty.ValType = 4
    nominal_type: ty.ValType = 5
    ordinal_type: ty.ValType = 6
    bounded_int_type: ty.ValType = 7
    bounded_real_type: ty.ValType = 8
    bounded_datetime_type: ty.ValType = 9
    external_type: ty.ValType = 10

    if val_type in [integer_type, nominal_type, ordinal_type, bounded_int_type]:
        well_typed = check_int(val)
    elif val_type in [real_type, bounded_real_type]:
        well_typed = check_real(val)
    elif val_type in [datetime_type, bounded_datetime_type]:
        well_typed = check_datetime(val)
    elif val_type == boolean_type:
        well_typed = check_boolean(val)
    elif val_type in [string_type, external_type]:
        well_typed = check_string(val)
    else:  # wrong valtype
        well_typed = False
    return well_typed


def check_value_type(val_type: ty.ValType, value: ty.Value, measurement_type: ty.MeasurementType,
                     bounds: Bounds) -> Tuple[bool, str]:
    """
    check valid value type, and set the entries in the measurement table's integer and real fields
    :param val_type: type of the value
    :param value: value to be inserted in the measurement table
    :param measurement_type: measurement type
    :param bounds: tuple holding bounds used for checking
    :return: success?, error message
    """
    integer_type: ty.ValType = 0
    real_type: ty.ValType = 1
    string_type: ty.ValType = 2
    datetime_type: ty.ValType = 3
    boolean_type: ty.ValType = 4
    nominal_type: ty.ValType = 5
    ordinal_type: ty.ValType = 6
    bounded_int_type: ty.ValType = 7
    bounded_real_type: ty.ValType = 8
    bounded_datetime_type: ty.ValType = 9
    external_type: ty.ValType = 10

    if val_type == integer_type:
        if check_int(value):
            return True, ''
        else:
            return False, 'Type Error: not an integer'
    elif val_type == real_type:
        if check_real(value):
            return True, ''
        else:
            return False, 'Type Error: not a real'
    elif val_type == string_type:
        if check_string(value):
            return True, ''
        else:
            return False, 'Type Error: not a string'
    elif val_type == datetime_type:
        if check_datetime(value):
            return True, ''
        else:
            return False, 'Type Error: not a datetime'
    elif val_type == boolean_type:
        if ok_bool_val(value):
            return True, ''
        else:
            return False, 'Type Error: not a boolean'
    elif val_type == nominal_type:
        if check_int(value) and cbv.check_category_id(category_ids(bounds), measurement_type, value):
            return True, ''
        else:
            return False, 'Type Error: not a valid nominal id'
    elif val_type == ordinal_type:
        if check_int(value) and cbv.check_category_id(category_ids(bounds), measurement_type, value):
            return True, ''
        else:
            return False, 'Type Error: not a valid ordinal id'
    elif val_type == bounded_int_type:
        if check_int(value) and cbv.check_bounded_int_in_bounds(int_bounds(bounds), measurement_type, value):
            return True, ''
        else:
            return False, 'Type Error: bounded integer out of range'
    elif val_type == bounded_real_type:
        if check_real(value) and cbv.check_bounded_real_in_bounds(real_bounds(bounds), measurement_type, value):
            return True, ''
        else:
            return False, 'Type Error: bounded real out of range'
    elif val_type == bounded_datetime_type:
        if check_datetime(value) and cbv.check_bounded_datetime_in_bounds(datetime_bounds(bounds),
                                                                          measurement_type, value):
            return True, ''
        else:
            return False, 'Type Error: bounded datetime out of range'
    elif val_type == external_type:
        if check_string(value):
            return True, ''
        else:
            return False, 'Type Error: not a string'
    else:
        return False, f'[Error in valType ({val_type}) in insert_measurement_group.]'
