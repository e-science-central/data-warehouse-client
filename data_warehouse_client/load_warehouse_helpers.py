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
import functools
from datetime import datetime
import unidecode
from typing import Tuple, List, Any, Callable
import itertools

from data_warehouse_client import type_checks
<<<<<<< Updated upstream
from data_warehouse_client.type_definitions import MeasurementGroup, DataToLoad, LoadHelperResult, LoaderResult
=======
from data_warehouse_client.type_definitions import (MeasurementGroup, DataToLoad, LoadHelperResult, LoaderResult,
                                                    MeasurementType, OptionalValue, ValType)
from data_warehouse_client.import_with_checks import get_field
>>>>>>> Stashed changes


def process_message_group(mg_triples):
    """
    takes the result of attempting to load each field in a message group and processes it
    :param mg_triples: [(Success?, [(measurement_type, valtype, value)], Error Message)]
    :return: Success?, [(measurement_type, valtype, value)], [error messages]
    """
    success_index = 0
    triple_index = 1
    error_message_index = 2
    oks = list(map(lambda r: r[success_index], mg_triples))     # get a list of the bools indicating success
    triples = list(map(lambda r: r[triple_index], mg_triples))  # get a list of the triples
    if functools.reduce(lambda x, y: x and y, oks):             # if all value in message group are correct....
        return True, sum(triples, []), []                       # return Success, the triples, empty error message list
    else:                                                       # there was a problem with >0 fields in message group
        # remove empty strings from error messages
        error_messages = list(filter(lambda s: s != "", list(map(lambda r: r[error_message_index], mg_triples))))
        return False, [], error_messages  # return Failure, no triples and the list of error messages


def missing_mandatory_type_error_message(jfield, measurement_type, data):
    """
    form error message for missing mandatory types
    :param jfield: the field name
    :param measurement_type: the measurement type
    :param data: the data in which the field is missing
    :return: error message
    """
    return f'Missing mandatory field {jfield} (measurement type {measurement_type}) in data: {data}'


def wrong_type_error_message(jfield, measurement_type, data, val_type):
    """
    form error message for missing mandatory types
    :param jfield: the field name
    :param measurement_type: the measurement type
    :param data: the data in which the field is missing
    :param val_type: the correct type of the field
    :return: error message
    """
    return f'Wrong type for {jfield} (measurement type {measurement_type}) in data {data};' +\
           f' it should be a {type_names(val_type)} (value type {val_type})'


def type_names(val_type):
    """
    return the name of a type represented by a number
    :param val_type: int representing a type in the warehouse
    :return: name
    """
    if val_type in [0, 5, 6, 7]:  # 5 and 6 are there for ordinals and nominals created from id
        return "integer"
    elif val_type in [1, 8]:
        return "real"
    elif val_type in [3, 9]:
        return "datetime"
    elif val_type == 4:
        return "boolean"
    else:
        return "string"


def convert_epoch_in_ms_to_string(timestamp_in_ms):
    """
    converts timestamp in ms epoch format to string
    :param timestamp_in_ms: epoch timestamp in ms
    :return: well_formed, date time string
    """
    if type_checks.check_int(timestamp_in_ms):
        timestamp_in_sec = int(timestamp_in_ms)//1000
        try:
            time_val = datetime.fromtimestamp(timestamp_in_sec)
            return True, str(time_val)
        except Exception:
            return False, None
    else:
        return False, None


def convert_posix_timestamp_to_string(val):
    """
    converts timestamp in ms epoch format to string
    :param val: posix timestamp
    :return: well_formed, date time string
    """
    return True, val.replace('T', ' ')


def get_and_check_value(measurement_type, val_type, data, jfield, optional):
    """
    check if a value exists, and if so its type
    :param measurement_type: measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :param optional: if the field is optional
    :return: (field exists,well typed, value, error_message)
    """
    error_message = ""
    val = data.get(jfield)
    if val is None:
        exists = False
        well_typed = False  # default
    elif val == "":
        exists = False
        well_typed = False  # default
    else:
        exists = True
        well_typed = type_checks.type_check(val, val_type)
    if (not optional) and (not exists):
        error_message = missing_mandatory_type_error_message(jfield, measurement_type, data)
    elif exists and (not well_typed):
        error_message = wrong_type_error_message(jfield, measurement_type, data, val_type)
    return exists, well_typed, val, error_message


def mk_basic_field(measurement_type, val_type, data, jfield):
    """
    Make triple (measurement_ttpe, valtype, value for the jfield in the data)
    :param measurement_type: measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :return: Error free, (measurement_type, valtype, value for the jfield in the data), error_message
    """
    (exists, well_formed, val, error_message) = get_and_check_value(measurement_type, val_type, data, jfield, False)
    if exists and well_formed:
        return True, [(measurement_type, val_type, val)], ""
    else:
        return False, [], error_message


def mk_optional_basic_field(measurement_type, val_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data:                json that may contain the jfield
    :param jfield:              the name of the field
    :return                     (Error free, if the field exists then a list is returned holding the appropriate entry;
                                if the field doesn't exist then an empty list is returned, error message)
    """
    (exists, well_formed, val, error_message) = get_and_check_value(measurement_type, val_type, data, jfield, True)
    if exists and well_formed:
        return True, [(measurement_type, val_type, val)], ""  # jfield is present in the json and no errors
    elif exists and not well_formed:  # jfield is present but there are errors
        return False, [], error_message
    else:
        return True, [], ""  # Field doesn't exist, which is OK as this is an optional field


def mk_int(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for an integer
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 0, data, jfield)


def mk_optional_int(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     if the field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned
        """
    return mk_optional_basic_field(measurement_type, 0, data, jfield)


def mk_bounded_int(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a bounded int
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 7, data, jfield)


def mk_optional_bounded_int(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     if the field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned
        """
    return mk_optional_basic_field(measurement_type, 7, data, jfield)


def mk_real(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a real
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 1, data, jfield)


def mk_optional_real(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     if the field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned
        """
    return mk_optional_basic_field(measurement_type, 1, data, jfield)


def mk_bounded_real(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a bounded int
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 8, data, jfield)


def mk_optional_bounded_real(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     if the field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned
        """
    return mk_optional_basic_field(measurement_type, 8, data, jfield)


def mk_string(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a string
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 2, data, jfield)


def mk_optional_string(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_type, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return if the field exists then a list is returned holding the appropriate entry
            if the field doesn't exist then an empty list is returned
    """
    return mk_optional_basic_field(measurement_type, 2, data, jfield)


def mk_unidecoded_string(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a string - remove accents
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    error_free, result, error_message = mk_basic_field(measurement_type, 2, data, jfield)
    if error_free:   # a result value exists
        measurement_type, val_type, val = result[0]  # pick the result triple from the list
        unidecoded_val = unidecode.unidecode(val)
        return error_free, [(measurement_type, val_type, unidecoded_val)], error_message
    else:
        return error_free, result, error_message


def mk_optional_unidecoded_string(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_type, valtype, value for the jfield in the data)]
    Remove accents
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return if the field exists then a list is returned holding the appropriate entry
            if the field doesn't exist then an empty list is returned
    """
    error_free, result, error_message = mk_optional_basic_field(measurement_type, 2, data, jfield)
    if error_free and (len(result) == 1):   # There is no error, and the optional string exists
        measurement_type, val_type, val = result[0]  # pick the result triple from the list
        unidecoded_val = unidecode.unidecode(val)
        return error_free, [(measurement_type, val_type, unidecoded_val)], error_message
    else:
        return error_free, result, error_message


def mk_datetime(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a datetime represented as a string
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 3, data, jfield)


def mk_optional_datetime(measurement_type, data, jfield):
    """
    Used for an optional datetime represented as a string
    If the jfield exists in the data then return [(measurement_type, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return if the field exists then a list is returned holding the appropriate entry
            if the field doesn't exist then an empty list is returned
    """
    return mk_optional_basic_field(measurement_type, 3, data, jfield)


def get_and_check_datetime_from_epoch_ms(data, jfield):
    """
    check if a value exists, and if so convert it from an epoch (in ms) to a datetime string
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :return: (field exists, well typed, value)
    """
    val = data.get(jfield)
    if val is None:
        exists = False
        well_typed = False  # default
        return exists, well_typed, ""
    elif val == "":
        exists = False
        well_typed = False  # default
        return exists, well_typed, ""
    else:
        exists = True
        well_typed, str_val = convert_epoch_in_ms_to_string(val)
        return exists, well_typed, str_val


def mk_datetime_from_epoch_in_ms(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple from epoch time in ms
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    date_time_type = 3
    (exists, well_formed, val) = get_and_check_datetime_from_epoch_ms(data, jfield)
    if exists and well_formed:
        return True, [(measurement_type, date_time_type, val)], ""
    else:
        if not exists:
            return False, [], missing_mandatory_type_error_message(jfield, measurement_type, data)
        else:  # must exist but not be well-formed
            return False, [], wrong_type_error_message(jfield, measurement_type, data, date_time_type)


def get_and_check_datetime_from_posix_timestamp(data, jfield):
    """
    check if a value exists, and if so convert it from a posix timestamp to a datetime string
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :return: (field exists, well typed, value)
    """
    val = data.get(jfield)
    if val is None:
        exists = False
        well_typed = False  # default
        return exists, well_typed, ""
    elif val == "":
        exists = False
        well_typed = False  # default
        return exists, well_typed, ""
    else:
        exists = True
        well_typed, str_val = convert_posix_timestamp_to_string(val)
        return exists, well_typed, str_val


def mk_datetime_from_posix_timestamp(measurement_type, data, jfield):
    """
    Transform a CSV Posix timestamp field held in a string to a date/time format
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :return: string formatted in year-month-day hrs:mins:sec
    """
    date_time_type = 3
    (exists, well_formed, val) = get_and_check_datetime_from_posix_timestamp(data, jfield)
    if exists and well_formed:
        return True, [(measurement_type, date_time_type, val)], ""
    else:
        if not exists:
            return False, [], missing_mandatory_type_error_message(jfield, measurement_type, data)
        else:  # must exist but not be well-formed
            return False, [],  wrong_type_error_message(jfield, measurement_type, data, date_time_type)


def mk_optional_datetime_from_epoch_in_ms(measurement_type, data, jfield):
    """
    Used when from epoch time in ms
    If the jfield exists in the data then return [(measurement_type, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return if the field exists then a list is returned holding the appropriate entry
            if the field doesn't exist then an empty list is returned
    """
    date_time_type = 3
    (exists, well_formed, val) = get_and_check_datetime_from_epoch_ms(data, jfield)
    if exists and well_formed:
        return True, [(measurement_type, date_time_type, val)], ""  # jfield is present in the json and no errors
    elif exists and not well_formed:  # jfield is present but there are errors
        return False, [], wrong_type_error_message(jfield, measurement_type, data, date_time_type)
    else:
        return True, [], ""  # Field doesn't exist, which is OK as this is an optional field


def mk_boolean(measurement_type, data, jfield):
    """
    Transform a boolean represented as a 'T' or 'F' string into boolean field with 0 for False or 1 for True
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    (exists, well_formed, val, error_messsage) = get_and_check_value(measurement_type, 4, data, jfield, False)
    # val_type is set to 2 for checking as the field is expected to be a string ("T" or "Y") or ("F" or "N")
    if exists and well_formed:
        if val in ['0', 'N', 'F', 0]:
            val01 = mk_bool(False)
        else:
            val01 = mk_bool(True)  # must be 'Y' or 'T' or '1' or 1
        return True, [(measurement_type, 4, val01)], ""
    else:
        return False, [], error_messsage


def mk_optional_boolean(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     if the field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned
        """
    (exists, well_formed, val, error_message) = get_and_check_value(measurement_type, 4, data, jfield, True)
    # val_type is set to 2 for checking as the field is expected to be a string "T" or "F"
    if exists and well_formed:
        if val in ['0', 'N', 'F', 0]:
            val01 = mk_bool(False)
        else:
            val01 = mk_bool(True)  # must be 'Y' or 'T' or '1' or 1
        return True, [(measurement_type, 4, val01)], ""
    elif exists and not well_formed:
        return False, [], error_message
    else:
        return True, [], ""  # it's optional so OK if the field is not found


def mk_category_from_dict(cat_name, cat_dict, measurement_type):
    """
    Returns the category id from the category name.
    :param cat_name: the category name from the category table
    :param cat_dict: a directory with the category names as keys, and the categoryid as the values
    :param measurement_type: the measurement type (only used for debugging)
    :return Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    val = cat_dict.get(cat_name)
    if val is None:
        return False, val, f'\"{cat_name}\" is not in the dictionary for measurement type {measurement_type}'
    else:
        return True, val, ""


def mk_category_field(measurement_type, val_type, data, jfield, cat_dict):
    """
    make a category triple (measurement_type, value type, value)
    :param measurement_type: the id of the measurementtype that holds the value
    :param val_type: the type of the value to be stored: it is either 5 (nominal) or 6 (ordinal)
    :param data: the json structure
    :param jfield: the name of the field
    :param cat_dict: the dictionary for this category
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    #  use val_type of 2 as string expected
    (exists, well_formed, val, error_message) = get_and_check_value(measurement_type, 2, data, jfield, False)
    if exists and well_formed:
        (ok, cat, category_error_message) = mk_category_from_dict(val, cat_dict, measurement_type)
        if ok:
            return True, [(measurement_type, val_type, cat)], ""
        else:
            return False, [], category_error_message
    else:
        return False, [], error_message


def mk_optional_category_field(measurement_type, val_type, data, jfield, cat_dict):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type: the id of the measurementtype that holds the value if it exists
    :param val_type: the type of the value to be stored: it is either 5 (nominal) or 6 (ordinal)
    :param data: the json structure
    :param jfield: the name of the optional field
    :param cat_dict: the dictionary for this category
    :return: if the field exists then a list is returned holding the appropriate entry
             if the field doesn't exist then an empty list is returned
    """
    #  use val_type of 2 as string expected
    (exists, well_formed, val, error_message) = get_and_check_value(measurement_type, 2, data, jfield, True)
    if exists and well_formed:
        (ok, cat, category_error_message) = mk_category_from_dict(val, cat_dict, measurement_type)
        if ok:
            return True, [(measurement_type, val_type, cat)], ""
        else:
            return False, [], category_error_message
    elif exists and not well_formed:
        return False, [], error_message
    else:
        return True, [], ""  # it's optional, so it's OK that the field is missing


def mk_nominal(measurement_type, data, jfield, cat_dict):
    """
    make a nominal triple (measurement_type, 5, value)
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param cat_dict: the dictionary for this category
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_category_field(measurement_type, 5, data, jfield, cat_dict)


def mk_ordinal(measurement_type, data, jfield, cat_dict):
    """
    make an ordinal triple (measurement_type, 6, value)
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param cat_dict: the dictionary for this category
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_category_field(measurement_type, 6, data, jfield, cat_dict)


def mk_nominal_from_id(measurement_type, data, jfield):
    """
    make a nominal triple (measurement_type, 5, value) where the id is stored in the field
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 5, data, jfield)


def mk_ordinal_from_id(measurement_type, data, jfield):
    """
    make an ordinal triple (measurement_type, 6, value) where the id is stored in the field
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    return mk_basic_field(measurement_type, 6, data, jfield)


def mk_categorical_from_id_with_id_check(measurement_type, data, jfield, id_list, val_type, optional: bool):
    """
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param id_list: list containing the acceptable value of the id
    :param val_type: 5 for nominal, 6 for ordinal
    :param optional: boolean that is True if the id is optional
    :return: Success?, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    if optional:
        success, result, error_message = mk_optional_basic_field(measurement_type, val_type, data, jfield)
    else:
        success, result, error_message = mk_basic_field(measurement_type, val_type, data, jfield)
    if success and result != []:  # need to check if value returned is in bounds
        (measurement_type_returned, val_type_returned, value) = result[0]  # pick the triple from the singleton list
        if value in id_list:
            return True, result, ""  # acceptable id value
        else:  # id is out of range
            return False, [], f'Category id error for {jfield} (measurement type {measurement_type}) in data: {data}'
    else:  # in all other cases just return without a check (as no result was returned)
        return success, result, error_message


def mk_nominal_from_id_with_id_check(measurement_type, data, jfield, id_list):
    """
    make a nominal triple (measurement_type, 5, value) where the id is stored in the jfield
    Check it's in range
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param id_list: list containing the acceptable value of the id
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    nominal_valtype: int = 5
    return mk_categorical_from_id_with_id_check(measurement_type, data, jfield, id_list, nominal_valtype, False)


def mk_optional_nominal_from_id_with_id_check(measurement_type, data, jfield, id_list):
    """
    make a nominal triple (measurement_type, 5, value) where the id is stored in the jfield
    Check it's in range
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param id_list: list containing the acceptable value of the id
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    nominal_valtype: int = 5
    return mk_categorical_from_id_with_id_check(measurement_type, data, jfield, id_list, nominal_valtype, True)


def mk_ordinal_from_id_with_id_check(measurement_type, data, jfield, id_list):
    """
    make a nominal triple (measurement_type, 6, value) where the id is stored in the jfield
    Check it's in range
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param id_list: list containing the acceptable value of the id
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    ordinal_valtype: int = 6
    return mk_categorical_from_id_with_id_check(measurement_type, data, jfield, id_list, ordinal_valtype, False)


def mk_optional_ordinal_from_id_with_id_check(measurement_type, data, jfield, id_list):
    """
    make an odinal triple (measurement_type, 6, value) where the id is stored in the jfield
    Check it's in range
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param id_list: list containing the acceptable value of the id
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    ordinal_valtype: int = 6
    return mk_categorical_from_id_with_id_check(measurement_type, data, jfield, id_list, ordinal_valtype, True)


def mk_optional_nominal_from_dict(measurement_type, data, jfield, cat_dict):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type: the id of the measurementtype that hold the value if it exists
    :param data: the json structure
    :param jfield: the name of the optional field
    :param cat_dict: the dictionary for this category
    :return: if the field exists then a list is returned holding the appropriate entry
             if the field doesn't exist then an empty list is returned
    """
    return mk_optional_category_field(measurement_type, 5, data, jfield, cat_dict)


def mk_optional_nominal_from_id(measurement_type, data, jfield):
    """
    make a nominal triple (measurement_type, 6, value) where the id is stored in the field if the field isn't empty
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: if the field exists then a list is returned holding the appropriate entry;
             if the field doesn't exist then an empty list is returned
    """
    return mk_optional_basic_field(measurement_type, 5, data, jfield)


def mk_optional_ordinal_from_dict(measurement_type, data, jfield, cat_dict):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type: the id of the measurementtype that holds the value if it exists
    :param data: the json structure
    :param jfield: the name of the optional field
    :param cat_dict: the dictionary for this category
    :return: if the field exists then a list is returned holding the appropriate entry
             if the field doesn't exist then an empty list is returned
    """
    return mk_optional_category_field(measurement_type, 6, data, jfield, cat_dict)


def mk_optional_ordinal_from_id(measurement_type, data, jfield):
    """
    make an ordinal triple (measurement_type, 5, value) where the id is stored in the field if the field isn't empty
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: if the field exists then a list is returned holding the appropriate entry;
             if the field doesn't exist then an empty list is returned
    """
    return mk_optional_basic_field(measurement_type, 6, data, jfield)


def mk_bool_string(bool_val):
    """
    represent a boolean as a string ready to be stored in the db as an integer value
    :param bool_val: True or False
    :return: '0' or '1'
    """
    if bool_val:
        return '1'
    else:
        return '0'


def mk_bool(bool_val: bool) -> int:
    """
    COnvert a boolean value to an integer ready to be inserted into the measurement table
    :param bool_val: boolean
    :return: integer (0 = False, 1 = True)
    """
    if bool_val:
        return 1
    else:
        return 0


def split_enum(measurement_types, data, jfield, valuelist):
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type.
    :param measurement_types: the list of measurement_types of the measurements into which the booleans are to be stored
    :param data: the json structure
    :param jfield: the name of the field
    :param valuelist: the list of values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :return The list of (measurement_type,valType,value) triples that are used by
                    insertMeasurementGroup to add the measurements
    """
    res = []
    values = data.get(jfield)  # try to read the the list of values
    if values is None:
        exists = False
    elif values == "":
        exists = False
    else:
        exists = True
    if exists:
        for (measurement_type, value) in zip(measurement_types, valuelist):
            res = res + [(measurement_type, 4, mk_bool(value in values))]  # the 4 is because the type is boolean
        return True, res, ""
    else:
        return False, [], f'Missing Mandatory ENUM field {jfield} for measurement_types {measurement_types} in {data}'


def split_optional_enum(measurement_types, data, jfield, valuelist):
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type.
    :param measurement_types: the list of measurement_types of the measurements into which the booleans are to be stored
    :param data: the json structure
    :param jfield: the name of the field
    :param valuelist: the list of values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :return The list of (measurement_type,valType,value) triples that are used by
                    insertMeasurementGroup to add the measurements
    """
    res = []
    values = data.get(jfield)  # try to read the the list of values
    if values is None:
        exists = False
    elif values == "":
        exists = False
    else:
        exists = True
    if exists:
        for (measurement_type, value) in zip(measurement_types, valuelist):
            res = res + [(measurement_type, 4, mk_bool(value in values))]  # the 4 is because the type is boolean
        return True, res, ""
    else:
        return True, [], ""  # Field doesn't exist, which is OK as this is an optional field


def mk_bounded_datetime(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a bounded datetime
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free?, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    bounded_datetime_type = 9
    return mk_basic_field(measurement_type, bounded_datetime_type, data, jfield)


def mk_optional_bounded_datetime(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     Error free?, if field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned, Error message
        """
    bounded_datetime_type = 9
    return mk_optional_basic_field(measurement_type, bounded_datetime_type, data, jfield)


def mk_optional_external(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
        :param measurement_type:    measurement type of jfield in the data warehouse
        :param data:                json that may contain the jfield
        :param jfield:              the name of the field
        :return                     Error free?, if field exists then a list is returned holding the appropriate entry
                                    if the field doesn't exist then an empty list is returned, Error message
        """
    external_type = 10
    return mk_optional_basic_field(measurement_type, external_type, data, jfield)


def mk_external(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for an external reference
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    external_type = 10
    return mk_basic_field(measurement_type, external_type, data, jfield)


def concat(ls: List[List[Any]]) -> List[Any]:
    """
    Concatenate a list of lists
    :param ls:  list of lists
    :return: list
    """
    return list(itertools.chain.from_iterable(ls))


def load_a_list(data: DataToLoad,
                jfield: str,
                loader: Callable[[DataToLoad], LoaderResult],
                mg_id: MeasurementGroup,
                optional: bool) ->\
        List[Tuple[MeasurementGroup, List[LoadHelperResult]]]:
    """
    Load a list within data loaded into the
    :param data: data to load into warehouse - held in a Dictionary
    :param jfield: name of the field in the Dictionary
    :param loader: function to load the data
    :param mg_id: measurement group id - only used if the jfield is not found
    :param optional: True if the list is optional
    :return: list of measurement group ids and the values to load into them
    """
    list_val: List[DataToLoad] = data.get(jfield)   # extract the list field
    if list_val is None:  # missing field
        if optional:
            return []    # if optional then it's not an error
        else:   # there should have been a list, so flag an error
            return [(mg_id, [(False, [], f'Missing mandatory field {jfield} (list expected) in data: {data}')])]
    else:  # the list exists
        # create a single, combined list and ignore the optional fields (Time, Trial, Participant, Source)
        return concat(list(map(lambda e: loader(e)[0], list_val)))


def load_list(data: DataToLoad, jfield: str, loader: Callable[[DataToLoad], LoaderResult],
              mg_id: MeasurementGroup) -> List[Tuple[MeasurementGroup, List[LoadHelperResult]]]:
    """

    :param data: data to load into warehouse - held in a Dictionary
    :param jfield: name of the field in the Dictionary
    :param loader: function to load the data
    :param mg_id: measurement group id - only used if the jfield is not found
    :return: list of measurement group ids and the values to load into them
    """
    return load_a_list(data, jfield, loader, mg_id, False)


def load_optional_list(data: DataToLoad, jfield: str, loader, mg_id: MeasurementGroup) ->\
        List[Tuple[MeasurementGroup, List[LoadHelperResult]]]:
    """

    :param data: data to load into warehouse - held in a Dictionary
    :param jfield: name of the field in the Dictionary
    :param loader: function to load the data
    :param mg_id: measurement group id - only used if the jfield is not found
    :return: list of measurement group ids and the values to load into them
    """
    return load_a_list(data, jfield, loader, mg_id, True)


def get_converter_fn(event_type, mapper_dict):
    """
    map from the event_type to the mapper function and message group
    :param event_type: the e-Science Central event type
    :param mapper_dict: the dictionary that maps from the event_type to the mapper function and message group
    :return: (boolean indicating if the event_type is found, mapper function, measurement group
    """
    found = event_type in mapper_dict
    if found:
        info = mapper_dict[event_type]
        res = found, info["fn"], info["mg"]
    else:
        res = (found, None, None)
    return res
