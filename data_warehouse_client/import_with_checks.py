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

import type_definitions as ty
from typing import Tuple, List, Optional, Dict, Callable, Any
import type_checks
import itertools
import functools
from type_definitions import Bounds, ValueTriple


def process_measurement_group(mg_triples):
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


def process_measurement_groups(
        vals_to_load_in_mgs: List[Tuple[ty.MeasurementGroup, List[Tuple[bool, List[ValueTriple]]], List[str]]]) ->\
        Tuple[bool, List[Tuple[ty.MeasurementGroup, List[ValueTriple]]], List[str]]:
    """
    takes the result of attempting to load each field in a message group and processes it
    :param vals_to_load_in_mgs: [(measurement_group_id, [(Success, [(measurement_type, valtype, val)], [Error Mess])])]
    :return: (Success, [(measurement_group_id, [(measurement_type, valtype, value)])], [Error Mess])
    """
    successful: bool = True
    all_mgs_and_triples: List[Tuple[int, List[ValueTriple]]] = []
    all_error_messages: List[str] = []
    for (measurement_group_id, vals_to_load_in_mg) in vals_to_load_in_mgs:
        success, triples, error_messages = process_measurement_group(vals_to_load_in_mg)
        successful = successful and success
        all_mgs_and_triples = [(measurement_group_id, triples)] + all_mgs_and_triples
        all_error_messages = error_messages + all_error_messages

    combined_error_messages = list(filter(lambda s: s != "", all_error_messages))
    if successful:
        return True, all_mgs_and_triples, combined_error_messages
    else:
        return False, [], combined_error_messages


def concat(ls: List[List[Any]]) -> List[Any]:
    """
    Concatenate a list of lists
    :param ls:  list of lists
    :return: list
    """
    return list(itertools.chain.from_iterable(ls))


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


def missing_mandatory_type_error_message(jfield: str, measurement_type: ty.MeasurementType, data: ty.DataToLoad) -> str:
    """
    form error message for missing mandatory types
    :param jfield: the field name
    :param measurement_type: the measurement type
    :param data: the data in which the field is missing
    :return: error message
    """
    return f'Missing mandatory field {jfield} (measurement type {measurement_type}) in data: {data}'


def wrong_type_error_message(jfield: str, measurement_type: ty.MeasurementType,
                             data: ty.DataToLoad, val_type: ty.ValType, error_message: str) -> str:
    """
    form error message for missing mandatory types
    :param jfield: the field name
    :param measurement_type: the measurement type
    :param data: the data in which the field is missing
    :param val_type: the correct type of the field
    :param error_message: detailed error message
    :return: error message
    """
    return f'Wrong type for {jfield} (measurement type {measurement_type}) in data {data};' +\
           f' it should be a {type_names(val_type)} (value type {val_type}): {error_message}'


def get_category_id_from_value(val: str, measurement_type: ty.MeasurementType,
                               category_value_map: Dict[ty.MeasurementType, Dict[str, int]]) -> \
        Tuple[bool, Optional[int]]:
    """
    Use a category value to id map to find the id associated with a value
    :param val: category value
    :param measurement_type: measurement type id
    :param category_value_map: map from measurement type to value type to id
    :return found?, category id
    """
    mt_dict: Dict[str, int] = category_value_map.get(measurement_type)
    if mt_dict is None:
        return False, None
    else:
        cat_id: int = mt_dict.get(val)
        if cat_id is None:
            return False, None
        else:
            return True, cat_id


def type_names(val_type: ty.ValType) -> str:
    """
    return the name of a type represented by a number
    :param val_type: int representing a type in the warehouse
    :return: name
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
        return "integer"
    elif val_type in [real_type, bounded_real_type]:
        return "real"
    elif val_type in [datetime_type, bounded_datetime_type]:
        return "datetime"
    elif val_type == boolean_type:
        return "boolean"
    elif val_type in [string_type, external_type]:
        return "string"
    else:
        print('type-names error')


def get_field(data: ty.DataToLoad, jfield: str) -> Tuple[bool, ty.FieldValue]:
    """
    get the value of a field in a dictionary (if the field exists)
    :param data: dictionary containing the loaded data
    :param jfield: field name
    :return: field exists in the data?, value of the field if it exists
    """
    val = data.get(jfield)
    if val is None:
        exists = False
    elif val == "":
        exists = False
    else:
        exists = True
    return exists, val


def make_field(measurement_type: ty.MeasurementType, val_type: ty.ValType, data: ty.DataToLoad,
               jfield: str, optional: bool, bounds: Bounds = None) -> Tuple[bool, List[ValueTriple], str]:
    """
    check if a value exists, and if so its type
    :param measurement_type: measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :param optional: if the field is optional
    :param bounds: tuple holding bounds used for checking
    :return: Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    exists, val = get_field(data, jfield)  # try to read the field from the dictionary
    if exists:   # field exists
        well_typed, error_message = type_checks.check_value_type(val_type, val, measurement_type, bounds)
        if well_typed:
            return True, [(measurement_type, val_type, val)], ""
        else:
            return False, [], wrong_type_error_message(jfield, measurement_type, data, val_type, error_message)
    else:
        if optional:  # optional field
            return True, [], ""  # Field doesn't exist, which is OK as this is an optional field
        else:  # compulsary field is missing
            return False, [], missing_mandatory_type_error_message(jfield, measurement_type, data)


def load_int(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) ->\
        Tuple[bool, List[ValueTriple], str]:
    """
    Load an integer from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    integer_type: ty.ValType = 0
    return make_field(measurement_type, integer_type, data, jfield, False)


def load_optional_int(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) ->\
        Tuple[bool, List[ValueTriple], str]:
    """
    Load an optional integer from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    integer_type: ty.ValType = 0
    return make_field(measurement_type, integer_type, data, jfield, True)


def load_real(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) ->\
        Tuple[bool, List[ValueTriple], str]:
    """
    Load an real from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    real_type: ty.ValType = 1
    return make_field(measurement_type, real_type, data, jfield, False)


def load_optional_real(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load an optional real from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    real_type: ty.ValType = 1
    return make_field(measurement_type, real_type, data, jfield, True)


def load_string(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load a string from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    string_type: ty.ValType = 2
    return make_field(measurement_type, string_type, data, jfield, False)


def load_optional_string(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load an optional string from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    string_type: ty.ValType = 2
    return make_field(measurement_type, string_type, data, jfield, True)


def load_datetime(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load a datetime from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    datetime_type: ty.ValType = 3
    return make_field(measurement_type, datetime_type, data, jfield, False)


def load_optional_datetime(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load optional datetime from the dictionary holding the data
    :param measurement_type: measurement type id
    :param data: dictionary holding the data
    :param jfield: field name
    :return: error-free, [measurement type, value type, value], error
    """
    datetime_type: ty.ValType = 3
    return make_field(measurement_type, datetime_type, data, jfield, True)


def load_boolean(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load boolean from the dictionary holding the data (must be 0 or 1)
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    boolean_type: ty.ValType = 4
    return make_field(measurement_type, boolean_type, data, jfield, False)


def load_optional_boolean(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load optional boolean from the dictionary holding the data (must be 0 or 1)
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    boolean_type: ty.ValType = 4
    return make_field(measurement_type, boolean_type, data, jfield, True)


def load_nominal_from_id(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str, bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load a nominal represented by its id in the category table from the dictionary holding the data
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :param bounds: tuple holding bounds used for checking data
    :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    nominal_type: ty.ValType = 5
    return make_field(measurement_type, nominal_type, data, jfield, False, bounds)


def load_optional_nominal_from_id(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                                  bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load optional nominal represented by its id in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    nominal_type: ty.ValType = 5
    return make_field(measurement_type, nominal_type, data, jfield, True, bounds)


def load_ordinal_from_id(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                         bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load a ordinal represented by its id in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    ordinal_type: ty.ValType = 6
    return make_field(measurement_type, ordinal_type, data, jfield, False, bounds)


def load_optional_ordinal_from_id(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                                  bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load optional ordinal represented by its id in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    ordinal_type: ty.ValType = 6
    return make_field(measurement_type, ordinal_type, data, jfield, True, bounds)


def load_categorical_from_value(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                                val_type: int, optional: bool, bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load a categorical represented by its value in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param val_type: can be nominal or ordinal
     :param optional: is the field optional?
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    exists, val = get_field(data, jfield)  # try to read the field from the data
    if exists:  # field exists
        category_value_map = type_checks.category_values(bounds)
        value_exists, cat_id = get_category_id_from_value(val, measurement_type, category_value_map)
        if value_exists:   # the value exists in the category table
            return True, [(measurement_type, val_type, cat_id)], ""
        else:
            return False, [], wrong_type_error_message(jfield, measurement_type, data, val_type,
                                                       "Value in category table doesn't exist for measurement type")
    else:
        if optional:  # optional field
            return True, [], ""  # Field doesn't exist, which is OK as this is an optional field
        else:  # compulsary field is missing
            return False, [], missing_mandatory_type_error_message(jfield, measurement_type, data)


def load_nominal_from_value(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                            bounds: Bounds) -> Tuple[bool, List[ValueTriple], str]:
    """
     Load a nominal represented by its value in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    nominal_type: ty.ValType = 5
    return load_categorical_from_value(measurement_type, data, jfield, nominal_type, False, bounds)


def load_optional_nominal_from_value(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                                     bounds: Bounds) -> Tuple[bool, List[ValueTriple], str]:
    """
     Load optional nominal represented by its value in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    nominal_type: ty.ValType = 5
    return load_categorical_from_value(measurement_type, data, jfield, nominal_type, True, bounds)


def load_ordinal_from_value(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                            bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load ordinal represented by its value in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    ordinal_type: ty.ValType = 6
    return load_categorical_from_value(measurement_type, data, jfield, ordinal_type, False, bounds)


def load_optional_ordinal_from_value(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                                     bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load optional ordinal represented by its value in the category table from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    ordinal_type: ty.ValType = 6
    return load_categorical_from_value(measurement_type, data, jfield, ordinal_type, True, bounds)


def load_bounded_int(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                     bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load  bounded int from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    bounded_int_type: ty.ValType = 7
    return make_field(measurement_type, bounded_int_type, data, jfield, False, bounds)


def load_optional_bounded_int(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                              bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
    Load optional bounded int from the dictionary holding the data
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :param bounds: tuple holding bounds used for checking data
    :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
    """
    bounded_int_type: ty.ValType = 7
    return make_field(measurement_type, bounded_int_type, data, jfield, True, bounds)


def load_bounded_real(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str, bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
     Load bounded real from the dictionary holding the data
     :param measurement_type:    measurement type of jfield in the data warehouse
     :param data:   json that may contain the jfield
     :param jfield: the name of the field
     :param bounds: tuple holding bounds used for checking data
     :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
     """
    bounded_real_type: ty.ValType = 8
    return make_field(measurement_type, bounded_real_type, data, jfield, False, bounds)


def load_optional_bounded_real(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                               bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
      Load optional bounded real from the dictionary holding the data
      :param measurement_type:    measurement type of jfield in the data warehouse
      :param data:   json that may contain the jfield
      :param jfield: the name of the field
      :param bounds: tuple holding bounds used for checking data
      :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
      """
    bounded_real_type: ty.ValType = 8
    return make_field(measurement_type, bounded_real_type, data, jfield, True, bounds)


def load_bounded_datetime(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str, bounds: Bounds) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
      Load bounded datetime from the dictionary holding the data
      :param measurement_type:    measurement type of jfield in the data warehouse
      :param data:   json that may contain the jfield
      :param jfield: the name of the field
      :param bounds: tuple holding bounds used for checking data
      :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
      """
    bounded_datetime_type: ty.ValType = 9
    return make_field(measurement_type, bounded_datetime_type, data, jfield, False, bounds)


def load_optional_bounded_datetime(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str,
                                   bounds: Bounds) -> Tuple[bool, List[ValueTriple], str]:
    """
      Load optional bounded datetime from the dictionary holding the data
      :param measurement_type:    measurement type of jfield in the data warehouse
      :param data:   json that may contain the jfield
      :param jfield: the name of the field
      :param bounds: tuple holding bounds used for checking data
      :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
      """
    bounded_datetime_type: ty.ValType = 9
    return make_field(measurement_type, bounded_datetime_type, data, jfield, True, bounds)


def load_external(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
      Load external from the dictionary holding the data
      :param measurement_type:    measurement type of jfield in the data warehouse
      :param data:   json that may contain the jfield
      :param jfield: the name of the field
      :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
      """
    external_type: ty.ValType = 10
    return make_field(measurement_type, external_type, data, jfield, False)


def load_optional_external(measurement_type: ty.MeasurementType, data: ty.DataToLoad, jfield: str) -> \
        Tuple[bool, List[ValueTriple], str]:
    """
       Load optional external from the dictionary holding the data
       :param measurement_type:    measurement type of jfield in the data warehouse
       :param data:   json that may contain the jfield
       :param jfield: the name of the field
       :return : Error free, [(measurement_type, valtype, value for the jfield in the data)], error_message
       """
    external_type: ty.ValType = 10
    return make_field(measurement_type, external_type, data, jfield, True)


def load_a_set(measurement_types: List[ty.MeasurementType], data: ty.DataToLoad, jfield: str, value_list: List[str],
               optional: bool) -> Tuple[bool, List[ValueTriple], str]:
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type.
    :param measurement_types: the list of measurement_types of the measurements into which the booleans are to be stored
    :param data: the json structure
    :param jfield: the name of the field
    :param value_list: the list of values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :param optional: is set optional?
    :return Success, The list of (measurement_type,valType,value) triples that are used by
                     insertMeasurementGroup to add the measurements, error message
    """
    boolean_type: ty.ValType = 4
    values = data.get(jfield)  # try to read the the list of values
    if values is None:
        exists = False
    elif values == "":
        exists = False
    else:
        exists = True
    if exists:  # create the boolean triples from the set values
        # check all the values are strings and set of valid values
        invalid_vals = list(filter(lambda v: not (type_checks.check_string(v) and (v in value_list)), values))
        if len(invalid_vals) > 0:  # there's at least 1 invalid value
            return False, [], \
                f'Invalid ENUM value(s) {invalid_vals} in {jfield} for measurement_types {measurement_types} in {data}'
        else:
            res = list(map(lambda ps: (ps[0], boolean_type, mk_bool(ps[1] in values)),
                           zip(measurement_types, value_list)))
            return True, res, ""
    else:
        if optional:
            return True, [], ""  # Field doesn't exist, which is OK as this is an optional field
        else:
            return False, [],\
                f'Missing Mandatory ENUM field {jfield} for measurement_types {measurement_types} in {data}'


def load_set(measurement_types: List[ty.MeasurementType], data: ty.DataToLoad, jfield: str,
             value_list: List[str]) -> Tuple[bool, List[ValueTriple], str]:
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type.
    :param measurement_types: the list of measurement_types of the measurements into which the booleans are to be stored
    :param data: the json structure
    :param jfield: the name of the field
    :param value_list: the list of values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :return Success, The list of (measurement_type,valType,value) triples that are used by
                     insertMeasurementGroup to add the measurements, Error Message
    """
    return load_a_set(measurement_types, data, jfield, value_list, False)


def load_optional_set(measurement_types: List[ty.MeasurementType], data: ty.DataToLoad, jfield: str,
                      value_list: List[str]) -> Tuple[bool, List[ValueTriple], str]:
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type. In this case the set is optional
    :param measurement_types: the list of measurement_types of the measurements into which the booleans are to be stored
    :param data: the json structure
    :param jfield: the name of the field
    :param value_list: the list of values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :return The list of (measurement_type,valType,value) triples that are used by
                    insertMeasurementGroup to add the measurements
    """
    return load_a_set(measurement_types, data, jfield, value_list, True)


def load_a_list(data: ty.DataToLoad,
                jfield: str,
                loader: ty.Loader,
                mg_id: ty.MeasurementGroup,
                optional: bool,
                bounds: Bounds
                ) -> List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]]:
    """
    Load a list of json data documents and create measurement group instances
    :param data: data to load into warehouse - held in a Dictionary
    :param jfield: name of the field in the Dictionary
    :param loader: function to load the data
    :param mg_id: measurement group id - only used if the jfield is not found
    :param optional: True if the list is optional
    :param bounds: tuple holding bounds used for checking data
    :return: list of measurement group ids and the values to load into them
    """
    list_val: List[ty.DataToLoad] = data.get(jfield)   # extract the list field
    if list_val is None:  # missing field
        if optional:
            return []    # if optional then it's not an error
        else:   # there should have been a list, so flag an error
            return [(mg_id, [(False, [], f'Missing mandatory field {jfield} (list expected) in data: {data}')])]
    else:  # the list exists
        # create a single, combined list and ignore the optional fields (Time, Trial, Participant, Source)
        return concat(list(map(lambda e: loader(e, bounds)[0], list_val)))


def load_list(data: ty.DataToLoad, jfield: str, loader: Callable[[ty.DataToLoad], ty.LoaderResult],
              mg_id: ty.MeasurementGroup, bounds: Bounds) -> \
        List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]]:
    """
    load mandatory list
    :param data: data to load into warehouse - held in a Dictionary
    :param jfield: name of the field in the Dictionary
    :param loader: function to load the data
    :param mg_id: measurement group id - only used if the jfield is not found
    :param bounds: dictionary holding integer bounds
    :return: list of measurement group ids and the values to load into them
    """
    return load_a_list(data, jfield, loader, mg_id, False, bounds)


def load_optional_list(data: ty.DataToLoad, jfield: str, loader: Callable[[ty.DataToLoad], ty.LoaderResult],
                       mg_id: ty.MeasurementGroup, bounds: Bounds) -> \
        List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]]:
    """
    load optional list
    :param data: data to load into warehouse - held in a Dictionary
    :param jfield: name of the field in the Dictionary
    :param loader: function to load the data
    :param mg_id: measurement group id - only used if the jfield is not found
    :param bounds: tuple holding bounds used for checking data
    :return: list of measurement group ids and the values to load into them
    """
    return load_a_list(data, jfield, loader, mg_id, True, bounds)
