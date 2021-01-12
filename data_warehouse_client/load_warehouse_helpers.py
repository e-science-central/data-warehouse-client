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


def process_message_group(mg_triples):
    oks = list(map(lambda r: r[0], mg_triples))
    triples = list(map(lambda r: r[1], mg_triples))
    if functools.reduce(lambda x, y: x and y, oks):
        return True, sum(triples, [])
    else:
        return False, []


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
    elif val_type == 3:
        return "datetime"
    elif val_type == 4:
        return "boolean"
    else:
        return "string"


def check_int(string):
    """
    Check if a string represents an integer
    :param string: string
    :return: True if the string represents an integer
    """
    try:
        val = int(string)
        return True
    except ValueError:
        return False


def check_real(string):
    """
    Check if a string represents a real
    :param string: string
    :return: True if the string represents a real
    """
    try:
        val = float(string)
        return True
    except ValueError:
        return False


def check_datetime(string):
    """
    Check if a string represents a datetime
    :param string: string
    :return: True if the string represents a datetime
    """
    return True  # need to add checking later


def check_boolean(string):
    """
    Check if a string represents a boolean
    :param string: 
    :return: True if the string represents a boolean
    """
    return string in ['T', 'Y', 'F', 'N', '0', '1']


def type_check(val, val_type):
    """
    Check the type of a value retrieved from a field
    :param val: value
    :param val_type: the type is should be
    :return: True if the value has the right type, False otherwise
    """
    if val_type in [0, 5, 6, 7]:  # 5 and 6 are there for ordinals and nominals created from id
        well_typed = check_int(val)
    elif val_type in [1, 8]:
        well_typed = check_real(val)
    elif val_type == 3:
        well_typed = check_datetime(val)
    elif val_type == 4:
        well_typed = check_boolean(val)
    else:
        well_typed = True  # everything else is a string
    return well_typed


def get_and_check_value(measurement_type, val_type, data, jfield, optional):
    """
    check if a value exists, and if so its type
    :param measurement_type: measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :param optional: if the field is optional
    :return: (field exists,well typed, value)
    """
    val = data.get(jfield)
    if val is None:
        exists = False
        well_typed = False  # default
    elif val == "":
        exists = False
        well_typed = False  # default
    else:
        exists = True
        well_typed = type_check(val, val_type)
    if (not optional) and (not exists):
        print(f'Missing mandatory field {jfield} (measurement type {measurement_type}) in data: {data}')
    elif exists and (not well_typed):
        print(f'Wrong type for {jfield} (measurement type {measurement_type}): it should be a {type_names(val_type)} (value type {val_type})')
    return exists, well_typed, val


def mk_basic_field(measurement_type, val_type, data, jfield):
    """
    Make triple (measurement_ttpe, valtype, value for the jfield in the data)
    :param measurement_type: measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :return: (Error free, (measurement_ttpe, valtype, value for the jfield in the data))
    """
    (exists, well_formed, val) = get_and_check_value(measurement_type, val_type, data, jfield, False)
    if exists and well_formed:
        return True, [(measurement_type, val_type, val)]
    else:
        return False, []


def mk_optional_basic_field(measurement_type, val_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data:                json that may contain the jfield
    :param jfield:              the name of the field
    :return                     (Error free, if the field exists then a list is returned holding the appropriate entry;
                                if the field doesn't exist then an empty list is returned)
    """
    (exists, well_formed, val) = get_and_check_value(measurement_type, val_type, data, jfield, True)
    if exists and well_formed:
        return True, [(measurement_type, val_type, val)]  # jfield is present in the json and no errors
    elif exists and not well_formed:  # jfield is present but there are errors
        return False, []
    else:
        return True, []  # Field doesn't exist, which is OK as this is an optional field


def mk_int(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for an integer
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: (measurement_type, valtype, value for the jfield in the data) triple
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
    :return: (measurement_type, valtype, value for the jfield in the data) triple
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
    :return: (measurement_type, valtype, value for the jfield in the data) triple
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
    :return: (measurement_type, valtype, value for the jfield in the data) triple
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
    :return: (measurement_type, valtype, value for the jfield in the data) triple
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


def mk_datetime(measurement_type, data, jfield):
    """
    create a (measurement_type, valtype, value for the jfield in the data) triple for a string
    :param measurement_type: measurement type of jfield in the data warehouse
    :param data: json that may contain the jfield
    :param jfield: the name of the field
    :return: (measurement_type, valtype, value for the jfield in the data) triple
    """
    return mk_basic_field(measurement_type, 3, data, jfield)


def mk_optional_datetime(measurement_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_type, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return if the field exists then a list is returned holding the appropriate entry
            if the field doesn't exist then an empty list is returned
    """
    return mk_optional_basic_field(measurement_type, 3, data, jfield)


def mk_boolean(measurement_type, data, jfield):
    """
    Create a Transform a boolean represented as a 'T' or 'F' string into boolean field with 0 for False or 1 for True
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return : the field
    """
    (exists, well_formed, val) = get_and_check_value(measurement_type, 4, data, jfield, False)
    # val_type is set to 2 for checking as the field is expected to be a string ("T" or "Y") or ("F" or "N")
    if exists and well_formed:
        if val in ['0', 'N', 'F']:
            val01 = '0'
        else:
            val01 = '1'  # must be Y or T or 1
        return True, [(measurement_type, 4, val01)]
    else:
        return False, []


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
    (exists, well_formed, val) = get_and_check_value(measurement_type, 2, data, jfield, True)
    # val_type is set to 2 for checking as the field is expected to be a string "T" or "F"
    if exists and well_formed:
        if val in ['0', 'N', 'F']:
            val01 = '0'
        else:
            val01 = '1'  # must be Y or T or 1
        return True, [(measurement_type, 4, val01)]
    elif exists and not well_formed:
        return False, []
    else:
        return True, []  # it's optional so OK if the field is not found


def mk_category_from_dict(cat_name, cat_dict, measurement_type):
    """
    Returns the category id from the category name.
    :param cat_name: the category name from the category table
    :param cat_dict: a directory with the category names as keys, and the categoryid as the values
    :param measurement_type: the measurement type (only used for debugging)
    :return (no error, the categoryid of the category)
    """
    val = cat_dict.get(cat_name)
    if val is None:
        print(f'\"{cat_name}\" is not in the dictionary for measurement type {measurement_type}')
        return False, val
    else:
        return True, val


def mk_category_field(measurement_type, val_type, data, jfield, cat_dict):
    """
    make a category triple (measurement_type, value type, value)
    :param measurement_type: the id of the measurementtype that holds the value
    :param val_type: the type of the value to be stored: it is either 5 (nominal) or 6 (ordinal)
    :param data: the json structure
    :param jfield: the name of the field
    :param cat_dict: the dictionary for this category
    :return: (measurement_ttpe, valtype, value for the jfield in the data)
    """
    #  use val_type of 2 as string expected
    (exists, well_formed, val) = get_and_check_value(measurement_type, 2, data, jfield, False)
    if exists and well_formed:
        (ok, cat) = mk_category_from_dict(val, cat_dict, measurement_type)
        if ok:
            return True, [(measurement_type, val_type, cat)]
        else:
            return False, []
    else:
        return False, []


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
    (exists, well_formed, val) = get_and_check_value(measurement_type, 2, data, jfield, True)
    if exists and well_formed:
        (ok, cat) = mk_category_from_dict(val, cat_dict, measurement_type)
        if ok:
            return True, [(measurement_type, val_type, cat)]
        else:
            return False, []
    elif exists and not well_formed:
        return False, []
    else:
        return True, []  # it's optional, so it's OK is the field is missing


def mk_nominal(measurement_type, data, jfield, cat_dict):
    """
    make a nominal triple (measurement_type, 5, value)
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param cat_dict: the dictionary for this category
    :return: (measurement_ttpe, valtype, value for the jfield in the data)
    """
    return mk_category_field(measurement_type, 5, data, jfield, cat_dict)


def mk_ordinal(measurement_type, data, jfield, cat_dict):
    """
    make an ordinal triple (measurement_type, 6, value)
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :param cat_dict: the dictionary for this category
    :return: (measurement_ttpe, valtype, value for the jfield in the data)
    """
    return mk_category_field(measurement_type, 6, data, jfield, cat_dict)


def mk_nominal_from_id(measurement_type, data, jfield):
    """
    make a nominal triple (measurement_type, 5, value) where the id is stored in the field
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: (measurement_ttpe, valtype, value for the jfield in the data)
    """
    return mk_basic_field(measurement_type, 5, data, jfield)


def mk_ordinal_from_id(measurement_type, data, jfield):
    """
    make an ordinal triple (measurement_type, 6, value) where the id is stored in the field
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: (measurement_ttpe, valtype, value for the jfield in the data)
    """
    return mk_basic_field(measurement_type, 6, data, jfield)


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
            res = res + [(measurement_type, 4, int(value in values))]  # the 4 is because the type is boolean
            return True, res
    else:
        print(f'Missing Mandatory ENUM field {jfield} for measurement_types {measurement_types}')
        return False, []


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
