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


def mk_basic_field(measurement_type, val_type, data, jfield):
    """
    Make triple (measurement_ttpe, valtype, value for the jfield in the data)
    :param measurement_type: measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data: json that contains the jfield
    :param jfield: the name of the field
    :return: (measurement_ttpe, valtype, value for the jfield in the data)
    """
    val = data.get(jfield)
    if (val is None) or (val == ""):
        print(f'Missing Mandatory Field {jfield} in measurement_type {measurement_type}')
    return [(measurement_type, val_type, val)]


def mk_optional_basic_field(measurement_type, val_type, data, jfield):
    """
    If the jfield exists in the data then return [(measurement_ttpe, valtype, value for the jfield in the data)].
    If not then return an empty list.
    :param measurement_type:    measurement type of jfield in the data warehouse
    :param val_type: the type of the value to be stored
    :param data:                json that may contain the jfield
    :param jfield:              the name of the field
    :return                     if the field exists then a list is returned holding the appropriate entry
                                if the field doesn't exist then an empty list is returned
    """
    val = data.get(jfield)
    if (val is None) or (val == ""):
        return []  # jfield is not present in the json
    else:
        return [(measurement_type, val_type, val)]  # jfield is present in the json


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
    mk_optional_basic_field(measurement_type, 0, data, jfield)


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
    val = data.get(jfield)
    if (val is None) or (val == ""):
        print(f'Missing Mandatory Field {jfield} in measurement_type {measurement_type}')
    if val == 'N':
        val01 = 0
    else:
        val01 = 1
    return [(measurement_type, 4, val01)]


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
    val = data.get(jfield)
    if (val is None) or (val == ""):
        return []  # jfield is not present in the json
    else:
        return mk_boolean(measurement_type, data, jfield)  # jfield is present in the json


def mk_category(cat_name, cat_list):
    """
    Returns the position of a category in a list of category names.
    (N.B. only works if categorids in the category table run consecutively from 0)
    :param cat_name: the category name from the category table
    :param cat_list: a list of all categories, in order of their categoryid in the category table
    :return the categoryid of the category
    """
    return cat_list.index(cat_name)


def mk_category_from_dict(cat_name, cat_dict, measurement_type):
    """
    Returns the category id from the category name.
    :param cat_name: the category name from the category table
    :param cat_dict: a directory with the category names as keys, and the categoryid as the values
    :param measurement_type: the measurement type (only used for debugging)
    :return the categoryid of the category
    """
    val = cat_dict.get(cat_name)
    if val is None:
        print(f'{cat_name} is not in the dictionary for measurement type {measurement_type}')
    return val


def mk_optional_category_from_dict(measurement_type, val_type, data, jfield, cat_dict):
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
    val = data.get(jfield)
    # print(f'category: {val}, {val_type}, {data},{jfield}, {cat_dict}')
    if (val is None) or (val == ""):
        return []
    else:
        return [(measurement_type, val_type, mk_category_from_dict(val, cat_dict, measurement_type))]


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
    val = data.get(jfield)
    if (val is None) or (val == ""):
        print(f'Missing Mandatory Category Field {jfield} in measurement_type {measurement_type}')
    return [(measurement_type, val_type, mk_category_from_dict(val, cat_dict, measurement_type))]


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
    return mk_optional_category_from_dict(measurement_type, 5, data, jfield, cat_dict)


def mk_optional_nominal_from_id(measurement_type, data, jfield):
    """
    make an ordinal triple (measurement_type, 6, value) where the id is stored in the field if the field isn't empty
    :param measurement_type: the id of the measurementtype that will hold the value
    :param data: the json structure
    :param jfield: the name of the field
    :return: if the field isn't "" then a list is returned holding the appropriate entry;
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
    return mk_optional_category_from_dict(measurement_type, 6, data, jfield, cat_dict)


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
    val = data.get(jfield)
    if (val is None) or (val == ""):
        print(f'Missing Mandatory ENUM field {jfield} for measurement_types {measurement_types}')
    for (measurement_type, value) in zip(measurement_types, valuelist):
        res = res + [(measurement_type, 4, int(value in val))]  # the 4 is because the type is boolean
    return res


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
        res = (found, info["fn"], info["mg"])
    else:
        res = (found, None, None)
    return res
