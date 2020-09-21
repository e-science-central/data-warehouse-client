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

import datetime

def mk_01(s :str):
    """
    Transform a boolean represented as a 'T' or 'F' string into 0 for False or 1 for True
    :param s: 'T' or 'F'
    :return integer 0 for False or 1 for True
    """
    if s == 'N':
        return 0
    else:
        return 1

def mk_optional_string(data, jfield):
    """
    Returns either the string represented in a json structure, or an empty string if it's not present
    :param data:   json that may contain the jfield
    :param jfield: the name of the field
    :return if the field exists then it's returned; otherwise an empty string is returned
    """
    val = data.get(jfield)
    if val is None :
        return ""
    else:
        return val

def mk_category(cat_name, cat_list):
    """
    Returns the position of a category in a list of category names.
    (N.B. only works if categorids in the category table run consecutively from 0)
    :param cat_name: the category name from the category table
    :param cat_list: a list of all categories, in order of their categoryid in the category table
    :return the categoryid of the category
    """
    return cat_list.index(cat_name)

def mk_category_from_dict(cat_name, cat_dict):
    """
    Returns the category id from the category name.
    :param cat_name: the category name from the category table
    :param cat_dict: a directory with the category names as keys, and the categoryid as the values
    :return the categoryid of the category
    """
    return cat_dict[cat_name]

def split_enum(jfields, typeids, valuelist):
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type.
    :param jfields: the list of possible values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :param typeids: the list of typeids of the measurements into which the booleans are to be stored
    :param valuelist: the list of values that are to be stored as measurements
    :return The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    res = []
    for (typeid,value) in zip(typeids, valuelist):
        res = res + [(typeid, 4, int(value in jfields))]   # the 4 is because the type is boolean
    return res

def get_timestamp(ts):
    """
            transforms a Posix timestamp field held in a string to a date/time format for inserting into
            the data warehouse tables
            :param ts: the json form
            :return: string formatted in year-month-day hrs:mins:sec
            """
    timestamp = datetime.datetime.fromtimestamp(int(ts) / 1000)
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

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
        res = (found,info["fn"],info["mg"])
    else:
        res = (found, None, None)
    return res
