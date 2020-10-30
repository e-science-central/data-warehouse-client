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

import json
import sys

import data_warehouse


def load_json_file(fname: str):
    """
    Load a json file
    :param fname: the filename of the json file
    :return the json file represented as a Python dictionary
    """
    try:
        with open(fname, 'r') as jIn:
            j = json.load(jIn)
            return j
    except Exception as e:
        sys.exit("Unable to load the json file! Exiting.\n" + str(e))


def mk_01(s: str):
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
    if val is None:
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
    for (typeid, value) in zip(typeids, valuelist):
        res = res + [(typeid, 4, int(value in jfields))]  # the 4 is because the type is boolean
    return res


def get_participantid(studyid, js):
    """
    maps from the participantid that is local to the study, to the unique id stored with measurements in the warehouse
    :poram studyid: the study id
    :param js: the json form
    :return The id of the participant
    """
    localid = js['metadata']['_userId']
    q = " SELECT id FROM participant " \
        "WHERE participant.study       = " + str(studyid) + \
        "AND participant.participantid = '" + localid + "';"
    res = data_warehouse.returnQueryResult(q)
    return res[0]


def mk_e_screening_chf(js):  # measurement group 24
    """
    transforms a e-screening-chf json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param js: the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return [(191, 4, mk_01(data['metre'])),
            (192, 4, mk_01(data['gold'])),
            (193, 4, mk_01(data['ageval'])),
            (194, 4, mk_01(data['impaired'])),
            (195, 4, mk_01(data['willing'])),
            (196, 4, mk_01(data['eligible'])),
            (197, 4, mk_01(data['available'])),
            (198, 4, mk_01(data['diagnosis'])),
            (199, 4, mk_01(data['history'])),
            (200, 4, mk_01(data['literate'])),
            (201, 4, mk_01(data['shoe']))]


def mk_e_screening_ha(js):  # measurement group 25
    """
        transforms a e-screening-ha json form into the triples used by insertMeasurementGroup to
            store each measurement that is in the form
        :param js: the json form
        :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return [(191, 4, mk_01(data['metre'])),
            (193, 4, mk_01(data['ageval'])),
            (195, 4, mk_01(data['willing'])),
            (196, 4, mk_01(data['eligible'])),
            (197, 4, mk_01(data['available'])),
            (199, 4, mk_01(data['history'])),
            (200, 4, mk_01(data['literate'])),
            (201, 4, mk_01(data['shoe']))]


def mk_walking_aids_group(js):  # measurement group 4
    """
    transforms a j-walking-aids.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param js: the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return split_enum(data['allaids'], [14, 15, 16, 17, 18, 19],
                      ['None', 'One cane/crutch', 'Two crutches', 'Walker', 'Rollator', 'Other']) + \
           [(20, 2, mk_optional_string(data, 'Other')),
            (21, 5,
             mk_category(data['indoor'], ['None', 'One cane/crutch', 'Two crutches', 'Walker', 'Rollator', 'Other'])),
            (22, 5, mk_category(data['indoorfreq'], ['Regularly', 'Occasionally'])),
            (23, 5,
             mk_category(data['outdoor'], ['None', 'One cane/crutch', 'Two crutches', 'Walker', 'Rollator', 'Other'])),
            (24, 5, mk_category(data['outdoorfreq'], ['Regularly', 'Occasionally']))]


def mk_falls_description(js):  # measurement group 8
    """
    transforms a j-walking-aids.json form into the triples used by insertMeasurementGroup to
       store each measurement that is in the form
    :param js: the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return [(48, 7, data['fallint']),
            (49, 2, data['falldesc']),
            (50, 2, data['fallinjury'])]


def mk_i_medication_usage(js):  # measurement group 13
    """
    transforms a i-medication-usage.json form into the triples used by insertMeasurementGroup to
          store each measurement that is in the form
    :param js: the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return [(108, 2, data['drug']),
            (109, 2, data['dose'])] + \
           split_enum(data['freq'], [110, 111, 112, 113], ['Morning', 'Afternoon', 'Evening', 'At night']) + \
           [(114, 5, mk_category(data['reg'], ['Regular', 'Occasional'])),
            (115, 5, mk_category(data['oral'], ['Oral', 'Sub-cutaneous', 'Intravenous']))]


def json_load(study, measurement_group, load_fn, fname):
    """
    load contents of json file into the data warehouse and then retrieve it and print it
    :param study: study id
    :param measurement_group: measurement group id
    :param load_fn: the function to perform the load
    :param fname: filename to load from
    """
    js = load_json_file(fname)
    participantid = get_participantid(study, js)
    instanceid = data_warehouse.insertMeasurementGroup(study, measurement_group, load_fn(js), participant=participantid)
    newdata = data_warehouse.getMeasurements(groupInstance=instanceid)
    dataInTabularForm = data_warehouse.formMeasurementGroup(study, newdata)
    data_warehouse.printMeasurementGroupInstances(dataInTabularForm, measurement_group, study)


# Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

print("\n Load measurements from e-screening-chf json file\n")
json_load(4, 24, mk_e_screening_chf, 'input\e-screening-chf.json')

print("\n Load measurements from j-walking-aids json file\n")
json_load(4, 4, mk_walking_aids_group, 'input\j-walking-aids.json')

print("\n Load measurements from h-falls-description json file\n")
json_load(4, 8, mk_falls_description, 'input\h-falls-description.json')

print("\n Load measurements from i-medication-usage json file\n")
json_load(4, 13, mk_i_medication_usage, 'input\i-medication-usage.json')

print("\n Load measurements from e-screening-ha json file\n")
json_load(4, 25, mk_e_screening_ha, 'input\e-screening-ha.json')
