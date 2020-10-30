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

#  create measurement group for a participant holding the condition and site

import re

from more_itertools import intersperse


def get_condition_and_site(study_code):
    """
    Returns condition and site. These are encoded in the studycode field in the form Disease Site
    e.g. COPD01, HA05
    The site codes are shown in the site_to_category fn below
    :param study_code: data warehouse study id
    :return: (condition, site)
    """
    site = int((re.findall('[0-9]+', study_code))[0])
    condition = (re.findall('[A-Z]+', study_code))[0]
    if not (condition in condition_to_mg().keys()):
        print(f'Error: The condition ({condition}) found in studycode {study_code} is not recognised')
    if not (site in site_to_category().values()):
        print(f'Error: The site ({site}) found in studycode {study_code} is not recognised')
    return (condition, site)


def mk_condition_values(condition):
    """
    creates the list of (measurementtype,valuetype,value) for measurement group 40 that stores conditions
    and site information for patients
    :param condition: the patient's condition
    :return: [(measurementtype,valuetype,value)]
    """
    res = []
    for c_name, c_mt in condition_to_mg().items():
        if c_name == condition:
            res = [(c_mt, 4, 1)] + res
        else:
            res = [(c_mt, 4, 0)] + res
    #  res = map(lambda c_name, c_mt: (c_mt, 4, 1) if (c_name == condition) else (c_mt, 4, 0),
    #          condition_to_mg().items())
    return res


def write_condition_and_site(dw, study, partic, study_code):
    """
    writes condition and site measurement group (40) in data warehouse for the participant
    these are encoded in the studycode field in the form Disease Site e.g. COPD01, HA05
    The site codes are shown in the site_to_category fn below
    :param dw: data warehouse handle
    :param study: data warehouse study id
    :param partic: the participant
    :param study_code: the study code
    :return: the message group instance for the newly inserted group
    """
    # extract condition and site from study code
    condition_and_site_measurement_group = 40
    (condition, site) = get_condition_and_site(study_code)
    # print(condition, " ", site)
    values = [(308, 5, site)] + mk_condition_values(condition)
    mgi = dw.insertMeasurementGroup(study, condition_and_site_measurement_group,
                                    values, participant=partic)
    # print(values)
    return mgi


def mk_sql_list(ls):
    """
    Turn Python list of strings into an SQL list
    :param ls: a list of strings
    :return: SQL list
    """
    res = "(" + ' '.join([str(elem) for elem in intersperse(",", ls)]) + ")"
    return res


def condition_to_mg():
    """
    Dictionary mapping conditions to measurementtypes
    :return: dictionary
    """
    return {"HA": 302, "CHF": 303, "COPD": 304, "MS": 305, "PD": 306, "PFF": 307}


def site_to_category():
    """
    Dictionary mapping site names to codes
    :return: dictionary
    """
    return {"UNEW": 1, "USFD": 2, "CAU": 3, "TASMC": 4, "RBMF": 5}


def get_mobilise_cohort(dw, study, sites, conditions):
    """
    returns a list of participants that are from the site and have one or more of the specified conditions
    :param dw: data warehouse handle
    :param study: study id
    :param sites: list of Mobilise-D sites
    :param conditions: list of Mobilise-D conditions
    :return: list of participant ids
    """
    participant_info_group = 40

    site_cat = list(map(lambda s: str(site_to_category()[s]), sites))
    res = []
    for condition in conditions:
        cps = dw.getMeasurementGroupInstancesWithValueTests(participant_info_group, study,
                                                            [(condition_to_mg()[condition], "=1"),
                                                             (308, " IN " + mk_sql_list(site_cat))])
        res = dw.get_participants_in_result(cps) + res
    return list(set(res))

# example get_mobilise_cohort(dw,4,["NCL","MUN"],["HA","MS","PFF"]

# test
# test_vals = write_condition_and_site(-1, 4, 3, "COPD04")
# print(test_vals)
