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
import load_warehouse_helpers as lwh


def mk_e_screening_chf(data_id, data):  # measurement group 24
    """
    transforms a e-screening-chf json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [(220, 2, data_id),
            (191, 4, lwh.mk_01(data['metre'])),
            (192, 4, lwh.mk_01(data['gold'])),
            (193, 4, lwh.mk_01(data['ageval'])),
            (194, 4, lwh.mk_01(data['impaired'])),
            (195, 4, lwh.mk_01(data['willing'])),
            (196, 4, lwh.mk_01(data['eligible'])),
            (197, 4, lwh.mk_01(data['available'])),
            (198, 4, lwh.mk_01(data['diagnosis'])),
            (199, 4, lwh.mk_01(data['history'])),
            (200, 4, lwh.mk_01(data['literate'])),
            (201, 4, lwh.mk_01(data['shoe']))]


def mk_e_screening_ha(data_id, data):  # measurement group 25
    """
        transforms a e-screening-ha json form into the triples used by insertMeasurementGroup to
            store each measurement that is in the form
        :param data_id: unique id from the json form
        :param data: data array from the json form
        :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [(220, 2, data_id),
            (191, 4, lwh.mk_01(data['metre'])),
            (193, 4, lwh.mk_01(data['ageval'])),
            (195, 4, lwh.mk_01(data['willing'])),
            (196, 4, lwh.mk_01(data['eligible'])),
            (197, 4, lwh.mk_01(data['available'])),
            (199, 4, lwh.mk_01(data['history'])),
            (200, 4, lwh.mk_01(data['literate'])),
            (201, 4, lwh.mk_01(data['shoe']))]


def mk_e_screening_copd(data_id, data):  # measurement group 28
    """
            transforms a e-screening-copd json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
        """
    return [(220, 2, data_id),
            (191, 4, lwh.mk_01(data['metre'])),
            (193, 4, lwh.mk_01(data['ageval'])),
            (215, 4, lwh.mk_01(data['resp'])),
            (195, 4, lwh.mk_01(data['willing'])),
            (197, 4, lwh.mk_01(data['available'])),
            (198, 4, lwh.mk_01(data['diagnosis'])),
            (199, 4, lwh.mk_01(data['history'])),
            (200, 4, lwh.mk_01(data['literate'])),
            (216, 4, lwh.mk_01(data['smoker'])),
            (194, 4, lwh.mk_01(data['impaired'])),
            (217, 4, lwh.mk_01(data['stable'])),
            (218, 4, lwh.mk_01(data['tumor'])),
            (196, 4, lwh.mk_01(data['eligible'])),
            (201, 4, lwh.mk_01(data['shoe'])),
            (219, 4, lwh.mk_01(data['surgery']))]


def mk_e_screening_ms(data_id, data):  # measurement group 29
    """
            transforms a e-screening-ms json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
        """
    return [(220, 2, data_id),
            (191, 4, lwh.mk_01(data['metre'])),
            (193, 4, lwh.mk_01(data['ageval'])),
            (195, 4, lwh.mk_01(data['willing'])),
            (197, 4, lwh.mk_01(data['available'])),
            (198, 4, lwh.mk_01(data['diagnosis'])),
            (199, 4, lwh.mk_01(data['history'])),
            (200, 4, lwh.mk_01(data['literate'])),
            (194, 4, lwh.mk_01(data['impaired'])),
            (196, 4, lwh.mk_01(data['eligible'])),
            (201, 4, lwh.mk_01(data['shoe']))]


def mk_e_screening_pd(data_id, data):  # measurement group 30
    """
            transforms a e-screening-pd json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
        """
    return [(220, 2, data_id),
            (191, 4, lwh.mk_01(data['metre'])),
            (193, 4, lwh.mk_01(data['ageval'])),
            (195, 4, lwh.mk_01(data['willing'])),
            (197, 4, lwh.mk_01(data['available'])),
            (198, 4, lwh.mk_01(data['diagnosis'])),
            (199, 4, lwh.mk_01(data['history'])),
            (200, 4, lwh.mk_01(data['literate'])),
            (194, 4, lwh.mk_01(data['impaired'])),
            (196, 4, lwh.mk_01(data['eligible'])),
            (201, 4, lwh.mk_01(data['shoe']))]


def mk_e_screening_pff(data_id, data):  # measurement group 31
    """
            transforms a e-screening-pff json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
        """
    return [(220, 2, data_id),
            (191, 4, lwh.mk_01(data['metre'])),
            (193, 4, lwh.mk_01(data['ageval'])),
            (195, 4, lwh.mk_01(data['willing'])),
            (197, 4, lwh.mk_01(data['available'])),
            (199, 4, lwh.mk_01(data['history'])),
            (200, 4, lwh.mk_01(data['literate'])),
            (194, 4, lwh.mk_01(data['impaired'])),
            (196, 4, lwh.mk_01(data['eligible'])),
            (201, 4, lwh.mk_01(data['shoe'])),
            (219, 4, lwh.mk_01(data['surgery'])),
            (221, 4, lwh.mk_01(data['xray']))]


def mk_walking_aids(data_id, data):  # measurement group 4
    """
    transforms a j-walking-aids.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return lwh.split_enum(data['allaids'], [14, 15, 16, 17, 18, 19],
           ['None', 'One cane/crutch', 'Two crutches', 'Walker', 'Rollator', 'Other']) + \
           [(20, 2, lwh.mk_optional_string(data, 'Other')),
            (21, 5, lwh.mk_category(data['indoor'], ['None', 'One cane/crutch', 'Two crutches', 'Walker', 'Rollator', 'Other'])),
            (22, 5, lwh.mk_category(data['indoorfreq'], ['Regularly', 'Occasionally'])),
            (23, 5, lwh.mk_category(data['outdoor'], ['None', 'One cane/crutch', 'Two crutches', 'Walker', 'Rollator', 'Other'])),
            (24, 5, lwh.mk_category(data['outdoorfreq'], ['Regularly', 'Occasionally'])),
            (220, 2, data_id)]


def mk_falls_description(data_id, data):  # measurement group 8
    """
    transforms a h-falls-description.json form into the triples used by insertMeasurementGroup to
       store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [(220, 2, data_id),
            (48, 7, data['fallint']),
            (49, 2, data['falldesc']),
            (50, 2, data['fallinjury'])]


def mk_i_medication_usage(data_id, data):  # measurement group 13
    """
    transforms a i-medication-usage.json form into the triples used by insertMeasurementGroup to
          store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [(220, 2, data_id),
            (108, 2, data['drug']),
            (109, 2, data['dose'])] + \
            lwh.split_enum(data['freq'], [110, 111, 112, 113], ['Morning', 'Afternoon', 'Evening', 'At night']) + \
           [(114, 5, lwh.mk_category(data['reg'], ['Regular', 'Occasional'])),
            (115, 5, lwh.mk_category(data['oral'], ['Oral', 'Sub-cutaneous', 'Intravenous']))]


def mk_comorbidity(data_id, data):  # measurement group 26
    """
    transforms a c-comorbidity.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    val_list = [(220, 2, data_id), (202, 4, lwh.mk_01(data['copd-sdiag'])),
                (203, 4, lwh.mk_01(data['copd-sdiagfx'])),
                (204, 4, lwh.mk_01(data['chf-sdiag'])),
                (205, 4, lwh.mk_01(data['chf-sdiagfx'])),
                (206, 4, lwh.mk_01(data['ms-sdiag'])),
                (207, 4, lwh.mk_01(data['ms-sdiagfx'])),
                (208, 4, lwh.mk_01(data['pd-sdiag'])),
                (209, 4, lwh.mk_01(data['pd-sdiagfx'])),
                (210, 4, lwh.mk_01(data['pff-sdiag'])),
                (211, 4, lwh.mk_01(data['pff-sdiagfx']))]
    for sublist in lwh.mk_optional_boolean(212, 222, data, 'pffq'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_boolean(213, 223, data, 'pffx'):
        val_list.append(sublist)
    return val_list


def mk_descriptives(data_id, data):  # measurement group 9
    """
        transforms a b-descriptives.json form into the triples used by insertMeasurementGroup to
            store each measurement that is in the form
        :param data_id: unique id from the json form
        :param data: data array from the json form
        :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
        """
    return [(220, 2, data_id),
            (51, 7, data['yob']),
            (52, 7, data['age']),
            (53, 5, lwh.mk_category(data['residence'], ['Community-dwelling', 'Nursing home'])),
            (54, 5, lwh.mk_category(data['education'], ['12 years or less', 'More than 12 years'])),
            (214, 5, lwh.mk_category(data['gender'], ['Male', 'Female', 'Prefer not to say']))]


def mk_moca(data_id, data):  # measurement group 14
    """
            transforms a d-moca.json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
            """
    return [(220, 2, data_id),
            (116, 7, data['dots']), (117, 7, data['cube']),
            (118, 7, data['contour']), (119, 7, data['numbers']),
            (120, 7, data['hands']), (121, 7, data['lion']),
            (122, 7, data['rhino']), (123, 7, data['camel']),
            (124, 7, data['numforward']), (125, 7, data['numbackward']),
            (126, 7, data['tap']), (127, 7, data['subtract']),
            (128, 7, data['lang1']), (129, 7, data['lang2']),
            (130, 7, data['fluency']), (131, 7, data['abstract1']),
            (132, 7, data['abstract2']), (133, 7, data['face']),
            (134, 7, data['velvet']), (135, 7, data['church']),
            (136, 7, data['daisy']), (137, 7, data['red']),
            (138, 7, data['mocadate']), (139, 7, data['mocamonth']),
            (140, 7, data['mocayear']), (141, 7, data['mocaday']),
            (142, 7, data['place']), (143, 7, data['city']),
            (144, 7, data['mocatotal'])]


def mk_living_arrangements(data_id, data):  # measurement group 11
    """
            transforms a f-living-arrangements.json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
            """
    return [(220, 2, data_id),
            (95, 6, lwh.mk_category(data['alone'], ['Alone', 'With someone'])),
            (96, 5, lwh.mk_category(data['arrange'], ['House', 'Apartment', 'Independent living unit', 'Other'])),
            (97, 2, data['othertext'])]


def mk_falls(data_id, data):  # measurement group 12
    """
            transforms a g-falls.json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
            """
    return [(220, 2, data_id),
            (98, 5, lwh.mk_category(data['falls'], ['Yes', 'No'])),
            (99, 0, data['fallsnum']),
            (100, 5, lwh.mk_category(data['hfalls'], ['Yes', 'No', 'N/A'])),
            (101, 0, data['hfallsnum']),
            (102, 5, lwh.mk_category(data['frax'], ['Yes', 'No', 'N/A'])),
            (103, 0, data['fraxfallsnum']),
            (104, 0, data['fraxtotal']),
            (105, 2, data['fraxdetails']),
            (106, 6, lwh.mk_category(data['injuries'], ['Yes', 'No', 'N/A'])),
            (107, 0, data['injuriesnum'])]


def mk_llfdi(data_id, data):  # measurement group 10
    """
            transforms a k-llfdi.json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
            """
    val_list = [(220, 2, data_id),
                (55, 7, data['f1']), (56, 7, data['f2']), (57, 7, data['f3']),
                (58, 7, data['f4']), (59, 7, data['f5']), (60, 7, data['f6']),
                (61, 7, data['f7']), (62, 7, data['f8']), (63, 7, data['f9']),
                (64, 7, data['f10']), (65, 7, data['f11']), (66, 7, data['f12']),
                (67, 7, data['f13']), (68, 7, data['f14']), (69, 7, data['f15']),
                (70, 7, data['f16']), (71, 7, data['f17']), (72, 7, data['f18']),
                (73, 7, data['f19']), (74, 7, data['f20']), (75, 7, data['f21']),
                (76, 7, data['f22']), (77, 7, data['f23']), (78, 7, data['f24']),
                (79, 7, data['f25']), (80, 7, data['f26']), (81, 7, data['f27']),
                (82, 7, data['f28']), (83, 7, data['f29']), (84, 7, data['f30']),
                (85, 7, data['f31']), (86, 7, data['f32'])]
    for sublist in lwh.mk_optional_int(87, 224, data, 'fd7'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(88, 225, data, 'fd8'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(89, 226, data, 'fd14'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(90, 227, data, 'fd15'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(91, 228, data, 'fd26'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(92, 229, data, 'fd29'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(93, 230, data, 'fd30'):
        val_list.append(sublist)
    for sublist in lwh.mk_optional_int(94, 231, data, 'fd32'):
        val_list.append(sublist)
    return val_list


def mk_rabinovich(data_id, data):  # measurement group 5
    """
            transforms a l-rabinovich.json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
            """
    return [(220, 2, data_id),
            (25, 7, data['rq1']), (26, 7, data['rq2']),
            (27, 7, data['rq3']), (28, 7, data['rq4']),
            (29, 7, data['rq5']), (30, 7, data['rq6']),
            (31, 7, data['rq7']), (32, 7, data['rq8']),
            (33, 7, data['rq9']), (34, 7, data['rq10']),
            (35, 7, data['rq11']), (36, 7, data['rq12']),
            (37, 7, data['rq13']), (38, 2, data['rq14']),
            (39, 2, data['rq15'])]


def mk_crs(data_id, data):  # measurement group 7
    """
            transforms a m-crs.json form into the triples used by insertMeasurementGroup to
                store each measurement that is in the form
            :param data_id: unique id from the json form
            :param data: data array from the json form
            :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
            """
    return [(220, 2, data_id),
            (42, 7, data['cq1']), (43, 7, data['cq2']),
            (44, 7, data['cq3']), (45, 7, data['cq4']),
            (46, 7, data['cq5']), (47, 7, data['cq6'])]


def mk_consent(data_id, data): # measurement group 6
    """
                transforms a a-consent.json form into the triples used by insertMeasurementGroup to
                    store each measurement that is in the form
                :param data_id: unique id from the json form
                :param data: data array from the json form
                :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
                """
    return [(220, 2, data_id),
            (40, 4, lwh.mk_01(data['pis'])),
            (41, 4, lwh.mk_01(data['consent']))]


def fn_mapper():
    """
    maps from the event_type used in e-Science Central to:
        the function that maps the data received by e-Science Central
            into the form used by insertMeasurementGroup to add the measurements into the Data Warehouse
        the measurement group
    :return: the dictionary
    """
    return {
            "e-screening-chf"       : {"fn": mk_e_screening_chf,        "mg": 24},
            "e-screening-ha"        : {"fn": mk_e_screening_ha,         "mg": 25},
            "e-screening-copd"      : {"fn": mk_e_screening_copd,       "mg": 28},
            "e-screening-ms"        : {"fn": mk_e_screening_ms,         "mg": 29},
            "e-screening-pd"        : {"fn": mk_e_screening_pd,         "mg": 30},
            "e-screening-pff"       : {"fn": mk_e_screening_pff,        "mg": 31},
            "j-walking-aids"        : {"fn": mk_walking_aids,           "mg":  4},
            "h-falls-description"   : {"fn": mk_falls_description,      "mg":  8},
            "i-medication-usage"    : {"fn": mk_i_medication_usage,     "mg": 13},
            "c-comorbidity"         : {"fn": mk_comorbidity,            "mg": 26},
            "b-descriptives"        : {"fn": mk_descriptives,           "mg":  9},
            "d-moca"                : {"fn": mk_moca,                   "mg": 14},
            "f-living-arrangements" : {"fn": mk_living_arrangements,    "mg": 11},
            "g-falls"               : {"fn": mk_falls,                  "mg": 12},
            "k-llfdi"               : {"fn": mk_llfdi,                  "mg": 10},
            "l-rabinovich"          : {"fn": mk_rabinovich,             "mg":  5},
            "m-crs"                 : {"fn": mk_crs,                    "mg":  7},
            "a-consent"             : {"fn": mk_consent,                "mg":  6}
    }
