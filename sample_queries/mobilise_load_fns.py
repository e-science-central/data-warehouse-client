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


def mobilise_data_id_field(data_id):
    """
    creates a unique data id field for the data_id in the mobilise json form
    :param data_id: unique id from the json form
    :return: (typeid,valType,value) triple used by insertMeasurementGroup
    """
    unique_data_message_type = 220
    text_val_type = 2
    return (unique_data_message_type, text_val_type, data_id)


def mk_e_screening_chf(data_id, data):  # measurement group 24
    """
    transforms a e-screening-chf json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(191, data, 'metre'),
            lwh.mk_boolean(192, data, 'gold'),
            lwh.mk_boolean(193, data, 'ageval'),
            lwh.mk_boolean(194, data, 'impaired'),
            lwh.mk_boolean(195, data, 'willing'),
            lwh.mk_boolean(196, data, 'eligible'),
            lwh.mk_boolean(197, data, 'available'),
            lwh.mk_boolean(198, data, 'diagnosis'),
            lwh.mk_boolean(199, data, 'history'),
            lwh.mk_boolean(200, data, 'literate'),
            lwh.mk_boolean(201, data, 'shoe')]


def mk_e_screening_ha(data_id, data):  # measurement group 25
    """
    transforms a e-screening-ha json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(191, data, 'metre'),
            lwh.mk_boolean(193, data, 'ageval'),
            lwh.mk_boolean(195, data, 'willing'),
            lwh.mk_boolean(196, data, 'eligible'),
            lwh.mk_boolean(197, data, 'available'),
            lwh.mk_boolean(199, data, 'history'),
            lwh.mk_boolean(200, data, 'literate'),
            lwh.mk_boolean(201, data, 'shoe')]


def mk_e_screening_copd(data_id, data):  # measurement group 28
    """
    transforms a e-screening-copd json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples used by insertMeasurementGroup to add the measurements
        """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(191, data, 'metre'),
            lwh.mk_boolean(193, data, 'ageval'),
            lwh.mk_boolean(215, data, 'resp'),
            lwh.mk_boolean(195, data, 'willing'),
            lwh.mk_boolean(197, data, 'available'),
            lwh.mk_boolean(198, data, 'diagnosis'),
            lwh.mk_boolean(199, data, 'history'),
            lwh.mk_boolean(200, data, 'literate'),
            lwh.mk_boolean(216, data, 'smoker'),
            lwh.mk_boolean(194, data, 'impaired'),
            lwh.mk_boolean(217, data, 'stable'),
            lwh.mk_boolean(218, data, 'tumor'),
            lwh.mk_boolean(196, data, 'eligible'),
            lwh.mk_boolean(201, data, 'shoe'),
            lwh.mk_boolean(219, data, 'surgery')]


def mk_e_screening_ms(data_id, data):  # measurement group 29
    """
    transforms a e-screening-ms json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(191, data, 'metre'),
            lwh.mk_boolean(193, data, 'ageval'),
            lwh.mk_boolean(195, data, 'willing'),
            lwh.mk_boolean(197, data, 'available'),
            lwh.mk_boolean(198, data, 'diagnosis'),
            lwh.mk_boolean(199, data, 'history'),
            lwh.mk_boolean(200, data, 'literate'),
            lwh.mk_boolean(194, data, 'impaired'),
            lwh.mk_boolean(196, data, 'eligible'),
            lwh.mk_boolean(201, data, 'shoe')]


def mk_e_screening_pd(data_id, data):  # measurement group 30
    """
    transforms a e-screening-pd json form into the triples used by insertMeasurementGroup to
            store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(191, data, 'metre'),
            lwh.mk_boolean(193, data, 'ageval'),
            lwh.mk_boolean(195, data, 'willing'),
            lwh.mk_boolean(197, data, 'available'),
            lwh.mk_boolean(198, data, 'diagnosis'),
            lwh.mk_boolean(199, data, 'history'),
            lwh.mk_boolean(200, data, 'literate'),
            lwh.mk_boolean(194, data, 'impaired'),
            lwh.mk_boolean(196, data, 'eligible'),
            lwh.mk_boolean(201, data, 'shoe')]


def mk_e_screening_pff(data_id, data):  # measurement group 31
    """
    transforms a e-screening-pff json form into the triples used by insertMeasurementGroup to
            store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(191, data, 'metre'),
            lwh.mk_boolean(193, data, 'ageval'),
            lwh.mk_boolean(195, data, 'willing'),
            lwh.mk_boolean(197, data, 'available'),
            lwh.mk_boolean(199, data, 'history'),
            lwh.mk_boolean(200, data, 'literate'),
            lwh.mk_boolean(194, data, 'impaired'),
            lwh.mk_boolean(196, data, 'eligible'),
            lwh.mk_boolean(201, data, 'shoe'),
            lwh.mk_boolean(219, data, 'surgery'),
            lwh.mk_boolean(221, data, 'xray')]


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
           lwh.mk_optional_string(20, data, 'Other') + \
           lwh.mk_optional_nominal_from_dict(21, data, 'indoor',
                                             {'None': 0, 'One cane/crutch': 1, 'Two crutches': 2,
                                              'Walker': 3, 'Rollator': 4, 'Other': 5}) + \
           lwh.mk_optional_nominal_from_dict(22, data, 'indoorfreq',
                                             {'Regularly': 0, 'Occasionally': 1}) + \
           lwh.mk_optional_nominal_from_dict(23, data, 'outdoor',
                                             {'None': 0, 'One cane/crutch': 1, 'Two crutches': 2,
                                              'Walker': 3, 'Rollator': 4, 'Other': 5}) + \
           lwh.mk_optional_nominal_from_dict(24, data, 'outdoorfreq',
                                             {'Regularly': 0, 'Occasionally': 1}) + \
           [mobilise_data_id_field(data_id)]


def mk_falls_description(data_id, data):  # measurement group 8
    """
    transforms a h-falls-description.json form into the triples used by insertMeasurementGroup to
       store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_bounded_int(48, data, 'fallint'),
            lwh.mk_string(49, data, 'falldesc'),
            lwh.mk_string(50, data, 'fallinjury')]


def mk_i_medication_usage(data_id, data):  # measurement group 13
    """
    transforms a i-medication-usage.json form into the triples used by insertMeasurementGroup to
          store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_string(108, data, 'drug'),
            lwh.mk_string(109, data, 'dose')] + \
           lwh.split_enum(data['freq'], [110, 111, 112, 113], ['Morning', 'Afternoon', 'Evening', 'At night']) + \
           [lwh.mk_nominal(114, data, 'reg', {'Regular': 0, 'Occasional': 1}),
            lwh.mk_nominal(115, data, 'oral', {'Oral': 0, 'Sub-cutaneous': 1, 'Intravenous': 2})]


def mk_comorbidity(data_id, data):  # measurement group 26
    """
    transforms a c-comorbidity.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(202, data, 'copd-sdiag'),
            lwh.mk_boolean(203, data, 'copd-sdiagfx'),
            lwh.mk_boolean(204, data, 'chf-sdiag'),
            lwh.mk_boolean(205, data, 'chf-sdiagfx'),
            lwh.mk_boolean(206, data, 'ms-sdiag'),
            lwh.mk_boolean(207, data, 'ms-sdiagfx'),
            lwh.mk_boolean(208, data, 'pd-sdiag'),
            lwh.mk_boolean(209, data, 'pd-sdiagfx'),
            lwh.mk_boolean(210, data, 'pff-sdiag'),
            lwh.mk_boolean(211, data, 'pff-sdiagfx')] + \
           lwh.mk_optional_boolean(212, data, 'pffq') + \
           lwh.mk_optional_boolean(213, data, 'pffx')


def mk_descriptives(data_id, data):  # measurement group 9
    """
    transforms a b-descriptives.json form into the triples used by insertMeasurementGroup to
    store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_bounded_int(51, data, 'yob'),
            lwh.mk_bounded_int(52, data, 'age'),
            lwh.mk_nominal(53, data, 'residence', {'Community-dwelling': 0, 'Nursing home': 1}),
            lwh.mk_nominal(54, data, 'education', {'12 years or less': 0, 'More than 12 years': 1}),
            lwh.mk_nominal(214, data, 'gender', {'Male': 0, 'Female': 1, 'Prefer not to say': 2})]


def mk_moca(data_id, data):  # measurement group 14
    """
    transforms a d-moca.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_bounded_int(116, data, 'dots'),
            lwh.mk_bounded_int(117, data, 'cube'),
            lwh.mk_bounded_int(118, data, 'contour'),
            lwh.mk_bounded_int(119, data, 'numbers'),
            lwh.mk_bounded_int(120, data, 'hands'),
            lwh.mk_bounded_int(121, data, 'lion'),
            lwh.mk_bounded_int(122, data, 'rhino'),
            lwh.mk_bounded_int(123, data, 'camel'),
            lwh.mk_bounded_int(124, data, 'numforward'),
            lwh.mk_bounded_int(125, data, 'numbackward'),
            lwh.mk_bounded_int(126, data, 'tap'),
            lwh.mk_bounded_int(127, data, 'subtract'),
            lwh.mk_bounded_int(128, data, 'lang1'),
            lwh.mk_bounded_int(129, data, 'lang2'),
            lwh.mk_bounded_int(130, data, 'fluency'),
            lwh.mk_bounded_int(131, data, 'abstract1'),
            lwh.mk_bounded_int(132, data, 'abstract2'),
            lwh.mk_bounded_int(133, data, 'face'),
            lwh.mk_bounded_int(134, data, 'velvet'),
            lwh.mk_bounded_int(135, data, 'church'),
            lwh.mk_bounded_int(136, data, 'daisy'),
            lwh.mk_bounded_int(137, data, 'red'),
            lwh.mk_bounded_int(138, data, 'mocadate'),
            lwh.mk_bounded_int(139, data, 'mocamonth'),
            lwh.mk_bounded_int(140, data, 'mocayear'),
            lwh.mk_bounded_int(141, data, 'mocaday'),
            lwh.mk_bounded_int(142, data, 'place'),
            lwh.mk_bounded_int(143, data, 'city'),
            lwh.mk_bounded_int(144, data, 'mocatotal')]


def mk_living_arrangements(data_id, data):  # measurement group 11
    """
    transforms a f-living-arrangements.json form into the triples used by insertMeasurementGroup to
    store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_nominal(95, data, 'alone', {'Alone': 0, 'With someone': 1}),
            lwh.mk_nominal(96, data, 'arrange', {'House': 0, 'Apartment': 1,
                                                 'Independent living unit': 2, 'Other': 3})] + \
           lwh.mk_optional_string(97, data, 'othertext')


def mk_falls(data_id, data):  # measurement group 12
    """
    transforms a g-falls.json form into the triples used by insertMeasurementGroup to
    store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_ordinal(98, data, 'falls', {'Yes': 0, 'No': 1}),
            lwh.mk_int(99, data, 'fallsnum'),
            lwh.mk_ordinal(100, data, 'hfalls', {'Yes': 0, 'No': 1, 'N/A': 2}),
            lwh.mk_int(101, data, 'hfallsnum'),
            lwh.mk_ordinal(102, data, 'frax', {'Yes': 0, 'No': 1, 'N/A': 2}),
            lwh.mk_int(103, data, 'fraxfallsnum'),
            lwh.mk_int(104, data, 'fraxtotal'),
            lwh.mk_nominal(106, data, 'injuries', {'Yes': 0, 'No': 1, 'N/A': 2}),  # was ordinal
            lwh.mk_int(107, data, 'injuriesnum')] + \
           lwh.mk_optional_string(105, data, 'fraxdetails')


def mk_llfdi(data_id, data):  # measurement group 10
    """
    transforms a k-llfdi.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_bounded_int(55, data, 'f1'),
            lwh.mk_bounded_int(56, data, 'f2'),
            lwh.mk_bounded_int(57, data, 'f3'),
            lwh.mk_bounded_int(58, data, 'f4'),
            lwh.mk_bounded_int(59, data, 'f5'),
            lwh.mk_bounded_int(60, data, 'f6'),
            lwh.mk_bounded_int(61, data, 'f7'),
            lwh.mk_bounded_int(62, data, 'f8'),
            lwh.mk_bounded_int(63, data, 'f9'),
            lwh.mk_bounded_int(64, data, 'f10'),
            lwh.mk_bounded_int(65, data, 'f11'),
            lwh.mk_bounded_int(66, data, 'f12'),
            lwh.mk_bounded_int(67, data, 'f13'),
            lwh.mk_bounded_int(68, data, 'f14'),
            lwh.mk_bounded_int(69, data, 'f15'),
            lwh.mk_bounded_int(70, data, 'f16'),
            lwh.mk_bounded_int(71, data, 'f17'),
            lwh.mk_bounded_int(72, data, 'f18'),
            lwh.mk_bounded_int(73, data, 'f19'),
            lwh.mk_bounded_int(74, data, 'f20'),
            lwh.mk_bounded_int(75, data, 'f21'),
            lwh.mk_bounded_int(76, data, 'f22'),
            lwh.mk_bounded_int(77, data, 'f23'),
            lwh.mk_bounded_int(78, data, 'f24'),
            lwh.mk_bounded_int(79, data, 'f25'),
            lwh.mk_bounded_int(80, data, 'f26'),
            lwh.mk_bounded_int(81, data, 'f27'),
            lwh.mk_bounded_int(82, data, 'f28'),
            lwh.mk_bounded_int(83, data, 'f29'),
            lwh.mk_bounded_int(84, data, 'f30'),
            lwh.mk_bounded_int(85, data, 'f31'),
            lwh.mk_bounded_int(86, data, 'f32')] + \
           lwh.mk_optional_int(87, data, 'fd7') + \
           lwh.mk_optional_int(88, data, 'fd8') + \
           lwh.mk_optional_int(89, data, 'fd14') + \
           lwh.mk_optional_int(90, data, 'fd15') + \
           lwh.mk_optional_int(91, data, 'fd26') + \
           lwh.mk_optional_int(92, data, 'fd29') + \
           lwh.mk_optional_int(93, data, 'fd30') + \
           lwh.mk_optional_int(94, data, 'fd32')


def mk_rabinovich(data_id, data):  # measurement group 5
    """
    transforms a l-rabinovich.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_bounded_int(25, data, 'rq1'),
            lwh.mk_bounded_int(26, data, 'rq2'),
            lwh.mk_bounded_int(27, data, 'rq3'),
            lwh.mk_bounded_int(28, data, 'rq4'),
            lwh.mk_bounded_int(29, data, 'rq5'),
            lwh.mk_bounded_int(30, data, 'rq6'),
            lwh.mk_bounded_int(31, data, 'rq7'),
            lwh.mk_bounded_int(32, data, 'rq8'),
            lwh.mk_bounded_int(33, data, 'rq9'),
            lwh.mk_bounded_int(34, data, 'rq10'),
            lwh.mk_bounded_int(35, data, 'rq11'),
            lwh.mk_bounded_int(36, data, 'rq12'),
            lwh.mk_bounded_int(37, data, 'rq13'),
            lwh.mk_string(38, data, 'rq14'),
            lwh.mk_string(39, data, 'rq15')]


def mk_crs(data_id, data):  # measurement group 7
    """
    transforms a m-crs.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_bounded_int(42, data, 'cq1'),
            lwh.mk_bounded_int(43, data, 'cq2'),
            lwh.mk_bounded_int(44, data, 'cq3'),
            lwh.mk_bounded_int(45, data, 'cq4'),
            lwh.mk_bounded_int(46, data, 'cq5'),
            lwh.mk_bounded_int(47, data, 'cq6')]


def mk_consent(data_id, data):  # measurement group 6
    """
    transforms a a-consent.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param data_id: unique id from the json form
    :param data: data array from the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    return [mobilise_data_id_field(data_id),
            lwh.mk_boolean(40, data, 'pis'),
            lwh.mk_boolean(41, data, 'consent')]


def fn_mapper():
    """
    maps from the event_type used in e-Science Central to:
        the function that maps the data received by e-Science Central
            into the form used by insertMeasurementGroup to add the measurements into the Data Warehouse
        the measurement group
    :return: the dictionary
    """
    return {
        "e-screening-chf": {"fn": mk_e_screening_chf, "mg": 24},
        "e-screening-ha": {"fn": mk_e_screening_ha, "mg": 25},
        "e-screening-copd": {"fn": mk_e_screening_copd, "mg": 28},
        "e-screening-ms": {"fn": mk_e_screening_ms, "mg": 29},
        "e-screening-pd": {"fn": mk_e_screening_pd, "mg": 30},
        "e-screening-pff": {"fn": mk_e_screening_pff, "mg": 31},
        "j-walking-aids": {"fn": mk_walking_aids, "mg": 4},
        "h-falls-description": {"fn": mk_falls_description, "mg": 8},
        "i-medication-usage": {"fn": mk_i_medication_usage, "mg": 13},
        "c-comorbidity": {"fn": mk_comorbidity, "mg": 26},
        "b-descriptives": {"fn": mk_descriptives, "mg": 9},
        "d-moca": {"fn": mk_moca, "mg": 14},
        "f-living-arrangements": {"fn": mk_living_arrangements, "mg": 11},
        "g-falls": {"fn": mk_falls, "mg": 12},
        "k-llfdi": {"fn": mk_llfdi, "mg": 10},
        "l-rabinovich": {"fn": mk_rabinovich, "mg": 5},
        "m-crs": {"fn": mk_crs, "mg": 7},
        "a-consent": {"fn": mk_consent, "mg": 6}
    }
