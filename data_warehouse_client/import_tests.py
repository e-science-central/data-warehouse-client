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

import pytest  # see https://realpython.com/pytest-python-testing/
from typing import Dict, List, Tuple
from datetime import datetime

from data_warehouse_client.check_bounded_values import get_inverse_category_ids_map
from data_warehouse_client.data_warehouse import DataWarehouse
from data_warehouse_client.load_data import load_data
from data_warehouse_client.load_data import process_measurement_groups as pmg
from data_warehouse_client.type_checks import check_int
from data_warehouse_client.type_definitions import DataToLoad, Bounds, LoaderResult, MeasurementGroup, \
    LoadHelperResult, Loader
from data_warehouse_client import import_with_checks as iwc
from data_warehouse_client.check_bounded_values import get_bounds


@pytest.fixture()
def walking_test_1() -> DataToLoad:
    data = {
        'visit-date': datetime.now(),
        'visit-code': 'visit3',
        'wb_id': "fred",
        'Turn_Id': 2345,
        'Turn_Start_SO': 12.5,
        'Turn_End_SO': 123.3,
        'Turn_Duration_SO': 103.0,
        'Turn_PeakAngularVelocity_SO': 99.9,
        'drugs': [{'drug': 'asprin', 'dose': 10}, {'drug': 'calpol', 'dose': 30}, {'drug': 'clopidogrel', 'dose': 20}]
    }
    return data


@pytest.fixture()
def credentials_file_name():
    return "dw-credentials-full-access.json"


@pytest.fixture()
def database_name():
    return "osm_dw"


@pytest.fixture()
def mk_dw_handle(credentials_file_name, database_name):
    data_warehouse_handle = DataWarehouse(credentials_file_name, database_name)
    yield data_warehouse_handle


@pytest.fixture()
def test_all_example() -> DataToLoad:
    data = {
        'Int': 4,
        'Real': 5.45,
        'Text': 'Test Data',
        'DateTime': datetime.now(),
        'Bool': 1,
        'NominalfromValue': 'First',
        'NominalfromId': 1,
        'OrdinalfromValue': 'Second',
        'OrdinalfromId': 2,
        'BoundedInt': 5,
        'BoundedReal': 8.6,
        'BoundedDateTime': datetime.now(),
        'External': 'External Data',
        'SplitEnum': ['1st', '3rd'],
        'OptionalInt': 4,
        'OptionalReal': 5.45,
        'OptionalText': 'Test Data',
        'OptionalDateTime': datetime.now(),
        'OptionalBool': 1,
        'OptionalNominalfromValue': 'First',
        'OptionalNominalfromId': 1,
        'OptionalOrdinalfromValue': 'Second',
        'OptionalOrdinalfromId': 1,
        'OptionalBoundedInt': 5,
        'OptionalBoundedReal': 8.6,
        'OptionalBoundedDateTime': datetime.now(),
        'OptionalExternal': 'External Data',
        'OptionalSplitEnum': ['Deux', 'Trois'],
        'drugs': [{'drug': 'asprin', 'dose': 10}, {'drug': 'calpol', 'dose': 30}, {'drug': 'clopidogrel', 'dose': 20}]
    }
    return data


def check_all_loader(data: DataToLoad, bounds: Bounds) -> LoaderResult:
    test_all_mg_id: MeasurementGroup = 50
    test_mgi = [(test_all_mg_id,
                [
                 iwc.load_int(410, data, 'Int'),
                 iwc.load_real(411, data, 'Real'),
                 iwc.load_string(412, data, 'Text'),
                 iwc.load_datetime(413, data, 'DateTime'),
                 iwc.load_boolean(414, data, 'Bool'),
                 iwc.load_nominal_from_value(415, data, 'NominalfromValue', bounds),
                 iwc.load_nominal_from_id(416, data, 'NominalfromId', bounds),
                 iwc.load_ordinal_from_value(417, data, 'OrdinalfromValue', bounds),
                 iwc.load_ordinal_from_id(418, data, 'OrdinalfromId', bounds),
                 iwc.load_bounded_int(419, data, 'BoundedInt', bounds),
                 iwc.load_bounded_real(420, data, 'BoundedReal', bounds),
                 iwc.load_bounded_datetime(421, data, 'BoundedDateTime', bounds),
                 iwc.load_external(422, data, 'External'),
                 iwc.load_set([423, 424, 425], data, 'SplitEnum', ['1st', '2nd', '3rd']),
                 iwc.load_optional_int(426, data, 'OptionalInt'),
                 iwc.load_optional_real(427, data, 'OptionalReal'),
                 iwc.load_optional_string(428, data, 'OptionalText'),
                 iwc.load_optional_datetime(429, data, 'OptionalDateTime'),
                 iwc.load_optional_boolean(430, data, 'OptionalBool'),
                 iwc.load_optional_nominal_from_value(431, data, 'OptionalNominalfromValue', bounds),
                 iwc.load_optional_nominal_from_id(432, data, 'OptionalNominalfromId', bounds),
                 iwc.load_optional_ordinal_from_value(433, data, 'OptionalOrdinalfromValue', bounds),
                 iwc.load_optional_ordinal_from_id(434, data, 'OptionalOrdinalfromId', bounds),
                 iwc.load_optional_bounded_int(435, data, 'OptionalBoundedInt', bounds),
                 iwc.load_optional_bounded_real(436, data, 'OptionalBoundedReal', bounds),
                 iwc.load_optional_bounded_datetime(437, data, 'OptionalBoundedDateTime', bounds),
                 iwc.load_optional_external(438, data, 'OptionalExternal'),
                 iwc.load_optional_set([439, 440, 441], data, 'OptionalSplitEnum', ['Un', 'Deux', 'Trois'])]
                 )]
    drug_group_instances: List[Tuple[MeasurementGroup, List[LoadHelperResult]]] = \
        iwc.load_list(data, 'drugs', drugs_loader, test_all_mg_id, bounds)
    return test_mgi + drug_group_instances, None, None, None, None


def check_all_loader_2(data: DataToLoad, bounds: Bounds) -> LoaderResult:
    test_all_mg_id: MeasurementGroup = 50
    test_mgi = [(test_all_mg_id,
                [
                 iwc.load_int(410, data, 'Int'),
                 iwc.load_real(411, data, 'Real'),
                 iwc.load_string(412, data, 'Text'),
                 iwc.load_datetime(413, data, 'DateTime'),
                 iwc.load_boolean(414, data, 'Bool'),
                 iwc.load_nominal_from_value(415, data, 'NominalfromValue', bounds),
                 iwc.load_nominal_from_id(416, data, 'NominalfromId', bounds),
                 iwc.load_ordinal_from_value(417, data, 'OrdinalfromValue', bounds),
                 iwc.load_ordinal_from_id(418, data, 'OrdinalfromId', bounds),
                 iwc.load_bounded_int(419, data, 'BoundedInt', bounds),
                 iwc.load_bounded_real(420, data, 'BoundedReal', bounds),
                 iwc.load_bounded_datetime(421, data, 'BoundedDateTime', bounds),
                 iwc.load_external(422, data, 'External'),
                 iwc.load_set([423, 424, 425], data, 'SplitEnum', ['1st', '2nd', '3rd']),
                 iwc.load_optional_int(426, data, 'OptionalInt'),
                 iwc.load_optional_real(427, data, 'OptionalReal'),
                 iwc.load_optional_string(428, data, 'OptionalText'),
                 iwc.load_optional_datetime(429, data, 'OptionalDateTime'),
                 iwc.load_optional_boolean(430, data, 'OptionalBool'),
                 iwc.load_optional_nominal_from_value(431, data, 'OptionalNominalfromValue', bounds),
                 iwc.load_optional_nominal_from_id(432, data, 'OptionalNominalfromId', bounds),
                 iwc.load_optional_ordinal_from_value(433, data, 'OptionalOrdinalfromValue', bounds),
                 iwc.load_optional_ordinal_from_id(434, data, 'OptionalOrdinalfromId', bounds),
                 iwc.load_optional_bounded_int(435, data, 'OptionalBoundedInt', bounds),
                 iwc.load_optional_bounded_real(436, data, 'OptionalBoundedReal', bounds),
                 iwc.load_optional_bounded_datetime(437, data, 'OptionalBoundedDateTime', bounds),
                 iwc.load_optional_external(438, data, 'OptionalExternal'),
                 iwc.load_optional_set([439, 440, 441], data, 'OptionalSplitEnum', ['Un', 'Deux', 'Trois'])]
                 )]
    drug_group_instances: List[Tuple[MeasurementGroup, List[LoadHelperResult]]] = \
        iwc.load_list(data, 'drugs', drugs_loader, test_all_mg_id, bounds)
    participant = data['participant']
    trial = data['trial']
    source = data['source']
    return test_mgi + drug_group_instances, None, trial, participant, source


def drugs_loader(data: DataToLoad, bounds: Bounds) -> LoaderResult:
    drug_mg_id: MeasurementGroup = 40
    drug_mgi = [(drug_mg_id,
                 [iwc.load_string(400, data, 'drug'),
                  iwc.load_int(401, data, 'dose')]
                 )]
    return drug_mgi, None, None, None, None


def walking_and_drugs_loader(data: DataToLoad, bounds: Bounds) -> LoaderResult:

    turn_group: MeasurementGroup = 39

    turn_group_instance: List[Tuple[MeasurementGroup, List[LoadHelperResult]]] = \
        [(turn_group,
          [iwc.load_datetime(370, data, 'visit-date'),
           iwc.load_string(371, data, 'visit-code'),
           iwc.load_string(1839, data, 'wb_id'),
           iwc.load_int(1843, data, 'Turn_Id'),
           iwc.load_real(1844, data, 'Turn_Start_SO'),
           iwc.load_real(1845, data, 'Turn_End_SO'),
           iwc.load_real(1846, data, 'Turn_Duration_SO'),
           iwc.load_real(1847, data, 'Turn_PeakAngularVelocity_SO')])]
    drug_group_instances: List[Tuple[MeasurementGroup, List[LoadHelperResult]]] = \
        iwc.load_list(data, 'drugs', drugs_loader, turn_group, bounds)
    return turn_group_instance+drug_group_instances, None, None, None, None


@pytest.fixture()
def fn_mapper() -> Dict[str, Loader]:
    """
    maps from the event_type used in e-SC (json script) to the function used by insertMeasurementGroup
    to add the measurements into the Data Warehouse
    """
    return {
        "walking_and_drugs": walking_and_drugs_loader,
        "test_all": check_all_loader,
        "test_all_2": check_all_loader_2
    }


@pytest.fixture()
def test_study():
    return 999


@pytest.fixture()
def bounds_ex1() -> Bounds:
    return ({419: {'minval': 0, 'maxval': 100}, 435: {'minval': 0, 'maxval': 100}},    # Int Bounds
            {420: {'minval': 0.0, 'maxval': 100.0}, 436: {'minval': 0, 'maxval': 100}},  # Real Bounds
            {421: {'minval': datetime(1665, 5, 17, 23, 17, 59),
                   'maxval': datetime(1668, 5, 17, 23, 17, 59)},
             437: {'minval': datetime(2400, 5, 17, 23, 17, 59),
                   'maxval': datetime(2600, 5, 17, 23, 17, 59)}},  # DateTime Bounds
            {415: [0, 1, 2], 416: [0, 1, 2], 417: [0, 1, 2], 418: [0, 1, 2],
             431: [0, 1, 2], 432: [0, 1, 2], 433: [0, 1, 2], 434: [0, 1, 2]},  # Category Ids
            {415: {'First': 0, 'Second': 1, 'Third': 2},
             416: {'First': 0, 'Second': 1, 'Third': 2},
             417: {'First': 0, 'Second': 1, 'Third': 2},
             418: {'First': 0, 'Second': 1, 'Third': 2},
             431: {'First': 0, 'Second': 1, 'Third': 2},
             432: {'First': 0, 'Second': 1, 'Third': 2},
             433: {'First': 0, 'Second': 1, 'Third': 2},
             434: {'First': 0, 'Second': 1, 'Third': 2}}
            )  # Category Values to Ids


@pytest.mark.parametrize("json_key, json_value, measurement_type, expected_result", [
    ('Int', 4, 410, True),
    ('Real', 5.45, 411, True),
    ('Text', 'Test Data', 412, True),
    ('DateTime', datetime.now(), 413, False),
    ('Bool', 1, 414, True),
    ('NominalfromValue', 'First', 415, True),
    ('NominalfromId', 1, 416, True),
    ('OrdinalfromValue', 'Second', 417, True),
    ('OrdinalfromId', 2, 418, True),
    ('BoundedInt', 5, 419, True),
    ('BoundedReal', 8.6, 420, True),
    ('BoundedDateTime', datetime.now(), 421, False),
    ('External', 'External Data', 422, True),
    ('SplitEnum1', True, 423, True),
    ('SplitEnum2', False, 424, True),
    ('SplitEnum3', True, 425, True),
    ('OptionalInt', 4, 426, True),
    ('OptionalReal', 5.45, 427, True),
    ('OptionalText', 'Test Data', 428, True),
    ('OptionalDateTime', datetime.now(), 429, False),
    ('OptionalBool', 1, 430, True),
    ('OptionalNominalfromValue', 'First', 431, True),
    ('OptionalNominalfromId', 1, 432, True),
    ('OptionalOrdinalfromValue', 'Second', 433, True),
    ('OptionalOrdinalfromId', 1, 434, True),
    ('OptionalBoundedInt', 5, 435, True),
    ('OptionalBoundedReal', 8.6, 436, True),
    ('OptionalBoundedDateTime', datetime.now(), 437, False),
    ('OptionalExternal', 'External Data', 438, True),
    ('OptionalSplitEnum1', False, 439, True),
    ('OptionalSplitEnum2', True, 440, True),
    ('OptionalSplitEnum3', True, 441, True)
])
def test_each_key(mk_dw_handle, test_all_example, fn_mapper, test_study,
                  json_key, json_value, measurement_type, expected_result):
    """
    Check each field in the measurement group instance has been inserted into the database
    """
    dw_handle = mk_dw_handle
    category_value_to_id_map = get_inverse_category_ids_map(dw_handle, test_study)
    success, mgis, error_msg = load_data(dw_handle, test_all_example, "test_all", fn_mapper, test_study)
    if success:
        main_mgi = mgis[0]  # Get the main mgi (not those of the Drug measurement group instances)
        test_all_mg_id: MeasurementGroup = 50
        #  retrieve value from warehouse
        measurements = dw_handle.get_measurements(test_study, measurement_type=measurement_type,
                                                  measurement_group=test_all_mg_id, group_instance=main_mgi)
        if len(measurements) == 1:  # there should only be one result returned
            ident, time, study, participant, measurement_type, type_name, measurement_group,\
                group_instance, trial, val_type, value = measurements[0]  # pick out the fields in that result
            if val_type == 4:   # is a boolean type
                if value == 'T':  # booleans are turned into 'T' and 'F' by get measurements
                    value_to_compare = True
                else:
                    value_to_compare = False
            elif val_type in [5, 6]:  # if categorical data
                if check_int(json_value):   # if it's a key that was in the json, get the value
                    value_to_compare = category_value_to_id_map[measurement_type][value]
                else:  # it was a value in the json, which is what will be retrived
                    value_to_compare = json_value
            else:
                value_to_compare = value
            if expected_result:  # if what was stored and retrieved are expected to be the same...
                assert value_to_compare == json_value and len(mgis) > 0 and len(error_msg) == 0
            else:
                assert value_to_compare != json_value and len(mgis) > 0 and len(error_msg) == 0
        else:
            assert False  # Did not return 1 result (may be 0 or >1)
    else:  # expect a failure to read the measurement from the data warehouse
        assert len(mgis) == 0 and len(error_msg) > 0


@pytest.mark.parametrize("json_key, json_value, valid", [
    ('Int', 4.1, False),
    ('Real', 5, False),
    ('Text', 0, False),
    ('DateTime', 'Not a Datetime', False),
    ('Bool', 3, False),
    ('NominalfromValue', 'Fifth', False),
    ('NominalfromId', 'First', False),
    ('OrdinalfromValue', 3.142, False),
    ('OrdinalfromId', 77, False),
    ('BoundedInt', 9999999, False),
    ('BoundedReal', 999999.9, False),
    ('BoundedDateTime', datetime(1666, 5, 17, 23, 17, 59), False),
    ('External', 5, False),
    ('SplitEnum', ['First', 'Fifth'], False),
    ('OptionalInt', '4', False),
    ('OptionalReal', '3.1', False),
    ('OptionalText', ['Text List'], False),
    ('OptionalDateTime', 3, False),
    ('OptionalBool', 'T', False),
    ('OptionalNominalfromValue', 'Seventh', False),
    ('OptionalNominalfromId', 'First', False),
    ('OptionalOrdinalfromValue', '1', False),
    ('OptionalOrdinalfromId', '1', False),
    ('OptionalBoundedInt', 7.89, False),
    ('OptionalBoundedReal', 4, False),
    ('OptionalBoundedDateTime', datetime(2500, 5, 17, 23, 17, 59), False),
    ('OptionalExternal', 5.6, False),
    ('OptionalSplitEnum', [1, 2], False)
])
def test_each_field(mk_dw_handle, test_all_example, fn_mapper, test_study,
                    json_key, json_value, valid):
    """
    Check each field in the measurement group instance has been inserted into the database
    """
    dw_handle = mk_dw_handle
    # category_value_to_id_map = check_bounded_values.get_inverse_category_ids_map(dw_handle, test_study)
    test_all_example[json_key] = json_value
    success, mgis, error_msg = load_data(dw_handle, test_all_example, "test_all", fn_mapper, test_study)
    if success == valid:
        assert True
    else:
        assert False


@pytest.mark.parametrize("json_key, json_value, valid", [
    ('Int', 4.1, False),
    ('Int', "4", False),
    ('Real', 5, False),
    ('Real', "5", False),
    ('Text', 0, False),
    ('DateTime', 'Not a Datetime', False),
    ('DateTime', 32.6, False),
    ('Bool', 3, False),
    ('NominalfromValue', 'Fifth', False),
    ('NominalfromId', 'First', False),
    ('OrdinalfromValue', 3.142, False),
    ('OrdinalfromId', 77, False),
    ('BoundedInt', 9999999, False),
    ('BoundedReal', 999999.9, False),
    ('BoundedDateTime', datetime(1666, 5, 17, 23, 17, 59), False),
    ('External', 5, False),
    ('SplitEnum', ['First', 'Fifth'], False),
    ('OptionalInt', '4', False),
    ('OptionalReal', '3.1', False),
    ('OptionalText', ['Text List'], False),
    ('OptionalDateTime', 3, False),
    ('OptionalBool', 'T', False),
    ('OptionalNominalfromValue', 'Seventh', False),
    ('OptionalNominalfromId', 'First', False),
    ('OptionalOrdinalfromValue', '1', False),
    ('OptionalOrdinalfromId', '1', False),
    ('OptionalBoundedInt', 7.89, False),
    ('OptionalBoundedReal', 4, False),
    ('OptionalBoundedDateTime', datetime(2500, 5, 17, 23, 17, 59), False),
    ('OptionalExternal', 5.6, False),
    ('OptionalSplitEnum', [1, 2], False)
])
def test_each_loader(mk_dw_handle, test_study, test_all_example, json_key, measurement_id, json_value, valid):
    """
    Check each field in the measurement group instance is handled correctly by the loader
    """
    dw_handle = mk_dw_handle
    bounds = get_bounds(dw_handle, test_study)
    test_all_example[json_key] = json_value
    vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = \
        check_all_loader(test_all_example, bounds)
    # check for errors in the values
    successful, all_mgs_and_triples, combined_error_messages = pmg(vals_to_load_in_msgs)
    if successful == valid:
        assert True
    else:
        print(combined_error_messages)
        assert False


@pytest.mark.parametrize("participant, trial, source, valid", [
    (1, 1, 1, True),
    (2, 1, 1, True),
    (0, 1, 1, False),  # incorrect study
    (1, 0, 1, False),  # incorrect trial
    (1, 1, 3, False)   # incorrect source
])
def test_participant_and_trial_and_source_fields(mk_dw_handle, test_all_example, fn_mapper, test_study,
                                                 participant, trial, source, valid):
    """
    test loading with valid and invalid participants and trials and sources
    """
    dw_handle = mk_dw_handle
    success, mgis, error_msg = load_data(dw_handle, test_all_example, "test_all", fn_mapper, test_study,
                                         participant=participant, trial=trial, source=source)
    if not success and not valid:  # failed when it should fail
        assert len(mgis) == 0 and len(error_msg) > 0
    elif success != valid:  # should not occur
        assert False
    else:  # success and valid
        total_bad_results: int = 0
        for mgi in mgis:
            #  retrieve measurements from the data warehouse
            measurements = dw_handle.get_measurements(test_study, group_instance=mgi)
            participant_index = 3
            trial_index = 8
            bad_results = list(filter(lambda measurement: (measurement[participant_index] != participant) or
                                                          (measurement[trial_index] != trial), measurements))
            total_bad_results = total_bad_results + len(bad_results)
        assert total_bad_results == 0


@pytest.mark.parametrize("participant, trial, source, valid", [
    (1, 1, 1, True),
    (2, 1, 1, True),
    (0, 1, 1, False),  # incorrect study
    (1, 0, 1, False),  # incorrect trial
    (1, 1, 3, False)   # incorrect source
])
def test_participant_and_trial_and_source_fields_2(mk_dw_handle, test_all_example, fn_mapper, test_study,
                                                   participant, trial, source, valid):
    """
    test loading with valid and invalid participants and trials and sources loaded from the json
    """
    dw_handle = mk_dw_handle
    test_all_example['participant'] = participant
    test_all_example['trial'] = trial
    test_all_example['source'] = source
    success, mgis, error_msg = load_data(dw_handle, test_all_example, "test_all_2", fn_mapper, test_study)
    if not success and not valid:  # failed when it should fail
        assert len(mgis) == 0 and len(error_msg) > 0
    elif success != valid:  # should not occur
        assert False
    else:  # success and valid
        total_bad_results: int = 0
        for mgi in mgis:
            #  retrieve measurements from the data warehouse
            measurements = dw_handle.get_measurements(test_study, group_instance=mgi)
            participant_index = 3
            trial_index = 8
            bad_results = list(filter(lambda measurement: (measurement[participant_index] != participant) or
                                                          (measurement[trial_index] != trial), measurements))
            total_bad_results = total_bad_results + len(bad_results)
        assert total_bad_results == 0
