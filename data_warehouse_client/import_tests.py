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

import pytest
import type_definitions as ty
from typing import Dict, Callable, List, Tuple
import load_warehouse_helpers as lwh
import datetime

# see https://realpython.com/pytest-python-testing/


def loader_test(mapper: Dict[str, Callable[[ty.DataToLoad], ty.LoaderResult]], data_name: str, data: ty.DataToLoad):
    loader_found, loader = lwh.get_loader_from_data_name(data_name, mapper)  # find the loader function
    if loader_found:
        # get the (message group id, value triples) and other (optional values) to use in the loading
        vals_to_load_in_msgs, time_from_data, trial_from_data, participant_from_data, source_from_data = loader(data)
        # check for errors in the values
        successful, all_mgs_and_triples, combined_error_messages = lwh.process_measurement_groups(
            vals_to_load_in_msgs)
        return successful, all_mgs_and_triples, combined_error_messages
    else:
        return False, [], "Loader Not Found"


@pytest.fixture()
def walking_test1() -> ty.DataToLoad:
    data = {
        'visit-date': 22333330,
        'visit-number': 'visit3',
        'wb_id': "fred",
        'Turn_Id': 2345,
        'Turn_Start_SO': 12.5,
        'Turn_End_SO': 123.3,
        'Turn_Duration_SO': 103.0,
        'Turn_PeakAngularVelocity_SO': 99.9   # ,
        # 'drugs': [{'drug': 'asprin', 'dose': 10}, {'drug': 'calpol', 'dose': 30}, {'drug': 'clopidogrel', 'dose': 20}]
    }
    return data


@pytest.fixture()
def fn_mapper() -> Dict[str, Callable[[ty.DataToLoad], ty.LoaderResult]]:
    """
    maps from the event_type used in e-SC (json script) to the function used by insertMeasurementGroup
    to add the measurements into the Data Warehouse
    """
    return {
        "walking": walking_loader,
        "ex3a": ex3a_loader,
        "ex3b": ex3b_loader
    }


#  @pytest.fixture()
def walking_loader(data: ty.DataToLoad) -> ty.LoaderResult:
    """
    # measurement groups example
    :param data: distionary mapping fields to values
    :return: vals to load, time, trial, participant, source
    """
    turn_group: ty.MeasurementGroup = 39

    turn_group_instance: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
        [(turn_group,
         [lwh.mk_datetime_from_epoch_in_ms(370, data, 'visit-date'),
          lwh.mk_string(371, data, 'visit-number'),
          lwh.mk_string(1839, data, 'wb_id'),
          lwh.mk_int(1843, data, 'Turn_Id'),
          lwh.mk_real(1844, data, 'Turn_Start_SO'),
          lwh.mk_real(1845, data, 'Turn_End_SO'),
          lwh.mk_real(1846, data, 'Turn_Duration_SO'),
          lwh.mk_real(1847, data, 'Turn_PeakAngularVelocity_SO')])]
    #  drug_group_instances: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
    #      lwh.load_list(data, 'drugs', drugs_loader, turn_group)
    return turn_group_instance, None, None, None, None


@pytest.fixture()
def ex3a() -> ty.DataToLoad:
    data = {
        'Int': 4,
        'Real': 5.45,
        'Text': 'Test Data',
        'DateTime': datetime.datetime.now(),
        'Bool': 1,
        'Nominal': 'First',
        'NominalfromId': 1,
        'Ordinal': 'Two',
        'OrdinalfromId': 2,
        'BoundedInt': 5,
        'BoundedReal': 8.6,
        'BoundedDateTime': datetime.datetime.now(),
        'External': 'External Data',
        'SplitEnum': ['1st', '3rd'],
        'OptionalInt': 4,
        'OptionalReal': 5.45,
        'OptionalText': 'Test Data',
        'OptionalDateTime': datetime.datetime.now(),
        'OptionalBool': 1,
        'OptionalNominal': 'First',
        'OptionalNominalfromId': 1,
        'OptionalOrdinal': 'One',
        'OptionalOrdinalfromId': 2,
        'OptionalBoundedInt': 5,
        'OptionalBoundedReal': 8.6,
        'OptionalBoundedDateTime': datetime.datetime.now(),
        'OptionalExternal': 'External Data',
        'OptionalSplitEnum': ['Deux', 'Trois']
    }
    return data


@pytest.fixture()
def ex3b() -> ty.DataToLoad:
    data = {
        'Int': 4,
        'Real': 5.45,
        'Text': 'Test Data',
        'DateTime': datetime.datetime.now(),
        'Bool': 1,
        'Nominal': 'First',
        'NominalfromId': 1,
        'Ordinal': 'Two',
        'OrdinalfromId': 2,
        'BoundedInt': 5,
        'BoundedReal': 8.6,
        'BoundedDateTime': datetime.datetime.now(),
        'External': 'External Data',
        'SplitEnum': ['1st', '3rd'],
        'OptionalInt': 4,
        'OptionalReal': 5.45,
        'OptionalText': 'Test Data',
        'OptionalDateTime': datetime.datetime.now(),
        'OptionalBool': 1,
        'OptionalNominal': 'First',
        'OptionalNominalfromId': 1,
        'OptionalOrdinal': 'One',
        'OptionalOrdinalfromId': 2,
        'OptionalBoundedInt': 5,
        'OptionalBoundedReal': 8.6,
        'OptionalBoundedDateTime': datetime.datetime.now(),
        'OptionalExternal': 'External Data',
        'OptionalSplitEnum': ['Deux', 'Trois'],
        'drugs': [{'drug': 'asprin', 'dose': 10}, {'drug': 'calpol', 'dose': 30}, {'drug': 'clopidogrel', 'dose': 20}]
    }
    return data


def ex3a_loader(data: ty.DataToLoad) -> ty.LoaderResult:
    ex3_mg_id: ty.MeasurementGroup = 120
    ex3_mgi = [(ex3_mg_id,
                [
                    lwh.mk_int(410, data, 'Int'),
                    lwh.mk_real(411, data, 'Real'),
                    lwh.mk_string(412, data, 'Text'),
                    lwh.mk_datetime(413, data, 'DateTime'),
                    lwh.mk_boolean(414, data, 'Bool'),
                    lwh.mk_nominal(415, data, 'Nominal', {'First': 0, 'Second': 1}),
                    lwh.mk_nominal_from_id_with_id_check(416, data, 'NominalfromId', [0, 1]),
                    lwh.mk_ordinal(417, data, 'Ordinal', {'One': 0, 'Two': 1}),
                    lwh.mk_ordinal_from_id_with_id_check(418, data, 'OrdinalfromId', [0, 1, 2]),
                    lwh.mk_bounded_int(419, data, 'BoundedInt'),
                    lwh.mk_bounded_real(420, data, 'BoundedReal'),
                    lwh.mk_bounded_datetime(421, data, 'BoundedDateTime'),
                    lwh.mk_external(422, data, 'External'),
                    lwh.split_enum([436, 437, 438], data, 'SplitEnum', ['1st', '2nd', '3rd']),
                    lwh.mk_optional_int(423, data, 'OptionalInt'),
                    lwh.mk_optional_real(424, data, 'OptionalReal'),
                    lwh.mk_optional_string(425, data, 'OptionalText'),
                    lwh.mk_optional_datetime(426, data, 'OptionalDateTime'),
                    lwh.mk_optional_boolean(427, data, 'OptionalBool'),
                    lwh.mk_optional_nominal_from_dict(428, data, 'OptionalNominal', {'First': 0, 'Second': 1}),
                    lwh.mk_optional_nominal_from_id_with_id_check(429, data, 'OptionalNominalfromId', [0, 1, 2, 3, 4]),
                    lwh.mk_optional_ordinal_from_dict(430, data, 'OptionalOrdinal', {'One': 0, 'Two': 1}),
                    lwh.mk_optional_ordinal_from_id_with_id_check(431, data, 'OptionalOrdinalfromId', [0, 1, 2, 3]),
                    lwh.mk_optional_bounded_int(432, data, 'OptionalBoundedInt'),
                    lwh.mk_optional_bounded_real(433, data, 'OptionalBoundedReal'),
                    lwh.mk_optional_bounded_datetime(434, data, 'OptionalBoundedDateTime'),
                    lwh.mk_optional_external(435, data, 'OptionalExternal'),
                    lwh.split_optional_enum([439, 440, 441], data, 'OptionalSplitEnum', ['Un', 'Deux', 'Trois'])
                ]
                )]
    # drug_group_instances: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
    #     lwh.load_list(data, 'drugs', drugs_loader, ex3_mg_id)
    return ex3_mgi, None, None, None, None


def ex3b_loader(data: ty.DataToLoad) -> ty.LoaderResult:
    ex3_mg_id: ty.MeasurementGroup = 120
    ex3_mgi = [(ex3_mg_id,
                [
                    lwh.mk_int(410, data, 'Int'),
                    lwh.mk_real(411, data, 'Real'),
                    lwh.mk_string(412, data, 'Text'),
                    lwh.mk_datetime(413, data, 'DateTime'),
                    lwh.mk_boolean(414, data, 'Bool'),
                    lwh.mk_nominal(415, data, 'Nominal', {'First': 0, 'Second': 1}),
                    lwh.mk_nominal_from_id_with_id_check(416, data, 'NominalfromId', [0, 1]),
                    lwh.mk_ordinal(417, data, 'Ordinal', {'One': 0, 'Two': 1}),
                    lwh.mk_ordinal_from_id_with_id_check(418, data, 'OrdinalfromId', [0, 1, 2]),
                    lwh.mk_bounded_int(419, data, 'BoundedInt'),
                    lwh.mk_bounded_real(420, data, 'BoundedReal'),
                    lwh.mk_bounded_datetime(421, data, 'BoundedDateTime'),
                    lwh.mk_external(422, data, 'External'),
                    lwh.split_enum([436, 437, 438], data, 'SplitEnum', ['1st', '2nd', '3rd']),
                    lwh.mk_optional_int(423, data, 'OptionalInt'),
                    lwh.mk_optional_real(424, data, 'OptionalReal'),
                    lwh.mk_optional_string(425, data, 'OptionalText'),
                    lwh.mk_optional_datetime(426, data, 'OptionalDateTime'),
                    lwh.mk_optional_boolean(427, data, 'OptionalBool'),
                    lwh.mk_optional_nominal_from_dict(428, data, 'OptionalNominal', {'First': 0, 'Second': 1}),
                    lwh.mk_optional_nominal_from_id_with_id_check(429, data, 'OptionalNominalfromId', [0, 1, 2, 3, 4]),
                    lwh.mk_optional_ordinal_from_dict(430, data, 'OptionalOrdinal', {'One': 0, 'Two': 1}),
                    lwh.mk_optional_ordinal_from_id_with_id_check(431, data, 'OptionalOrdinalfromId', [0, 1, 2, 3]),
                    lwh.mk_optional_bounded_int(432, data, 'OptionalBoundedInt'),
                    lwh.mk_optional_bounded_real(433, data, 'OptionalBoundedReal'),
                    lwh.mk_optional_bounded_datetime(434, data, 'OptionalBoundedDateTime'),
                    lwh.mk_optional_external(435, data, 'OptionalExternal'),
                    lwh.split_optional_enum([439, 440, 441], data, 'OptionalSplitEnum', ['Un', 'Deux', 'Trois'])
                ]
                )]
    drug_group_instances: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
        lwh.load_list(data, 'drugs', drugs_loader, ex3_mg_id)
    return ex3_mgi + drug_group_instances, None, None, None, None


def drugs_loader(data: ty.DataToLoad) -> ty.LoaderResult:
    """
    @param data:
    @type data:
    @return:
    @rtype:
    """
    drug_mg_id: ty.MeasurementGroup = 40
    drug_mgi = [(drug_mg_id,
                 [lwh.mk_string(400, data, 'drug'),
                  lwh.mk_int(401, data, 'dose')]
                 )]
    return drug_mgi, None, None, None, None


def test_walking_test(walking_test1, fn_mapper):
    assert loader_test(fn_mapper, "walking", walking_test1)[0]


def test_ex3a_test(ex3a, fn_mapper):
    assert loader_test(fn_mapper, "ex3a", ex3a)[0]
