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
import datetime
import import_with_checks as iwc
import load_data
import data_warehouse

# see https://realpython.com/pytest-python-testing/


@pytest.fixture()
def walking_test_1() -> ty.DataToLoad:
    data = {
        'visit-date': datetime.datetime.now(),
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
def mk_dw_handle():
    data_warehouse_handle = data_warehouse.DataWarehouse("fenland-dw-credentials-full-access.json", "osm_dw")
    return data_warehouse_handle


@pytest.fixture()
def test_all_example() -> ty.DataToLoad:
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


def check_all_loader(data: ty.DataToLoad,
                     int_bounds: Dict[ty.MeasurementType, Dict[str, int]],
                     real_bounds: Dict[ty.MeasurementType, Dict[str, float]],
                     datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]],
                     category_id_map: Dict[ty.MeasurementType, List[int]],
                     category_value_map: Dict[ty.MeasurementType, Dict[str, int]]
                     ) -> ty.LoaderResult:
    test_all_mg_id: ty.MeasurementGroup = 50
    test_mgi = [(test_all_mg_id,
                [
                 iwc.load_int(410, data, 'Int'),
                 iwc.load_real(411, data, 'Real'),
                 iwc.load_string(412, data, 'Text'),
                 iwc.load_datetime(413, data, 'DateTime'),
                 iwc.load_boolean(414, data, 'Bool'),
                 iwc.load_nominal_from_value(415, data, 'NominalfromValue', category_value_map),
                 iwc.load_nominal_from_id(416, data, 'NominalfromId', category_id_map),
                 iwc.load_ordinal_from_value(417, data, 'OrdinalfromValue', category_value_map),
                 iwc.load_ordinal_from_id(418, data, 'OrdinalfromId', category_id_map),
                 iwc.load_bounded_int(419, data, 'BoundedInt', int_bounds),
                 iwc.load_bounded_real(420, data, 'BoundedReal', real_bounds),
                 iwc.load_bounded_datetime(421, data, 'BoundedDateTime', datetime_bounds),
                 iwc.load_external(422, data, 'External'),
                 iwc.load_set([423, 424, 425], data, 'SplitEnum', ['1st', '2nd', '3rd']),
                 iwc.load_optional_int(426, data, 'OptionalInt'),
                 iwc.load_optional_real(427, data, 'OptionalReal'),
                 iwc.load_optional_string(428, data, 'OptionalText'),
                 iwc.load_optional_datetime(429, data, 'OptionalDateTime'),
                 iwc.load_optional_boolean(430, data, 'OptionalBool'),
                 iwc.load_optional_nominal_from_value(431, data, 'OptionalNominal', category_value_map),
                 iwc.load_optional_nominal_from_id(432, data, 'OptionalNominalfromId', category_id_map),
                 iwc.load_optional_ordinal_from_value(433, data, 'OptionalOrdinal', category_value_map),
                 iwc.load_optional_ordinal_from_id(434, data, 'OptionalOrdinalfromId', category_id_map),
                 iwc.load_optional_bounded_int(435, data, 'OptionalBoundedInt', int_bounds),
                 iwc.load_optional_bounded_real(436, data, 'OptionalBoundedReal', real_bounds),
                 iwc.load_optional_bounded_datetime(437, data, 'OptionalBoundedDateTime', datetime_bounds),
                 iwc.load_optional_external(438, data, 'OptionalExternal'),
                 iwc.load_optional_set([439, 440, 441], data, 'OptionalSplitEnum', ['Un', 'Deux', 'Trois'])]
                 )]
    drug_group_instances: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
        iwc.load_list(data, 'drugs', drugs_loader, test_all_mg_id,
                      int_bounds, real_bounds, datetime_bounds, category_id_map, category_value_map)
    return test_mgi + drug_group_instances, None, None, None, None


def drugs_loader(data: ty.DataToLoad,
                 int_bounds: Dict[ty.MeasurementType, Dict[str, int]],
                 real_bounds: Dict[ty.MeasurementType, Dict[str, float]],
                 datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]],
                 category_id_map: Dict[ty.MeasurementType, List[int]],
                 category_value_map: Dict[ty.MeasurementType, Dict[str, int]]
                 ) -> ty.LoaderResult:
    drug_mg_id: ty.MeasurementGroup = 40
    drug_mgi = [(drug_mg_id,
                 [iwc.load_string(400, data, 'drug'),
                  iwc.load_int(401, data, 'dose')]
                 )]
    return drug_mgi, None, None, None, None


def walking_and_drugs_loader(data: ty.DataToLoad,
                             int_bounds: Dict[ty.MeasurementType, Dict[str, int]],
                             real_bounds: Dict[ty.MeasurementType, Dict[str, float]],
                             datetime_bounds: Dict[ty.MeasurementType, Dict[str, ty.DateTime]],
                             category_id_map: Dict[ty.MeasurementType, List[int]],
                             category_value_map: Dict[ty.MeasurementType, Dict[str, int]]) -> ty.LoaderResult:

    turn_group: ty.MeasurementGroup = 39

    turn_group_instance: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
        [(turn_group,
          [iwc.load_datetime(370, data, 'visit-date'),
           iwc.load_string(371, data, 'visit-code'),
           iwc.load_string(1839, data, 'wb_id'),
           iwc.load_int(1843, data, 'Turn_Id'),
           iwc.load_real(1844, data, 'Turn_Start_SO'),
           iwc.load_real(1845, data, 'Turn_End_SO'),
           iwc.load_real(1846, data, 'Turn_Duration_SO'),
           iwc.load_real(1847, data, 'Turn_PeakAngularVelocity_SO')])]
    drug_group_instances: List[Tuple[ty.MeasurementGroup, List[ty.LoadHelperResult]]] = \
        iwc.load_list(data, 'drugs', drugs_loader, turn_group,
                      int_bounds, real_bounds, datetime_bounds, category_id_map, category_value_map)
    return turn_group_instance+drug_group_instances, None, None, None, None


@pytest.fixture()
def fn_mapper() -> Dict[str, Callable[[ty.DataToLoad], ty.LoaderResult]]:
    """
    maps from the event_type used in e-SC (json script) to the function used by insertMeasurementGroup
    to add the measurements into the Data Warehouse
    """
    return {
        "walking_and_drugs": walking_and_drugs_loader   # ,
        #  "test_all": test_all_loader
    }


@pytest.fixture()
def test_study():
    return 999


def test_walking_test_1(mk_dw_handle, walking_test_1, fn_mapper, test_study):
    assert load_data.load_data(mk_dw_handle(), walking_test_1, "walking_and_drugs", fn_mapper, test_study)[0]


# def test_all(mk_dw_handle, test_all_example, fn_mapper, test_study):
#    assert load_data.load_data(mk_dw_handle(), test_all_example, "test_all", fn_mapper, test_study)[0]
