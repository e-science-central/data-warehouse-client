import pytest
from data_warehouse_client import load_warehouse_helpers as lwh
#  from data_warehouse_client import import_with_checks as iwc
from data_warehouse_client.type_definitions import Bounds
from datetime import datetime


@pytest.fixture()
def bounds_ex1() -> Bounds:
    int_bounds = {1: {'minval': 0, 'maxval': 100}}
    real_bounds = {999: {'minval': 0.0, 'maxval': 100.0}}
    start_date = datetime(1900, 1, 1)
    end_date = datetime(2100, 1, 1)
    datetime_bounds = {3: {'minval': start_date, 'maxval': end_date}}
    category_ids = {4: [1, 2, 3, 4, 5]}
    inverse_categy_ids = {5: {'first': 1, 'second': 2, 'third': 3}}
    return int_bounds, real_bounds, datetime_bounds, category_ids, inverse_categy_ids


def test_load_warehouse_helpers_examples(bounds_ex1):
    # bool
    success, triple, error_msg = lwh.mk_boolean(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 4, 0)]

    success, triple, error_msg = lwh.mk_boolean(999, {'test_field': '0'}, 'test_field')
    assert success and triple == [(999, 4, 0)]

    success, triple, error_msg = lwh.mk_boolean(999, {'test_field': 'wrong'}, 'test_field')
    assert not success and triple == []

    # optional bool
    success, triple, error_msg = lwh.mk_optional_boolean(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 4, 0)]

    success, triple, error_msg = lwh.mk_optional_boolean(999, {'test2_field': 0}, 'test_field')
    assert success and triple == []

    success, triple, error_msg = lwh.mk_optional_boolean(999, {'test_field': 999}, 'test_field')
    assert not success and triple == []

    # real
    success, triple, error_msg = lwh.mk_real(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 1, 0.0)]

    success, triple, error_msg = lwh.mk_real(999, {'test_field': '0'}, 'test_field')
    assert not success and triple == []

    success, triple, error_msg = lwh.mk_real(999, {'test_field': 0.5}, 'test_field')
    assert success and triple == [(999, 1, 0.5)]

    # optional real
    success, triple, error_msg = lwh.mk_optional_real(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 1, 0.0)]

    success, triple, error_msg = lwh.mk_optional_real(999, {'test_field': '0'}, 'test_field')
    assert not success and triple == []

    success, triple, error_msg = lwh.mk_optional_real(999, {'test_field': 0.5}, 'test_field')
    assert success and triple == [(999, 1, 0.5)]

    success, triple, error_msg = lwh.mk_optional_real(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 1, 0.0)]

    success, triple, error_msg = lwh.mk_optional_real(999, {'test_field2': '0'}, 'test_field')
    assert success and triple == []

    success, triple, error_msg = lwh.mk_optional_real(999, {'test_field2': 0.5}, 'test_field')
    assert success and triple == []

    # int
    success, triple, error_msg = lwh.mk_int(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 0, 0)]

    success, triple, error_msg = lwh.mk_int(999, {'test_field': '0'}, 'test_field')
    assert not success and triple == []

    success, triple, error_msg = lwh.mk_int(999, {'test_field': 0.5}, 'test_field')
    assert not success and triple == []

    # bounded real
    success, triple, error_msg = lwh.mk_bounded_real(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 8, 0.0)]

    success, triple, error_msg = lwh.mk_bounded_real(999, {'test_field': '0'}, 'test_field')
    assert not success and triple == []

    success, triple, error_msg = lwh.mk_bounded_real(999, {'test_field': 0.5}, 'test_field')
    assert success and triple == [(999, 8, 0.5)]

    # optional bounded real
    success, triple, error_msg = lwh.mk_optional_bounded_real(999, {'test_field': 0}, 'test_field')
    assert success and triple == [(999, 8, 0.0)]

    success, triple, error_msg = lwh.mk_optional_bounded_real(999, {'test_field': '0'}, 'test_field')
    assert not success and triple == []

    success, triple, error_msg = lwh.mk_optional_bounded_real(999, {'test_field': 0.5}, 'test_field')
    assert success and triple == [(999, 8, 0.5)]

    success, triple, error_msg = lwh.mk_optional_bounded_real(999, {'test_field2': 0}, 'test_field')
    assert success and triple == []

    success, triple, error_msg = lwh.mk_optional_bounded_real(999, {'test_field2': '0'}, 'test_field')
    assert success and triple == []

    success, triple, error_msg = lwh.mk_optional_bounded_real(999, {'test_field2': 0.5}, 'test_field')
    assert success and triple == []


"""
print('\n')
print('iwc test')
print('bools')
print(0, iwc.load_boolean(999, {'test_field': 0}, 'test_field'))
print('0', iwc.load_boolean(999, {'test_field': '0'}, 'test_field'))
print('wrong', iwc.load_boolean(999, {'test_field': 'wrong'}, 'test_field'))
print('\n')
print('optional bools')
print(1, iwc.load_optional_boolean(999, {'test_field': 1}, 'test_field'))
print(0, iwc.load_optional_boolean(999, {'test2_field': 0}, 'test_field'))
print(999, iwc.load_optional_boolean(999, {'test_field': 999}, 'test_field'))
print('\n')
print('real')
print(0, iwc.load_real(999, {'test_field': 0}, 'test_field'))
print('0', iwc.load_real(999, {'test_field': '0'}, 'test_field'))
print(0.5, iwc.load_real(999, {'test_field': 0.5}, 'test_field'))
print('\n')
print('optional reals')
print(0, iwc.load_optional_real(999, {'test_field': 0}, 'test_field'))
print('0', iwc.load_optional_real(999, {'test_field': '0'}, 'test_field'))
print(0.5, iwc.load_optional_real(999, {'test_field': 0.5}, 'test_field'))
print('\n')
print('optional reals')
print(0, iwc.load_optional_real(999, {'test_field2': 0}, 'test_field'))
print('0', iwc.load_optional_real(999, {'test_field2': '0'}, 'test_field'))
print(0.5, iwc.load_optional_real(999, {'test_field2': 0.5}, 'test_field'))
print('\n')
print('int')
print(0, iwc.load_int(999, {'test_field': 0}, 'test_field'))
print('0', iwc.load_int(999, {'test_field': '0'}, 'test_field'))
print(0.5, iwc.load_int(999, {'test_field': 0.5}, 'test_field'))
print('\n')
print('bounded real')
print(0, iwc.load_bounded_real(999, {'test_field': 0}, 'test_field', bounds_ex1()))
print('0', iwc.load_bounded_real(999, {'test_field': '0'}, 'test_field', bounds_ex1()))
print(0.5, iwc.load_bounded_real(999, {'test_field': 0.5}, 'test_field', bounds_ex1()))
print('\n')
print('optional bounded real')
print(0, iwc.load_optional_bounded_real(999, {'test_field': 0}, 'test_field', bounds_ex1()))
print('0', iwc.load_optional_bounded_real(999, {'test_field': '0'}, 'test_field', bounds_ex1()))
print(0.5, iwc.load_optional_bounded_real(999, {'test_field': 0.5}, 'test_field', bounds_ex1()))
print('\n')
print('optional bounded real')
print(0, iwc.load_optional_bounded_real(999, {'test_field2': 0}, 'test_field', bounds_ex1()))
print('0', iwc.load_optional_bounded_real(999, {'test_field2': '0'}, 'test_field', bounds_ex1()))
print(0.5, iwc.load_optional_bounded_real(999, {'test_field2': 0.5}, 'test_field', bounds_ex1()))
"""
