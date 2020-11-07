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

from unittest import TestCase

from data_warehouse_client import transform_result_format
import datetime


class TestTransformResultFormat(TestCase):
    def test_form_measurements(self):
        rid = 101
        time = datetime.datetime.now()
        study = 102
        trial = 103
        measurement_group = 104
        group_inst = 99
        measurement_type = 105
        measurement_name = "Measurement Name"
        participant = 106
        int_type = 0
        real_type = 1
        val_type = int_type
        val_integer = 108
        val_real = 109.00
        val_text = "Text Value"
        val_datetime = time
        category_name = "A Category"
        ipr1 = [rid, time, study, participant, measurement_type, measurement_name, measurement_group, group_inst, trial,
                int_type, val_integer, val_real, val_text, val_datetime, category_name]
        ipr2 = [rid, time, study, participant, measurement_type, measurement_name, measurement_group, group_inst, trial,
                real_type, val_integer, val_real, val_text, val_datetime, category_name]
        ip = [ipr1, ipr2]
        opr1 = [rid, time, study, participant, measurement_type, measurement_name, measurement_group, group_inst,
                trial, int_type, val_integer]
        opr2 = [rid, time, study, participant, measurement_type, measurement_name, measurement_group, group_inst,
                trial, real_type, val_real]
        op = [opr1, opr2]
        self.assertEqual(op, transform_result_format.form_measurements(ip))
