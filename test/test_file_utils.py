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
from data_warehouse_client import file_utils


class TestFileUtils(TestCase):
    def test_process_sql_template(self):
        mappings = {"measurement_group": str(16), "study": str(4)}
        expected_result = "SELECT COUNT(*) FROM measurementtypetogroup WHERE measurementtypetogroup.measurementgroup = 16 AND measurementtypetogroup.study = 4;"
        self.assertEqual(expected_result, file_utils.process_sql_template("num_types_in_a_measurement_group.sql",
                                                                          mappings))
