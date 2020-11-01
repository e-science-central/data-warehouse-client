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

from data_warehouse_client import json_loader


class TestJsonLoader(TestCase):
    def test_mk_01s(self):
        self.assertEqual(0, json_loader.mk_01("N"))
        self.assertEqual(1, json_loader.mk_01("Y"))

    def test_other_method(self):
        self.assertEqual(0, json_loader.mk_01("N"))
        self.assertEqual(1, json_loader.mk_01("Y"))