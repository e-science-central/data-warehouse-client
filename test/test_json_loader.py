import unittest

from data_warehouse_client import json_loader


class TestJsonLoader(unittest.TestCase):
    def test_mk_01s(self):
        self.assertEqual(0, json_loader.mk_01("N"))
        self.assertEqual(1, json_loader.mk_01("Y"))
