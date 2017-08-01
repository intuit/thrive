
# Copyright 2016 Intuit
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import ConfigParser
import thrive.config_loader
from thrive.exceptions import ConfigLoaderException


class TestConfigLoader(unittest.TestCase):
    def test_config_loader_nofile(self):
        with self.assertRaises(thrive.config_loader.ConfigLoaderException):
            clh = thrive.config_loader.ConfigLoader("")

    def test_config_loader_parse_error(self):
        config_file = "test/testdata/invalid_config.cfg"
        with self.assertRaises(ConfigLoaderException):
            clh = thrive.config_loader.ConfigLoader(config_file)

    def test_config_loader_get_config(self):
        config_file = "test/testdata/valid_config.cfg"
        clh = thrive.config_loader.ConfigLoader(config_file)
        self.assertEqual(clh.get_config("main", "test_get_config"), "Success")

    def test_config_loader_bad_config_item(self):
        config_file = "test/testdata/valid_config.cfg"
        clh = thrive.config_loader.ConfigLoader(config_file)
        with self.assertRaises(ConfigLoaderException):
            clh.get_config("main","non_existent_item")

    def test_config_loader_header(self):
        config_file = "test/testdata/config_header.cfg"
        clh = thrive.config_loader.ConfigLoader(config_file)
        self.assertDictEqual({"key1": "val1", "key2": "val2"},
                             dict(clh.parser.items("main")))

    def test_config_loader_noheader(self):
        config_file = "test/testdata/config_noheader.cfg"
        clh = thrive.config_loader.ConfigLoader(config_file)
        self.assertDictEqual({"key1": "val1", "key2": "val2"},
                             dict(clh.parser.items("main")))
