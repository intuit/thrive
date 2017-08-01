
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
import mock
import thrive.thrive_handler as tth


class TestThriveHandler(unittest.TestCase):
    def setUp(self):
        self.config_loader_patcher = mock.patch("thrive.thrive_handler.ConfigLoader")
        self.mock_config_loader = self.config_loader_patcher.start()
        self.mcl = self.mock_config_loader.return_value
        self.mcl.get_config.return_value = self.config_value = "foo"

        self.md_patcher = mock.patch("thrive.thrive_handler.MetadataManager")
        self.mock_mm = self.md_patcher.start()

        self.hdfs_patcher = mock.patch("thrive.thrive_handler.HdfsManager")
        self.mock_hdfs = self.hdfs_patcher.start()

        self.hive_patcher = mock.patch("thrive.thrive_handler.HiveManager")
        self.mock_hive = self.hive_patcher.start()

        self.vertica_patcher = mock.patch("thrive.thrive_handler.VerticaManager")
        self.mock_vtica = self.vertica_patcher.start()

        self.shell_patcher = mock.patch("thrive.thrive_handler.ShellExecutor")
        self.mock_shell = self.shell_patcher.start()

        self.uuid_patcher = mock.patch("thrive.thrive_handler.uuid")
        self.mock_uuid = self.uuid_patcher.start()
        self.mock_uuid.uuid1.return_value = "12345"

        self.th = tth.ThriveHandler(datacfg_file="foo", envcfg_file="bar",
                                    resources_file="baz.zip")

    def tearDown(self):
        self.config_loader_patcher.stop()
        self.md_patcher.stop()
        self.hdfs_patcher.stop()
        self.hive_patcher.stop()
        self.vertica_patcher.stop()
        self.shell_patcher.stop()

    def test_init_ConfigLoader_instantiations(self):
        config_loader_calls = [mock.call("foo"), mock.call("bar")]
        self.mock_config_loader.assert_has_calls(config_loader_calls)

    def test_init_MetadataManager_instantiation(self):
        cv = self.config_value
        credtypes = ["dbtype", "dbhost", "dbport", "dbuser", "dbpass", "dbname"]
        md_credentials = dict([(cred, cv) for cred in credtypes])
        self.mock_mm.assert_called_with(credentials=md_credentials)

    def test_init_ShellExecutor_instantiation(self):
        self.mock_shell.assert_called_with()

    def test_init_HdfsManager_instantiation(self):
        cv = self.config_value
        self.mock_hdfs.assert_called_with()
        self.mock_hive.assert_called_with(db=cv, table=cv)

    def test_init_VerticaManager_instantiation(self):
        cv = self.config_value
        vconfigs = ["vertica_db", "vertica_vsql_path", "vertica_krb_svcname",
                    "vertica_krb_host", "vertica_host", "vertica_port",
                    "vertica_user"]
        vconnection_info = dict((key, cv) for key in vconfigs)
        self.mock_vtica.assert_called_with(vconnection_info)

    def test_init_load_id_generation(self):
        self.mock_uuid.uuid1.assert_called_with()

    def test_init_load_id_val(self):
        self.assertTrue(self.th.load_id, "12345")

    def test_init_exception(self):
        with self.assertRaises(tth.ThriveHandlerException):
            tth.ThriveHandler(datacfg_file="foo", envcfg_file="bar",
                              resources_file="baz.xxx")

    def test_get_config_default(self):
        self.assertTrue(self.th.get_config("abc"), self.config_value)

    def test_get_config_data(self):
        self.assertTrue(self.th.get_config("abc", configtype="data"),
                        self.config_value)

    def test_get_config_env(self):
        self.assertTrue(self.th.get_config("abc", configtype="env"),
                        self.config_value)

    def test_get_config_exception(self):
        with self.assertRaises(tth.ThriveHandlerException):
            self.th.get_config("abc", configtype="foo")