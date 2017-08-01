
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
import thrive.cleanup_handler as tch
import thrive.shell_executor as tse
import thrive.exceptions as thex


class TestCleanupHandler(unittest.TestCase):
    def setUp(self):
        self.config_loader_patcher = mock.patch("thrive.thrive_handler.ConfigLoader")
        self.mock_config_loader = self.config_loader_patcher.start()

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

        self.th_get_config_patcher = mock.patch("thrive.thrive_handler.ThriveHandler.get_config")
        self.mock_get_config = self.th_get_config_patcher.start()

        self.ch = tch.CleanupHandler(datacfg_file="foo", envcfg_file="bar",
                                     resources_file="baz.zip")

        self.config_value = "foo"
        self.mock_get_config.return_value = self.config_value

    def tearDown(self):
        self.config_loader_patcher.stop()
        self.md_patcher.stop()
        self.hdfs_patcher.stop()
        self.hive_patcher.stop()
        self.vertica_patcher.stop()
        self.shell_patcher.stop()
        self.th_get_config_patcher.stop()

    def test_cleanup_nfs(self):
        self.ch.execute()
        shexec = self.mock_shell.return_value
        shexec.safe_execute.assert_called_with("rm -rf %s" % self.config_value)

    def test_cleanup_nfs_exception(self):
        """
        Tests that shell exception is *not* raised by CleanupHandler
        """
        self.mock_shell.return_value.safe_execute.side_effect = tse.ShellException()
        try:
            self.ch.execute()
        except tse.ShellException:
            self.assertTrue(False)

    def test_cleanup_hdfs(self):
        self.ch.execute()
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.rmdir.assert_called_with(self.config_value)

    def test_cleanup_hdfs_exception(self):
        """
        Tests that shell exception is *not* raised by CleanupHandler
        """
        self.mock_hdfs.return_value.rmdir.side_effect = thex.HdfsManagerException()
        try:
            self.ch.execute()
        except thex.HdfsManagerException:
            self.assertTrue(False)

    def test_cleanup_hive(self):
        self.ch.execute()
        hive_mgr = self.mock_hive.return_value
        hive_mgr.drop_table.assert_called_with()

    def test_cleanup_hive_exception(self):
        """
        Tests that shell exception is *not* raised by CleanupHandler
        """
        self.mock_hive.return_value.drop_table.side_effect = thex.HiveManagerException()
        try:
            self.ch.execute()
        except thex.HiveManagerException:
            self.assertTrue(False)

    def test_cleanup_vertica(self):
        val = "true"
        self.mock_get_config.return_value = val
        self.ch.execute()
        vertica_mgr = self.mock_vtica.return_value
        calls = [mock.call(val, val), mock.call(val, val)]
        vertica_mgr.drop_table.assert_has_calls(calls)

    def test_cleanup_vertica_exception(self):
        """
        Tests that shell exception is *not* raised by CleanupHandler
        """
        self.mock_vtica.return_value.drop_table.side_effect = thex.VerticaManagerException()
        try:
            val = "true"
            self.mock_get_config.return_value = val
            self.ch.execute()
        except thex.VerticaManagerException:
            self.assertTrue(False)

    def test_cleanup_metadata(self):
        self.ch.execute()
        mm = self.mock_mm.return_value
        mm.purge.assert_called_with(self.config_value)

    def test_cleanup_metadata_exception(self):
        """
        Tests that shell exception is *not* raised by CleanupHandler
        """
        self.mock_mm.return_value.purge.side_effect = thex.MetadataManagerException()
        try:
            self.ch.execute()
        except thex.MetadataManagerException:
            self.assertTrue(False)





