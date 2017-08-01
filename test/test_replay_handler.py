
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
import thrive.replay_handler as trh
from test.utils.utils import make_tempfile, tempfile_write


class TestReplayHandler(unittest.TestCase):
    def setUp(self):
        self.config_loader_patcher = mock.patch("thrive.thrive_handler.ConfigLoader")
        self.mock_config_loader = self.config_loader_patcher.start()

        self.md_patcher = mock.patch("thrive.thrive_handler.MetadataManager")
        self.mock_mm = self.md_patcher.start()

        self.hdfs_patcher = mock.patch("thrive.thrive_handler.HdfsManager")
        self.mock_hdfs = self.hdfs_patcher.start()

        self.hive_patcher = mock.patch("thrive.thrive_handler.HiveManager")
        self.mock_hive = self.hive_patcher.start()

        self.vertica_patcher = mock.patch("thrive.thrive_handler.VerticaManager",
                                          autospec=True)
        self.mock_vtica = self.vertica_patcher.start()

        self.shell_patcher = mock.patch("thrive.thrive_handler.ShellExecutor")
        self.mock_shell = self.shell_patcher.start()

        self.th_get_config_patcher = mock.patch("thrive.thrive_handler.ThriveHandler.get_config")
        self.mock_get_config = self.th_get_config_patcher.start()

        self.load_handler_patcher = mock.patch("thrive.rollback_handler.LoadHandler")
        self.load_handler_patcher.start()

        self.oozie_patcher = mock.patch("thrive.load_handler.OozieManager")
        self.mock_oozie = self.oozie_patcher.start()

        self.newrelic_patcher = mock.patch("thrive.load_handler.NewRelicManager")
        self.mock_newrelic = self.newrelic_patcher.start()

        self.uuid_patcher = mock.patch("thrive.thrive_handler.uuid")
        self.mock_uuid = self.uuid_patcher.start()
        self.mock_uuid.uuid1.return_value = "12345"

        self.int_patcher = mock.patch("__builtin__.int")
        self.mock_int = self.int_patcher.start()

        self.float_patcher = mock.patch("__builtin__.float")
        self.mock_float = self.float_patcher.start()

        self.config_value = "foo"
        self.mock_get_config.return_value = self.config_value

        self.newdirs = ["d_201608%02d-1410" % d for d in range(1, 10)]
        self.replay_file_contents = "\n".join(self.newdirs)

    def tearDown(self):
        self.config_loader_patcher.stop()
        self.md_patcher.stop()
        self.hdfs_patcher.stop()
        self.hive_patcher.stop()
        self.vertica_patcher.stop()
        self.shell_patcher.stop()
        self.th_get_config_patcher.stop()
        self.load_handler_patcher.stop()
        self.oozie_patcher.stop()
        self.newrelic_patcher.stop()
        self.int_patcher.stop()
        self.float_patcher.stop()

    def test_get_newdirs(self):
        tf = make_tempfile()
        tempfile_write(tf, self.replay_file_contents)
        rh = trh.ReplayHandler(datacfg_file="foo", envcfg_file="bar",
                               resources_file="baz.zip", replaydirs_file=tf.name)
        self.assertEqual(rh.get_newdirs(), self.newdirs)

    @mock.patch("thrive.replay_handler.LoadHandler.execute")
    def test_execute(self, mock_lh_execute):
        tf = make_tempfile()
        tempfile_write(tf, self.replay_file_contents)
        rh = trh.ReplayHandler(datacfg_file="foo", envcfg_file="bar",
                               resources_file="baz.zip", replaydirs_file=tf.name)
        rh.execute()
        mock_lh_execute.assert_called_with(load_type="replay")

    def test_init_exception(self):
        with self.assertRaises(trh.ReplayHandlerException):
            rh = trh.ReplayHandler(datacfg_file="foo", envcfg_file="bar",
                                   resources_file="baz.zip",
                                   replaydirs_file="doesnotexist")
