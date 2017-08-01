
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
import thrive.rollback_handler as trh
from test.utils.utils import make_tempfile, tempfile_write


class TestRollbackHandler(unittest.TestCase):
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

        self.ptns = ["2016/08/%d/00/0" % d for d in range(1, 10)]
        self.ptn_file_contents = "\n".join(self.ptns)

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

    def test_execute_VerticaManager_clone_schema_call(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        vmgr = self.mock_vtica.return_value
        vmgr.clone_schema.assert_called_with(cv, cv, cv, "%s__rollback__" % cv)

    def test_execute_HdfsManager_path_exists_calls(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        hdfs_paths = ["%s/%s" % (cv, ptn) for ptn in self.ptns]
        path_exist_calls = [mock.call(s) for s in hdfs_paths]
        hdfs_mgr = self.mock_hdfs.return_value
        self.assertEqual(path_exist_calls, hdfs_mgr.path_exists.call_args_list)

    @mock.patch("thrive.load_handler.LoadHandler.vload_copy")
    def test_execute_LoadHandler_vload_copy_direct_calls(self, mock_vcopy):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        src_schema, src_table, rb_schema, rb_table = cv, cv, cv, "%s__rollback__" % cv
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        copy_calls = [mock.call(p, rb_schema, rb_table, mode="direct")
                      for p in self.ptns]
        mock_vcopy.assert_has_calls(copy_calls)

    def test_execute_VerticaManager_rollback_calls(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        src_schema, src_table, rb_schema, rb_table = cv, cv, cv, "%s__rollback__" % cv
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        rollback_calls = [mock.call(src_schema, src_table, rb_schema, rb_table,
                                    rkey=cv)
                          for p in self.ptns]
        vmgr = self.mock_vtica.return_value
        vmgr.rollback.assert_has_calls(rollback_calls)

    def test_execute_VerticaManager_truncate_calls(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        src_schema, src_table, rb_schema, rb_table = cv, cv, cv, "%s__rollback__" % cv
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        truncate_calls = [mock.call(rb_schema, rb_table) for p in self.ptns]
        vmgr = self.mock_vtica.return_value
        vmgr.truncate.assert_has_calls(truncate_calls)

    def test_execute_HdfsManager_rmdir_calls(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        hdfs_paths = ["%s/%s" % (cv, ptn) for ptn in self.ptns]
        rmdir_calls = [mock.call(s) for s in hdfs_paths]
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.rmdir.assert_has_calls(rmdir_calls)

    def test_execute_HiveManager_drop_partition_calls(self):
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        ptn_strs = ["year=%s, month=%s, day=%s, hour=%s, part=%s" \
                    % tuple(p.split("/")) for p in self.ptns]
        drop_ptn_calls = [mock.call(ps) for ps in ptn_strs]
        hive_mgr = self.mock_hive.return_value
        hive_mgr.drop_partition.assert_has_calls(drop_ptn_calls)

    def test_execute_MetadataManager_delete_partition_calls(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        md_delete_calls = [mock.call(cv, mdcolname="hive_last_partition", mdcolvalue=p)
                           for p in self.ptns]
        md_mgr = self.mock_mm.return_value
        md_mgr.delete.assert_has_calls(md_delete_calls)

    def test_execute_VerticaManager_drop_table_call(self):
        cv = self.config_value
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        rb_schema, rb_table = cv, "%s__rollback__" % cv
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        rh.execute()
        vmgr = self.mock_vtica.return_value
        vmgr.truncate.assert_called_with(rb_schema, rb_table)

    def test_execute_exception(self):
        tf = make_tempfile()
        tempfile_write(tf, self.ptn_file_contents)
        vmgr = self.mock_vtica.return_value
        vmgr.truncate.side_effect = Exception()
        rh = trh.RollbackHandler(datacfg_file="foo", envcfg_file="bar",
                                 resources_file="baz.zip", partitions_file=tf.name)
        with self.assertRaises(Exception):
            rh.execute()
