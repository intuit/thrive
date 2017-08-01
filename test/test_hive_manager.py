
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
import thrive.hive_manager as thm
import thrive.shell_executor as tse
from thrive.exceptions import HiveManagerException

class TestHiveManager(unittest.TestCase):
    def setUp(self):
        self.db = "dbfoo"
        self.table = "tablefoo"
        self.hm = thm.HiveManager(self.db, self.table)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_execute(self, mock_shell_exec):
        stmt = "select * from foo;"
        self.hm.execute(stmt)
        mock_shell_exec.assert_called_with(stmt, as_shell=True, splitcmd=False)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_execute_exception(self, mock_shell_exec):
        mock_shell_exec.side_effect = tse.ShellException()
        with self.assertRaises(HiveManagerException):
            self.hm.execute("select * from foo;")

    @mock.patch("thrive.hive_manager.HiveManager.execute")
    def test_drop_partition(self, mock_hexec):
        ptn_str = "/foo/year=2016/month=08/day=12/hour=14/part=0"
        dropcmd = ''' hive -e "use %s; alter table %s drop if exists partition (%s)"''' \
                  % (self.db, self.table, ptn_str)
        self.hm.drop_partition(ptn_str)
        mock_hexec.assert_called_with(dropcmd)

    @mock.patch("thrive.hive_manager.HiveManager.execute")
    def test_drop_partition_exception(self, mock_hexec):
        mock_hexec.side_effect = tse.ShellException()
        with self.assertRaises(HiveManagerException):
            self.hm.drop_partition("foo")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    @mock.patch("thrive.hive_manager.HdfsManager.path_exists")
    @mock.patch("thrive.hive_manager.HiveManager.check_partition")
    def test_create_partition_hdfs_path_exists_partition_does_not_exist(self, mock_chk_ptn, mock_pth_exists, mock_safe_exec):
        ptn_path = "/foo/2016/08/12/14/0"
        mock_pth_exists.return_value = True
        mock_chk_ptn.return_value = False
        mock_safe_exec.return_value = tse.ShellResult(0, "", "")
        self.hm.create_partition(ptn_path)
        mock_pth_exists.assert_called_with(ptn_path)

    @mock.patch("thrive.hive_manager.HdfsManager.path_exists")
    def test_create_partition_hdfs_path_does_not_exist(self, mock_pth_exists):
        mock_pth_exists.return_value = False
        with self.assertRaises(HiveManagerException):
            self.hm.create_partition("foo")

    @mock.patch("thrive.hive_manager.HdfsManager.path_exists")
    @mock.patch("thrive.hive_manager.HiveManager.check_partition")
    def test_create_partition_hdfs_path_exists_partition_exists(self, mock_chk_ptn, mock_pth_exists):
        mock_pth_exists.return_value = True
        mock_chk_ptn.return_value = True
        with self.assertRaises(HiveManagerException):
            self.hm.create_partition("/foo/2016/08/12/14/0")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    @mock.patch("thrive.hive_manager.HdfsManager.path_exists")
    @mock.patch("thrive.hive_manager.HiveManager.check_partition")
    def test_create_partition_exception(self,  mock_chk_ptn, mock_pth_exists, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        mock_pth_exists.return_value = True
        mock_chk_ptn.return_value = False
        with self.assertRaises(HiveManagerException):
            self.hm.create_partition("/foo/2016/08/12/14/0")

    def test_create_table(self):
        pass

    def test_create_db(self):
        pass

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_drop_db(self, mock_shell_exec):
        self.hm.drop_db()
        dropcmd = ''' hive -e "drop database if exists %s cascade;" ''' % (self.db)
        mock_shell_exec.assert_called_with(dropcmd, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_drop_db_exception(self, mock_shell_exec):
        mock_shell_exec.side_effect = tse.ShellException()
        with self.assertRaises(HiveManagerException):
            self.hm.drop_db()

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_drop_table(self, mock_shell_exec):
        self.hm.drop_table()
        dropcmd = ''' hive -e "use %s; drop table %s" ''' %(self.db, self.table)
        mock_shell_exec.assert_called_with(dropcmd, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_drop_table_exception(self, mock_shell_exec):
        mock_shell_exec.side_effect = tse.ShellException()
        with self.assertRaises(HiveManagerException):
            self.hm.drop_table()

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_check_partition_ptn_exists(self, mock_safe_exec):
        ptn_str = "/foo/2016/08/12/14/0"
        mock_safe_exec.return_value = tse.ShellResult(0, ptn_str, "")
        self.assertTrue(self.hm.check_partition(ptn_str))

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_check_partition_ptn_doesnt_exist(self, mock_safe_exec):
        ptn_str = "/foo/2016/08/12/14/0"
        mock_safe_exec.return_value = tse.ShellResult(0, "/bar/2016/08/12/14/0", "")
        self.assertFalse(self.hm.check_partition(ptn_str))

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_check_partition_exception(self, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        with self.assertRaises(HiveManagerException):
            self.hm.check_partition("/bar/2016/08/12/14/0")
