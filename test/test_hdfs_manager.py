
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
import thrive.hdfs_manager as thm
import thrive.exceptions as thex
import thrive.shell_executor as tse
import datetime as dt


class TestHdfsManager(unittest.TestCase):
    def setUp(self):
        self.hm = thm.HdfsManager()
        self.hdfspath = "hdfsfoo/hdfsbar"
        self.lastdir = "d_20160812-1400"
        self.loadts = dt.datetime(2016, 8, 12, 14, 0)
        self.process_delay_hrs = 4
        self.returned_dirs = "\n".join(["d_20160812-%s" % i for i in range(1400, 1460, 10)])

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_makedir(self, mock_safe_execute):
        dirpath = "/foo/bar"
        self.hm.makedir(dirpath)
        mock_safe_execute.assert_called_with("hadoop fs -mkdir -p %s" % dirpath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_makedir_exception(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            self.hm.makedir("/foo/bar")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_rmdir(self, mock_safe_execute):
        dirpath = "/foo/bar"
        self.hm.rmdir(dirpath)
        mock_safe_execute.assert_called_with("hadoop fs -rm -r -skipTrash %s" % dirpath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_rmdir_exception(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            self.hm.rmdir("/foo/bar")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_putfile(self, mock_safe_execute):
        localpath = "/localfoo/localbar"
        self.hm.putfile(localpath, self.hdfspath)
        mock_safe_execute.assert_called_with("hadoop fs -put %s %s"
                                             % (localpath, self.hdfspath))

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_putfile_exception(self, mock_safe_execute):
        localpath = "/localfoo/localbar"
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            self.hm.putfile(localpath, self.hdfspath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_path_exists(self, mock_safe_execute):
        self.assertTrue(self.hm.path_exists(self.hdfspath))
        mock_safe_execute.assert_called_with("hadoop fs -test -e %s" % self.hdfspath,
                                             as_shell=True, splitcmd=False)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_path_exists_exception(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        self.assertFalse(self.hm.path_exists(self.hdfspath))

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_decompress(self, mock_safe_execute):
        srcpath = "/srcfoo/srcbar"
        dstpath = "/dstfoo/dstbar"
        self.hm.decompress(srcpath, dstpath)
        cmd = "hadoop fs -text %s | hadoop fs -put - %s" % (srcpath, dstpath)
        mock_safe_execute.assert_called_with(cmd, as_shell=True, splitcmd=False,
                                             verbose=False)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_decompress_exception(self, mock_safe_execute):
        srcpath = "/srcfoo/srcbar"
        dstpath = "/dstfoo/dstbar"
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            self.hm.decompress(srcpath, dstpath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_grantall(self, mock_safe_execute):
        perms = "rwx"
        self.hm.grantall(perms, self.hdfspath)
        cmd = "hadoop fs -chmod -R a+%s %s" % (perms, self.hdfspath)
        mock_safe_execute.assert_called_with(cmd)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_grantall_exception(self, mock_safe_execute):
        perms = "rwx"
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            self.hm.grantall(perms, self.hdfspath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_newdirs_shell(self, mock_safe_execute):
        mock_safe_execute.return_value = tse.ShellResult(0, self.returned_dirs, "")
        _ = self.hm.get_newdirs(self.hdfspath, self.lastdir, self.loadts,
                                self.process_delay_hrs)
        mock_safe_execute.assert_called_with("hadoop fs -ls %s" % self.hdfspath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_newdirs_return_val_no_lastdir(self, mock_safe_execute):
        mock_safe_execute.return_value = tse.ShellResult(0, self.returned_dirs, "")
        newdirs = self.hm.get_newdirs(self.hdfspath, None, self.loadts,
                                      self.process_delay_hrs)
        self.assertListEqual(newdirs, self.returned_dirs.split("\n"))

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_newdirs_return_val_with_lastdir(self, mock_safe_execute):
        mock_safe_execute.return_value = tse.ShellResult(0, self.returned_dirs, "")
        newdirs = self.hm.get_newdirs(self.hdfspath, self.lastdir, self.loadts,
                                      self.process_delay_hrs)
        self.assertListEqual(newdirs, self.returned_dirs.split("\n")[1:])

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_newdirs_delay(self, mock_safe_execute):
        mock_safe_execute.return_value = tse.ShellResult(0, self.returned_dirs, "")
        loadts = self.loadts - dt.timedelta(hours=8)
        newdirs = self.hm.get_newdirs(self.hdfspath, self.lastdir, loadts,
                                      self.process_delay_hrs)
        self.assertListEqual(newdirs, [])

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_newdirs_exception(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            _ = self.hm.get_newdirs(self.hdfspath, self.lastdir, self.loadts,
                                    self.process_delay_hrs)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_subdirs_shell(self, mock_safe_execute):
        mock_safe_execute.return_value = tse.ShellResult(0, "", "")
        _ = self.hm.get_subdirs(self.hdfspath)
        mock_safe_execute.assert_called_with("hadoop fs -ls %s" % self.hdfspath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_subdirs_return_val(self, mock_safe_execute):
        ptns = map(str, range(15))
        hdfsdirs = ["%s/%s" % (self.hdfspath, p) for p in ptns]
        ls_output= "\n".join(hdfsdirs)
        mock_safe_execute.return_value = tse.ShellResult(0, ls_output, "")
        subdirs = self.hm.get_subdirs(self.hdfspath)
        self.assertListEqual(subdirs, ptns)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_subdirs_exception(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        mock_safe_execute.return_value = tse.ShellResult(0, "", "")
        with self.assertRaises(thex.HdfsManagerException):
            _ = self.hm.get_subdirs(self.hdfspath)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_primary_namenode_shell(self, mock_safe_execute):
        namenodes = ["nn1", "nn2"]
        webhdfs_path = "/foo/bar"
        hdfs_user = "userfoo"
        _ = self.hm.get_primary_namenode(namenodes, webhdfs_path, hdfs_user)
        cmds = ["curl --negotiate -u:%s '%s?op=GETFILESTATUS' " \
                % (hdfs_user, "%s/foo/bar" % nn) for nn in namenodes]
        self.assertEqual(mock_safe_execute.call_count, 2)

        calls = [mock.call(cmd, splitcmd=False, as_shell=True) for cmd in cmds]
        mock_safe_execute.assert_has_calls(calls, any_order=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_primary_namenode_return_val_namenode_found(self, mock_safe_execute):
        namenodes = ["nn1", "nn2"]
        webhdfs_path = "/foo/bar"
        hdfs_user = "userfoo"
        mock_safe_execute.return_value = tse.ShellResult(0, "FileStatus", "")
        nn = self.hm.get_primary_namenode(namenodes, webhdfs_path, hdfs_user)
        self.assertEqual(nn, "nn1")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_primary_namenode_return_val_namenode_not_found(self, mock_safe_execute):
        namenodes = ["nn1", "nn2"]
        webhdfs_path = "/foo/bar"
        hdfs_user = "userfoo"
        mock_safe_execute.return_value = tse.ShellResult(0, "", "")
        self.assertIsNone(self.hm.get_primary_namenode(namenodes, webhdfs_path,
                                                       hdfs_user))

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_primary_namenode_exception(self, mock_safe_execute):
        namenodes = ["nn1", "nn2"]
        webhdfs_path = "/foo/bar"
        hdfs_user = "userfoo"
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(thex.HdfsManagerException):
            _ = self.hm.get_primary_namenode(namenodes, webhdfs_path, hdfs_user)
