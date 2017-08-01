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

import re
import os
import logging
from thrive.shell_executor import ShellException
from thrive.thrive_manager import ThriveManager
from thrive.utils import pathjoin, logkv, SOURCE_DIR_PATTERN
from thrive.utils import dirname_to_dto, utc_to_pst, hour_diff
from thrive.exceptions import HdfsManagerException

logger = logging.getLogger(__name__)


class HdfsManager(ThriveManager):
    """
    Class to manage HDFS operations. Sample responsibilities include:
    (1) getting directories to process
    (2) checking if HDFS paths exist
    """
    def makedir(self, dirpath):
        """
        Makes an HDFS directory at dirpath

        @type dirpath: str
        @param dirpath: HDFS path

        @rtype: None
        @return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -mkdir -p %s" % dirpath)
            logkv(logger, {"msg": "Created %s" % dirpath})
        except ShellException:
            logkv(logger, {"msg": "HDFS makedir failed. %s" % dirpath}, "error")
            raise HdfsManagerException()

    def rmdir(self, dirpath):
        """
        Removes HDFS directory at dirpath

        @type dirpath: str
        @param dirpath: HDFS path

        @rtype: None
        @return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -rm -r -skipTrash %s" % dirpath)
            logkv(logger, {"msg": "Deleted %s" % dirpath})
        except ShellException:
            logkv(logger, {"msg": "HDFS rmdir failed. %s" % dirpath}, "warning")
            raise HdfsManagerException()

    def putfile(self, localpath, hdfspath):
        """
        Copies a resource from local path to HDFS path

        @type localpath: str
        @param localpath: Path to resource on local fileystem

        @type hdfspath: str
        @param hdfspath: HDFS destination path

        @rtype: None
        @return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -put %s %s" % (localpath, hdfspath))
            logkv(logger, {"msg": "Put %s to HDFS path %s" % (localpath, hdfspath)}, "info")
        except ShellException:
            logkv(logger, {"msg": "HDFS putfile failed. %s %s" % (localpath, hdfspath)}, "error")
            raise HdfsManagerException()

    def path_exists(self, hdfspath):
        """
        Checks if the specified HDFS path exists.

        @type hdfspath: str
        @param hdfspath: HDFS path to check

        @rtype: bool
        @return: True if the hdfspath exists, False otherwise
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -test -e %s" % hdfspath,
                                         splitcmd=False, as_shell=True)
            return True
        except ShellException:
            # ShellException implies hdfspath does not exist
            logkv(logger, {"msg": "Path does not exist",
                           "path": hdfspath}, "info")
            return False

    def decompress(self, srcpath, dstpath):
        """
        Decompresses data file(s) form 'srcpath' and output to 'dstpath'. Both srcpath
        and dstpath are HDFS paths

        @type srcpath: str
        @param srcpath: Location of compressed files

        @type dstpath: str
        @param dstpath: Location of uncompressed files

        @rtype: None
        @return: None
        """
        cmd = "hadoop fs -text %s | hadoop fs -put - %s" % (srcpath, dstpath)
        try:
            self.shell_exec.safe_execute(cmd,
                                         as_shell=True,
                                         splitcmd=False,
                                         verbose=False)
            logkv(logger,
                  {"msg": "Decompressed source data into target",
                   "source": srcpath, "target": dstpath}, "info")
        except ShellException:
            logkv(logger, {"msg": "HDFS decompress failed. %s", "cmd": cmd}, "error")
            raise HdfsManagerException()

    def grantall(self, permissions, hdfspath):
        """
        Grants 'permissions' to all on HDFS hdfspath

        @type permissions: str
        @param permissions: Permission string. E.g: "rwx"

        @type hdfspath: str
        @param hdfspath: HDFS path to grant permissions on

        @rtype: None
        @return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -chmod -R a+%s %s"
                                         % (permissions, hdfspath))

            logkv(logger,
                  {"msg": "Granted permissions",
                   "permissions": permissions,
                   "path": hdfspath}, "info")
        except ShellException:
            logkv(logger,
                  {"msg": "Error granting permissions",
                   "permissions": permissions,
                   "path": hdfspath}, "warning")
            raise HdfsManagerException()

    def get_newdirs(self, hdfspath, lastdir, loadts, process_delay_hrs):
        """
        Gets new HDFS folders generated since the last data load.

        @type hdfspath: str
        @param hdfspath: HDFS to query for new directories

        @type lastdir: str
        @param lastdir: last directory whose data was processed

        @rtype: list
        @return: List of directories created at this location since processing of lastdir

        @raises: ValueError
        """

        # Get all folders in topic
        try:
            result = self.shell_exec.safe_execute("hadoop fs -ls %s" % hdfspath)
            alldirs = re.findall(SOURCE_DIR_PATTERN, result.output)
        except ShellException:
            logkv(logger, {"msg": "Error in fetching HDFS directories"}, "error")
            raise HdfsManagerException()

        # Sort dirs according to date. Date is obtained from dirname
        # (e.g. 'd_20150311-1610') by retaining only the numeric parts of
        # the string and converting to int (e.g. 201503111610)
        alldirs.sort(key=lambda s: int(re.sub("[^0-9]", "", s)))

        # Get pending dirs (i.e. dirs generated since last load and yet to be processed)
        try:
            if lastdir is None:
                newdirs = alldirs
            else:
                lastindex = alldirs.index(lastdir)
                newdirs = alldirs[lastindex + 1:]
            logkv(logger, {"msg": "Last processed directory %s" % lastdir,
                           "process_delay": process_delay_hrs}, "info")

            # From newdirs, select those which are older than the chosen delay
            # period (in hours) (Source/CAMUS may still be processing newer dirs)
            dirs_to_process = []
            for d in newdirs:
                dir_dto = dirname_to_dto(d)
                if dir_dto and hour_diff(utc_to_pst(dirname_to_dto(d)), loadts) > process_delay_hrs:
                    dirs_to_process.append(d)
            return dirs_to_process
        except Exception:
            logkv(logger, {"msg": "Error in generating list of directories to process"}, "error")
            raise HdfsManagerException()

    def get_subdirs(self, hdfspath):
        """
        Gets subdirectories under 'hdfspath'

        @type hdfspath: str
        @param hdfspath: Parenth HDFS path

        @rtype: list
        @return: Basenames of subdirectories under 'hdfspath'
        """
        HIVE_PTN_PATTERN = ".*(%s)$" % os.path.join(hdfspath, "[0-9]+")
        try:
            cmd = "hadoop fs -ls %s" % hdfspath
            result = self.shell_exec.safe_execute(cmd)
            subpaths = re.findall(HIVE_PTN_PATTERN, result.output, re.MULTILINE)
            subdirs = [os.path.basename(sp) for sp in subpaths]
            return subdirs
        except Exception:
            logkv(logger, {"msg": "Error in listing HDFS path: %s" % hdfspath},
                  "warning")
            raise HdfsManagerException()

    def get_primary_namenode(self, namenodes, webhdfs_path, hdfs_user):
        """
        Vertica load via Webhdfs needs a primary (active) namenode. Because of
        failovers, the active namenodes can change anytime to the next available
        namenode. The config file specifies the possible namenodes in the
        cluster (in the Dev, prod, or QA environments.

        The present function pings service user's home directory in each
        namenode in the supplied list of namenodes and determines the which one
        is active. It returns the first active namenode. If none of the
        namenodes is active, it returns None and lets the caller figure out the
        appropriate course of action

        @type namenodes: list
        @param namenodes: list of HDFS namenode urls

        @rtype: str
        @return: one item from the list of webhdfs urls, which is the active
        namenode or None if none of them are active
        """

        for nn in namenodes:
            path = pathjoin(nn, webhdfs_path)
            cmd = "curl --negotiate -u:%s '%s?op=GETFILESTATUS' " % (hdfs_user, path)
            try:
                res = self.shell_exec.safe_execute(cmd, splitcmd=False, as_shell=True)
                # If "FileStatus" key is present in output, the namenode is active
                if "FileStatus" in res.output:
                    logkv(logger, {"msg": "Retrived namenode", "namenode": nn}, "info")
                    return nn
            except ShellException:
                logkv(logger, {"msg": "Error pinging namenode", "namenode": nn}, "error")
                raise HdfsManagerException()

        # If no result had "FileStatus" key, then all namenodes are dead. Return None
        return None
