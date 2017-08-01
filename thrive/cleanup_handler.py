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

import logging
from thrive.shell_executor import ShellException
from thrive.thrive_handler import ThriveHandler
from thrive.utils import logkv
from thrive.exceptions import HdfsManagerException, MetadataManagerException, \
    HiveManagerException, VerticaManagerException

logger = logging.getLogger(__name__)


class CleanupHandler(ThriveHandler):
    """
    Handles cleanup of datasets. Cleans up local file system (NFS), HDFS,
    Hive databases and tables, and MySQL metadata
    """
    def _cleanup_nfs(self):
        try:
            nfs_dataset_path = self.get_config("nfs_dataset_path")
            self.shell_exec.safe_execute("rm -rf %s" % nfs_dataset_path)
            logkv(logger, {"msg": "Purged local file system"})
        except ShellException as ex:
            logkv(logger, {"msg": "Purging local filesystem failed"}, "warning")

    def _cleanup_hdfs(self):
        try:
            hdfs_resource_path = self.get_config("hdfs_resource_path")
            self.hdfs_mgr.rmdir(hdfs_resource_path)
            hdfs_dataset_path = self.get_config("target_root")
            self.hdfs_mgr.rmdir(hdfs_dataset_path)
            logkv(logger, {"msg": "Purged HDFS"})
        except HdfsManagerException as ex:
            logkv(logger, {"msg": "Purging HDFS failed"}, "warning", ex)

    def _cleanup_hive(self):
        try:
            self.hive_mgr.drop_table()
            logkv(logger, {"msg": "Cleaning Hive tables"})
        except HiveManagerException as ex:
            logkv(logger, {"msg": "Cleaning Hive tables failed"}, "warning", ex)

    def _cleanup_vertica(self):
        if self.get_config("vertica_load").lower() == "true":
            try:
                vschema = self.get_config("vertica_schema")
                vtable = self.get_config("vertica_table")
                vreject_table = self.get_config("vertica_rejected_data_table")
                self.vertica_mgr.drop_table(vschema, vtable)
                self.vertica_mgr.drop_table(vschema, vreject_table)
                logkv(logger, {"msg": "Purged Vertica"})
            except VerticaManagerException as ex:
                logkv(logger, {"msg": "Purging vertica failed"}, "warning", ex)

    def _cleanup_metadata(self):
        try:
            topic = self.get_config("dataset_name")
            self.metadata_mgr.purge(topic)
            logkv(logger, {"msg": "Purged metadata"})
        except MetadataManagerException as ex:
            logkv(logger, {"msg": "Purging metadata failed"}, "warning", ex)

    def execute(self):
        self._cleanup_nfs()
        self._cleanup_hdfs()
        self._cleanup_hive()
        self._cleanup_vertica()
        self._cleanup_metadata()
