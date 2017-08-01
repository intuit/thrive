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

import os
import logging
from thrive.utils import logkv
from thrive.load_handler import LoadHandler
from thrive.exceptions import RollbackHandlerException

logger = logging.getLogger(__name__)


class RollbackHandler(LoadHandler):
    """
    Class to handle rollback events
    """

    def __init__(self, datacfg_file=None, envcfg_file=None,
                 resources_file=None, partitions_file=None):
        super(RollbackHandler, self).__init__(datacfg_file, envcfg_file, resources_file)

        self.partitions_file = partitions_file

    def execute(self, **kwargs):
        """
        Top level method for rollback handler. The general workflow is the
        following. It drops the Vertica data followed by Hive partitions, HDFS
        files, and MySQL metadata

        @type kwargs: dict
        @param kwargs: Not actually used in execute() function. Added to match
        signature to super.execute()

        @rtype: None
        @return: None
        """

        logkv(logger, {"msg": "Starting rollback"}, "info")

        # Get source schema, source table, destination schema and destination table
        srcschema = self.get_config("vertica_schema")
        srctable = self.get_config("vertica_table")
        rollbackschema = srcschema
        rollbacktable = "%s__rollback__" % srctable

        try:
            # Clone the Vertica table containing the rollback data
            self.vertica_mgr.clone_schema(srcschema, srctable, rollbackschema, rollbacktable)

            with open(self.partitions_file) as pf:
                for line in pf:
                    ptn_path = line.strip()
                    logkv(logger, {"msg": "Rolling back partition",
                                   "partition": ptn_path}, "info")

                    # Construct HDFS path to the data
                    hdfspath = os.path.join(self.get_config("target_root"), ptn_path)

                    # If the hdfspath exists, perform rollback operation in Vertica
                    if self.hdfs_mgr.path_exists(hdfspath):

                        logkv(logger, {"msg": "Proceeding with vertica rollback",
                                       "partition": hdfspath}, "info")

                        # Load data in current partition to Vertica
                        try:
                            logkv(logger,
                                  {"msg": "Trying Vertica COPY via 'direct'"},
                                  "info")
                            self.vload_copy(ptn_path, rollbackschema,
                                            rollbacktable, mode="direct")
                        except Exception:
                            logkv(logger,
                                  {"msg": "Trying Vertica COPY via 'decompress'"},
                                  "info")
                            self.vload_copy(ptn_path, rollbackschema,
                                            rollbacktable, mode="decompress")

                        # Delete rows in Vertica main table contained in __rollback__ table
                        self.vertica_mgr.rollback(srcschema, srctable,
                                                  rollbackschema, rollbacktable,
                                                  rkey=self.get_config("vertica_rollback_key"))

                        # Truncate the __rollback__ table
                        self.vertica_mgr.truncate(rollbackschema, rollbacktable)

                        # Delete source HDFS data
                        self.hdfs_mgr.rmdir(hdfspath)

                    # Drop Hive Partition
                    ptn_str = "year=%s, month=%s, day=%s, hour=%s, part=%s" \
                              % tuple(ptn_path.split("/"))
                    self.hive_mgr.drop_partition(ptn_str)

                    # Delete row in Metadata tables
                    self.metadata_mgr.delete(self.get_config("dataset_name"),
                                             mdcolname="hive_last_partition",
                                             mdcolvalue=ptn_path)
        except Exception:
            logkv(logger, {"msg": "Rollback error"}, "error")
            raise RollbackHandlerException()
        finally:
            # Drop the __rollback__ table
            self.vertica_mgr.drop_table(rollbackschema, rollbacktable)







