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
import re
from thrive.shell_executor import ShellExecutor, ShellException
from thrive.utils import logkv, parse_partition
from thrive.hdfs_manager import HdfsManager
from thrive.exceptions import HiveManagerException

logger = logging.getLogger(__name__)


class HiveManager(object):
    """
    Manager for Hive tables. Creates partition after each load from HDFS
    """
    def __init__(self, db, table):
        """
        Initializes the manager object for a Hive table
        @type db: str
        @param db: Hive db

        @type table: str
        @param table: Hive table

        @rtype: None
        @return: None
        """
        self.db = db
        self.table = table
        self.shell_exec = ShellExecutor()

    def execute(self, stmt):
        """
        Executes Hive SQL statement 'stmt'

        @type stmt: str
        @param stmt: Hive query

        @rtype: None
        @return: None
        """
        try:
            # Create hive partition
            self.shell_exec.safe_execute(stmt, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Executed query", "query": stmt}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error executing Hive statement",
                           "query": stmt}, "error")
            raise HiveManagerException()

    def create_partition(self, ptn_path):
        """
        Creates a timestmp-based partition and points the partition to the location of
        the data

        @type ptn_path: str
        @param ptn_path: location of the parsed JSON data

        @rtype: None
        @return: None
        """

        # Check if HDFS path exists before attempting to point the new partition to it
        hdfs_mgr = HdfsManager()
        if not hdfs_mgr.path_exists(ptn_path):
            logkv(logger, {"msg": "Hadoop path does not exist", "path": ptn_path}, "error")
            raise HiveManagerException()
        else:
            logkv(logger, {"msg": "Hadoop path exists", "path": ptn_path}, "info")

        # Construct Hive a partition string
        ptn_vals = parse_partition(ptn_path)
        ptn_year, ptn_month, ptn_day, ptn_hour, ptn_part = ptn_vals
        ptn_str = "year=%s/month=%s/day=%s/hour=%s/part=%s" % ptn_vals

        # Check if the Hive table partition we're about to create already exists
        if self.check_partition(ptn_str):
            errmsg = "Partition %s for table %s.%s already exists" \
                     % (ptn_str, self.db, self.table)
            logkv(logger, {"msg": errmsg}, "error")
            raise HiveManagerException()

        # If the partition does not exist, proceed to create it
        # Construct partition command
        partition_cmd = ''' \
        hive -e "use %s; \
        alter table %s \
        add partition (year = '%s', month = '%s', day = '%s', hour = '%s', part = '%s') \
        location '%s';" ''' % (self.db,
                               self.table,
                               ptn_year,
                               ptn_month,
                               ptn_day,
                               ptn_hour,
                               ptn_part,
                               ptn_path)
        try:
            self.shell_exec.safe_execute(partition_cmd,
                                         splitcmd=False,
                                         as_shell=True)
            logkv(logger, {"msg": "Created partition",
                           "partition": ptn_str}, "info")

        except ShellException:
            logkv(logger, {"msg": "Error creating Hive partition",
                           "partition": ptn_str}, "error")
            raise HiveManagerException()

    def drop_partition(self, ptn_str):
        """
        Deletes hive partition.

        @type ptn_str: str
        @param ptn_str: partition to delete in the yyyy/mm/dd/hh/part format

        @rtype: None
        @return: None
        """
        # Compose drop partition command
        dropcmd = ''' hive -e "use %s; alter table %s drop if exists partition (%s)"''' \
                  % (self.db, self.table, ptn_str)
        try:
            self.execute(dropcmd)
            logkv(logger, {"msg": "Dropped partition", "partition": ptn_str}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error dropping Hive partition",
                           "partition": ptn_str}, "error")
            raise HiveManagerException()

    def create_table(self, ddlfile):
        """
        Creates a table by executing a 'create table' statement inside 'ddlfile'

        @type ddlfile: str
        @param ddlfile: path to the ddlfile

        @rtype: None
        @return: None
        """
        pass

    def drop_table(self):
        """
        Drops self.table

        @rtype: None
        @return: None
        """
        dropcmd = ''' hive -e "use %s; drop table %s" ''' %(self.db, self.table)
        try:
            # Drop the table
            self.shell_exec.safe_execute(dropcmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Dropped table",
                           "table": "%s.%s" % (self.db, self.table)}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error dropping table",
                           "table": "%s.%s" % (self.db, self.table)}, "error")
            raise HiveManagerException()

    def drop_db(self):
        """
        Drops self.db, deletes all tables inside

        @rtype: None
        @return: None
        """
        dropcmd = ''' hive -e "drop database if exists %s cascade;" ''' % (self.db)
        try:
            # Drop the table
            self.shell_exec.safe_execute(dropcmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Dropped database", "database": self.db}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error dropping database",
                           "database": self.db}, "warning")
            raise HiveManagerException()

    def check_partition(self, ptn_str):
        """
        Checks if a Hive partition exists. Purpose of this function is as
        follows: Hive will throw an exception if the partition we're about to
        create already exists. So the create_partition uses this method to make
        sure that the partition does not* exist.

        @type ptn_str: str
        @param ptn_str: Partition string.
                        E.g. "year=2016/month=06/day=06/hour=18/part=1"

        @rtype: bool
        @return: True if a partition exists, False otherwise
        """
        cmd = ''' hive -e "use %s; show partitions %s" ''' % (self.db, self.table)
        try:
            result = self.shell_exec.safe_execute(cmd, splitcmd=False, as_shell=True)
            return bool(re.search(ptn_str, result.output))
        except Exception:
            logkv(logger, {"msg": "Error checking Hive partition",
                           "partition": ptn_str}, "error")
            raise HiveManagerException()
