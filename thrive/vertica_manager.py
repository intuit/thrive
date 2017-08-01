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
import re
from thrive.shell_executor import ShellExecutor, ShellException
from thrive.utils import logkv
from thrive.exceptions import VerticaManagerException


logger = logging.getLogger(__name__)


class VerticaManager(object):
    """
    Manager class for Vertica operations
    """
    def __init__(self, connection_info):
        """
        Initialize vertica cmd line

        @rtype: None
        @return: None
        """
        self.connection_info = connection_info

        # Set the locale explicitly to utf-8. Otherwise, loads triggered manually
        # by our headless service account will succeed and the loads triggered by the scheduler will fail.
        # The reason for this is that the value of the 'locale' environment variables for
        # the scheduler and our headless service account are different. This difference can be verified by
        # viewing the output of the 'locale' command manually triggered from
        # our headless service account's home folder and the output of 'locale' command from our scheduler.

        self.set_locale = "export LC_ALL='en_US.UTF-8'"
        self.vsql = "%s && %s -k %s -K %s -h %s -p %s -d %s -U %s" \
                    % (self.set_locale,
                       self.connection_info["vertica_vsql_path"],
                       self.connection_info["vertica_krb_svcname"],
                       self.connection_info["vertica_krb_host"],
                       self.connection_info["vertica_host"],
                       self.connection_info["vertica_port"],
                       self.connection_info["vertica_db"],
                       self.connection_info["vertica_user"])
        self.shell_exec = ShellExecutor()

    @staticmethod
    def getrows(vcopy_output):
        """
        Parses the console output of COPY command and extracts number of rows loaded

        @type vcopy_output: str
        @param vcopy_output: Output of COPY command

        @rtype: int
        @return: Number of rows loaded
        """
        pattern = "\s*(Rows Loaded|count|OUTPUT)\s*.*\s*([0-9]*)\s*"

        try:
            return re.findall(pattern, vcopy_output)[0][1]
        except Exception:
            logkv(logger,
                  {"msg": "Error retriving rows loaded from output of COPY command",
                   "voutput": vcopy_output,
                   "pattern": pattern}, "error")

            raise VerticaManagerException()

    def execute(self, stmt=None, scriptfile=None):
        """
        Execute sql stmt 'stmt' or sql script file 'scriptfile'

        @type stmt: str
        @param stmt: SQL query string

        @type scriptfile: str
        @param scriptfile: Location of scriptfile containing commands to execute

        @rtype: str
        @return: Output of the shell command enclosing Vertica sql command
        """

        # Get the execution mode and the argument (statement or filename)
        if stmt and not scriptfile:
            # The stmt *needs* to be in double quotes since it could contain
            # single-quoted strings (see load method below)
            vsql_cmd = '%s -c "%s" ' % (self.vsql, stmt)
        elif scriptfile and not stmt:
            vsql_cmd = "%s -f '%s'" % (self.vsql, scriptfile)
        else:
            logkv(logger, {"msg": "Received incorrect or conflicting execution mode",
                           "stmt": stmt, "scriptfile": scriptfile}, "info")
            raise VerticaManagerException("Incorrect execution mode")
        try:
            vresult = self.shell_exec.safe_execute(vsql_cmd, verbose=False,
                                                   as_shell=True,splitcmd=False)
            return vresult
        except ShellException:
            logkv(logger, {"msg": "VSQL command failed",
                           "cmd": vsql_cmd}, "error")
            raise VerticaManagerException()

    def create_table(self, ddlfile):
        """
        Creates Vertica schema from the schema file

        @type ddlfile: str
        @param ddlfile: Full path to file containing table-creation SQL

        @rtype: None
        @return: None
        """
        try:
            self.execute(scriptfile=ddlfile)
            logkv(logger, {"msg": "Created Vertica table DDL",
                           "ddlfile": ddlfile}, "info")
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "VSQL table create failed"}, "error", ex)
            raise

    def clone_schema(self, srcschema, srctable, dstschema, dsttable):
        """
        Clones schema of the 'srcschema.srctable' and creates 'dsttable'. If 'dsttable'
        exists already, it'll be deleted.

        @type srcschema: str
        @param srcschema: Vertica schema of the source table

        @type srctable: str
        @parasrctablele: Source table in Vertica

        @type dstschema: str
        @param dstschema: Vertica schema of the destination table

        @type dsttable: str
        @param dsttable: Destination table in Vertica

        @rtype: None
        @return: None
        """
        try:
            vsql_stmt = "drop table if exists %s.%s; create table %s.%s as select * from %s.%s where false;" \
                        % (dstschema, dsttable,dstschema, dsttable, srcschema, srctable)
            self.execute(stmt=vsql_stmt)
            logkv(logger, {"msg": "Cloned schema",
                           "source": "%s.%s" % (srcschema, srctable),
                           "destination": "%s.%s" % (dstschema, dsttable)}, "info")
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "Failed to clone schema",
                           "source": "%s.%s" % (srcschema, srctable),
                           "destination": "%s.%s" % (dstschema, dsttable)},
                  "error", ex)
            raise

    def drop_table(self, vschema, vtable):
        """
        Drops table 'table' in schema 'schema'
        @type vschema: str
        @param vschema: Vertica schema

        @type vtable: str
        @param vtable: Vertica table

        @rtype: None
        @return: None
        """
        try:
            vsql_stmt = "drop table if exists %s.%s" % (vschema, vtable)
            self.execute(stmt=vsql_stmt)
            logkv(logger, {"msg": "Dropped table", "vschema": vschema,
                           "vtable": vtable}, "info")
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "VSQL table drop failed"}, "error", ex)
            raise

    def load(self, webhdfs_root, hdfs_path, vschema,
             dtable, rtable, mode="direct"):
        """
        Loads data from HDFS into Vertica table 'dtable

        @type webhdfs_root: str
        @param hdfs_path: WebHdfs prefix. Same as hadoop name node

        @type hdfs_path: str
        @param hdfs_path: HDFS path to dataset

        @type vschema: str
        @param vschema: Vertica schema

        @type dtable: str
        @param dtable: Vertica table for data

        @type rtable: str
        @param rtable: Vertica table for rejected rows

        @type mode: str
        @param mode: Copy mode. Possible values: 'direct' or 'decompress'. If mode is
        'decompress', the data in Hive partition will be decompressed to a plain text
        format using a single node to a temporary HDFS location. IF the mode='direct'
        an attempt will be made to directly load compressed data to vertica using the
        appropriate filter.

        mode='decompress' is required if a the MapReduce job outputs data in a compression
        format not supported by Vertica filter function. For example BZip2. In this
        case, we'll decompress the data before passing it to the COPY command

        @rtype: str
        @return: Number of rows loaded
        """

        if mode == "direct":
            _filter = "FILTER GZIP()"
        elif mode == "decompress":
            _filter = ""
        else:
            logkv(logger, {"msg": "Invalid load mode supplied to Vertica COPY command",
                           "mode": mode}, "error")
            raise VerticaManagerException()

        # Discard the leading "/" in HDFS path. We're going to be pre-pending it with
        # webhdfs base URL
        if hdfs_path.startswith("/"):
            hdfs_path = hdfs_path[1:]

        webhdfs_url = os.path.join(webhdfs_root, hdfs_path)

        copy_cmd = '''COPY %s.%s
                      SOURCE Hdfs(url=\'%s\', username=\'%s\', low_speed_limit=1048576)
                      %s
                      DELIMITER E\'\\001\'
                      REJECTMAX 0
                      REJECTED DATA AS TABLE %s.%s
                      DIRECT
                      COMMIT
                    ''' % (vschema,
                           dtable,
                           webhdfs_url,
                           self.connection_info["vertica_user"],
                           _filter, vschema, rtable)

        try:
            vresult = self.execute(stmt=copy_cmd)
            rows_loaded = VerticaManager.getrows(vresult.output)

            logkv(logger, {"msg": "Loaded data in HDFS path to Vertica table",
                           "hdfs_path": hdfs_path, "vschema": vschema,
                           "dtable": dtable, "rows_loaded": rows_loaded}, "info")
            return rows_loaded
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "Load to Vertica via WebHdfs failed"},
                  "error", ex)
            raise

    def grant(self, privilege, level, vschema, vtable=None, to=None):
        """
        Grants 'privilege' on 'vschema'.'vtable' to 'entity'. An 'entity' can be a
        user or a group.

        @type privilege: str
        @param privilege: 'USAGE', 'SELECT', 'CREATE', 'DELETE' etc

        @type vschema: str
        @param vschema: Vertica schema

        @type vtable: str
        @param vtable: Vertica table

        @type to: str
        @param to: user or a group, separated by commas

        @rtype: None
        @return: None
        """

        # If table is not specified, the privileges are to be granted at the Schema level

        if (level.lower() == "schema") and (privilege.upper() == "SELECT"):
            grant_stmt = "grant SELECT on all tables in schema %s to %s" % (vschema, to)
        elif (level.lower() == "schema") and (privilege.upper() == "USAGE"):
            grant_stmt = "grant USAGE on schema %s to %s" % (vschema, to)
        elif (level.lower() == "table") and (privilege.upper() == "SELECT"):
            grant_stmt = "grant SELECT on table %s.%s to %s" % (vschema, vtable, to)
        else:
            logkv(logger, {"msg": "Incorrect level/privilege combination",
                           "level": level, "privilege": privilege}, "error")
            raise VerticaManagerException()

        try:
            self.execute(stmt=grant_stmt)
            logkv(logger, {"msg": "Granted privileges", "granted_to": to,
                           "privilege": privilege, "vschema": vschema,
                           "vtable": vtable}, "info")
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "Error executing grant statement",
                           "stmt": grant_stmt}, "error", ex)
            raise

    def rollback(self, srcschema, srctable,
                 rollbackschema, rollbacktable,
                 rkey=None):
        """
        Deletes rows in srctable that are present in rollbacktable.

        @type srcschema: str
        @param srcschema: Schema of the souce table

        @type srctable: str
        @param srctable: Source table

        @type rollbackschema: str
        @param rollbackschema: Schema of the table containing rollback data

        @type rollbacktable: str
        @param rollbacktable: Table containing the rollback data

        @rtype: str
        @return: Count of rows deleted
        """
        try:
            rbk_stmt = '''
                       set session autocommit to on;

                       delete from %s.%s
                       where %s in (
                           select %s
                           from %s.%s
                       );''' % (srcschema, srctable,
                                rkey, rkey,
                                rollbackschema, rollbacktable)

            vresult = self.execute(stmt=rbk_stmt)
            rows = VerticaManager.getrows(vresult.output)

            logkv(logger, {"msg": "rollback successful",
                           "source_schema": srcschema,
                           "source_table": srctable,
                           "rows": rows}, "info")
            return rows
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "VSQL table drop failed"}, "error", ex)
            raise VerticaManagerException()

    def truncate(self, vschema, vtable):
        """
        Truncates 'vschema.vtable'.

        @type vschema: str
        @param vschema: Schema of the table to be truncated

        @type vtable: str
        @param vtable: Target table

        @rtype: None
        @return: None
        """
        try:
            truncate_stmt = "truncate table %s.%s;" % (vschema, vtable)
            self.execute(stmt=truncate_stmt)
            logkv(logger, {"msg": "Truncated table",
                           "schema": vschema,
                           "table": vtable}, "info")
        except VerticaManagerException as ex:
            logkv(logger, {"msg": "Failed to truncate table",
                           "schema": vschema,
                           "table": vtable}, "error", ex)
            raise