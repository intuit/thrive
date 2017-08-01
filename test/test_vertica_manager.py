
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
import thrive.vertica_manager as tvm
import thrive.shell_executor as tse
from thrive.exceptions import VerticaManagerException


class TestVerticaManager(unittest.TestCase):
    def setUp(self):
        self.connection_info = {
            "vertica_vsql_path": "/foo/vsql",
            "vertica_krb_svcname": "krb_svcname",
            "vertica_krb_host": "krb_host",
            "vertica_host": "vertica_host",
            "vertica_port": "vertica_port",
            "vertica_db": "vertica_db",
            "vertica_user": "vertica_user"
        }
        set_locale = "export LC_ALL='en_US.UTF-8'"
        self.tvm = tvm.VerticaManager(self.connection_info)
        self.vsql = "%s && %s -k %s -K %s -h %s -p %s -d %s -U %s" \
                    % (set_locale,
                       self.connection_info["vertica_vsql_path"],
                       self.connection_info["vertica_krb_svcname"],
                       self.connection_info["vertica_krb_host"],
                       self.connection_info["vertica_host"],
                       self.connection_info["vertica_port"],
                       self.connection_info["vertica_db"],
                       self.connection_info["vertica_user"])

    def test_init(self):
        with self.assertRaises(KeyError):
            _ = tvm.VerticaManager(dict())

    def test_getrows_positive(self):
        vcopy_output = "Rows Loaded\n-----\n3100\n"
        self.assertEqual(self.tvm.getrows(vcopy_output), "3100")

    def test_getrows_no_rows(self):
        vcopy_output = "Rows Loaded\n-----\n"
        self.assertEqual(self.tvm.getrows(vcopy_output), "")

    def test_getrows_exception(self):
        with self.assertRaises(VerticaManagerException):
           _ = self.tvm.getrows("")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_execute_stmt(self, mock_safe_execute):
        stmt = "select * from foo;"
        _ = self.tvm.execute(stmt)
        mock_safe_execute.assert_called_with('%s -c "%s" ' % (self.vsql, stmt),
                                             as_shell=True, splitcmd=False,
                                             verbose=False)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_execute_file(self, mock_safe_execute):
        scriptfile = "/foo/bar.sql"
        _ = self.tvm.execute(scriptfile=scriptfile)
        mock_safe_execute.assert_called_with("%s -f '%s'" % (self.vsql, scriptfile),
                                             as_shell=True, splitcmd=False,
                                             verbose=False)

    def test_execute_stmt_and_file_empty(self):
        with self.assertRaises(VerticaManagerException):
            _ = self.tvm.execute(stmt=None, scriptfile=None)

    def test_execute_stmt_and_file_populated(self):
        with self.assertRaises(VerticaManagerException):
            _ = self.tvm.execute(stmt="select * from foo;", scriptfile="/foo/bar.sql")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_execute_stmt_output(self, mock_safe_execute):
        stmt = "select * from foo;"
        mock_safe_execute.return_value = tse.ShellResult(0, "a,b,c", "")
        result = self.tvm.execute(stmt=stmt)
        self.assertEqual(result.output, "a,b,c")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_execute_exception(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        with self.assertRaises(VerticaManagerException):
            _ = self.tvm.execute(stmt="select * from foo;")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_create_table(self, mock_vexec):
        ddlfile = "foo/bar.sql"
        self.tvm.create_table(ddlfile)
        mock_vexec.assert_called_with(scriptfile=ddlfile)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_create_table_vertica_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            self.tvm.create_table("")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_create_table_shell_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            self.tvm.create_table("")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_clone_schema(self, mock_vexec):
        srcschema, srctable = "schema_foo", "table_foo"
        dstschema, dsttable = "schema_bar", "table_bar"
        self.tvm.clone_schema(srcschema, srctable, dstschema, dsttable)
        vsql_stmt = "drop table if exists %s.%s; create table %s.%s as select * from %s.%s where false;" \
                    % (dstschema, dsttable,
                       dstschema, dsttable,
                       srcschema, srctable)
        mock_vexec.assert_called_with(stmt=vsql_stmt)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_clone_schema_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            self.tvm.clone_schema("foo", "bar", "foo", "bar")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_drop_table(self, mock_vexec):
        vschema, vtable = "foo", "bar"
        self.tvm.drop_table(vschema, vtable)
        vsql_stmt = "drop table if exists %s.%s" % (vschema, vtable)
        mock_vexec.assert_called_with(stmt=vsql_stmt)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_drop_table_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            self.tvm.drop_table("foo", "bar")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_load_direct(self, mock_vexec):
        mock_vexec.return_value = tse.ShellResult(0, "Rows Loaded\n-----\n3100\n", "")
        nrows = self.tvm.load("foo", "bar", "foo", "bar", "foo", "direct")
        self.assertEqual(nrows, "3100")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_load_decompress(self, mock_vexec):
        mock_vexec.return_value = tse.ShellResult(0, "Rows Loaded\n-----\n3100\n", "")
        nrows = self.tvm.load("foo", "bar", "foo", "bar", "foo", "decompress")
        self.assertEqual(nrows, "3100")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_load_wrong_mode(self, mock_vexec):
        with self.assertRaises(VerticaManagerException):
            _ = self.tvm.load("foo", "bar", "foo", "bar", "foo", "wrong_mode")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_load_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            _ = self.tvm.load("foo", "bar", "foo", "bar", "foo")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_grant_schema_select(self, mock_vexec):
        privilege, level, vschema, to = "SELECT", "schema", "foo", "userfoo"
        self.tvm.grant(privilege, level, vschema, None, to)
        grant_stmt = "grant SELECT on all tables in schema %s to %s" % (vschema, to)
        mock_vexec.assert_called_with(stmt=grant_stmt)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_grant_schema_usage(self, mock_vexec):
        privilege, level, vschema, to = "USAGE", "schema", "foo", "userfoo"
        self.tvm.grant(privilege, level, vschema, None, to)
        grant_stmt = "grant USAGE on schema %s to %s" % (vschema, to)
        mock_vexec.assert_called_with(stmt=grant_stmt)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_grant_table_select(self, mock_vexec):
        privilege, level, vschema, to = "SELECT", "table", "foo", "userfoo"
        vtable = "vtablefoo"
        self.tvm.grant(privilege, level, vschema, vtable, to)
        grant_stmt = "grant SELECT on table %s.%s to %s" % (vschema, vtable, to)
        mock_vexec.assert_called_with(stmt=grant_stmt)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_grant_wrong_privilege(self, mock_vexec):
        privilege, level, vschema, to = "WRONG_PRIVILEGE", "table", "foo", "userfoo"
        with self.assertRaises(VerticaManagerException):
            self.tvm.grant(privilege, level, vschema, None, to)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_grant_wrong_level(self, mock_vexec):
        privilege, level, vschema, to = "SELECT", "wrong_level", "foo", "userfoo"
        with self.assertRaises(VerticaManagerException):
            self.tvm.grant(privilege, level, vschema, None, to)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_grant_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        privilege, level, vschema, to = "SELECT", "schema", "foo", "userfoo"
        with self.assertRaises(VerticaManagerException):
            self.tvm.grant(privilege, level, vschema, None, to)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_rollback(self, mock_vexec):
        mock_vexec.return_value = tse.ShellResult(0, "count\n-----\n3100\n", "")
        nrows = self.tvm.rollback("schema_foo", "table_foo", "schema_bar",
                                  "table_bar", "keyfoo")
        self.assertEqual(nrows, "3100")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_rollback_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            _ = self.tvm.rollback("schema_foo", "table_foo", "schema_bar",
                                  "table_bar", "keyfoo")

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_truncate(self, mock_vexec):
        vschema, vtable = "foo", "bar"
        self.tvm.truncate(vschema, vtable)
        truncate_stmt = "truncate table %s.%s;" % (vschema, vtable)
        mock_vexec.assert_called_with(stmt=truncate_stmt)

    @mock.patch("thrive.vertica_manager.VerticaManager.execute")
    def test_truncate_exception(self, mock_vexec):
        mock_vexec.side_effect = VerticaManagerException()
        with self.assertRaises(VerticaManagerException):
            self.tvm.truncate("foo", "bar")
