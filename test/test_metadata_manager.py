
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
import pyodbc
import thrive.metadata_manager as tmm
from thrive.exceptions import MetadataManagerException
from test.utils.utils import squeeze


class TestMetadataManager(unittest.TestCase):
    def setUp(self):
        self.credentials = {
            "dbtype": "MySQL",
            "dbhost": "foo_host",
            "dbport": "0000",
            "dbuser": "user",
            "dbpass": "pass",
            "dbname": "dbname"
        }

        self.patcher = mock.patch("pyodbc.connect")
        self.mock_connect = self.patcher.start()
        self.mock_connection = self.mock_connect.return_value
        self.mock_cursor = self.mock_connection.cursor.return_value
        self.mm = tmm.MetadataManager(self.credentials)

    def tearDown(self):
        self.patcher.stop()

    def test_init(self):
        _ = tmm.MetadataManager(self.credentials)
        conn_str = "DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DB=%s" \
                    % (self.credentials["dbtype"], self.credentials["dbhost"],
                       self.credentials["dbport"], self.credentials["dbuser"],
                       self.credentials["dbpass"], self.credentials["dbname"])
        self.mock_connect.assert_called_with(conn_str)

    def test_init_exception(self):
        self.mock_connect.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            _ = tmm.MetadataManager(self.credentials)

    def test_execute(self):
        stmt = "select * from foo;"
        self.mm.execute(stmt)
        self.mock_cursor.execute.assert_called_with(stmt)
        self.mock_connection.commit.assert_called_with()
        self.mock_cursor.close.assert_called_with()

    def test_execute_cursor_instantiation_exception(self):
        self.mock_connection.cursor.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute("foo")

    def test_execute_execute_exception(self):
        self.mock_cursor.execute.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute("foo")

    def test_execute_commit_exception(self):
        self.mock_connection.commit.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute("foo")

    def test_execute_close_exception(self):
        self.mock_cursor.close.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute("foo")

    def test_execute_return_calls(self):
        stmt = "select * from foo;"
        self.mm.execute_return(stmt)
        self.mock_cursor.execute.assert_called_with(stmt)
        self.mock_connection.commit.assert_called_with()
        self.mock_cursor.close.assert_called_with()

    def test_execute_return_val(self):
        val = "foo,bar"
        self.mock_cursor.fetchall.return_value = val
        self.assertEqual(val, self.mm.execute_return("foo"))

    def test_execute_return_cursor_instantiation_exception(self):
        self.mock_connection.cursor.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute_return("foo")

    def test_execute_return_execute_exception(self):
        self.mock_cursor.execute.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute_return("foo")

    def test_execute_return_commit_exception(self):
        self.mock_connection.commit.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute_return("foo")

    def test_execute_return_close_exception(self):
        self.mock_cursor.close.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.execute_return("foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_insert(self, mock_exec):
        mdmap = {
            "setup": "thrive_setup",
            "load": "thrive_load_metadata",
            "lock": "thrive_dataset_lock"
        }

        data = {"fookey": "fooval", "barkey": "barval"}
        columns = ",".join(data.keys())
        values = ",".join("'%s'" % v for v in data.values())

        for mdtype, mdtable in mdmap.items():
            stmt = "insert into %s (%s) values (%s);" % (mdtable, columns, values)
            self.mm.insert(data, mdtype=mdtype)
            mock_exec.assert_called_with(stmt)

    def test_insert_unallowed_mdtype_exception(self):
        with self.assertRaises(MetadataManagerException):
            self.mm.insert({"foo": "bar"}, mdtype="foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_insert_duplicate_primary_key_exception(self, mock_exec):
        mock_exec.side_effect = pyodbc.IntegrityError()
        with self.assertRaises(MetadataManagerException):
            self.mm.insert({"foo": "bar"}, mdtype="foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_insert_pyodbc_error(self, mock_exec):
        mock_exec.side_effect = pyodbc.Error()
        with self.assertRaises(MetadataManagerException):
            self.mm.insert({"foo": "bar"}, mdtype="foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_update(self, mock_exec):
        pk = ("pk1", "pk2")
        mdmap = { "setup": "thrive_setup", "load": "thrive_load_metadata"}
        mdfilter = {
            "setup": "dataset_id = '%s'" % pk[0],
            "load": "(load_id, hive_last_partition) = ('%s', '%s')" % pk
        }
        data = {"fookey": "fooval", "barkey": "barval"}

        for mdtype, mdtable in mdmap.items():
            updates = ",".join('%s="%s"' % (key, val) for key, val in data.items())
            stmt = "update %s set %s where %s;" \
                         % (mdtable, updates, mdfilter[mdtype])
            self.mm.update(pk, data, mdtype=mdtype)
            self.assertEqual(squeeze(stmt),
                             squeeze(mock_exec.call_args[0][0]))

    def test_update_unallowed_mdtype_exception(self):
        with self.assertRaises(MetadataManagerException):
            self.mm.update(("foo", "bar"), {"foo": "bar"}, mdtype="foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_update_execute_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.update(("foo", "bar"), {"foo": "bar"}, mdtype="foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_last_dir_call(self, mock_exec):
        dataset_name, hive_table, load_type = "foo", "bar", "baz"
        stmt = '''
                 select last_load_folder
                 from thrive_load_metadata
                 where dataset_name = '%s'
                 and hive_table = '%s'
                 and load_type = '%s'
                 order by hive_end_ts desc
                 limit 1;
              ''' % (dataset_name, hive_table, load_type)
        _ = self.mm.get_lastdir(dataset_name, hive_table, load_type)
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_last_dir_val(self, mock_exec):
        result = [["foo", "bar"], ["baz", "quux"]]
        mock_exec.return_value = result
        last_dir = self.mm.get_lastdir("foo", "bar", "baz")
        self.assertEqual(last_dir, "foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_last_dir_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            _ = self.mm.get_lastdir("foo", "bar", "baz")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_purge(self, mock_exec):
        thrive_tables = ["thrive_setup", "thrive_load_metadata",
                         "thrive_dataset_lock"]
        calls = []
        dataset_name = "foo"
        for tt in thrive_tables:
            stmt = "delete from %s where dataset_name = '%s';" % (tt, dataset_name)
            calls.append(mock.call(stmt))
        self.mm.purge(dataset_name)
        mock_exec.assert_has_calls(calls, any_order=True)

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_purge_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.purge("foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_unprocessed_partitions_call(self, mock_exec):
        hive_db, hive_table = "foo", "bar"
        stmt = '''
                  SELECT load_id,
                         hive_last_partition,
                         hive_rows_loaded,
                         hadoop_records_processed
                  from thrive_load_metadata
                  where hive_db = '%s'
                  and hive_table = '%s'
                  and hive_last_partition <> ''
                  and vertica_last_partition is NULL;
              ''' % (hive_db, hive_table)
        _ = self.mm.get_unprocessed_partitions("foo", "bar")
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_unprocessed_partitions_val(self, mock_exec):
        ptns = ["partition0", "partition1"]
        mock_exec.return_value = ptns
        returned = self.mm.get_unprocessed_partitions("foo", "bar")
        self.assertEqual(ptns, returned)

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_unprocessed_partitions_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            _ = self.mm.get_unprocessed_partitions("foo", "bar")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_lock_call(self, mock_exec):
        dsname = "foo"
        stmt = '''
                  update thrive_dataset_lock
                  set locked = TRUE, release_attempts = 0
                  where  dataset_name = '%s';
               ''' % dsname
        self.mm.lock(dsname)
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_lock_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.lock("foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_release_call(self, mock_exec):
        dsname = "foo"
        stmt = '''
                  update thrive_dataset_lock
                  set locked = FALSE
                  where dataset_name='%s';
               ''' % dsname
        self.mm.release(dsname)
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_release_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.release("foo")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_lock_status_call(self, mock_exec):
        dsname = "foo"
        stmt = '''
                  select locked, release_attempts
                  from thrive_dataset_lock
                  where dataset_name = '%s'; ''' % dsname
        _ = self.mm.get_lock_status(dsname)
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_lock_status_val(self, mock_exec):
        retval = [("foo1", "foo2"), ("bar1", "bar2")]
        mock_exec.return_value = retval
        result = self.mm.get_lock_status("foo")
        self.assertEqual(result, retval[0])

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_lock_status_indexerror(self, mock_exec):
        mock_exec.return_value = []
        with self.assertRaises(MetadataManagerException):
            _ = self.mm.get_lock_status("bar")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute_return")
    def test_get_lock_status_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            _ = self.mm.get_lock_status("bar")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_increment_release_attempt_call(self, mock_exec):
        dsname = "foo"
        stmt = '''
                 update thrive_dataset_lock
                 set release_attempts = release_attempts + 1
                 where dataset_name = '%s';
               ''' % dsname
        self.mm.increment_release_attempt(dsname)
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_increment_release_attempt_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.increment_release_attempt("bar")

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_delete_call(self, mock_exec):
        dataset, mdcolname, mdcolvalue = "foo", "bar", "baz"

        stmt = '''
                  delete from thrive_load_metadata
                  where dataset_name = '%s'
                  and %s = '%s';
               ''' % (dataset, mdcolname, mdcolvalue)
        self.mm.delete(dataset, mdcolname, mdcolvalue)
        self.assertEqual(squeeze(stmt),
                         squeeze(mock_exec.call_args[0][0]))

    @mock.patch("thrive.metadata_manager.MetadataManager.execute")
    def test_delete_exception(self, mock_exec):
        mock_exec.side_effect = Exception()
        with self.assertRaises(MetadataManagerException):
            self.mm.delete("foo", "bar", "baz")

    def test_close(self):
        self.mm.close()
        self.mock_connection.close.assert_called_with()
