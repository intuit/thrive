
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
import ConfigParser as cp
import thrive.setup_handler as tsh
import thrive.exceptions as thex
from test.utils.utils import make_tempfile, tempfile_write, squeeze


class TestSetupHandler(unittest.TestCase):
    def setUp(self):
        self.config_loader_patcher = mock.patch("thrive.thrive_handler.ConfigLoader")
        self.mock_config_loader = self.config_loader_patcher.start()

        self.md_patcher = mock.patch("thrive.thrive_handler.MetadataManager")
        self.mock_mm = self.md_patcher.start()

        self.hdfs_patcher = mock.patch("thrive.thrive_handler.HdfsManager")
        self.mock_hdfs = self.hdfs_patcher.start()

        self.hive_patcher = mock.patch("thrive.thrive_handler.HiveManager")
        self.mock_hive = self.hive_patcher.start()

        self.vertica_patcher = mock.patch("thrive.thrive_handler.VerticaManager")
        self.mock_vtica = self.vertica_patcher.start()

        self.shell_patcher = mock.patch("thrive.thrive_handler.ShellExecutor")
        self.mock_shell = self.shell_patcher.start()

        self.th_get_config_patcher = mock.patch("thrive.thrive_handler.ThriveHandler.get_config")
        self.mock_get_config = self.th_get_config_patcher.start()

        self.sh = tsh.SetupHandler(datacfg_file="foo", envcfg_file="bar",
                                   resources_file="baz.zip")

        self.config_value = "foo"
        self.mock_get_config.return_value = self.config_value

    def tearDown(self):
        self.config_loader_patcher.stop()
        self.md_patcher.stop()
        self.hdfs_patcher.stop()
        self.hive_patcher.stop()
        self.vertica_patcher.stop()
        self.shell_patcher.stop()
        self.th_get_config_patcher.stop()

    def test__make_schema(self):
        tf_cols = make_tempfile()
        tf_template = make_tempfile()
        cols = "col1 int\ncol2 float\ncol3 varchar(20)"
        template = """
        use @DATABASE;
        create external table @TABLE (
            @COLUMNMAPPINGS
        )
          partitioned by (year string, month string, day string, hour string, part string)
          row format delimited
          fields terminated by '\u0001'
          null defined as '';
        """
        tempfile_write(tf_cols, cols)
        tempfile_write(tf_template, template)
        substitutions = {
            "@DATABASE": "dbfoo",
            "@COLUMNMAPPINGS": cols,
            "@TABLE": "tablefoo"
        }

        tf_outfile = make_tempfile()
        self.sh._make_schema(tf_cols.name, tf_template.name, tf_outfile.name,
                             substitutions)
        tf_outfile.seek(0)

        expected = \
            """
            use dbfoo;
                    create external table tablefoo (
                        col1 int,
                        col2 float,
                        col3 varchar(20)
                    )
                      partitioned by (year string, month string, day string, hour string, part string)
                      row format delimited
                      fields terminated by '\u0001'
                      null defined as '';

            """
        self.assertEqual(squeeze(expected), squeeze(tf_outfile.read()))

    @mock.patch("thrive.setup_handler.SetupHandler._make_schema")
    def test_make_schema(self, mock__make_schema):
        mock_hive_substitutions = {"@DATABASE": "foo", "@TABLE": "foo"}
        mock_projection_clause = "order by foo "
        mock_seg_clause = "segmented by modularhash(foo) all nodes"
        mock_partition_clause = "partition by foo"
        mock_vertica_substitutions = {
            "@VSCHEMA": "foo",
            "@TABLE": "foo",
            "@PROJECTION_CLAUSE": mock_projection_clause,
            "@SEGMENTATION_CLAUSE": mock_seg_clause,
            "@PARTITION_CLAUSE": mock_partition_clause}
        mock_substitutions = {"hive": mock_hive_substitutions,
                              "vertica": mock_vertica_substitutions}
        calls = []
        #for pl in ["hive", "vertica"]:
        for pl in ["hive"]:
            c = mock.call("foo/foo", "foo/%s_schema_template.sql" % pl,
                          "foo/foo", mock_substitutions[pl])
            calls.append(c)
        self.sh.make_schema()
        mock__make_schema.assert_has_calls(calls)

    @mock.patch("thrive.setup_handler.materialize")
    @mock.patch("__builtin__.open")
    def test_make_oozie_workflow(self, mock_open, mock_materialize):
        mock_template_str = "templatefoo"
        tf = make_tempfile()
        tempfile_write(tf, mock_template_str)
        mock_open.return_value = tf
        mock_joinval = "foo/script/foo"
        mock_outfile = "foo/foo"
        mock_subs = {"@MAPPER": "foo", "@HDFS_PATH": mock_joinval,
                     "@CODEC": "foo"}
        self.sh.make_oozie_workflow()
        mock_materialize.assert_called_with(mock_template_str, mock_subs,
                                            mock_outfile)

    @mock.patch("thrive.setup_handler.is_camus_dir")
    def test_setup_metadata(self, mock_is_camus_dir):
        mock_is_camus_dir.return_value = True
        md_columns = ["dataset_name", "hive_db", "hive_table", "hive_ddl",
                      "vertica_db", "vertica_schema", "vertica_table", "vertica_ddl",
                      "mapper"]
        setup_metadata = dict((col, "foo") for col in md_columns)
        load_metadata = {"dataset_name": "foo", "load_id": "foo",
                         "load_type": "foo", "hive_db": "foo",
                         "hive_table": "foo", "hive_start_ts": "foo",
                         "hive_end_ts": "foo", "vertica_db": "foo",
                         "vertica_schema": "foo", "vertica_table": "foo",
                         "vertica_start_ts": "foo", "vertica_end_ts": "foo",
                         "last_load_folder": "foo"}
        lock_metadata = {"dataset_name": "foo", "locked": 0,
                         "release_attempts": 0}
        expected_dct = {
            "setup": set(setup_metadata.keys()),
            "load": set(load_metadata.keys()),
            "lock": set(lock_metadata.keys())
        }

        self.sh.setup_metadata()
        mock_mm = self.mock_mm.return_value
        mdtypes = [g[1] for g in mock_mm.insert.call_args_list]
        mdload = [g[0][0] for g in mock_mm.insert.call_args_list]
        actual_dct = dict()
        for t, l in zip(mdtypes, mdload):
            actual_dct.update({t["mdtype"]: set(l.keys())})
        self.assertDictEqual(actual_dct, expected_dct)
        mock_mm.close.assert_called_with()

    @mock.patch("thrive.setup_handler.is_camus_dir")
    def test_setup_metadata_exception(self, mock_is_camus_dir):
        mock_is_camus_dir.side_effect = thex.SetupHandlerException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.setup_metadata()

    @mock.patch("thrive.setup_handler.is_camus_dir")
    def test_setup_metadata_ConfigParserError(self, mock_is_camus_dir):
        mock_is_camus_dir.side_effect = cp.Error()
        with self.assertRaises(tsh.SetupHandlerException):
            self.sh.setup_metadata()

    @mock.patch("thrive.setup_handler.zipfile.ZipFile")
    @mock.patch("thrive.setup_handler.SetupHandler.make_schema")
    @mock.patch("thrive.setup_handler.SetupHandler.make_oozie_workflow")
    @mock.patch("thrive.setup_handler.glob.glob")
    @mock.patch("__builtin__.open")
    def test_setup_NFS(self, mock_open, mock_glob, mock_make_oozie_workflow,
                       mock_make_schema, mock_zf):

        shexec = self.mock_shell.return_value.safe_execute
        archive = mock_zf.return_value
        mock_glob.return_value = ["foo", "bar"]
        self.sh.setup_NFS()

        configval = self.config_value
        zf_calls = [mock.call(configval, configval)] * 2
        archive.extract.assert_has_calls(zf_calls)
        mock_make_oozie_workflow.assert_called_with()
        mock_make_schema.assert_called_with()
        shell_calls = [mock.call("mkdir -p %s" % configval),
                       mock.call("mkdir -p %s" % configval),
                       mock.call("mkdir -p %s" % configval),
                       mock.call("chmod a+rx %s" % configval),
                       mock.call("chmod a+r foo"),
                       mock.call("chmod a+r bar"),
                       mock.call("chmod a+x foo"),
                       mock.call("chmod a+x bar")]
        shexec.assert_has_calls(shell_calls)

    def test_setup_hive(self):
        shexec = self.mock_shell.return_value.safe_execute
        self.sh.setup_hive()
        shexec.assert_called_with("hive -f foo/foo")

    def test_setup_vertica(self):
        self.sh.setup_vertica()
        cv = self.config_value
        ddl_file = "%s/%s" % (cv, cv)
        vm = self.mock_vtica.return_value
        vm.create_table.assert_called_with(ddl_file)
        grant_calls = [
            mock.call(privilege="USAGE", level="schema", vschema=cv, to=cv),
            mock.call(privilege="SELECT", level="schema", vschema=cv, to=cv)
        ]
        vm.grant.assert_has_calls(grant_calls)

    def test_setup_hdfs(self):
        self.sh.setup_HDFS()
        cv = self.config_value
        hdfs_resource_path = nfs_dataset_path = cv
        hdfs_paths = ["%s/%s" % (hdfs_resource_path, "workflow"),
                      "%s/%s" % (hdfs_resource_path, "script")]
        resources = ["%s/%s" % (cv, cv)] * 2

        hdfs_makedir_calls = []
        hdfs_putfile_calls = []
        for hpath, resource in zip(hdfs_paths, resources):
            hdfs_makedir_calls.append(mock.call(hpath))
            hdfs_putfile_calls.append(mock.call(resource, hpath))
        hm = self.mock_hdfs.return_value
        hm.makedir.assert_has_calls(hdfs_makedir_calls)
        hm.putfile.assert_has_calls(hdfs_putfile_calls)
        hm.grantall.assert_called_with("rx", cv)

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute(self, mmd, mvtica, mhive, mhdfs, mnfs):
        self.mock_get_config.return_value = "true"
        self.sh.execute()
        mvtica.assert_called_with()
        mhive.assert_called_with()
        mhdfs.assert_called_with()
        mnfs.assert_called_with()
        mmd.assert_called_with()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_nfs_ShellException(self, mmd, mvtica, mhive, mhdfs, mnfs):
        mnfs.side_effect = tsh.ShellException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_nfs_KeyError(self, mmd, mvtica, mhive, mhdfs, mnfs):
        mnfs.side_effect = KeyError()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_hdfs_ShellException(self, mmd, mvtica, mhive, mhdfs, mnfs):
        mhdfs.side_effect = tsh.ShellException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_hive_ShellException(self, mmd, mvtica, mhive, mhdfs, mnfs):
        mhive.side_effect = tsh.ShellException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_vertica_ShellException(self, mmd, mvtica, mhive, mhdfs, mnfs):
        self.mock_get_config.return_value = "true"
        mvtica.side_effect = tsh.ShellException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_vertica_VerticaManagerException(self, mmd, mvtica, mhive, mhdfs, mnfs):
        self.mock_get_config.return_value = "true"
        mvtica.side_effect = tsh.VerticaManagerException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()

    @mock.patch("thrive.setup_handler.SetupHandler.setup_NFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_HDFS")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_hive")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_vertica")
    @mock.patch("thrive.setup_handler.SetupHandler.setup_metadata")
    def test_execute_metadata_MetadataException(self, mmd, mvtica, mhive, mhdfs, mnfs):
        self.mock_get_config.return_value = "true"
        mmd.side_effect = thex.MetadataManagerException()
        with self.assertRaises(thex.SetupHandlerException):
            self.sh.execute()
