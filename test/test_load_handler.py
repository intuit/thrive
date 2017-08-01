
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
import unittest
import mock
import thrive.load_handler as tlh
from datetime import datetime
from test.utils.utils import squeeze
from thrive.utils import CAMUS_FOLDER_FREQ
import thrive.exceptions as thex

class TestLoadHandler(unittest.TestCase):
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

        self.oozie_patcher = mock.patch("thrive.load_handler.OozieManager")
        self.mock_oozie = self.oozie_patcher.start()

        self.newrelic_patcher = mock.patch("thrive.load_handler.NewRelicManager")
        self.mock_newrelic = self.newrelic_patcher.start()

        self.uuid_patcher = mock.patch("thrive.thrive_handler.uuid")
        self.mock_uuid = self.uuid_patcher.start()
        self.mock_uuid.uuid1.return_value = "12345"

        self.int_patcher = mock.patch("__builtin__.int")
        self.mock_int = self.int_patcher.start()
        self.mock_int.return_value = 3333

        self.float_patcher = mock.patch("__builtin__.float")
        self.mock_float = self.float_patcher.start()
        self.mock_float.return_value = 4.0

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
        self.oozie_patcher.stop()
        self.newrelic_patcher.stop()
        self.int_patcher.stop()
        self.float_patcher.stop()

    def test_init(self):
        cv = self.config_value
        _ = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                            resources_file="baz.zip")
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.get_primary_namenode.assert_called_with([cv], cv, cv)

        self.mock_oozie.assert_called_with()
        #self.mock_newrelic.assert_called_with(cv, cv, cv)

    def test_init_hdfs_manager_exception(self):
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.get_primary_namenode.side_effect = thex.HdfsManagerException()
        with self.assertRaises(thex.LoadHandlerException):
            _ = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                                resources_file="baz.zip")

    def test_init_float_exception(self):
        self.mock_float.side_effect = ValueError()
        with self.assertRaises(thex.LoadHandlerException):
            _ = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                                resources_file="baz.zip")

    # def test_init_newrelic_manager_exception(self):
    #     self.mock_newrelic.side_effect = Exception()
    #     with self.assertRaises(thex.LoadHandlerException):
    #         _ = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
    #                             resources_file="baz.zip")

    def test_get_newdirs_call_metadata(self):
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        _ = lh.get_newdirs()
        cv = self.config_value
        mm = self.mock_mm.return_value
        mm.get_lastdir.assert_called_with(cv, cv, None)

    @mock.patch("thrive.thrive_handler.datetime")
    def test_get_newdirs_call_hdfs(self, mock_dt):
        mdt = datetime(2016, 8, 18, 14, 10)
        mock_dt.now.return_value = mdt
        cv = self.config_value
        mm = self.mock_mm.return_value
        lastdir = "d_20160818-1410"
        mm.get_lastdir.return_value = lastdir
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        _ = lh.get_newdirs()
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.get_newdirs.assert_called_with(cv, lastdir, mdt, 4.0)

    def test_get_newdirs_val(self):
        hdfs_mgr = self.mock_hdfs.return_value
        expected_newdirs = "foo bar baz".split()
        hdfs_mgr.get_newdirs.return_value = expected_newdirs

        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        actual_newdirs = lh.get_newdirs()
        self.assertEqual(actual_newdirs, expected_newdirs)

    def test_make_tmproot_call(self):
        hdfs_mgr = self.mock_hdfs.return_value
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        _ = lh.make_tmproot()
        cv = self.config_value
        tmproot = "/tmp/thrive/%s/%s" % (cv, cv)
        hdfs_mgr.rmdir.assert_called_with(tmproot)
        hdfs_mgr.makedir.assert_called_with(tmproot)

    def test_make_tmproot_val(self):
        hdfs_mgr = self.mock_hdfs.return_value
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        cv = self.config_value
        expected_tmproot = "/tmp/thrive/%s/%s" % (cv, cv)
        hdfs_mgr.makedir.return_value = expected_tmproot
        actual_tmproot = lh.make_tmproot()
        self.assertEqual(actual_tmproot, expected_tmproot)

    def test_make_tmproot_hdfs_rmdir_exception(self):
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.rmdir.side_effect = tlh.HdfsManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            _ = lh.make_tmproot()

    def test_make_tmproot_hdfs_mkdir_exception(self):
        hdfs_mgr = self.mock_hdfs.return_value
        hdfs_mgr.makedir.side_effect = tlh.HdfsManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            _ = lh.make_tmproot()

    @mock.patch("__builtin__.open")
    @mock.patch("thrive.thrive_handler.datetime")
    @mock.patch("thrive.load_handler.materialize")
    def test_make_workflowpropsfile(self, mock_mtz, mock_dt, mock_open):
        mdt = datetime(2016, 8, 18, 14, 10)
        mock_dt.now.return_value = mdt

        cv = self.config_value
        workflow_xml_path = "%s/workflow/%s" % (cv, cv)

        template_str = squeeze(
            """
            jobTracker=@JOBTRACKER
            oozie.wf.application.path=@WORKFLOWXML
            nameNode=@NAMENODE
            inputFile=@INPUTFILES
            outputDir=@OUTPUTDIR
            numReducers=@NUM_REDUCERS
            """
        )

        dirlist = ["foo", "bar"]
        input_files = os.path.join(cv, "{" + "\\\\\\\\,".join(dirlist) + "}")
        output_path = "output/path"

        substitutions = {
            "@JOBTRACKER": cv,
            "@WORKFLOWXML": workflow_xml_path,
            "@NAMENODE": cv,
            "@INPUTFILES": input_files,
            "@OUTPUTDIR": output_path,
            "@NUM_REDUCERS": cv}

        mf = mock.MagicMock(spec=file)
        mock_open.return_value.__enter__.return_value = mf
        mf.read.return_value = template_str

        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        lh.make_workflowpropsfile(output_path, dirlist)

        shexec = self.mock_shell.return_value
        shexec.safe_execute.assert_called_with("mkdir -p %s" % cv)
        propfile_name = "%s/workflow_%s.properties" % (cv, mdt.strftime("%Y%m%d-%H"))
        mock_mtz.assert_called_with(template_str, substitutions, outfile=propfile_name)

    def test_vload_copy_direct_call(self):
        cv = self.config_value
        hiveptn_ = "2016/08/16/14/0"
        partfiles = "%s/%s/*" % (cv, hiveptn_)
        vschema = dtable = rtable = cv
        hm = self.mock_hdfs.return_value
        hm.get_primary_namenode.return_value = cv
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        _ = lh.vload_copy(hiveptn_, mode="direct")
        vm = self.mock_vtica.return_value
        vm.load.assert_called_with(cv, partfiles, vschema, dtable, rtable, mode="direct")

    def test_vload_copy_direct_val(self):
        cv = self.config_value
        hiveptn_ = "2016/08/16/14/0"
        hm = self.mock_hdfs.return_value
        hm.get_primary_namenode.return_value = cv
        vm = self.mock_vtica.return_value
        vm.load.return_value = 100
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        numrows = lh.vload_copy(hiveptn_, mode="direct")
        self.assertEqual(numrows, 100)

    @mock.patch("thrive.load_handler.os.path.join")
    def test_vload_copy_decompress_call(self, mock_join):
        mock_pth = "a/b/c"
        mock_join.return_value = mock_pth
        cv = self.config_value
        hiveptn_ = "2016/08/16/14/0"
        hm = self.mock_hdfs.return_value
        hm.get_primary_namenode.return_value = cv
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        _ = lh.vload_copy(hiveptn_, mode="decompress")
        vm = self.mock_vtica.return_value
        hm.makedir.assert_called_with(mock_pth)
        hm.decompress.assert_called_with(mock_pth, mock_pth)
        vm.load.assert_called_with(cv, mock_pth, cv, cv, cv, mode="decompress")

    def test_lock(self):
        cv = self.config_value
        mm = self.mock_mm.return_value
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        lh.lock()
        mm.lock.assert_called_with(cv)

    def test_lock_exception(self):
        mm = self.mock_mm.return_value
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        mm.lock.side_effect = thex.MetadataManagerException()
        with self.assertRaises(thex.LoadHandlerException):
            lh.lock()

    def test_proceed_dataset_locked(self):
        mm = self.mock_mm.return_value
        mm.get_lock_status.return_value = (1, 0)
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        self.assertFalse(lh.proceed())

    @mock.patch("thrive.load_handler.LoadHandler.get_newdirs")
    def test_proceed_no_newdirs(self, mock_gn):
        mock_gn.return_value = []
        mm = self.mock_mm.return_value
        mm.get_lock_status.return_value = (0, 0)
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        self.assertFalse(lh.proceed())

    @mock.patch("thrive.load_handler.LoadHandler.get_newdirs")
    def test_proceed_no_primary_namenode(self, mock_gn):
        mock_gn.return_value = ["foo", "bar"]
        mm = self.mock_mm.return_value
        mm.get_lock_status.return_value = (0, 0)
        hm = self.mock_hdfs.return_value
        hm.get_primary_namenode.return_value = None
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        self.assertFalse(lh.proceed())

    @mock.patch("thrive.load_handler.LoadHandler.get_newdirs")
    def test_proceed_all_conditions_satisfied(self, mock_gn):
        mock_gn.return_value = ["foo", "bar"]
        mm = self.mock_mm.return_value
        mm.get_lock_status.return_value = (0, 0)
        hm = self.mock_hdfs.return_value
        hm.get_primary_namenode.return_value = "foo"
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        self.assertTrue(lh.proceed())

    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    def test_execute_no_proceed(self, mock_proceed):
        mock_proceed.return_value = False
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        # If proceed() is False, execute() returns None
        self.assertEqual(lh.execute(), None)

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_vertica_load_false(self, mock_wpf, mock_proceed, mock_lock,
                                        mock_chunk_dirs, mock_iso_fmt):
        cv = self.config_value
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        mock_iso_fmt.return_value = "2016-08-19 14:10:00"
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        ptn_parent = "%s/%s" % (cv, "2016/08/19/14")
        ptn_path = "%s/0" % ptn_parent
        mo = self.mock_oozie.return_value
        oozie_jobid = "job: 12345"
        mo.launch.return_value = oozie_jobid
        mo.get_counts.return_value = {"map_input_records": 12466493,
                                      "map_output_records": 28701197,
                                      "reduce_input_records": 28701197,
                                      "reduce_output_records": 28701197,
                                      "skipped": 9}
        mm = self.mock_mm.return_value
        mm.get_unprocessed_partitions.return_value = ["2016/08/19/14"]
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")

        lh.execute()

        # workflow properties file tests
        mock_wpf.assert_called_with(ptn_path, ["d_20160819-1410"])

        # Oozie tests
        mo.launch.assert_called_with(propfile=None)
        mo.poll.assert_called_with(oozie_jobid, interval=10)
        mo.get_counts.assert_called_with(oozie_jobid)

        # Hive tests
        hive_mgr = self.mock_hive.return_value
        hive_mgr.create_partition.assert_called_with(ptn_path)

        # HDFS tests
        hm.grantall.assert_called_with("rx", cv)

        # Metadata manager tests
        hive_load_metadata = {
            "load_id": "12345",
            "load_type": "scheduled",
            "dataset_name": cv,
            "hive_db": cv,
            "hive_table": cv,
            "hive_start_ts": "2016-08-19 14:10:00",
            "hive_end_ts": "2016-08-19 14:10:00",
            "last_load_folder": "d_20160819-1410",
            "hive_last_partition": "2016/08/19/14/0",
            "hadoop_records_processed": 12466493,
            "hive_rows_loaded": 28701197}
        mm.insert.assert_called_with(hive_load_metadata, mdtype="load")

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_vertica_load_false_hdfs_path_exists(self, mock_wpf,
                                                         mock_proceed, mock_lock,
                                                         mock_chunk_dirs,
                                                         mock_iso_fmt):
        cv = self.config_value
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        mock_iso_fmt.return_value = "2016-08-19 14:10:00"
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = True
        hm.get_subdirs.return_value = ["0", "1"]
        ptn_parent = "%s/%s" % (cv, "2016/08/19/14")
        ptn_path = "%s/3334" % ptn_parent
        mo = self.mock_oozie.return_value
        oozie_jobid = "job: 12345"
        mo.launch.return_value = oozie_jobid
        mo.get_counts.return_value = {"map_input_records": 12466493,
                                      "map_output_records": 28701197,
                                      "reduce_input_records": 28701197,
                                      "reduce_output_records": 28701197,
                                      "skipped": 9}
        mm = self.mock_mm.return_value
        mm.get_unprocessed_partitions.return_value = ["2016/08/19/14"]
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")

        lh.execute()

        # workflow properties file tests
        mock_wpf.assert_called_with(ptn_path, ["d_20160819-1410"])

    #@mock.patch("thrive.load_handler.dirname_to_dto")
    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    @mock.patch("thrive.load_handler.LoadHandler.vload_copy")
    def test_execute_vertica_load_true(self, mock_vload_copy, mock_wpf,
                                       mock_proceed, mock_lock, mock_chunk_dirs,
                                       mock_iso_fmt):
        cv = self.mock_get_config.return_value = "true"
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        ts = "2016-08-19 14:10:00"
        mock_iso_fmt.return_value = ts
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        hive_ptn = "2016/08/19/14/0"
        ptn_path = "%s/%s" % (cv, hive_ptn)
        mo = self.mock_oozie.return_value
        oozie_jobid = "job: 12345"
        mo.launch.return_value = oozie_jobid
        mo.get_counts.return_value = {"map_input_records": 12466493,
                                      "map_output_records": 28701197,
                                      "reduce_input_records": 28701197,
                                      "reduce_output_records": 28701197,
                                      "skipped": 9}
        mm = self.mock_mm.return_value
        mm.get_unprocessed_partitions.return_value = \
            [("12345", hive_ptn, "100", "50")]

        mock_vload_copy.return_value = 100

        dto = datetime(2016, 8, 19, 14)
        #mock_dtd.return_value = dto
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")

        mn = self.mock_newrelic.return_value
        mn.get_count.return_value = 100

        lh.execute()

        # workflow properties file tests
        mock_wpf.assert_called_with(ptn_path, ["d_20160819-1410"])

        # Oozie tests
        mo.launch.assert_called_with(propfile=None)
        mo.poll.assert_called_with(oozie_jobid, interval=10)
        mo.get_counts.assert_called_with(oozie_jobid)

        # Hive tests
        hive_mgr = self.mock_hive.return_value
        hive_mgr.create_partition.assert_called_with(ptn_path)

        # HDFS tests
        hm.grantall.assert_called_with("rx", cv)

        # Metadata manager tests
        hive_load_metadata = {
            "load_id": "12345",
            "load_type": "scheduled",
            "dataset_name": cv,
            "hive_db": cv,
            "hive_table": cv,
            "hive_start_ts": ts,
            "hive_end_ts": ts,
            "last_load_folder": "d_20160819-1410",
            "hive_last_partition": hive_ptn,
            "hadoop_records_processed": 12466493,
            "hive_rows_loaded": 28701197}
        mm.insert.assert_called_with(hive_load_metadata, mdtype="load")

        vertica_metadata = {
            "vertica_db": cv,
            "vertica_schema": cv,
            "vertica_table": cv,
            "vertica_start_ts": ts,
            "vertica_end_ts": ts,
            "vertica_last_partition": hive_ptn,
            "vertica_rows_loaded": 100,
            "status": "SUCCESS"}
        mm.update.assert_called_with(("12345", hive_ptn), vertica_metadata,
                                     mdtype="load")

        # Vertica manager tests
        mock_vload_copy.assert_called_with(hive_ptn, mode="direct")

        # dirname_to_dto_tests
        # mock_dtd.assert_has_calls([mock.call("d_20160819-1410"),
        #                            mock.call("d_20160819-1410")])

        # NewRelicManager tests
        # nm = self.mock_newrelic.return_value
        # CFF = CAMUS_FOLDER_FREQ
        # nm.get_count.assert_has_calls([mock.call("jetty", dto, dto + CFF),
        #                                mock.call("kafka", dto, dto + CFF)])

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_OozieManager_launch_exception(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):

        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mo = self.mock_oozie.return_value
        mo.launch.side_effect = tlh.OozieManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_OozieManager_poll_exception(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        ts = "2016-08-19 14:10:00"
        mock_iso_fmt.return_value = ts
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mo = self.mock_oozie.return_value
        mo.poll.side_effect = tlh.OozieManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_OozieManager_get_counts_exception(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mo = self.mock_oozie.return_value
        mo.get_counts.side_effect = tlh.OozieManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_HiveManager_create_partition_exception(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        hive_mgr = self.mock_hive.return_value
        hive_mgr.create_partition.side_effect = tlh.HiveManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_HdfsManager_grantall_exception(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        hm.grantall.side_effect = thex.HdfsManagerException
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_MetadataManager_insert_exception(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mm = self.mock_mm.return_value
        mm.insert.side_effect = tlh.MetadataManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_BaseException(self, mock_wpf, mock_proceed,
                                            mock_lock, mock_chunk_dirs,
                                            mock_iso_fmt):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mm = self.mock_mm.return_value
        mm.insert.side_effect = BaseException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(BaseException):
            lh.execute()

    @mock.patch("thrive.load_handler.dirname_to_dto")
    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_VerticaManager_load_exception(self, mock_wpf,
                                       mock_proceed, mock_lock, mock_chunk_dirs,
                                       mock_iso_fmt, mock_dtd):
        cv = self.mock_get_config.return_value = "true"
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mm = self.mock_mm.return_value
        mm.get_unprocessed_partitions.return_value = \
            [("12345", "2016/08/19/14/0", "100", "50")]
        vm = self.mock_vtica.return_value
        vm.load.side_effect = tlh.VerticaManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.dirname_to_dto")
    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_MetadataManager_update_exception(self, mock_wpf,
                                       mock_proceed, mock_lock, mock_chunk_dirs,
                                       mock_iso_fmt, mock_dtd):
        cv = self.mock_get_config.return_value = "true"
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mm = self.mock_mm.return_value
        mm.get_unprocessed_partitions.return_value = \
            [("12345", "2016/08/19/14/0", "100", "50")]
        mm.update.side_effect = tlh.MetadataManagerException()
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        with self.assertRaises(thex.LoadHandlerException):
            lh.execute()

    @mock.patch("thrive.load_handler.dirname_to_dto")
    @mock.patch("thrive.load_handler.iso_format")
    @mock.patch("thrive.load_handler.chunk_dirs")
    @mock.patch("thrive.load_handler.LoadHandler.lock")
    @mock.patch("thrive.load_handler.LoadHandler.proceed")
    @mock.patch("thrive.load_handler.LoadHandler.make_workflowpropsfile")
    def test_execute_finally(self, mock_wpf, mock_proceed, mock_lock,
                             mock_chunk_dirs, mock_iso_fmt, mock_dtd):
        mock_proceed.return_value = True
        mock_chunk_dirs.return_value = {"2016/08/19/14": ["d_20160819-1410"]}
        hm = self.mock_hdfs.return_value
        hm.path_exists.return_value = False
        mm = self.mock_mm.return_value
        mm.get_unprocessed_partitions.return_value = \
            [("12345", "2016/08/19/14/0", "100", "50")]
        lh = tlh.LoadHandler(datacfg_file="foo", envcfg_file="bar",
                             resources_file="baz.zip")
        lh.locked = True
        lh.execute()
        mm.release.assert_called_with(self.config_value)
