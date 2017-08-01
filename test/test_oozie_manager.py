
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
import thrive.oozie_manager as tom
import thrive.shell_executor as tse

INFO_MSG = \
"""
Job ID : 0436651-160203234824430-oozie-oozi-W
------------------------------------------------------------------------------------------------------------------------------------
Workflow Name : rohan-test
App Path      : /user/headless_user/workflows/oozie_hello_world.xml
Status        : KILLED
Run           : 0
User          : headless_user
Group         : -
Created       : 2016-08-16 01:06 GMT
Started       : 2016-08-16 01:06 GMT
Last Modified : 2016-08-16 01:06 GMT
Ended         : 2016-08-16 01:06 GMT
CoordAction ID: -

Actions
------------------------------------------------------------------------------------------------------------------------------------
ID                                                                            Status    Ext ID                 Ext Status Err Code
------------------------------------------------------------------------------------------------------------------------------------
0436651-160203234824430-oozie-oozi-W@fail                                     OK        -                      OK         E0729
------------------------------------------------------------------------------------------------------------------------------------
0436651-160203234824430-oozie-oozi-W@:start:                                  OK        -                      OK         -
------------------------------------------------------------------------------------------------------------------------------------
0436651-160203234824430-oozie-oozi-W@set-input                                ERROR     -                      ERROR      FS001
------------------------------------------------------------------------------------------------------------------------------------
"""

COUNTER_MSG = \
"""
ID : 0436835-160203234824430-oozie-oozi-W@parse-json
------------------------------------------------------------------------------------------------------------------------------------
Console URL       : http://mytesttracker:8088/proxy/application_1454571375726_4974108/
Error Code        : -
Error Message     : -
External ID       : job_1454571375726_4974108
External Status   : SUCCEEDED
Name              : parse-json
Retries           : 0
Tracker URI       : mytesttracker.net:8032
Type              : map-reduce
Started           : 2016-08-16 01:18:51 GMT
Status            : OK
Ended             : 2016-08-16 01:30:56 GMT
External Stats    : {"org.apache.hadoop.mapreduce.JobCounter":{"SLOTS_MILLIS_MAPS":128366838,"TOTAL_LAUNCHED_REDUCES":1,"MB_MILLIS_REDUCES":1299742720,"RACK_LOCAL_MAPS":1,"MB_MILLIS_MAPS":131447642112,"TOTAL_LAUNCHED_MAPS":2340,"VCORES_MILLIS_REDUCES":634640,"MILLIS_MAPS":64183419,"MILLIS_REDUCES":634640,"DATA_LOCAL_MAPS":2339,"VCORES_MILLIS_MAPS":64183419,"SLOTS_MILLIS_REDUCES":1269280},"org.apache.hadoop.mapred.JobInProgress$Counter":{"SLOTS_MILLIS_MAPS":128366838,"TOTAL_LAUNCHED_REDUCES":1,"MB_MILLIS_REDUCES":1299742720,"RACK_LOCAL_MAPS":1,"MB_MILLIS_MAPS":131447642112,"TOTAL_LAUNCHED_MAPS":2340,"VCORES_MILLIS_REDUCES":634640,"MILLIS_MAPS":64183419,"MILLIS_REDUCES":634640,"DATA_LOCAL_MAPS":2339,"VCORES_MILLIS_MAPS":64183419,"SLOTS_MILLIS_REDUCES":1269280},"Shuffle Errors":{"CONNECTION":0,"BAD_ID":0,"WRONG_REDUCE":0,"IO_ERROR":0,"WRONG_LENGTH":0,"WRONG_MAP":0},"ACTION_TYPE":"MAP_REDUCE","org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter":{"BYTES_WRITTEN":2159306610},"org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter":{"BYTES_READ":5872206739},"THRIVE":{"SKIPPED":9},"FileSystemCounters":{"FILE_WRITE_OPS":0,"FILE_READ_OPS":0,"FILE_LARGE_READ_OPS":0,"FILE_BYTES_READ":4207986076,"HDFS_BYTES_READ":5872662349,"FILE_BYTES_WRITTEN":8628669301,"HDFS_LARGE_READ_OPS":0,"HDFS_WRITE_OPS":2,"HDFS_READ_OPS":7023,"HDFS_BYTES_WRITTEN":2159306610},"org.apache.hadoop.mapreduce.TaskCounter":{"MAP_OUTPUT_MATERIALIZED_BYTES":4130899898,"MAP_INPUT_RECORDS":12466493,"MERGED_MAP_OUTPUTS":2340,"REDUCE_SHUFFLE_BYTES":4130899898,"SPILLED_RECORDS":57402394,"MAP_OUTPUT_BYTES":16437224124,"COMMITTED_HEAP_BYTES":3092836777984,"CPU_MILLISECONDS":45902420,"FAILED_SHUFFLE":0,"SPLIT_RAW_BYTES":455610,"COMBINE_INPUT_RECORDS":0,"REDUCE_INPUT_RECORDS":28701197,"REDUCE_INPUT_GROUPS":28701197,"COMBINE_OUTPUT_RECORDS":0,"PHYSICAL_MEMORY_BYTES":1997327265792,"REDUCE_OUTPUT_RECORDS":28701197,"VIRTUAL_MEMORY_BYTES":5961115013120,"MAP_OUTPUT_RECORDS":28701197,"SHUFFLED_MAPS":2340,"GC_TIME_MILLIS":94574},"org.apache.hadoop.mapreduce.FileSystemCounter":{"FILE_WRITE_OPS":0,"FILE_READ_OPS":0,"FILE_LARGE_READ_OPS":0,"FILE_BYTES_READ":4207986076,"HDFS_BYTES_READ":5872662349,"FILE_BYTES_WRITTEN":8628669301,"HDFS_LARGE_READ_OPS":0,"HDFS_WRITE_OPS":2,"HDFS_READ_OPS":7023,"HDFS_BYTES_WRITTEN":2159306610},"org.apache.hadoop.mapred.Task$Counter":{"MAP_OUTPUT_MATERIALIZED_BYTES":4130899898,"MAP_INPUT_RECORDS":12466493,"MERGED_MAP_OUTPUTS":2340,"REDUCE_SHUFFLE_BYTES":4130899898,"SPILLED_RECORDS":57402394,"MAP_OUTPUT_BYTES":16437224124,"COMMITTED_HEAP_BYTES":3092836777984,"CPU_MILLISECONDS":45902420,"FAILED_SHUFFLE":0,"SPLIT_RAW_BYTES":455610,"COMBINE_INPUT_RECORDS":0,"REDUCE_INPUT_RECORDS":28701197,"REDUCE_INPUT_GROUPS":28701197,"COMBINE_OUTPUT_RECORDS":0,"PHYSICAL_MEMORY_BYTES":1997327265792,"REDUCE_OUTPUT_RECORDS":28701197,"VIRTUAL_MEMORY_BYTES":5961115013120,"MAP_OUTPUT_RECORDS":28701197,"SHUFFLED_MAPS":2340,"GC_TIME_MILLIS":94574}}
External ChildIDs : job_1454571375726_4974116
------------------------------------------------------------------------------------------------------------------------------------
"""


class TestOozieManager(unittest.TestCase):
    def setUp(self):
        self.shell_patcher = mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
        self.mock_exec = self.shell_patcher.start()

        self.om = tom.OozieManager()

    def tearDown(self):
        self.shell_patcher.stop()

    def test_launch_no_propfile(self):
        with self.assertRaises(tom.OozieManagerException):
            self.om.launch(propfile=None)

    def test_launch_call(self):
        propfile = "foo"
        _ = self.om.launch(propfile=propfile)
        cmd = "oozie job -config %s -run" % propfile
        self.mock_exec.assert_called_with(cmd)

    def test_launch_val(self):
        self.mock_exec.return_value = tse.ShellResult(0, "job: 12-34", "")
        jobid = self.om.launch(propfile="foo")
        self.assertEqual(jobid, "12-34")

    @mock.patch("re.findall")
    def test_get_status(self, mock_findall):
        jobid = "Status: abc-123"
        _ = self.om.get_status(jobid)
        self.mock_exec.assert_called_with("oozie job -info %s" % jobid)

    @mock.patch("re.findall")
    def test_get_status_indexerror(self, mock_findall):
        mock_findall.side_effect = IndexError()
        with self.assertRaises(Exception):
            _ = self.om.get_status("1234")

    def test_get_status_val(self):
        self.mock_exec.return_value = tse.ShellResult(0, INFO_MSG, "")
        jobstatus = self.om.get_status("0436651-160203234824430-oozie-oozi-W")
        self.assertDictEqual(jobstatus,
                             {"overall": "KILLED", "fail": "OK", ":start:": "OK",
                              "set-input": "ERROR"})

    @mock.patch("__builtin__.dict")
    @mock.patch("re.findall")
    def test_get_status_exception(self, mock_findall, mock_dict):
        mock_dict.side_effect = Exception()
        with self.assertRaises(Exception):
            _ = self.om.get_status("foo")

    def test_get_logtrace_call(self):
        jobid = "job: 12-34"
        _ = self.om.get_logtrace(jobid)
        cmd = "oozie job -log %s" % jobid
        self.mock_exec.assert_called_with(cmd)

    def test_get_logtrace_val(self):
        self.mock_exec.return_value = tse.ShellResult(0, "foo", "")
        lt = self.om.get_logtrace("1234")
        self.assertEqual(lt, "foo")

    def test_get_logtrace_exception(self):
        self.mock_exec.side_effect = tse.ShellException()
        with self.assertRaises(Exception):
            _ = self.om.get_logtrace("1234")

    @mock.patch("re.findall")
    @mock.patch("json.loads")
    def test_get_counts_call(self, mock_loads, mock_findall):
        jobid, action = "job: 12-34", "parse-json"
        cmd = "oozie job -info %s@%s -verbose" % (jobid, action)
        self.om.get_counts(jobid)
        self.mock_exec.assert_called_with(cmd)

    def test_get_counts_val(self):
        self.mock_exec.return_value = tse.ShellResult(0, COUNTER_MSG, "")
        counts = self.om.get_counts("0436835-160203234824430-oozie-oozi-W")
        self.assertDictEqual(counts,
                             {"map_input_records": 12466493,
                              "map_output_records": 28701197,
                              "reduce_input_records": 28701197,
                              "reduce_output_records": 28701197,
                              "skipped": 9})

    def test_get_counts_exception(self):
        self.mock_exec.side_effect = Exception()
        with self.assertRaises(Exception):
            _ = self.om.get_counts("1234")

    @mock.patch("thrive.oozie_manager.OozieManager.get_status")
    def test_poll_job_fail(self, mock_get_status):
        mock_get_status.return_value = {"step0": "OK", "step2": "FAIL", "overall": "KILLED"}
        with self.assertRaises(tom.OozieManagerException):
            self.om.poll("foo", interval=0)
