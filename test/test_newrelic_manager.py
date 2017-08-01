
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
import thrive.newrelic_manager as tnr
from thrive.newrelic_manager import NewRelicManagerException
import thrive.shell_executor as tse
import datetime as dt


class TestNewRelicManager(unittest.TestCase):
    def setUp(self):
        self.nr = tnr.NewRelicManager('asdf', 'fake_name', 'http://test.com')
        self.metric = "fake"
        self.start = "2016-01-27T02:59:00+00:00"
        self.end = "2016-01-28T02:59:00+00:00"

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_response_valid(self, mock_safe_execute):
        self.nr._get_response(self.metric, self.start, self.end)
        mock_safe_execute.assert_called_with(
            '''curl -X GET '%s' -H '%s' -i -G -d 'names[]=%s.%s&from=%s&to=%s&summarize=true' '''
            % (self.nr.endpt_url, self.nr.api_key, self.metric, self.nr.dataset, self.start, self.end),
            as_shell=True, splitcmd=False)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_response_error(self, mock_safe_execute):
        mock_safe_execute.side_effect = tse.ShellException()
        self.assertRaises(NewRelicManagerException, self.nr._get_response, self.metric, self.start, self.end)

    def test_extract_count_valid(self):
        response = '''{"metric_data":{"from":"2016-08-27T02:59:00+00:00","to":"2016-08-28T03:30:00+00:00", \
        "metrics_not_found":[],"metrics_found":["posted.idea-jabba-applicationData"],"metrics": \
        [{"name":"posted.idea-jabba-applicationData","timeslices":[{"from":"2016-08-27T03:30:00+00:00", \
        "to":"2016-08-28T03:30:00+00:00","values":{"average_response_time":0,"calls_per_minute":0,"call_count":50, \
        "min_response_time":0,"max_response_time":0,"average_exclusive_time":0,"average_value":0, \
        "total_call_time_per_minute":0,"requests_per_minute":0,"standard_deviation":0}}]}]}}'''

        self.assertEqual(50, self.nr._extract_count(response))

    def test_extract_count_invalid_no_regex_match(self):
        response = '''{"metri_data":{"from":"2016-08-27T02:59:00+00:00","to":"2016-08-28T03:30:00+00:00", \
        "metrics_not_found":[],"metrics_found":["posted.idea-jabba-applicationData"],"metrics": \
        [{"name":"posted.idea-jabba-applicationData","timeslices":[{"from":"2016-08-27T03:30:00+00:00", \
        "to":"2016-08-28T03:30:00+00:00","values":{"average_response_time":0,"calls_per_minute":0,"call_count":50, \
        "min_response_time":0,"max_response_time":0,"average_exclusive_time":0,"average_value":0, \
        "total_call_time_per_minute":0,"requests_per_minute":0,"standard_deviation":0}}]}]}}'''

        self.assertEqual(-1, self.nr._extract_count(response))

    def test_extract_count_invalid_key_error(self):
        response = '''{"metric_data":{"from":"2016-08-27T02:59:00+00:00","to":"2016-08-28T03:30:00+00:00", \
        "metrics_not_found":[],"metrics_found":["posted.idea-jabba-applicationData"],"metrics": \
        [{"name":"posted.idea-jabba-applicationData","timeslices":[{"from":"2016-08-27T03:30:00+00:00", \
        "to":"2016-08-28T03:30:00+00:00","values":{"average_response_time":0,"calls_per_minute":0,"call_coun":50, \
        "min_response_time":0,"max_response_time":0,"average_exclusive_time":0,"average_value":0, \
        "total_call_time_per_minute":0,"requests_per_minute":0,"standard_deviation":0}}]}]}}'''

        self.assertEqual(-1, self.nr._extract_count(response))

    def test_extract_count_invalid_index_error(self):
        response = '''{"metric_data":{"from":"2016-08-27T02:59:00+00:00","to":"2016-08-28T03:30:00+00:00", \
        "metrics_not_found":[],"metrics_found":["posted.idea-jabba-applicationData"],"metrics": \
        {"name":"posted.idea-jabba-applicationData","timeslices":[{"from":"2016-08-27T03:30:00+00:00", \
        "to":"2016-08-28T03:30:00+00:00","values":{"average_response_time":0,"calls_per_minute":0,"call_count":50, \
        "min_response_time":0,"max_response_time":0,"average_exclusive_time":0,"average_value":0, \
        "total_call_time_per_minute":0,"requests_per_minute":0,"standard_deviation":0}}]}}}'''

        self.assertEqual(-1, self.nr._extract_count(response))

    def test_extract_count_invalid_value_error(self):
        response = '''{"metric_data":{"from":"2016-08-27T02:59:00+00:00","to":"2016-08-28T03:30:00+00:00", \
        "metrics_not_found":[],"metrics_found":["posted.idea-jabba-applicationData"],"metrics": \
        [{"name":"posted.idea-jabba-applicationData","timeslices":[{"from":"2016-08-27T03:30:00+00:00", \
        "to":"2016-08-28T03:30:00+00:00","values":{"average_response_time":0,"calls_per_minute":0,"call_count":a, \
        "min_response_time":0,"max_response_time":0,"average_exclusive_time":0,"average_value":0, \
        "total_call_time_per_minute":0,"requests_per_minute":0,"standard_deviation":0}}]}]}}'''

        self.assertEqual(-1, self.nr._extract_count(response))

    def test_extract_count_invalid_type_error(self):
        response = '''{"metric_data":{"from":"2016-08-27T02:59:00+00:00","to":"2016-08-28T03:30:00+00:00", \
        "metrics_not_found":[],"metrics_found":["posted.idea-jabba-applicationData"],"metrics": \
        [{"name":"posted.idea-jabba-applicationData","timeslices":[{"from":"2016-08-27T03:30:00+00:00", \
        "to":"2016-08-28T03:30:00+00:00","values":{"average_response_time":0,"calls_per_minute":0,"call_count":None, \
        "min_response_time":0,"max_response_time":0,"average_exclusive_time":0,"average_value":0, \
        "total_call_time_per_minute":0,"requests_per_minute":0,"standard_deviation":0}}]}]}}'''

        self.assertEqual(-1, self.nr._extract_count(response))

    @mock.patch("thrive.newrelic_manager.NewRelicManager._extract_count")
    @mock.patch("thrive.newrelic_manager.NewRelicManager._get_response")
    def test_get_count_error_valid(self, mock_get_response, mock_extract_count):
        self.stage = 'jetty'

        mock_get_response.return_value = "test1"
        mock_extract_count.return_value = 65

        self.assertEqual(65, self.nr.get_count(self.stage, dt.datetime.now(), dt.datetime.now()))

    def test_get_count_error_invalid_stage(self):
        self.assertRaises(NewRelicManagerException, self.nr.get_count, "asdf", dt.datetime.now(), dt.datetime.now())




