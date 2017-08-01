
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
import thrive.splunk_alert as tsa
from thrive.exceptions import SplunkAlertException


class TestSplunkAlert(unittest.TestCase):

    def setUp(self):
        self.sa = tsa.SplunkAlert("thrive_test", "/user/headless_user/logs", {"type": "dataset_locked",
                                                                               "time_window": "1h"})

    def test_init_valid(self):

        self.assertEqual("dataset_locked", self.sa.alert_type)
        self.assertEqual("1h", self.sa.time_window)

    def test_init_invalid(self):
        self.assertRaises(SplunkAlertException, tsa.SplunkAlert, "thrive_test", "/user/headless_user/logs",
                          {"type": "dataset_locked"})

    # def test_check_valid_alert_valid(self):
    #     pass
    #
    # def test_check_valid_alert_invalid(self):
    #     pass

    def test_get_msg_body_valid(self):
        expected = '"Dataset lock errors are usually due to a runaway job in the scheduler. In Preprod, an autounlock will occur after 3 runs where the dataset is locked. In Prod, this step is manual and will require you to run the unlock script."'
        self.assertEqual(expected, self.sa.get_msg_body())

    def test_get_msg_body_invalid(self):
        sa_invalid = tsa.SplunkAlert("thrive_test", "/user/headless_user/logs", {"type": "dataset_lock",
                                                                                  "time_window": "1h"})
        self.assertRaises(SplunkAlertException, sa_invalid.get_msg_body)

    def test_get_search_str_valid(self):
        base_search_str = "index=%s source=%s/*" % (self.sa.index, self.sa.log_path)
        freq_str = "earliest=-%s" % self.sa.time_window
        query_str = " lock_status=1"
        search_str = ''''%s %s %s' ''' % (base_search_str, freq_str, query_str)
        self.assertEqual(search_str, self.sa.get_search_str())

    def test_get_search_str_invalid(self):
        sa_invalid = tsa.SplunkAlert("thrive_test", "/user/headless_user/logs", {"type": "dataset_lock",
                                                                                  "time_window": "1h"})
        self.assertRaises(SplunkAlertException, sa_invalid.get_search_str)
