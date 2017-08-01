
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
import thrive.splunk_manager as tsm
import thrive.shell_executor as tse
from thrive.exceptions import SplunkManagerException


class TestSplunkManager(unittest.TestCase):
    def setUp(self):
        self.sm = tsm.SplunkManager('test_data', 'new_env', '/user/headless_user/logs', 'thrive_new_env', 'username',
                                    'passwd', 'http://url', 'my_app',
                                    {'Alert1': {'type': 'dataset_locked',
                                                'time_window': '1h',
                                                'action.email.to': '"abcd@xyz.com"',
                                                'action.email.priority': '2',
                                                'alert_threshold': '0',
                                                'cron_schedule': "*/60 * * * *",
                                                'action.email.inline': '1',
                                                'actions': 'email',
                                                'alert.suppress': '0',
                                                'is_scheduled': '1',
                                                'alert_type': '"number of events"',
                                                'alert_comparator': '"greater than"',
                                                'action.email.include.search': '0',
                                                'action.email.include.trigger': '0',
                                                'action.email.include.view_link': '0',
                                                'action.email.include.trigger_time': '1',
                                                'action.email.sendresults': '1'}})

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_dashboard_perm_xml(self, mock_safe_exec):
        expected = '''curl -k -u %s:%s %s/%s/%s/data/ui/views/%s/acl''' \
                   % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, self.sm.base_name)
        _ = self.sm._get_dashboard_perm_xml()
        mock_safe_exec.assert_called_with(expected, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_dashboard_perm_xml_exception(self, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(SplunkManagerException, self.sm._get_dashboard_perm_xml)

    @mock.patch("thrive.splunk_manager.SplunkManager._get_dashboard_perm_xml")
    def test_get_dash_owner_nobody(self, mock_get_xml):
        mock_get_xml.return_value = """ <id>https://example.com:8089/servicesNS/nobody/ \
                                        bio_search/alerts/fired_alerts/Preprod_thrive_test_eng_run_failed</id> """
        self.assertEqual("nobody", self.sm._get_dash_owner())

    @mock.patch("thrive.splunk_manager.SplunkManager._get_dashboard_perm_xml")
    def test_get_dash_owner_user(self, mock_get_xml):
        mock_get_xml.return_value = """ <id>https://example.com:8089/servicesNS/sdandekar/ \
                                        bio_search/alerts/fired_alerts/Preprod_thrive_test_eng_run_failed</id> """
        self.assertEqual("username", self.sm._get_dash_owner())

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    @mock.patch("thrive.splunk_manager.SplunkManager._get_dash_owner")
    def test_cleanup_dashboard_valid(self, mock_get_dash, mock_safe_exec):
        mock_get_dash.return_value = "test1"

        curl_cmd = '''curl -k -u %s:%s --request DELETE %s/%s/%s/data/ui/views/%s''' \
           % (self.sm.user, self.sm.passwd, self.sm.url, "test1", self.sm.app, self.sm.base_name)

        self.sm.cleanup_dashboard()
        mock_safe_exec.assert_called_with(curl_cmd, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    @mock.patch("thrive.splunk_manager.SplunkManager._get_dash_owner")
    def test_cleanup_dashboard_error(self, mock_get_dash, mock_safe_exec):
        mock_get_dash.return_value = "test1"
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(tsm.SplunkManagerException, self.sm.cleanup_dashboard)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_setup_dashboard(self, mock_safe_exec):
        dash_xml = "test.xml"
        expected = '''curl -k -u %s:%s %s/%s/%s/data/ui/views -d 'name=%s&eai:data=%s' ''' \
                   % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, self.sm.base_name, dash_xml)
        self.sm.setup_dashboard(dash_xml)
        mock_safe_exec.assert_called_with(expected, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_setup_dashboard_exception(self, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(SplunkManagerException, self.sm.setup_dashboard, "test.xml")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_grant_permissions_dash(self, mock_safe_exec):
        expected = '''curl -k -u %s:%s %s/%s/%s/data/ui/views/%s/acl -d sharing=%s -d owner=%s''' \
                   % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, self.sm.base_name, "app", 
                      self.sm.user)
        self.sm.grant_permissions_dash()
        mock_safe_exec.assert_called_with(expected, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_grant_permissions_dash_exception(self, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(SplunkManagerException, self.sm.grant_permissions_dash)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_cleanup_alert(self, mock_safe_exec):
        alert = "test1"
        expected = '''curl -k -u %s:%s --request DELETE %s/%s/%s/saved/searches/%s''' \
                   % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, alert)
        self.sm.cleanup_alert(alert)
        mock_safe_exec.assert_called_with(expected, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_cleanup_alert_exception(self, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(SplunkManagerException, self.sm.cleanup_alert, "test2")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_xml_saved_searches_valid(self, mock_safe_exec):

        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/saved/searches?search=%s*''' \
                   % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, self.sm.base_name)
        self.sm._get_xml_saved_searches()
        mock_safe_exec.assert_called_with(curl_cmd, splitcmd=False, as_shell=True)


    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_get_xml_saved_searches_error(self, mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(tsm.SplunkManagerException, self.sm._get_xml_saved_searches)

    @mock.patch("thrive.splunk_manager.SplunkManager.cleanup_alert")
    @mock.patch("thrive.splunk_manager.SplunkManager._get_xml_saved_searches")
    def test_cleanup_all_alerts_valid(self, mock_get_xml, mock_cleanup_alert):
        mock_get_xml.return_value = "<title>%s_asdf</title> <title>%s_test2</title>" \
                                    % (self.sm.base_name, self.sm.base_name)
        self.sm.cleanup_all_alerts()
        mock_cleanup_alert.assert_has_calls([mock.call(self.sm.base_name + x) for x in ["_asdf", "_test2"]])

    @mock.patch("thrive.splunk_manager.SplunkManager._get_xml_saved_searches")
    def test_cleanup_all_alerts_no_alerts(self, mock_get_xml):
        mock_get_xml.return_value = "test1"
        self.assertEquals(self.sm.cleanup_all_alerts(), None)

    def test_agg_alert_params_valid(self):
        alert_name = "%s_%s_%s" % (self.sm.env, self.sm.dataset, "Alert1")
        output = self.sm._agg_alert_params(alert_name)
        self.assertTrue("-d alert.suppress=0" in output)
        self.assertTrue('-d action.email.to="abcd@xyz.com"' in output)

    def test_agg_alert_params_error(self):
        alert_name = "%s_%s_%s" % (self.sm.env, self.sm.dataset, "abcd")
        self.assertRaises(SplunkManagerException, self.sm._agg_alert_params, alert_name)

    @mock.patch("thrive.splunk_manager.SplunkManager._agg_alert_params")
    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_create_alert_valid(self, mock_safe_exec, mock_agg_alert):
        alert_name = "%s_%s_%s" % (self.sm.env, self.sm.dataset, "Alert1")
        base_curl_cmd = '''curl -k -u %s:%s %s/%s/%s/saved/searches/ -d name=%s --data-urlencode search=%s''' \
                            % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, alert_name,
                               self.sm.splunk_alerts[alert_name].search_str)

        mock_agg_alert.return_value = "test3"
        self.sm.create_alert(alert_name)
        mock_safe_exec.assert_called_with(base_curl_cmd + "test3",
                                          splitcmd=False, as_shell=True)

    @mock.patch("thrive.splunk_manager.SplunkManager._agg_alert_params")
    def test_create_alert_invalid_name(self, mock_agg_alert):
        mock_agg_alert.return_value = "asdf"
        self.assertRaises(SplunkManagerException, self.sm.create_alert, "dkdl")

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    @mock.patch("thrive.splunk_manager.SplunkManager._agg_alert_params")
    def test_create_alert_error_shell_exec(self, mock_agg_alert, mock_safe_exec):
        mock_agg_alert.return_value = "asdf"
        mock_safe_exec.side_effect = tse.ShellException()
        alert_name = "%s_%s_%s" % (self.sm.env, self.sm.dataset, "Alert1")
        self.assertRaises(SplunkManagerException, self.sm.create_alert, alert_name)

    @mock.patch("thrive.splunk_manager.SplunkManager.grant_permissions_alert")
    @mock.patch("thrive.splunk_manager.SplunkManager.create_alert")
    def test_setup_all_alerts_valid(self, mock_create_alert, mock_grant_perm):
        self.sm.setup_all_alerts()
        mock_create_alert.assert_has_calls([mock.call(self.sm.base_name + x) for x in ["_Alert1"]])
        mock_grant_perm.assert_has_calls([mock.call(self.sm.base_name + x) for x in ["_Alert1"]])

    @mock.patch("thrive.splunk_manager.SplunkManager.grant_permissions_alert")
    @mock.patch("thrive.splunk_manager.SplunkManager.create_alert")
    def test_setup_all_alerts_error(self, mock_create_alert, mock_grant_perm):
        mock_grant_perm.side_effect = Exception()
        mock_create_alert.return_value = "asdf"
        self.assertRaises(SplunkManagerException, self.sm.setup_all_alerts)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_grant_permissions_alert_valid(self,mock_safe_exec):
        alert_name = "test1"
        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/saved/searches/%s/acl -d sharing=%s -d owner=%s''' \
                   % (self.sm.user, self.sm.passwd, self.sm.url, self.sm.user, self.sm.app, "test1", "app",
                      self.sm.user)
        self.sm.grant_permissions_alert(alert_name)
        mock_safe_exec.assert_called_with(curl_cmd, splitcmd=False, as_shell=True)

    @mock.patch("thrive.shell_executor.ShellExecutor.safe_execute")
    def test_grant_permissions_alert_error(self,mock_safe_exec):
        mock_safe_exec.side_effect = tse.ShellException()
        self.assertRaises(tsm.SplunkManagerException, self.sm.grant_permissions_alert, "test1")

