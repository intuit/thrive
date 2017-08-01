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
from thrive.utils import logkv
from thrive.shell_executor import ShellException
from thrive.thrive_manager import ThriveManager
from thrive.exceptions import SplunkManagerException
from thrive.splunk_alert import SplunkAlert

logger = logging.getLogger(__name__)


class SplunkManager(ThriveManager):
    """
    Class to manage Splunk alerts and dashboards
    """

    def __init__(self, dataset, env, log_path, index, user, passwd, url, app, alert_param_dict):
        """
        Initialize Splunk manager based on configs

        @type dataset: str
        @param dataset: Name of dataset being loaded

        @type env: str
        @param env: Environment (prod or preprod)

        @type log_path: str
        @param log_path: Path where logs are stored

        @type index: str
        @param index: Name of Splunk index (thrive_perf or thrive_prod)

        @type user: str
        @param user: Name of Splunk user to run as

        @type passwd: str
        @param passwd: Password for Splunk user

        @type url: str
        @param url: Prefix URL for accessing / modifying Splunk resources

        @type app: str
        @param app: Splunk app (biosearch)

        @type alert_param_dict: dict(str, dict(str, object))
        @param alert_param_dict: Key-value pairs of alert section name, and values as dictionaries defining
        config / config value pairs for each alert

        @rtype: None
        @return: None
        """
        super(SplunkManager, self).__init__()

        try:
            self.dataset = dataset
            self.env = env
            self.log_path = log_path
            self.index = index
            self.user = user
            self.passwd = passwd
            self.url = url
            self.app = app
            self.base_name = "%s_%s" % (self.env, self.dataset)

            self.splunk_alerts = dict()

            for section in alert_param_dict:
                alert_name = "%s_%s_%s" % (self.env, self.dataset, section)
                logkv(logger, {"msg": "Instantiating SplunkAlert object for %s" % alert_name}, "info")
                salert = SplunkAlert(self.index, self.log_path, alert_param_dict[section])

                salert.search_str = salert.get_search_str()
                salert.alert_configs["action.email.message.alert"] = salert.get_msg_body()
                self.splunk_alerts[alert_name] = salert
        except:
            logkv(logger, {"msg": "Error setting up Splunk Manager"}, "error")
            raise SplunkManagerException()

    def _get_dashboard_perm_xml(self):
        """
        Internal method which returns XML containing info on permissions for dashboard.  This is used by
        cleanup_dashboard() to figure out which REST endpoint to delete.

        @rtype: str
        @return: XML from ACL endpoint for dashboard
        """

        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/data/ui/views/%s/acl''' \
                   % (self.user, self.passwd, self.url, self.user, self.app, self.base_name)
        try:
            # Execute curl command
            result = self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Got dashboard permission XML"}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error getting dashboard permission XML"}, "error")
            raise SplunkManagerException()

        return result.output


    def _get_dash_owner(self):
        """
        Retrieve owner for dash.  Internal method used for figuring out REST endpoint for dashboard.

        @rtype: str
        @return: Name of owner for Splunk dashboard
        """

        # Check who owner is
        if len(re.findall("nobody", self._get_dashboard_perm_xml())) != 0:
            return "nobody"
        else:
            return self.user

    def cleanup_dashboard(self):
        """
        Delete an existing Splunk dashboard

        @rtype: None
        @return: None
        """

        owner = self._get_dash_owner()

        curl_cmd = '''curl -k -u %s:%s --request DELETE %s/%s/%s/data/ui/views/%s''' \
                   % (self.user, self.passwd, self.url, owner, self.app, self.base_name)

        try:
            # Execute curl command
            self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Removed dashboard",
                           "dataset": self.dataset}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error in removing dashboard",
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    def setup_dashboard(self, dash_xml):
        """
        Creates a new Splunk dashboard or overwrites it if it already exists

        @type dash_xml: str
        @param dash_xml: Dashboard XML to post

        @rtype: None
        @return: None
        """

        # Construct curl command (change this to use urllib2)
        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/data/ui/views -d 'name=%s&eai:data=%s' ''' \
                   % (self.user, self.passwd, self.url, self.user, self.app, self.base_name, dash_xml)

        try:
            # Execute curl command
            self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Set up dashboard",
                           "dataset": self.dataset}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error in setting up dashboard",
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    def grant_permissions_dash(self, group="app"):
        """
        Grant permissions for an existing Splunk dashboard for the specified group

        @type group: str
        @param group: Level to grant permissions for.  Default is "app"

        @rtype: None
        @return: None
        """

        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/data/ui/views/%s/acl -d sharing=%s -d owner=%s''' \
                   % (self.user, self.passwd, self.url, self.user, self.app, self.base_name, group, self.user)

        try:
            # Execute curl command
            self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Granted permissions on dashboard",
                           "dataset": self.dataset}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error in granting permissions on dashboard",
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    def cleanup_alert(self, alert):
        """
        Deletes specified Splunk alert

        @type alert: str
        @param alert: Last part of URL path for alert REST endpoint

        @rtype: None
        @return: None
        """
        curl_cmd = '''curl -k -u %s:%s --request DELETE %s/%s/%s/saved/searches/%s''' \
                   % (self.user, self.passwd, self.url, self.user, self.app, alert)

        try:
            # Execute curl command
            self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Removed alert %s" % alert,
                           "dataset": self.dataset}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error in removing alert %s" % alert,
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    # def calc_alert_name(self, alert_sec):
    #     """
    #     Builds alert name from environment, dataset name, and config section header
    #
    #     @type alert_suffix: str
    #     @param alert_suffix: Name from section header in configs
    #
    #     @rtype: str
    #     @return: Full name of alert in Splunk
    #     """

    def _get_xml_saved_searches(self):
        """
        Get XML for all saved searches for given dataset

        @rtype: str
        @return: XML for all saved searches from this dataset's REST endpoint
        """

        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/saved/searches?search=%s*''' \
                   % (self.user, self.passwd, self.url, self.user, self.app, self.base_name)

        try:
            # Execute curl command
            xml = self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True).output
            logkv(logger, {"msg": "Got XML for all alerts",
                           "dataset": self.dataset}, "info")
            return xml
        except ShellException:
            logkv(logger, {"msg": "Error in getting XML for all alerts",
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    # def _parse_alert_list(self, xml):
    #     """
    #     Parses out alert names from an XML response for saved searches
    #
    #     @type xml: str
    #     @param xml: XML string returned for saved searches
    #
    #     @rtype: [str]
    #     @return: List of alert names
    #     """
    #     return

    def cleanup_all_alerts(self):
        """
        Delete all Splunk alerts for this dataset

        @rtype: None
        @return: None
        """

        xml = self._get_xml_saved_searches()

        # Parse to get the list of REST endpoints for all alerts
        # try:
        #     alert_name_list = re.findall('\<title\>(%s.*)\</title\>' % self.base_name, xml)
        # except:
        #     logkv(logger, {"msg": "Error in parsing XML for all alerts",
        #                    "dataset": self.dataset}, "error")
        #     raise SplunkManagerException()

        alert_name_list = re.findall('<title>(%s.+?)</title>' % self.base_name, xml)
        logkv(logger, {"msg": "Parsed XML for all alerts",
                       "dataset": self.dataset}, "info")

        if len(alert_name_list) != 0:
            # Call cleanup alert
            for alert in alert_name_list:
                self.cleanup_alert(alert)

        else:
            logkv(logger, {"msg": "No alerts to clean up",
                           "dataset": self.dataset}, "info")

    def grant_permissions_alert(self, alert_name, group="app"):
        """
        Grant permissions for an existing Splunk alert for the specified group

        @type group: str
        @param group: Level to grant permissions for.  Default is "app"

        @rtype: None
        @return: None
        """

        curl_cmd = '''curl -k -u %s:%s %s/%s/%s/saved/searches/%s/acl -d sharing=%s -d owner=%s''' \
                   % (self.user, self.passwd, self.url, self.user, self.app, alert_name, group, self.user)

        try:
            # Execute curl command
            self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Granted permissions on alert",
                           "dataset": self.dataset}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error in granting permissions on alert",
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    def setup_all_alerts(self):
        """
        Create all alerts based on stored SplunkAlerts

        @rtype: None
        @return: None
        """

        try:
            for alert_name in self.splunk_alerts.keys():
                self.create_alert(alert_name)
                self.grant_permissions_alert(alert_name)
        except:
            logkv(logger, {"msg": "Error in setting up all alerts",
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

    def create_alert(self, alert_name):
        """
        Makes a call to the Splunk REST API in order to create the specified alert

        @type alert_name: str
        @param alert_name: Name of alert to be created

        @rtype: None
        @return: None
        """

        try:
            base_curl_cmd = '''curl -k -u %s:%s %s/%s/%s/saved/searches/ -d name=%s --data-urlencode search=%s''' \
                            % (self.user, self.passwd, self.url, self.user, self.app, alert_name,
                               self.splunk_alerts[alert_name].search_str)
            curl_cmd = base_curl_cmd + self._agg_alert_params(alert_name)
        except KeyError:
            logkv(logger, {"msg": "Error finding alert name %s" % alert_name,
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

        try:
            # Execute curl command
            self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Created Splunk email alert %s" % alert_name,
                           "dataset": self.dataset}, "info")
        except ShellException:
            logkv(logger, {"msg": "Error in creating Splunk email alert",
                           "dataset": self.dataset, "curl_cmd": curl_cmd}, "error")
            raise SplunkManagerException()

    def _agg_alert_params(self, alert_name):
        """
        Returns a string that specifies the various fields / values associated with this alert

        @type alert_name: str
        @param alert_name: Name of alert whose params are to be aggregated

        @rtype: str
        @return: String specifying the parameters for the alert to be used in a call to Splunk REST API
        """

        params_curl_cmd = """"""
        try:
            alert_dict = self.splunk_alerts[alert_name].alert_configs
        except:
            logkv(logger, {"msg": "Error finding alert name %s" % alert_name,
                           "dataset": self.dataset}, "error")
            raise SplunkManagerException()

        for key in alert_dict.keys():
            params_curl_cmd += """ -d %s=%s""" % (key, alert_dict[key])
        return params_curl_cmd


