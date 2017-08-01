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
from thrive.exceptions import SplunkAlertException
from thrive.utils import logkv
import json

logger = logging.getLogger(__name__)


class SplunkAlert(object):
    """
    Object which represents a SplunkAlert.
    """

    def __init__(self, index, log_path, alert_configs):
        """
        Creates a SplunkAlert from the specified configs

        @type index: str
        @param index: Name of Splunk index (thrive_perf or thrive_prod)

        @type log_path: str
        @param log_path: Path where logs are stored

        @type alert_configs: dict(str, str)
        @param alert_configs: Dict containing configs / config values for this alert.  It must specify
        type as one of dataset_locked | run_failed | hive_vertica_mismatch | no_new_hdfs_dirs | hdfs_in_parse_mismatch,
        time_window as a string in valid Splunk time format (e.g. "2h")

        @rtype: None
        @return: None
        """

        try:
            self.index = index
            self.log_path = log_path
            self.alert_configs = alert_configs

            self.alert_type = self.alert_configs.pop("type")
            self.time_window = self.alert_configs.pop("time_window")
        except:
            logkv(logger, {"msg": "Error initializing Splunk Alert"}, "error")
            raise SplunkAlertException()

    def check_valid_alert(self):
        """
        Checks if all the required alert fields are there, if not will raise SplunkAlertException.

        @rtype: None
        @return: None
        """
        pass

    def get_msg_body(self):
        """
        Returns the body of the email message sent in the alert

        @rtype: str
        @return: Body of email message sent in the alert
        """
        try:
            msg_dict = json.loads(open('resources/splunk_message_body.json').read())
            msg_body = '''"%s"''' % ("".join(msg_dict[self.alert_type]))
            return msg_body
        except:
            logkv(logger, {"msg": "Error getting message body for Splunk Alert"}, "error")
            raise SplunkAlertException()

    def get_search_str(self):
        """
        Returns the Splunk search string based on the type of alert

        @type alert_type: str
        @param alert_type: Specifies which type of alert (e.g. "dataset_locked")

        @rtype: str
        @return: Portion of Splunk search string corresponding to this alert type
        """
        try:
            base_search_str = "index=%s source=%s/*" % (self.index, self.log_path)
            freq_str = "earliest=-%s" % self.time_window
            qry_file_text = open('resources/splunk_qry_str.json').read()
            qry_dict = json.loads(qry_file_text)
            query_str = qry_dict[self.alert_type]
            search_str = ''''%s %s %s' ''' % (base_search_str, freq_str, query_str)
            return search_str
        except:
            logkv(logger, {"msg": "Error getting search string for Splunk Alert"}, "error")
            raise SplunkAlertException()







