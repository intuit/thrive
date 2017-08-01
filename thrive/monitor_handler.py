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
from thrive.thrive_handler import ThriveHandler
import logging
from thrive.splunk_manager import SplunkManager
from thrive.utils import materialize, logkv

logger = logging.getLogger(__name__)


class MonitorHandler(ThriveHandler):

    def __init__(self, datacfg_file=None, envcfg_file=None, resources_file=None):
        """
        Initializes the ThriveHandler superclass and instantiates manager classes
        needed to performing various monitor-related actions

        @type datacfg_file:  str
        @param datacfg_file: Full or relative path of the dataset-specific config file

        @type envcfg_file:  str
        @param envcfg_file: Full or relative path of the global environment config file

        @type resources_file: str
        @param resources_file: Full or relative path of the resources file

        @rtype: None
        @return: None
        """
        super(MonitorHandler, self).__init__(datacfg_file, envcfg_file, resources_file)
        logkv(logger, {"msg": "Initializing Splunk Manager",
                       "dataset": self.get_config("dataset_name")}, "info")

        self.alert_param_dict = dict()

        # loop over sections to get configs for each alert
        self.alert_sections = [x for x in self.datacfg.get_sections() if x.lower().startswith("alert")]
        for section in self.alert_sections:
            self.alert_param_dict[section] = self.datacfg.get_section_configs(section)

        logkv(logger, {"msg": "Read in config params for Splunk Alerts",
                       "dataset": self.get_config("dataset_name")}, "info")

        print self.alert_param_dict

        self.splunk_mgr = SplunkManager(self.get_config("dataset_name"), self.get_config("splunk_env"),
                                        self.get_config("nfs_log_path"),
                                        self.get_config("splunk_index"),
                                        self.get_config("splunk_user", configtype="env"),
                                        self.get_config("splunk_passwd", configtype="env"),
                                        self.get_config("splunk_url"), self.get_config("splunk_app"),
                                        self.alert_param_dict)

    def execute(self):
        """
        Top-level method for MonitorHandler, manages the workflow which creates dashboards and email alerts.

        @rtype: None
        @return: None
        """
        try:
            # Cleanup alerts, cleanup dashboard
            logkv(logger, {"msg": "Cleaning up alerts",
                           "dataset": self.get_config("dataset_name")}, "info")
            self.splunk_mgr.cleanup_all_alerts()

            logkv(logger, {"msg": "Cleaning up dashboard",
                           "dataset": self.get_config("dataset_name")}, "info")
            self.splunk_mgr.cleanup_dashboard()

            logkv(logger, {"msg": "Starting monitoring setup",
                           "dataset": self.get_config("dataset_name")}, "info")
            # Create xml string from template
            template_file = os.path.join(self.get_config("nfs_resource_path"),
                                         "splunk_dashboard_template.xml")

            dash_template = open(template_file, "r").read()

            substitutions = {
                "@DATASETNAME": self.get_config("dataset_name"),
                "@ENV": self.get_config("splunk_env"),
                "@INDEX": self.get_config("splunk_index"),
                "@LOGDIR": self.get_config("nfs_log_path")
            }

            logkv(logger, {"msg": "Materializing dashboard XML template",
                           "dataset": self.get_config("dataset_name")}, "info")
            dash_xml = materialize(dash_template, substitutions)

            # Set up dashboard
            logkv(logger, {"msg": "Setting up dashboard",
                           "dataset": self.get_config("dataset_name")}, "info")
            self.splunk_mgr.setup_dashboard(dash_xml)

            # Granting permissions
            logkv(logger, {"msg": "Setting dashboard permissions",
                           "dataset": self.get_config("dataset_name")}, "info")
            self.splunk_mgr.grant_permissions_dash()

            logkv(logger, {"msg": "Creating Splunk email alert",
                           "dataset": self.get_config("dataset_name")}, "info")

            # Setting up Splunk alerts
            self.splunk_mgr.setup_all_alerts()

        except Exception as ex:
            logkv(logger, {"msg": "Thrive monitoring setup failed", "exception": ex}, "error")
            raise
