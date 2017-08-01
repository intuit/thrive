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

import uuid
import logging

from datetime import datetime
from thrive.hive_manager import HiveManager
from thrive.metadata_manager import MetadataManager
from thrive.config_loader import ConfigLoader
from thrive.hdfs_manager import HdfsManager
from thrive.shell_executor import ShellExecutor
from thrive.vertica_manager import VerticaManager
from thrive.utils import logkv
from thrive.exceptions import ThriveHandlerException

logger = logging.getLogger(__name__)


class ThriveHandler(object):
    """
    Base handler class for managing and executing setup and load actions. The base
    class contains common functionality such as obtaining a MetadataManager
    instance, parsing config file, and logging
    """
    def __init__(self, datacfg_file=None, envcfg_file=None, resources_file=None):
        """
        Parses config file and performs basic checks on filetypes

        @type datacfg_file:  str
        @param datacfg_file: Full or relative path of the dataset-specific config file

        @type envcfg_file:  str
        @param envcfg_file: Full or relative path of the global environment config file

        @type resources_file: str
        @param resources_file: Full or relative path of the resources file

        @type return: None
        @return: None
        """

        # self.parser = SafeConfigParser()
        # self.parser.read(config_file)

        # Instantiate ConfigLoader for managing dataset-specific config
        self.datacfg = ConfigLoader(datacfg_file)

        # Instantiate ConfigLoader for managing dataset-independent global configs
        self.envcfg = ConfigLoader(envcfg_file)

        # Resources file
        self.resources = resources_file

        if self.resources is not None:
            if not self.resources.endswith(".zip"):
                logkv(logger, {"msg": "Resource is not a zip file",
                               "resource_file": self.resources}, "info")
                raise ThriveHandlerException

        credtypes = ["dbtype", "dbhost", "dbport", "dbuser", "dbpass", "dbname"]
        md_credentials = dict([(cred, self.get_config(cred, configtype="env"))
                               for cred in credtypes])

        self.metadata_mgr = MetadataManager(credentials=md_credentials)

        # Get the timestamp at which the present load started
        self.loadts = datetime.now()

        # Create a ShellExecutor instance for managing execution of Shell commands for
        # all subclasses
        self.shell_exec = ShellExecutor()

        # Instantiate HdfsManager for HDFS-related tasks
        self.hdfs_mgr = HdfsManager()

        # Instantiate Vertica manager for Vertica-related tasks
        vconfigs = ["vertica_db", "vertica_vsql_path", "vertica_krb_svcname",
                    "vertica_krb_host", "vertica_host", "vertica_port",
                    "vertica_user"]

        # Create connection_info. We're working with Python 2.6, so cannot use
        # dictionary comprehension and have to resort to passing tupes to the dict
        # constructor
        vconnection_info = dict((key, self.get_config(key)) for key in vconfigs)
        self.vertica_mgr = VerticaManager(vconnection_info)

        # Instantiate a HiveManager for Hive-related tasks
        self.hive_mgr = HiveManager(db=self.get_config("hive_db"),
                                    table=self.get_config("hive_table"))

        # Create a load_id for this load. Used by 'setup' and 'load' phases
        self.load_id = uuid.uuid1()

    def get_config(self, config, configtype="data"):
        """
        Returns value of requested "config" of type "configtype". "data" configs are
        specific to dataset being processed. "env" are global configs apply to the
        environment in which the processing is happening (production, preproduction,
        dev, etc).

        @type configtype: str
        @param configtype: type of configuration "env" or "data"

        @type config: str
        @param config: Key of the config whose value is desired

        @type return: str
        @return: value of configuration parameter "config"
        """

        if configtype == "data":
            return self.datacfg.get_config("main", config).strip()
        elif configtype == "env":
            return self.envcfg.get_config("main", config).strip()
        else:
            logkv(logger, {"msg": "Unknown configuration type",
                           "configtype": configtype}, "error")
            raise ThriveHandlerException()