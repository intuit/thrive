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

import ConfigParser
import logging
import os
import StringIO
from ConfigParser import SafeConfigParser, MissingSectionHeaderError
from thrive.utils import logkv
from thrive.exceptions import ConfigLoaderException

logger = logging.getLogger(__name__)


class ConfigLoader(object):
    """
    Class to manage loading of different config files.

    In Thrive, we use datset-specific and global (common to all datasets) configs.
    These different types of configs are stored in different files. This class provides
    a uniform interface for managing and extracting these configs
    """

    def __init__(self, config_file):
        """
        Constructor. Each ConfigLoader instance is initialized with a config file that
        it owns and manages

        @type config_file:  str
        @param config_file: Full or relative path of the config file

        @rtype: None
        @return: None
        """

        # Raise exception if config file not found
        if not os.path.exists(config_file):
            logkv(logger, {"msg": "Config file %s not found" % config_file}, "error")
            raise ConfigLoaderException()

        self.parser = SafeConfigParser()

        # If the config file has a section, load it as as. If attempting to load a
        # config file without section headers, create a file object from the string,
        # append section header "default" to it, and then populate the parser
        try:
            self.parser.read(config_file)
        except MissingSectionHeaderError:
            with open(config_file) as cf:
                properties = cf.read()
                configs = "[main]\n%s" % properties
                config_file_stringio = StringIO.StringIO(configs)
                self.parser.readfp(config_file_stringio)
        except ConfigParser.Error:
            logkv(logger, {"msg": "ConfigParser Error"}, "error")
            raise ConfigLoaderException()

    def get_config(self, section, config):
        """
        Returns a config named 'config'

        @type config: str
        @param config: Key of the config whose value is desired

        @rtype: str
        @return: Value of the config with key 'config'
        """
        try:
            return self.parser.get(section, config)
        except ConfigParser.NoOptionError:
            logkv(logger, {"msg": "ConfigParser no option error"}, "error")
            raise ConfigLoaderException()

    def get_sections(self):
        """
        Returns section headers for this config file

        @rtype: [str]
        @return: List of section header names
        """
        try:
            return self.parser.sections()
        except ConfigParser.NoSectionError:
            logkv(logger, {"msg": "ConfigParser no section error"}, "error")
            raise ConfigLoaderException()

    def get_section_configs(self, section):
        """
        Returns a dict of all config / config values for the given section

        @type section: str
        @param section: Name of section header

        @rtype: dict(str, str)
        @return: Dict of configs / config values for this section
        """
        try:
            config_dict = dict()
            options = self.parser.options(section)
            for option in options:
                config_dict[option] = self.get_config(section, option)
            return config_dict
        except:
            logkv(logger, {"msg": "ConfigParser no option error"}, "error")
            raise ConfigLoaderException()




