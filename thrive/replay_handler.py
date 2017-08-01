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

import re
import os
import logging
from thrive.load_handler import LoadHandler
from thrive.utils import logkv, SOURCE_DIR_PATTERN
from thrive.exceptions import ReplayHandlerException
logger = logging.getLogger(__name__)


class ReplayHandler(LoadHandler):
    """
    Class for managing data replay, i.e. on-demand reprocessing of requested HDFS
    directories
    """

    def __init__(self, datacfg_file=None, envcfg_file=None,
                 resources_file=None, replaydirs_file=None):

        super(ReplayHandler, self).__init__(datacfg_file, envcfg_file, resources_file)

        # Raise exception if `replaydirs_file` file does not exist
        if not os.path.exists(replaydirs_file):
            logkv(logger, {"msg": "Path does not exist",
                           "file": replaydirs_file}, "error")
            raise ReplayHandlerException()

        self.replaydirs_file = replaydirs_file

    def get_newdirs(self):
        """
        Overrides the get_newdirs function in LoadHandler to return the list of
        source folders supplied in the replaydirs_file.

        @rtype: list
        @return: List of requested folders to process
        """
        with open(self.replaydirs_file) as tdf:
            newdirs = [line.strip() for line in tdf
                       if re.match(SOURCE_DIR_PATTERN, line)]
        return newdirs

    def execute(self, **kwargs):
        """
        Top level function for replay handler. Only needs to execute
        super.execute with update_metadata=False

        @type kwargs: dict
        @param kwargs: Not actually used in execute() function. Added to match
        signature to super.execute()

        @rtype: None
        @return: None
        """
        super(ReplayHandler, self).execute(load_type="replay")