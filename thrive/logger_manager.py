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


class ContextFilter(logging.Filter):
    """
    Manager for injecting contextual information to logs
    """

    def __init__(self, dataset):
        """
        @type dataset: str
        @param dataset: Name of the dataset

        @rtype: None
        @return: None
        """
        # For new style classes (i.e. inherited from 'object' the init call to super
        # should be as follows:
        # super(ContextFilter, self).__init__()
        # However, it breaks Python2.6 code, so we're preferring to perform
        # initialization with this 'old stype' technique. 
        logging.Filter.__init__(self)
        self.dataset = dataset

    def filter(self, record):
        """
        Filter method of the context manager

        @type record: Record
        @param record: Log record

        @rtype: bool
        @return:
        """
        record.dataset = self.dataset
        return True

