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
import logging
import json
from datetime import datetime
from thrive.thrive_manager import ThriveManager
from thrive.utils import logkv, iso_format
from thrive.shell_executor import ShellException
from thrive.exceptions import NewRelicManagerException


logger = logging.getLogger(__name__)

JSON_PATTERN = '({"metric_data".*})'

class NewRelicManager(ThriveManager):
    """
    Class to get info from NewRelic via REST API
    """
    def __init__(self, api_key, dataset, endpt_url):
        super(NewRelicManager, self).__init__()

        self.api_key = api_key
        self.dataset = dataset
        self.endpt_url = endpt_url

    @staticmethod
    def _extract_count(response):
        """
        Internal method for getting event count from NewRelic API response.

        @type response: str
        @param response: Response from NewRelic API call, which contains header and JSON containing number of counts

        @rtype: int
        @return: Number of counts in call_count field
        """
        try:
            response_json = re.findall(JSON_PATTERN, response)[0]
            metrics_dict = json.loads(response_json)
            count_value = metrics_dict["metric_data"]["metrics"][0]["timeslices"][0]["values"]["call_count"]
            counts = int(count_value)
        except (IndexError, KeyError, ValueError, TypeError) as ex:
            logkv(logger, {"msg": "Error in retrieving requested metric. Returning -1",
                           "error": ex.message}, "warning")
            counts = -1

        return counts

    def _get_response(self, metric, start, end):
        """
        Internal method for making NewRelic API call

        @type metric: str
        @param metric: Endpoint to get counts from.  Acceptable values are "posted" and "produced".

        @type start: str
        @param start: String specifiying start time and time zone, e.g. "2016-01-27T02:59:00+00:00"

        @type end: str
        @param end: String specifiying start time and time zone, e.g. "2016-01-27T02:59:00+00:00"

        @rtype: str
        @return: Response from NewRelic API call to given endpoint
        """

        curl_cmd = '''curl -X GET '%s' -H '%s' -i -G -d 'names[]=%s.%s&from=%s&to=%s&summarize=true' ''' \
                   % (self.endpt_url, self.api_key, metric, self.dataset, start, end)

        try:
            # Execute curl command
            result = self.shell_exec.safe_execute(curl_cmd, splitcmd=False, as_shell=True)
            logkv(logger, {"msg": "Queried NewRelic"}, "info")
        except ShellException:
            raise NewRelicManagerException()

        return result.output

    def get_count(self, stage, start, end):
        """
        Get counts at given stage of source pipeline for a given time window.

        @type stage: str
        @param stage: Stage to get counts from.  Acceptable values are "jetty" and "kafka".

        @type start: datetime
        @param start: Datetime object specifiying start time in PST time zone

        @type end: datetime
        @param end: Datetime object specifiying end time in PST time zone

        @rtype: int
        @return: Value of call_count field
        """
        if stage == 'jetty':
            metric = 'posted'
        elif stage == 'kafka':
            metric = 'produced'
        else:
            logkv(logger, {"msg": "Invalid stage in NewRelic"}, "error")
            raise NewRelicManagerException

        start_str = iso_format(start, sep="T")
        end_str = iso_format(end, sep="T")

        logkv(logger, {"msg": "Getting response from NewRelic"}, "info")
        response = self._get_response(metric, start_str, end_str)

        logkv(logger, {"msg": "Extracting event count"}, "info")
        count = NewRelicManager._extract_count(response)

        return count
