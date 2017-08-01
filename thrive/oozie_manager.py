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
import time
import re
import json
from thrive.shell_executor import ShellExecutor, ShellException
from thrive.utils import logkv
from thrive.exceptions import OozieManagerException

logger = logging.getLogger(__name__)


class OozieManager(object):
    """
    Manager class for Oozie jobs. This class is concerned with launch, status fetch
    and monitoring Oozie jobs using the Oozie command-line interface
    """
    def __init__(self):
        """
        Instantiate a ShellExecutor to execute oozie commands

        @rtype: None
        @return: None
        """
        self.shell_exec = ShellExecutor()

    def launch(self, propfile=None):
        """
        Launches oozie job with properties in "propfile"

        @type propfile: str
        @param propfile: Job properties file

        @rtype: str
        @return: Launched Oozie jobid
        """
        try:
            if propfile is None:
                logkv(logger, {"msg": "Workflow properties file not found",
                               "propfile": propfile}, "error")
                raise OozieManagerException()

            launchcmd = "oozie job -config %s -run" % propfile
            result = self.shell_exec.safe_execute(launchcmd)
            jobid = result.output.split(":")[1].strip()
            logkv(logger, {"msg": "Launched Oozie job",
                           "jobid": jobid}, "info")
            return jobid
        except Exception:
            logkv(logger, {"msg": "Failed to launch Oozie job"}, "error")
            raise OozieManagerException()

    def get_status(self, jobid):
        """
        Parses output of "oozie job -info <jobid>" command to get
        overall and per-step status of the launched Oozie job

        @type jobid: str
        @param jobid: Oozie jobid

        @rtype: dict
        @return: {"step": "<status>"} for all steps in workflow
        """
        # Get raw job status from command line
        pollcmd = "oozie job -info %s" % jobid
        result = self.shell_exec.safe_execute(pollcmd)

        # Get overall job status
        try:
            ostat = re.findall("Status\s*:\s*(\w+)", result.output)[0]
        except IndexError:
            logkv(logger, {"msg": "Failed to get overall job status"}, "error")
            raise OozieManagerException()

        # Now get status of individual steps
        # Escape dashes in jobid, otherwise regex matches will fail
        jobid_esc = jobid.replace("-", "\-")
        step_status = re.findall("%s@(.+?)\n" % jobid_esc, result.output)

        # Combine statuses of individual steps into a status dictionary
        # for easier processing later
        try:
            jobstatus = dict([tuple(ss.split()[:2]) for ss in step_status])
            jobstatus.update({"overall": ostat})
            return jobstatus
        except Exception:
            logkv(logger, {"msg": "Failed to get status of workflow steps"}, "error")
            raise OozieManagerException()

    def get_logtrace(self, jobid):
        """
        Gets log trace of the oozie job of given jobid

        @type jobid: str
        @param jobid: Oozie jobid whose status is desired

        @rtype: str
        @return: log trace as a string
        """
        try:
            logcmd = "oozie job -log %s" % jobid
            result = self.shell_exec.safe_execute(logcmd)
            return result.output
        except ShellException:
            logkv(logger, {"msg": "Failed to get logtrace"}, "error")
            raise OozieManagerException()

    def get_counts(self, jobid):
        """
        Gets HDFS counts after the parse-json job is done. These counts are the source
        of truth for number of records processed and number of hive row counts generated

        @type jobid: str
        @param jobid: Jobid of Oozie job.

        @rtype: dict
        @return: A dictionary containing the counter name and its value.
        """

        try:
            # Get hadoop counts from Oozie. The action name is hardcoded, for now,
            # but should probably think about how to factor this out without leading to
            # config profusion.
            action = "parse-json"
            pollcmd = "oozie job -info %s@%s -verbose" % (jobid, action)
            result = self.shell_exec.safe_execute(pollcmd)

            res = re.findall('{.*}', result.output)[0]
            counts = json.loads(res)
            task_counter = counts.get("org.apache.hadoop.mapreduce.TaskCounter", dict())
            thrive_counter = counts.get("THRIVE", dict())

            return {"map_input_records": task_counter.get("MAP_INPUT_RECORDS", "0"),
                    "map_output_records": task_counter.get("MAP_OUTPUT_RECORDS", "0"),
                    "reduce_input_records": task_counter.get("REDUCE_INPUT_RECORDS", "0"),
                    "reduce_output_records": task_counter.get("REDUCE_OUTPUT_RECORDS", "0"),
                    "skipped": thrive_counter.get("SKIPPED", "0")
            }
        except Exception:
            logkv(logger, {"msg": "Error getting Hadoop counts through Oozie"}, "error")
            raise OozieManagerException()

    def poll(self, jobid, interval=10):
        """
        Polls the Oozie job to get status

        @type jobid: str
        @param jobid: Oozie jobid

        @type interval: int
        @param interval: Interval between polls

        @rtype: bool
        @return: SUCCESS/FAIL code
        """
        jobrunning = True
        while jobrunning:
            jobstatus = self.get_status(jobid)
            logkv(logger, {"jobid": jobid, "status": jobstatus}, "info")

            if jobstatus["overall"] != "RUNNING":
                jobrunning = False
            time.sleep(interval)

        # Once the job finishes, analyse the status of all steps and see if any failed
        for step, status in jobstatus.items():
            if "FAIL" in status.upper() or "ERROR" in status.upper():
                errmsg = "Oozie error. Step: %s, Error: %s" % (step, status)
                logkv(logger, {"msg": "Oozie error","step": step,
                               "error": status}, "error")
                logkv(logger, {"oozie_logtrace": self.get_logtrace(jobid)}, "info")
                raise OozieManagerException(errmsg)
