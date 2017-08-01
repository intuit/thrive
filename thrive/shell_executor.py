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

import subprocess as sp
import logging
from thrive.utils import logkv

logger = logging.getLogger(__name__)


class ShellException(Exception):
    pass


class ShellResult(object):
    def __init__(self, retcode, output_iterator, error_iterator):
        self.retcode = retcode
        self.output = output_iterator
        self.error = error_iterator

    def __str__(self):
        return "retcode = {}\noutput = {}\nerror = {}".format(
            self.retcode, self.output, self.error)

    def __repr__(self):
        return self.__str__()


class ShellExecutor(object):
    """
    An abstraction over bash shell command layer. This class facilitates executing
    Shell commands needed by all classes in thrive core. There can be no logging in
    this class because this class is used to set up the logger itself.
    """

    @staticmethod
    def execute(cmd_string, verbose=False, splitcmd=True, as_shell=False):
        """
        Executes command string.

        @type cmd_string: string
        @param cmd_string: Command string with arguments separated by spaces

        @type verbose: bool
        @param verbose: Echoes the command being executed

        @type splitcmd: bool
        @param splitcmd: If true, command is split into a list of arguments before
        being passed to Popen

        @type as_shell: bool
        @param as_shell: Sets the "shell" option of Popen

        @rtype: ShellResult
        @return: ShellResult instance containing return code, output, and error messages
        """

        if splitcmd:
            cmd = cmd_string.split(" ")
        else:
            cmd = cmd_string

        if verbose:
            logkv(logger, {"cmd": cmd}, "info")

        try:
            result = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=as_shell)
            output, error = result.communicate()
            retcode = result.returncode
            return ShellResult(retcode, output, error)
        except OSError:
            raise ShellException

    @staticmethod
    def safe_execute(cmd_string, **kwargs):
        """
        Executes command string and raises exception if return code is not 0

        @type cmd_string: string
        @param cmd_string: Command string with arguments separated by spaces

        @type verbose: bool
        @param verbose: Echoes the command being executed

        @type splitcmd: bool
        @param splitcmd: If true, command is split into a list of arguments before
        being passed to Popen

        @type as_shell: bool
        @param as_shell: Sets the "shell" option of Popen

        @rtype: ShellResult
        @return: ShellResult instance containing return code, output, and error messages
        """

        result = ShellExecutor.execute(cmd_string, **kwargs)
        if result.retcode != 0:
            logkv(logger, {"msg": "Error in execution of shell command",
                           "cmd": cmd_string,
                           "retcode": result.retcode,
                           "error": result.error}, "warning")
            raise ShellException
        else:
            return result
