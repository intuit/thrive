
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

import unittest
import thrive.shell_executor as shell_exec


class TestShellExecutor(unittest.TestCase):
    def setUp(self):
        self.exec_ = shell_exec.ShellExecutor()

    def test_shell_exception(self):
        with self.assertRaises(shell_exec.ShellException):
            self.exec_.execute("notacommand")

    def test_execute_with_defaults(self):
        str_ = "testing execute with default args"
        cmd = "echo %s" % str_
        result = self.exec_.execute(cmd)
        self.assertEqual(result.retcode, 0)
        self.assertEqual(result.output, str_ + "\n")
        self.assertEqual(result.error, "")

    def test_execute_no_defaults(self):
        str_ = "testing execute with default args"
        cmd = "echo %s" % str_
        result = self.exec_.execute(cmd, verbose=False,
                                    splitcmd=False, as_shell=True)
        self.assertEqual(result.retcode, 0)
        self.assertEqual(result.output, str_ + "\n")
        self.assertEqual(result.error, "")

    def test_execute_with_defaults_nonzero_retcode(self):
        cmd = "ls -l | grep 'doesnotexist'"
        result = self.exec_.execute(cmd)
        self.assertNotEqual(result.retcode, 0)

    def test_safe_execute_with_defaults(self):
        str_ = "hello"
        cmd = "echo %s" % str_
        result = self.exec_.safe_execute(cmd)
        self.assertEqual(result.retcode, 0)
        self.assertEqual(result.output, str_ + "\n")
        self.assertEqual(result.error, "")

    def test_safe_execute_no_defaults(self):
        str_ = "testing execute with default args"
        cmd = "echo %s" % str_
        result = self.exec_.safe_execute(cmd, verbose=False,
                                         splitcmd=False, as_shell=True)
        self.assertEqual(result.retcode, 0)
        self.assertEqual(result.output, str_ + "\n")
        self.assertEqual(result.error, "")

    def test_safe_execute_nonzero_retcode(self):
        cmd = "ls -l | grep 'doesnotexist'"
        with self.assertRaises(shell_exec.ShellException):
            self.exec_.safe_execute(cmd)

    def test_shell_result_all_args(self):
        self.assertEqual(shell_exec.ShellResult("123", "456", "789").__repr__(),
                         "retcode = 123\noutput = 456\nerror = 789")

    def test_shell_result_not_enough_args(self):
        with self.assertRaises(TypeError):
            shell_exec.ShellResult(1, 2)
