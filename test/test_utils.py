
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
import datetime
import thrive.utils as tu
import mock
from test.utils.utils import make_tempfile, tempfile_write


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.dto = datetime.datetime(2015, 9, 10, 15, 30, 45)
        self.dtstr = "2015-09-10 15:30:45"
        self.dirname = "d_20150910-1450"

    @mock.patch("thrive.utils.sp")
    @mock.patch("thrive.utils.logging")
    def test_init_logging(self, mock_logging, mock_sp):
        mm = mock.MagicMock()
        mock_sp.Popen.return_value = mm
        mm.communicate.return_value = ("foo", "bar")
        mm.returncode = 0

        mock_logger = mock_logging.getLogger.return_value
        mock_logging.INFO = "foo"
        configs = "[main]\nnfs_log_path=/path/to/foo\ndataset_name=dsfoo\n"
        tf = make_tempfile()
        tempfile_write(tf, configs)

        tu.init_logging(tf.name)

        mock_logger.setLevel.assert_called_with("foo")
        console_handler = mock_logging.StreamHandler.return_value
        console_handler.setLevel.assert_called_with("foo")
        add_handler_calls = [mock.call(console_handler)]
        file_handler = mock_logging.FileHandler.return_value
        file_handler.setLevel.assert_called_with("foo")
        add_handler_calls.append(mock.call(file_handler))
        mock_logger.addHandler.assert_has_calls(add_handler_calls)

    @mock.patch("thrive.utils.sp")
    def test_init_logging_exception(self, mock_sp):
        mm = mock.MagicMock()
        mock_sp.Popen.return_value = mm
        mm.returncode = 1
        with self.assertRaises(Exception):
            tu.init_logging("foo")

    def test_split_timestamp(self):
        self.assertEqual(tu.split_timestamp(self.dto),
                         ("2015", "09", "10", "15"))

    def test_iso_format_no_sep(self):
        self.assertEqual(tu.iso_format(self.dto), self.dtstr)

    def test_iso_format_sep(self):
        self.assertEqual(tu.iso_format(self.dto, "T"), "2015-09-10T15:30:45")

    def test_unix_timestamp_positive(self):
        self.assertEqual(tu.unix_timestamp(self.dtstr), "1441924245")

    def test_unix_timestamp_negative(self):
        with self.assertRaises(ValueError):
            tu.unix_timestamp("2015-09-10")

    def test_is_camus_dir_positive(self):
        self.assertTrue(tu.is_camus_dir(self.dirname))

    def test_is_camus_dir_negative(self):
        dirname = "d_20150910-xxxx"
        self.assertFalse(tu.is_camus_dir(dirname))

    def test_dirname_to_dto_positive(self):
        self.assertEqual(tu.dirname_to_dto(self.dirname),
                         datetime.datetime(2015, 9, 10, 14, 50))

    def test_dirname_to_dto_negative(self):
        self.assertIsNone(tu.dirname_to_dto("d_20150231-2350"))

    def test_utc_to_pst_positive(self):
        delta = tu.utc_to_pst(self.dto) - datetime.datetime(2015, 9, 10, 8, 30, 45)
        self.assertAlmostEqual(delta.total_seconds(), 0, places=1)

    def test_utc_to_pst_negative(self):
        delta = tu.utc_to_pst(self.dto) - datetime.datetime(2015, 9, 10, 15, 30, 45)
        self.assertNotAlmostEqual(delta.total_seconds(), 0, places=1)

    def test_hour_diff_positive(self):
        self.assertEqual(
            tu.hour_diff(self.dto, self.dto + datetime.timedelta(hours=10)), 10)

    def test_hour_diff_negative(self):
        self.assertEqual(
            tu.hour_diff(self.dto, self.dto + datetime.timedelta(hours=-10)), -10)

    def test_pathjoin_no_slash(self):
        self.assertEqual(tu.pathjoin("a", "b"), "a/b")

    def test_pathjoin_with_slash(self):
        self.assertEqual(tu.pathjoin("a", "/b"), "a/b")

    def test_pathjoin_double_slash(self):
        self.assertEqual(tu.pathjoin("a/", "/b"), "a/b")

    def test_pathjoin_double_negative(self):
        self.assertNotEqual(tu.pathjoin("a", "/b"), "a//b")

    def test_parse_partition_positive(self):
        self.assertEqual(tu.parse_partition("foo/2015/10/11/12/0"),
                         ("2015", "10", "11", "12", "0"))

    def test_parse_partition_negative(self):
        with self.assertRaises(AttributeError):
            _ = tu.parse_partition("foo/2015/10/11/12")

    def test_materialize_no_outfile(self):
        self.assertEqual(tu.materialize("a=A, b=B", {"A": "1", "B": "2"}),
                         "a=1, b=2")

    def test_materialize_with_outfile(self):
        with mock.patch("__builtin__.open", create=True) as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            mstr = tu.materialize("a=A, b=B", {"A": "1", "B": "2"}, outfile="foo")
        fh = mock_open.return_value.__enter__.return_value
        fh.write.assert_called_with(mstr)

    def test_percentdiff_positive(self):
        self.assertAlmostEqual(float(tu.percentdiff(99, 100)), 1.0, places=1)

    def test_percentdiff_negative(self):
        self.assertAlmostEqual(float(tu.percentdiff(100, 99)), -1.0, places=1)

    def test_percentdiff_zero_division(self):
        self.assertEqual(tu.percentdiff(100, 0), 0.0)

    def test_chunk_dirs_hour(self):
        dirlist = ["d_20160606-2010", "d_20160606-2010",
                   "d_20160606-2100", "d_20160606-2210"]
        expected = {"2016/06/06/20": ["d_20160606-2010", "d_20160606-2010"],
                    "2016/06/06/21": ["d_20160606-2100"],
                    "2016/06/06/22": ["d_20160606-2210"]}
        result = tu.chunk_dirs(dirlist, groupby="hour")
        self.assertDictEqual(expected, result)

    def test_chunk_dirs_day(self):
        dirlist = ["d_20160606-2010", "d_20160606-2010",
                   "d_20160607-2100", "d_20160607-2210"]
        expected = {"2016/06/06/00": ["d_20160606-2010", "d_20160606-2010"],
                    "2016/06/07/00": ["d_20160607-2100", "d_20160607-2210"]}
        result = tu.chunk_dirs(dirlist, groupby="day")
        self.assertDictEqual(expected, result)

    def test_chunk_dirs_month(self):
        dirlist = ["d_20160606-2010", "d_20160606-2010",
                   "d_20160707-2100", "d_20160707-2210"]
        expected = {"2016/06/01/00": ["d_20160606-2010", "d_20160606-2010"],
                    "2016/07/01/00": ["d_20160707-2100", "d_20160707-2210"]}
        result = tu.chunk_dirs(dirlist, groupby="month")
        self.assertDictEqual(expected, result)

    def test_chunk_dirs_exception(self):
        dirlist = ["d_20160606-2010", "d_20160606-2010",
                   "d_20160707-2100", "d_20160707-2210"]
        with self.assertRaises(RuntimeError):
            _ = tu.chunk_dirs(dirlist, groupby="foo")
