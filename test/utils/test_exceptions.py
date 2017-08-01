import unittest
import mock
import thrive.exceptions as thex


class TestExceptions(unittest.TestCase):

    @mock.patch("inspect.trace")
    def test_ThriveBaseException_info(self, mock_trace):
        mock_trace.return_value = [(None, "/path/to/foo", 1, "myfunc", [], 0)]
        exc = thex.ThriveBaseException()
        expected = {"function": "myfunc",
                    "exception": "ThriveBaseException()",
                    "module": "foo"}
        returned = exc.info()
        self.assertDictEqual(expected, returned)

    @mock.patch("inspect.trace")
    def test_ThriveBaseException_info_indexerror(self, mock_trace):
        mock_trace.return_value = []
        exc = thex.ThriveBaseException()
        expected = {"function": "",
                    "exception": "ThriveBaseException()",
                    "module": ""}
        returned = exc.info()
        self.assertDictEqual(expected, returned)



