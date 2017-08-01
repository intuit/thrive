#!/usr/bin/python

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


"""
It is recommended to use this template to seed your mapper script. Doing so would
ensure that all essential elements expected by Thrive and Hadoop exist in the code.
It also considerably increases the chances that the script will run within the
framework at runtime without additional debugging.
"""

import sys
import json
from StringIO import StringIO
import base64
import gzip
from datetime import datetime
import re
import xml.etree.ElementTree as ET

ISOFMT = "%Y-%m-%d %H:%M:%S"
MAXCHARS = slice(0, 65000)
DELIMITER = "\x01"


def base64_decode(_payload):
    """
    Decoding function for base64 encoded payload. You may not need this if the payload
    is plain text
    @type _payload: str
    @param _payload: GZipped payload
    @type return: str
    @return: Source payload
    """
    gz_stream = StringIO(base64.decodestring(_payload))
    return json.load(gzip.GzipFile(fileobj=gz_stream))


def milliseconds_to_isotimestamp(ms):
    """
    Converter for source server timestamp to ISO8601 format.
    :type ms: int
    :param ms: timestamp in milliseconds
    :type return: str
    :return: timestamp string in ISO8601 format
    """
    return datetime.fromtimestamp(float(ms) / 1000.0).strftime(ISOFMT)


def encode_str(s, _limit):
    """
    Converts s to unicode. Before composing a row, we need to ensure that all ints,
    floats etc are converted into strings so they can be JSONified.
    :param s: obj to be converted
    :param _limit: Max length of output string
    :return: Output string where new lines are escaped
    """
    result = unicode(s).encode("utf-8")[_limit]
    return result.replace("\n", "\\n")


def emit_row(_dct, _allkeys, _delim):
    """
    Composes a row from a dictionary and prints it
    :param _dct: dictionary of fields and their values
    :param _allkeys: keys to be extracted from the dictionary
    :param _delim: delimiter
    :return: delimited row
    """
    row_vals = [encode_str(_dct[k], MAXCHARS) for k in _allkeys]
    print _delim.join(row_vals)


# Main
def main():
    thrive_keys = ["event_id", "ts", "hive_ts"]
    hive_ts = datetime.now().strftime(ISOFMT)

    # Keys that should be extracted from the input JSON should be listed in all_keys.
    # Here is it populated with some dummy names. Please replace with keys specific to
    # your application

    item_keys=[
    'Date',
    'Platform',
    'ProductID',
    'ProductTitle',
    'Rating',
    'RatingScale',
    'ReviewID',
    'Reviewer',
    'ReviewText',
    'Title',
    'VerifiedPurchaseIndicator'
    ]

    print_keys=['event_id', 'ts', 'hive_ts'] + item_keys

    # Thrive uses Hadoop streaming which expects the mapper to consume one line of
    # input and output one or more lines of output. Hadoop will pass each record on a
    # separate line and expect the mapper to make sense of it and spit out one or more
    # records.
    for line in sys.stdin:
        try:
            # Many clients send base64 encoded Gzipped data to reduce payload size. Data
            # that is *not* zipped and encoded, can proceed to parsing stage
            match_id = re.search('\"event_id\":\"(.*?)\"', line)
            event_id = match_id.groups()[0]

            match_ts = re.search('\"server_timestamp\":([0-9]+)', line)
            ts = milliseconds_to_isotimestamp(match_ts.groups()[0])

            match_xml= re.search('<ViewGetItemsResponse.*</ViewGetItemsResponse>',line,re.DOTALL)
            root_str=match_xml.group()
            root=ET.fromstring(root_str)
            for item in root.find('ItemList').getchildren():
                # Create empty dictionary from keys
                row_dict = dict([(dk, "") for dk in print_keys])

                # Update the dict with event metadata
                row_dict.update({
                    "event_id": event_id,
                    "ts": ts,
                    "hive_ts": hive_ts
                })

                # Populate with available values
                for key in item_keys:
                    row_dict[key]=item.find(key).text

                emit_row(row_dict, print_keys, DELIMITER)

        except Exception:
            sys.stderr.write("reporter:counter:THRIVE,SKIPPED,1\n")

if __name__ == "__main__":
    main()