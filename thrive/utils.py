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
import os
import subprocess as sp
import re
import inspect
from ext.colorlog.colorlog import ColoredFormatter
from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta
from collections import defaultdict
from thrive.logger_manager import ContextFilter
from thrive.exceptions import ThriveBaseException

# Regex patterns
SOURCE_DIR_PATTERN = re.compile(".*(d_[0-9]{8}\-[0-9]{4})")
PARTITION_PATTERN = re.compile(".*/(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/(?P<hour>[0-9]{2})/(?P<part>[0-9]+)$")

# Numeric and string constants
SECONDS_PER_HOUR = 3600.0
HOURS_PER_DAY = 24.0
CAMUS_FOLDER_FREQ = timedelta(0, 600)


def init_logging(config_file):
    """
    Initializes the root logger. This function is called from the top level run script
    'runthrive.py'. This function does not have access to any Handlers or Managers and
    so must parse config file on its own.

    @type config_file: str
    @param config_file: Configuraiton file, used to locate log directory

    @rtype: None
    @return: None
    """

    # Get the path of log file
    parser = SafeConfigParser()
    parser.read(config_file)
    logdir = parser.get("main", "nfs_log_path")
    dataset_name = parser.get("main", "dataset_name")

    # Create the log dir
    cmd = "mkdir -p %s" % logdir
    result = sp.Popen(cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE)
    output, error = result.communicate()
    retcode = result.returncode
    if retcode != 0:
        raise Exception("[utils.init_logging] Logging not initialized")

    # Create the timestamped log file in log dir
    loadts = datetime.now()
    logfile = "%s_%s.log" % (dataset_name, loadts.strftime("%Y%m%d-%H%M%S"))
    logfilepath = os.path.join(logdir, logfile)

    # To get child loggers working properly, it is critical to get the root logger
    # *without* any name. I.e. without any argument to the getLogger() method
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    # logfmt = "%(asctime)s [%(name)-0.50s.%(funcName)s] [lvl=%(levelname)-5.5s] [dataset=%(dataset)-0.25s] %(message)s"
    # clogfmt = "%(log_color)s%(asctime)s [%(name)-0.50s.%(funcName)s] [lvl=%(levelname)-5.5s] [dataset=%(dataset)-0.25s] %(message)s"
    logfmt = "%(asctime)s [%(name)-0.50s] [lvl=%(levelname)-5.5s] [dataset=%(dataset)-0.25s] %(message)s"
    clogfmt = "%(log_color)s%(asctime)s [%(name)-0.50s] [lvl=%(levelname)-5.5s] [dataset=%(dataset)-0.25s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    file_formatter = logging.Formatter(logfmt, datefmt)

    console_formatter = ColoredFormatter(
        clogfmt,
        datefmt=datefmt,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )

    # Create a ContextFilter instance
    cf = ContextFilter(dataset_name)

    # Create console handler logger
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(cf)
    logger.addHandler(console_handler)

    # Create File handler logger
    file_handler = logging.FileHandler(logfilepath)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    file_handler.addFilter(cf)
    logger.addHandler(file_handler)


def logkv(_logger, msgdict, level="info", exception=None):
    """
    Composes the log message consumable by Splunk and logs it using 'logger' at '_level'.
    'msgdict' is a dictionary of key-value pairs to be composed into a log message.

    @type _logger: logger instance
    @param _logger: Instance of logging class obtained via call to getLogger() method

    @type level: str
    @param level: Requested logging level

    @type msgdict: dict
    @param msgdict: Dictionary of key-value pairs to be composed into a log message

    @type exception: ThriveBaseException
    @param exception: Exception to log in case of error

    @rtype: None
    @return: None
    """
    try:
        # Get the appropriate method of the logger instance
        logfunc = getattr(_logger, level)

        # Get the name of function which called logkv()
        current_frame = inspect.currentframe()
        call_frame = inspect.getouterframes(current_frame, 2)
        msgdict.update({"__funcname__": call_frame[1][3]})
        if exception:
            msgdict.update(exception.info())
    except Exception:
        raise Exception("Error in logkv()")

    delim = ", "
    msg = delim.join(['%s="%s"' % (k, v) for k, v in msgdict.items()])
    logfunc(msg)


def split_timestamp(dto):
    """
    Extracts (year, month, day, hour) from a datetime object 'dto' and returns as a
    tuple

    @type dto: datetime.datetime
    @param dto: datetime object that needs splitting

    @rtype: tuple
    @return: tuple containing Y, mo, day, and hour of the input datetime object
    """
    return tuple(dto.strftime("%Y %m %d %H").split())


def iso_format(dto, sep=" "):
    """
    Formats datetime object to ISO8601 format "yyyy-mm-dd HH:MM:SS"

    @type dto: datetime.datetime
    @param dto: datetime object

    @type sep: str
    @param sep: Separator for ISO timestamp

    @rtype: str
    @return: String representation of dto in ISO timestamp format
    """
    FMT = "%%Y-%%m-%%d%s%%H:%%M:%%S" % sep
    return dto.strftime(FMT)


def unix_timestamp(dtstr):
    """
    Converts a datetime string in ISO 8601 format to unix-timestamp in seconds

    @type dtstr: str
    @param dtstr: Datetime string in ISO-8601 format

    @rtype: int
    @return: Number of seconds since epoch
    """
    dto = datetime.strptime(dtstr, "%Y-%m-%d %H:%M:%S")
    return dto.strftime("%s")


def is_camus_dir(dirname):
    """
    Returns True if dirname is consistent with expected pattern for Camus dir names.
    E.g. "d_20150817-1010".

    @type dirname: str
    @param dirname: dirname string

    @rtype: bool
    @return: True if dirname is consistent with Camus dirname pattern
    """
    return bool(re.search("d_[0-9]{8}\-[0-9]{4}", dirname))


def dirname_to_dto(dirname):
    """
    Parses dir in Camus format (e.g. "d_20150817-1010") as a datetime object.

    @type dirname: str
    @param dirname: name of directory in Camus format (e.g. "d_20150817-1010")

    @rtype: datetime
    @return: datetime object corresponding to folder time
    """
    try:
        dt_str = re.findall('d_([0-9]{8}\-[0-9]{4})', dirname)[0]
        dto = datetime.strptime(dt_str, '%Y%m%d-%H%M')
        return dto
    except ValueError:
        return None


def utc_to_pst(dto_utc):
    """
    Converts time for a datetime object from UTC to PST. Conversion is aware of
    daylight savings time.

    @type dto_utc: datetime
    @param dto_utc: datetime object in UTC time zone

    @rtype: datetime
    @return: datetime object in PST time zone
    """
    delta_ = datetime.now() - datetime.utcnow()
    return dto_utc + delta_


def hour_diff(dto1, dto2):
    """
    Returns time difference in hours between two datetime objects

    @type dto1: datetime.datetime
    @param dto1: datetime object 1

    @type dto2: datetime.datetime
    @param dto2: datetime object 2

    @rtype: float
    @return: Time difference in hours between dto1 and dto2
    """

    # timedelta.total_seconds() not available in Python 2.6
    # return (dto2 - dto1).total_seconds() / SECONDS_PER_HOUR
    timedelta_ = dto2 - dto1
    total_seconds = timedelta_.seconds + timedelta_.days * HOURS_PER_DAY * SECONDS_PER_HOUR
    return total_seconds / SECONDS_PER_HOUR


def pathjoin(p1, p2):
    """
    Safe joining of path where the second argument starts with a "/"

    @type p1: str
    @param p1: First path component

    @type p2: str
    @param p2: Second path component

    @rtype: str
    @return: Joined path
    """
    if p2.startswith("/"):
        return os.path.join(p1, p2[1:])
    else:
        return os.path.join(p1, p2)


def parse_partition(pth):
    """
    Splits full HDFS path *pth* into output parent directory and the partition
    and returns the partition part.

    @type pth: str
    @param pth: Full  hdfs path

    @rtype: tuple
    @return: (year, month, day, hour, part) tuple parsed from the path
    """
    ptn_dict = re.match(PARTITION_PATTERN, pth).groupdict()
    return tuple([ptn_dict.get(p, "") for p in ["year", "month", "day", "hour", "part"]])


def materialize(template, substitutions, outfile=None):
    """
    Returns materialized string by making substitutions from 'substitutions'
    dictionary into 'template'. Optionally writes 'materialized_str' to
    'outfile'

    @type template: str
    @param template: String with template parameters

    @type substitutions: dict
    @param substitutions: dictionary of substitutions with key = template
    parameter and value = desired substitution

    @type outfile: str
    @param outfile: full path to output file

    @rtype: str
    @return: materialized string with substitutions performed
    """
    materialized_str = template
    for param, val in substitutions.items():
        materialized_str = re.sub(param, val, materialized_str)

    if outfile:
        with open(outfile, "w") as of:
            of.write(materialized_str)

    return materialized_str


def percentdiff(target_rows, source_rows):
    """
    Calculates percent rows lost between source and target

    @type target_rows: int
    @param target_rows: Number of rows in the target

    @type source_rows: int
    @param source_rows: Number of rows in the source

    @rtype: str
    @return: Percent data loss (a float) converted to string
    """
    try:
        _pct_loss = 100.0 * (1 - float(target_rows) / float(source_rows))
        return str(_pct_loss)
    except ZeroDivisionError:
        return 0.0


def chunk_dirs(dir_list, groupby="day"):
    """
    Given a list of directory names, *dir_list* in the CAMUS naming format,
    returns a dictionary containing with files grouped by *groupby* and the
    partition label associated with the list

    E.g. Input = dir_list = ["d_20160606_2010", "d_20160606_2020", "d_20160607_2010"]
    Output = {
            "2016/06/06/20": ["d_20160606_2010", "d_20160606_2020"],
            "2016/06/07/20": ["d_20160607_2010"]
        }
    @type dir_list: list
    @param dir_list: List of directories in the CAMUS convention

    @type groupby: str
    @param groupby: Granularity for grouping the directories

    @rtype: dict
    @return: Dictionary with group label and the group elements
    """
    chunks = defaultdict(list)
    for dirname in dir_list:
        dto = dirname_to_dto(dirname)
        if groupby == "hour":
            year, month, day, hour = dto.year, dto.month, dto.day, dto.hour
        elif groupby == "day":
            year, month, day, hour = dto.year, dto.month, dto.day, 0
        elif groupby == "month":
            year, month, day, hour = dto.year, dto.month, 1, 0
        else:
            raise RuntimeError("Invalid chunk size 'groupby'")

        label = "%04d/%02d/%02d/%02d" % (year, month, day, hour)
        chunks[label].append(dirname)

    return dict(chunks)
