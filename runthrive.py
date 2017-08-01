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

from optparse import OptionParser
import logging
import os
import sys
from thrive.cleanup_handler import CleanupHandler
from thrive.rollback_handler import RollbackHandler
from thrive.setup_handler import SetupHandler
from thrive.prepare_handler import PrepareHandler
from thrive.load_handler import LoadHandler
from thrive.monitor_handler import MonitorHandler
from thrive.replay_handler import ReplayHandler
from thrive.utils import init_logging, logkv
from thrive.exceptions import ThriveBaseException

def add_options(_parser):
    """
    Adds currently required and phase-specific options to "_parser"

    @type _parser: OptionParser
    @param _parser: OptionParser instance managing options for Thrive workflow

    @rtype: None
    @return: None
    """

    _parser.add_option("--phase", dest="phase", action="store",
                       help="[required] Specify the thrive workflow phase")

    _parser.add_option("--data-config", dest="datacfg_file", action="store",
                       help="[required] Path to dataset-specific config file")

    _parser.add_option("--env-config", dest="envcfg_file", action="store",
                       help="[required] Path to global environment config file")

    _parser.add_option("--resources", dest="resources_file", action="store",
                       help="[only if phase=setup] Path to resources file")

    _parser.add_option("--partitions", dest="partitions_file", action="store",
                       help="[only if phase=rollback] Path to partitions file")

    _parser.add_option("--replay-dirs", dest="replaydirs_file", action="store",
                       help="[only if phase=replay] Path to replay-dirs file")


def check_options(_parser, _options):
    """
    Checks options and raises OptionParser.error if required options are absent

    @type _parser: OptionParser
    @param _parser: OptionParser instance managing options for Thrive workflow

    @type _options: list
    @param _options: List of option names parsed by "_parser"

    @rtype: None
    @return: None

    @exception: OptionParser.error
    """

    opterr = False
    errmsg = ""

    if not _options.phase:
        opterr, errmsg = True, "Workflow required option \"phase\" missing"

    if not _options.datacfg_file:
        opterr, errmsg = True, "Workflow required option \"data-config\" missing"

    if not _options.envcfg_file:
        opterr, errmsg = True, "Workflow required option \"env-config\" missing"

    if (_options.phase == "setup") and (not _options.resources_file):
        opterr, errmsg = True, "Workflow option \"resources\" is required for phase \"setup\""

    if (_options.phase == "rollback") and (not _options.partitions_file):
        opterr, errmsg = True, "Workflow option \"partitions\" is required for phase \"rollback\""

    if (_options.phase == "replay") and (not _options.replaydirs_file):
        opterr, errmsg = True, "Workflow option \"replay-dirs\" is required for phase \"replay\""

    if opterr:
        _parser.print_help()
        _parser.error(errmsg)


def main():
    """
    Main method

    @rtype: None
    @return: None

    @except: Exception
    """
    USAGE_MSG = \
        """python runthrive.py  --phase=<phase>
                                --data-config=<path/to/data_config_file>
                                --env-config=<path/to/env_config_file>
                                [--resources=<path/to/resources_file>]
                                [--partitions=<path/to/partitions_file>]
                                [--replay-dirs=<path/to/replaydirs_file>]

           'phase' = [cleanup | setup | load | rollback | monitor | replay]
        """

    # Instantiate parser
    parser = OptionParser(USAGE_MSG)

    # Add options to the parser and parse
    add_options(parser)
    (options, args) = parser.parse_args()

    # Check options for any errors
    check_options(parser, options)

    # Exit if config files dont exist
    for cfgfile in [options.envcfg_file, options.datacfg_file]:
        if not os.path.exists(cfgfile):
            sys.stderr.write("Config file %s does not exist\n" % cfgfile)
            sys.exit(1)

    # Initialize logger
    init_logging(options.datacfg_file)
    logger = logging.getLogger(__name__)

    # Run Thrive
    try:
        if options.phase == "cleanup":
            handler = CleanupHandler(datacfg_file=options.datacfg_file,
                                     envcfg_file=options.envcfg_file)

        elif options.phase == "prepare":
            handler = PrepareHandler(datacfg_file=options.datacfg_file,
                                     envcfg_file=options.envcfg_file)

        elif options.phase == "setup":
            handler = SetupHandler(datacfg_file=options.datacfg_file,
                                   envcfg_file=options.envcfg_file,
                                   resources_file=options.resources_file)

        elif options.phase == "load":
            handler = LoadHandler(datacfg_file=options.datacfg_file,
                                  envcfg_file=options.envcfg_file)

        elif options.phase == "rollback":
            handler = RollbackHandler(datacfg_file=options.datacfg_file,
                                      envcfg_file=options.envcfg_file,
                                      partitions_file=options.partitions_file)

        elif options.phase == "replay":
            handler = ReplayHandler(datacfg_file=options.datacfg_file,
                                    envcfg_file=options.envcfg_file,
                                    replaydirs_file=options.replaydirs_file)

        elif options.phase == "monitor":
            handler = MonitorHandler(datacfg_file=options.datacfg_file,
                                     envcfg_file=options.envcfg_file)

        else:
            handler = None
            logger.error("Illegal option phase: %s" % options.phase)
            raise ValueError

        # Execute the handler for the requested workflow phase
        handler.execute()

    except ThriveBaseException as ex:
        logkv(logger, {"msg": "Thrive run failed"}, "error", ex)
        sys.exit(1)


if __name__ == "__main__":
    main()

