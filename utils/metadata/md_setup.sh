#!/bin/bash

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


# Script to setup Thrive metadata tables in the MySQL database based on the
# configuration file provided on the command line. The script executes sql commands
# from ./md_schama.sql after connecting to the MySQL database using the configs
# in the config-file.
#
# Example1: To create metadata in Preprod environment
# ./setup_thrive_metatada -f ../resources/globalpreprod.cfg
#
#
# Example2: To create metadata in Prod environment
# ./setup_thrive_metatada -f ../resources/globalprod.cfg

function usage {
    echo "Usage: $0 -f <config-file>"
}

while getopts f: opt;
    do
        case "$opt" in
            f) CONFIG_FILE="$OPTARG";;
            *) usage
               exit 1
               ;;
        esac
    done

# Exit if no config was provided
if [ -z "$CONFIG_FILE" ]; then
    usage
    echo "No config file passed"
    exit 1
fi

# Exit if config file does not exist
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file \"$CONFIG_FILE\" not found"
    exit 1
fi

# Source the config file
echo "Metadata setup running with configs: $CONFIG_FILE"
source "$CONFIG_FILE"

MD_SCHEMA_FILE="./md_schema.sql"

# Exit if schema file does not exist
if [ ! -f "$MD_SCHEMA_FILE" ]; then
    echo "Schema file does not exist. Exiting."
    exit 1
fi

echo "Setting up thrive metadata"
mysql -h "$dbhost" \
      -u "$dbuser" \
      --password="$dbpass" \
      -D "$dbname" < "$MD_SCHEMA_FILE"

echo "done"