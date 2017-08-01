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

import ConfigParser
import os
import glob
import zipfile
import logging
from thrive.shell_executor import ShellException
from thrive.thrive_handler import ThriveHandler
from thrive.utils import materialize, is_camus_dir, logkv, iso_format
from thrive.exceptions import VerticaManagerException, \
    MetadataManagerException, SetupHandlerException


logger = logging.getLogger(__name__)


class SetupHandler(ThriveHandler):
    """
    Handler for setup actions.
    """
    @staticmethod
    def _make_schema(column_file, template_file, outfile, substitutions):
        """
        Parses column file, generates substitutions and writes to output file

        @type column_file: str
        @param column_file: path to columns.csv file

        @type template_file: str
        @param template_file: path to Hive/vertica schema template file

        @type outfile: str
        @param: path to output file

        @type substitutions: dict
        @param substitutions: dictionary of substitutions to make in template file

        @rtype: None
        @return: None
        """
        # Create Hive column ddl statements from non-empty lines of hive_column_file
        columns = ",\n".join([item.strip() for item in open(column_file) if item.strip() != ""])

        # Materialize the output file from template
        template_str = open(template_file, "r").read()

        # Append substitutions to column mappings
        substitutions.update({"@COLUMNMAPPINGS": columns})

        # Write out SQL file for Hive DDL
        materialize(template_str, substitutions, outfile)

    def make_schema(self):
        """
        Generates schema by materializing Hive and Vertica schema templates

        @rtype: None
        @return: None
        """
        nfs_dataset_path = self.get_config("nfs_dataset_path")

        # Substitutions need to be generated manually, one time
        hive_substitutions = {
            "@DATABASE": self.get_config("hive_db"),
            "@TABLE": self.get_config("hive_table")
        }

        # specify substitutions
        substitutions = {
            "hive": hive_substitutions,
        }

        if self.get_config("vertica_load").lower() == "true":

            # Get the columns for Vertica optimization clauses
            projection_keys = self.get_config("vertica_projection_keys")
            projection_clause = ""
            if projection_keys:
                projection_clause = "order by %s " % projection_keys

            segmentation_keys = self.get_config("vertica_segmentation_keys")
            segmentation_clause = ""
            if segmentation_keys:
                segmentation_clause = "segmented by modularhash(%s) all nodes" % segmentation_keys

            partition_expr = self.get_config("vertica_partition_expr")
            partition_clause = ""
            if partition_expr:
                partition_clause = "partition by %s" % partition_expr

            vertica_substitutions = {
                "@VSCHEMA": self.get_config("vertica_schema"),
                "@TABLE": self.get_config("vertica_table"),
                "@PROJECTION_CLAUSE": projection_clause,
                "@SEGMENTATION_CLAUSE": segmentation_clause,
                "@PARTITION_CLAUSE": partition_clause
            }

            substitutions["vertica"] = vertica_substitutions

        for platform in substitutions.keys():
            # Locate column file in NFS
            column_file = os.path.join(nfs_dataset_path,
                                       self.get_config("%s_columns" % platform))

            # Materialize the output file from template
            template_file = os.path.join(self.get_config("nfs_resource_path"),
                                         "%s_schema_template.sql" % platform)

            # Create output file path
            outfile = os.path.join(nfs_dataset_path,
                                   self.get_config("%s_ddl" % platform))

            # Generate the DDL by calling the static _make_schema method
            SetupHandler._make_schema(column_file,
                                      template_file,
                                      outfile,
                                      substitutions[platform])

    def make_oozie_workflow(self):
        """
        Generates Oozie workflow by materializing the workflow template

        @rtype: None
        @return: None
        """
        nfs_dataset_path = self.get_config("nfs_dataset_path")
        outfile = os.path.join(nfs_dataset_path, self.get_config("workflow_xml"))
        template_file = os.path.join(self.get_config("nfs_resource_path"),
                                     "workflow_template.xml")
        template_str = open(template_file, "r").read()
        mapper_hdfs_path = os.path.join(self.get_config("hdfs_resource_path"), "script",
                                        self.get_config("mapper"))
        substitutions = {
            "@MAPPER": self.get_config("mapper"),
            "@HDFS_PATH": mapper_hdfs_path,
            "@CODEC": self.get_config("mr_output_codec")
        }

        materialize(template_str, substitutions, outfile)

    def setup_metadata(self):
        """
        Populates tables in metadata DB

        @rtype: None
        @return: None
        """

        # Set up the 'thrive_setup' table
        md_columns = ["dataset_name", "hive_db", "hive_table", "hive_ddl",
                      "vertica_db", "vertica_schema", "vertica_table", "vertica_ddl",
                      "mapper"]

        # Create metadata. We're working with Python 2.6, so cannot use dictionary
        # comprehension and have to resort to passing tupes to the dict constructor
        setup_metadata = dict((col, self.get_config(col)) for col in md_columns)
        self.metadata_mgr.insert(setup_metadata, mdtype="setup")

        # Set up the 'thrive_load_metadata' table
        current_ts = iso_format(self.loadts)

        load_metadata = {
            "dataset_name": self.get_config("dataset_name"),
            "load_id": self.load_id,
            "load_type": "scheduled", # Initial load is considered 'scheduled'
            "hive_db": self.get_config("hive_db"),
            "hive_table": self.get_config("hive_table"),
            "hive_start_ts": current_ts,
            "hive_end_ts": current_ts,
            "vertica_db": self.get_config("vertica_db"),
            "vertica_schema": self.get_config("vertica_schema"),
            "vertica_table": self.get_config("vertica_table"),
            "vertica_start_ts": current_ts,
            "vertica_end_ts": current_ts
        }

        # Make last load folder None if its name is inconsistent with Camus naming
        # convention
        try:
            lastdir = self.get_config("hive_last_load_folder")
            if is_camus_dir(lastdir):
                load_metadata.update({"last_load_folder": lastdir})
        except ConfigParser.Error:
            logkv(logger, {"msg": "Last load folder not found in configs"}, "error")
            raise SetupHandlerException()
        except MetadataManagerException:
            logkv(logger, {"msg": "Metadata setup failed"}, "error")
            raise SetupHandlerException()
        except Exception:
            logkv(logger, {"msg": "Metadata setup failed"}, "error")
            raise SetupHandlerException()

        self.metadata_mgr.insert(load_metadata, mdtype="load")

        # Set up the 'thrive_dataset_lock' table
        lock_metadata = {
            "dataset_name": self.get_config("dataset_name"),
            "locked": 0,  # 0 = not locked, 1 = locked.
            "release_attempts": 0
        }

        self.metadata_mgr.insert(lock_metadata, mdtype="lock")
        self.metadata_mgr.close()

    def setup_NFS(self):
        """
        Sets up project structure on local file system with proper file permissions

        @rtype: None
        @return: None
        """

        # Create the folders on local file system
        nfs_dataset_path = self.get_config("nfs_dataset_path")

        # Create the parent folder
        self.shell_exec.safe_execute("mkdir -p %s" % nfs_dataset_path)

        # Create workflow.properties folder
        self.shell_exec.safe_execute("mkdir -p %s"
                                     % self.get_config("nfs_workflow_properties_path"))

        # Create jobinput.properties folder.
        self.shell_exec.safe_execute("mkdir -p %s"
                                     % self.get_config("nfs_jobinput_properties_path"))

        # Grant read/execute permissions to NFS folder
        self.shell_exec.safe_execute("chmod a+rx %s" % nfs_dataset_path)

        # Extract contents of zipfile to project structure on local file system
        archive = zipfile.ZipFile(open(self.resources, "rb"))
        #for fconfig in ["hive_columns", "mapper", "vertica_columns", "splunk_bu_emails"]:
        for fconfig in ["hive_columns", "mapper"]:
            archive.extract(self.get_config(fconfig), nfs_dataset_path)

        # Generate hive schema SQL
        self.make_schema()

        # Generate workflow_xml
        self.make_oozie_workflow()

        # Grant read permissions to all files in resources folder and grant execute
        # permissions to all python files
        for rf in glob.glob("%s/*" % nfs_dataset_path):
            self.shell_exec.safe_execute("chmod a+r %s" % rf)

        for pf in glob.glob("%s/*py" % nfs_dataset_path):
            self.shell_exec.safe_execute("chmod a+x %s" % pf)

    def setup_hive(self):
        """
        Creates hive schema

        @rtype: None
        @return: None
        """
        nfs_dataset_path = self.get_config("nfs_dataset_path")
        hive_ddl = self.get_config("hive_ddl")
        cmd = "hive -f %s" % os.path.join(nfs_dataset_path, hive_ddl)
        self.shell_exec.safe_execute(cmd)

    def setup_vertica(self):
        """
        Creates Vertica table schema

        @rtype: None
        @return: None
        """
        ddlfile = os.path.join(self.get_config("nfs_dataset_path"),
                               self.get_config("vertica_ddl"))

        # Create data table
        self.vertica_mgr.create_table(ddlfile)

        # Grant 'USAGE' on schema
        self.vertica_mgr.grant(privilege="USAGE",
                               level="schema",
                               vschema=self.get_config("vertica_schema"),
                               to=self.get_config("vertica_roles"))

        # Grant 'SELECT' on data table. This is redundant since we just granted select on
        # all tables in the schema. But regranting on the specific table in case we want
        # remove the global table read permissions in the future
        self.vertica_mgr.grant(privilege="SELECT",
                               level="schema",
                               vschema=self.get_config("vertica_schema"),
                               to=self.get_config("vertica_roles"))

    def setup_HDFS(self):
        """
        Create a project structure for the dataset on HDFS.
        Steps:
        (1) Create the required HDFS folders
        (2) Copy the workflow and mapper scripts to the appropriate folders
        (3) Give read/exec permissions to the entire folder hierarchy so that
            Oozie can read from there.

        @rtype: None
        @return: None
        """
        hdfs_resource_path = self.get_config("hdfs_resource_path")

        hdfs_paths = ["%s/%s" % (hdfs_resource_path, "workflow"),
                      "%s/%s" % (hdfs_resource_path, "script")]

        nfs_dataset_path = self.get_config("nfs_dataset_path")
        resources = [os.path.join(nfs_dataset_path, self.get_config("workflow_xml")),
                     os.path.join(nfs_dataset_path, self.get_config("mapper"))]

        # Create workflow and scripts folders
        for hpath, resource in zip(hdfs_paths, resources):
            self.hdfs_mgr.makedir(hpath)
            self.hdfs_mgr.putfile(resource, hpath)

        # Grant read-execute persmissions to everyone on HDFS project path
        self.hdfs_mgr.grantall("rx", hdfs_resource_path)

    def execute(self):
        """
        Execute performs one-time setup actions for a dataset being onboarded. These
        actions include
        (1) Extract contents of the zipfile and copy to local folder project structure
        (2) Creating metadata entries in the MySQL database
        (3) Creating Hive schema by executing the DDL script

        @rtype: None
        @return: None
        """

        try:
            # Setup project structure on local file system
            try:
                self.setup_NFS()
                logkv(logger, {"msg": "NFS setup complete"}, "info")
            except (KeyError, ShellException):
                logkv(logger, {"msg": "NFS setup failed"}, "error")
                raise SetupHandlerException()

            # Populate entries in the metadata database
            try:
                self.setup_metadata()
                logkv(logger, {"msg": "Metadata insertion complete"}, "info")
            except MetadataManagerException as ex:
                logkv(logger, {"msg": "Metadata insertion failed"}, "error")
                raise SetupHandlerException()

            # Setup Hive schema
            try:
                self.setup_hive()
                logkv(logger, {"msg": "Hive setup complete"}, "info")
            except ShellException:
                logkv(logger, {"msg": "Hive setup failed"}, "error")
                raise SetupHandlerException()

            # Setup Vertica only if vertica load is requested
            if self.get_config("vertica_load").lower() == "true":
                try:
                    self.setup_vertica()
                    logkv(logger, {"msg": "Vertica setup complete"}, "info")
                except (ShellException, VerticaManagerException):
                    logkv(logger, {"msg": "Vertica setup failed"}, "error")
                    raise SetupHandlerException()

            # Setup project structure on HDFS
            try:
                self.setup_HDFS()
                logkv(logger, {"msg": "HDFS setup complete"}, "info")
            except ShellException:
                logkv(logger, {"msg": "HDFS setup failed"}, "error")
                raise SetupHandlerException()
        except Exception as ex:
            logkv(logger, {"msg": "Thrive setup failed", "exception": ex}, "error")
            raise SetupHandlerException()
