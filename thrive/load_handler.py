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

import os
import logging
from datetime import datetime
from thrive.utils import iso_format, logkv, materialize, percentdiff, \
     dirname_to_dto, CAMUS_FOLDER_FREQ, chunk_dirs, parse_partition
from thrive.thrive_handler import ThriveHandler
from thrive.oozie_manager import OozieManager
from thrive.newrelic_manager import NewRelicManager, NewRelicManagerException
from thrive.exceptions import LoadHandlerException, OozieManagerException, \
    VerticaManagerException, HdfsManagerException, HiveManagerException, \
    MetadataManagerException, ThriveBaseException

logger = logging.getLogger(__name__)


class LoadHandler(ThriveHandler):
    """
    Handler for load phase of the pipeline
    """
    def __init__(self, datacfg_file=None, envcfg_file=None, resources_file=None):
        """
        Initializes the ThriveHandler superclass and instantiates manager classes
        needed to performing various load-related actions

        @type datacfg_file:  str
        @param datacfg_file: Full or relative path of the dataset-specific config file

        @type envcfg_file:  str
        @param envcfg_file: Full or relative path of the global environment config file

        @type resources_file: str
        @param resources_file: Full or relative path of the resources file

        @rtype: None
        @return: None
        """

        super(LoadHandler, self).__init__(datacfg_file, envcfg_file, resources_file)

        logkv(logger, {"msg": "Starting load",
                       "dataset": self.get_config("dataset_name")}, "info")

        # Initialize fields that will be filled in methods of this class.
        self.propfile = None
        self.newdirs = None
        self.locked = False
        self.load_type = None

        # Get primary HDFS namenode before proceeding with load
        namenodes = self.get_config("webhdfs_root").split(",")
        try:
            self.primary_namenode = \
                self.hdfs_mgr.get_primary_namenode(namenodes,
                                                   self.get_config("hdfs_root"),
                                                   self.get_config("hdfs_user"))
        except HdfsManagerException as ex:
            logkv(logger, {"msg": "Failed to get primary namenode"}, "error", ex)
            raise LoadHandlerException()

        # Instantiate Oozie manager
        self.oozie_mgr = OozieManager()

        # Get folder processing delay
        try:
            self.process_delay = float(self.get_config("folder_processing_delay"))
        except ValueError as ex:
            logkv(logger,
                  {"msg": "Could not parse folder_processing_delay as a float"},
                  "error")
            raise LoadHandlerException()

        # # Instantiate NewRelic manager
        # try:
        #     self.newrelic_mgr = NewRelicManager(self.get_config("newrelic_api_key", configtype="env"),
        #                                         self.get_config("newrelic_dataset_name"),
        #                                         self.get_config("newrelic_url"))
        #
        # except Exception as ex:
        #     logkv(logger, {"msg": "Failed to initialize NewRelic Manager"}, "error")
        #     raise LoadHandlerException()

    def get_newdirs(self):
        """
        Get new, unprocessed HDFS directories created in the topic since the last
        successful run of the load process. It gets the last successfully processed HDFS
        directory from MySQL metadata and a list of all directories in the topic from
        HDFS. It then slices this list to return only the new directories.

        @rtype: list
        @return: List of pending, unprocessed HDFS dirs since the last successful load
        """

        # Get the name of last processed HDFS folder from metadata
        old_lastdir = self.metadata_mgr.get_lastdir(self.get_config("dataset_name"),
                                                    self.get_config("hive_table"),
                                                    self.load_type)

        # Get all folders in topic in HDFS
        newdirs = self.hdfs_mgr.get_newdirs(self.get_config("source_root"),
                                            old_lastdir, self.loadts,
                                            self.process_delay)
        return newdirs

    def make_tmproot(self):
        """
        Construct HDFS tmp target location. This will be needed to decompress data
        from Hive partition into a plan text file in HDFS

        @rtype: str
        @return: Location of the created HDFS tmp directory
        """
        tmproot = "/tmp/thrive/%s/%s" % (self.get_config("hive_db"),
                                         self.get_config("hive_table"))

        # Clean up HDFS path in tmp for the current partition. If the path does not
        # exist issue warning and proceed.
        try:
            self.hdfs_mgr.rmdir(tmproot)
        except HdfsManagerException:
            logkv(logger, {"msg": "Failed to remove HDFS dir", "directory":tmproot},
                  "warning")
            raise LoadHandlerException

        try:
            self.hdfs_mgr.makedir(tmproot)
            logkv(logger, {"msg": "Created HDFS directory", "directory": tmproot}, "info")
            return tmproot
        except HdfsManagerException as ex:
            logkv(logger, {"msg": "Failed to create HDFS directory",
                           "directory": tmproot}, "error", ex)
            raise LoadHandlerException

    def make_workflowpropsfile(self, output_path, dirlist):
        """
        Makes a timestamped Oozie workflow.properties file for the current run. The
        generated properties file is stored in the workflow-properties dir in the
        datset directory.

        @rtype: None
        @return: None
        """

        # Get the path to workflow_xml file on HDFS
        workflow_xml_path = os.path.join(self.get_config("hdfs_resource_path"),
                                         "workflow/%s" % self.get_config("workflow_xml"))

        # Configure inputFile parameter of workflow.properties file.
        # For initial load, with no inputdirs folders (pending_folders = None),
        # inputFile is simply sourceRoot/*. The last "*" is crucial for
        # recursive traversal of directorues. Otherwise "Not a file" exception
        # will result.
        input_files = os.path.join(self.get_config("source_root"),
                                   "{" + "\\\\\\\\,".join(dirlist) + "}")

        # Create the properties file directory if its not created during the setup phase
        self.shell_exec.safe_execute("mkdir -p %s"
                                     % self.get_config("nfs_workflow_properties_path"))

        # Read the template file and perform the substitutions
        template_file = os.path.join(self.get_config("nfs_resource_path"),
                                     "workflow_template.properties")

        with open(template_file, "r") as tf:
            template_str = tf.read()

        substitutions = {
            #"@OOZIELIBPATH": self.get_config("oozie_libpath"),
            "@JOBTRACKER": self.get_config("jobtracker"),
            "@WORKFLOWXML": workflow_xml_path,
            "@NAMENODE": self.get_config("namenode"),
            "@INPUTFILES": input_files,
            "@OUTPUTDIR": output_path,
            "@NUM_REDUCERS": self.get_config("mr_num_reducers")
        }

        # Materialize the properties file
        propfile_name = "workflow_%s.properties" % self.loadts.strftime("%Y%m%d-%H")
        self.propfile = os.path.join(self.get_config("nfs_workflow_properties_path"),
                                     propfile_name)
        materialize(template_str, substitutions, outfile=self.propfile)

        logkv(logger, {"msg": "Generated properties file",
                       "properties_file": self.propfile}, "info")

    def vload_copy(self, _hiveptn, vschema=None, dtable=None, mode="direct"):
        """
        Loads data in Hive partition '_hiveptn' into Vertica table 'dtable' in Vertica
        schema 'vschema' using the requested 'mode'.

        This is a general purpose wrapper on Vertica COPY command which is used by
        other methods in this class as well as classes derived from this class (such
        as RollbackHandler)

        'vschema' and 'dtable', if not supplied, are taken from the config file.

        @type _hiveptn: str
        @param _hiveptn: Hive partition to be loaded into Vertica

        @type vschema: str
        @param vschema: Vertica schema

        @type dtable: str
        @param dtable: Vertica data table

        @type mode: str
        @param mode: Copy mode. Possible values: 'direct' or 'decompress'. If mode is
        'decompress', the data in Hive partition will be decompressed to a plain text
        format using a single node to a temporary HDFS location. IF the mode='direct'
        an attempt will be made to directly load compressed data to vertica using the
        appropriate filter.

        mode='decompress' is required if a the MapReduce job outputs data in a compression
        format other than GZIP. For example BZip2. In this case, we'll decompress the
        data before passing it to the COPY command

        @rtype: int
        @return: Number of rows loaded to Vertica
        """
        logkv(logger, {"msg": "Performing Vertica copy",
                       "method": "COPY command",
                       "mode": mode}, "info")

        # Construct the location of source data
        partfiles = os.path.join(self.get_config("target_root"), _hiveptn, "*")

        if mode == "decompress":
            # Construct HDFS tmp target location. This will be needed to decompress data
            # from Hive partition into a plain text file in HDFS
            tmpdir = os.path.join(self.make_tmproot(), _hiveptn)
            self.hdfs_mgr.makedir(tmpdir)
            logkv(logger, {"msg": "Created directory", "directory": tmpdir}, "info")
            tmpdatafile = os.path.join(tmpdir, "partition_data.txt")

            # Decompress the data for the given Hive partition to plain-text and store it
            # in tmpdatafile (in HDFS)
            self.hdfs_mgr.decompress(partfiles, tmpdatafile)

            # In decompress mode, data_location is the decompressed temp file
            data_location = tmpdatafile
        else:
            # In "direct" mode, the data location is simply the partfiles location
            data_location = partfiles

        # Set values for dtable and vschema
        if vschema is None:
            vschema = self.get_config("vertica_schema")

        if dtable is None:
            dtable = self.get_config("vertica_table")

        # Get the name of rejected rows table
        rtable = self.get_config("vertica_rejected_data_table")

        # Load the _datafile to Vertica table
        rows_loaded = self.vertica_mgr.load(self.primary_namenode, data_location,
                                            vschema, dtable, rtable, mode=mode)
        return rows_loaded

    def lock(self):
        """
        Lock the dataset

        @rtype None
        @return: None
        """
        dataset_name = self.get_config("dataset_name")
        try:
            self.metadata_mgr.lock(dataset_name)
            self.locked = True
            logkv(logger, {"msg": "Acquired lock",
                           "dataset": dataset_name}, "info")
        except MetadataManagerException as ex:
            logkv(logger, {"msg": "Error acquiring lock on dataset",
                           "dataset": dataset_name}, "error", ex)
            raise LoadHandlerException()

    def proceed(self):
        """
        Checks if the current run should be allowed. The load process is not allowed
        to run if:
        (1) The output folder that it'd create already exists.
        (2) There are no new HDFS directories to process since the last successful run.
        This function has a side effect of assigning values to self.partition_path and
        self.newdirs.
        (3) HDFS has no active namenode

        @rtype: bool
        @return: _proceed. Value indicating if the current run should proceed

        """
        dataset_name = self.get_config("dataset_name")

        # If the dataset is locked, increment release attempt and return.
        lock_status, release_attempts = self.metadata_mgr.get_lock_status(dataset_name)
        logkv(logger, {"lock_status": lock_status,
                       "release_attempts": release_attempts},
              "info")

        max_unlock_attempts = int(self.get_config("max_unlock_attempts", configtype="env"))

        # Auto unlock dataset if number of release attempts too high
        if (max_unlock_attempts != -1) and (release_attempts >= max_unlock_attempts):
            self.metadata_mgr.release(dataset_name)
            lock_status = 0
            logkv(logger, {"msg": "Auto-unlocked dataset since # release attempts: %d >= # max_unlock_attempts: %d"
                                  % (release_attempts, max_unlock_attempts), "dataset": dataset_name}, "warning")

        if lock_status == 1:
            logkv(logger, {"msg": "Dataset locked by another load",
                           "dataset": dataset_name}, "info")

            self.metadata_mgr.increment_release_attempt(dataset_name)
            return False
        else:
            logkv(logger, {"msg": "Dataset unlocked and available for load",
                           "dataset": dataset_name}, "info")

        # Get a list of new HDFS directories created since the last load and pending
        # for processing. # Return if no directories are pending processing.
        self.newdirs = self.get_newdirs()

        if not self.newdirs:
            logkv(logger, {"msg": "No new HDFS directories to process."}, "info")
            return False

        # Return if no active namenode is found
        if not self.primary_namenode:
            return False

        return True

    def execute(self, load_type="scheduled"):
        """
        Top level method for LoadHandler; manages the load workflow.
        The method (1) launches and monitors Oozie Hadoop streaming MapReduce job (2)
        creates a new Hive partition (3) Performs incremental Vertica load

        @rtype: bool
        @return: None
        """

        dataset_name = self.get_config("dataset_name")
        self.load_type = load_type
        try:
            # End load process if conditions for proceeding are invalidated
            if not self.proceed():
                logkv(logger, {"msg": "Ending load"}, "info")
                return

            logkv(logger, {"msg": "Proceeding with load"}, "info")

            # Acquire lock on the dataset
            self.lock()

            # Chunk the new directories for processing
            dirchunks = chunk_dirs(self.newdirs,
                                   groupby=self.get_config("mr_chunk_size"))

            chunk_labels_asc = sorted(dirchunks.keys(),
                                      key=lambda x: int("".join(x.split("/"))))

            for ptn_label in chunk_labels_asc:
                # Save Hive end timestamp
                hive_start_ts = iso_format(datetime.now())

                mr_input_dirs = dirchunks.get(ptn_label)
                ptn_parent = os.path.join(self.get_config("target_root"),
                                          ptn_label)

                # If parent partition doesnt exist, create it with a
                # subpartition 0, else increment the subpartition number
                # and create it.
                if self.hdfs_mgr.path_exists(ptn_parent):
                    subdirs = self.hdfs_mgr.get_subdirs(ptn_parent)
                    last_subdir = sorted(subdirs, key=int)[-1]
                    ptn_path = os.path.join(ptn_parent,
                                            str(int(last_subdir) + 1))
                else:
                    ptn_path = os.path.join(ptn_parent, "0")

                # Generate properties file for this load
                logkv(logger, {"msg": "Generating properties file for load"}, "info")
                self.make_workflowpropsfile(ptn_path, mr_input_dirs)

                # Trigger oozie job for JSON parsing
                logkv(logger, {"msg": "Triggering Oozie workflow",
                               "directories": ",".join(mr_input_dirs),
                               "properties_file": self.propfile}, "info")

                try:
                    # Launch Oozie job
                    jobid = self.oozie_mgr.launch(propfile=self.propfile)

                    # Poll job status every 10 seconds until the job finishes
                    self.oozie_mgr.poll(jobid, interval=10)

                    # Extract Hadoop statistics
                    counts = self.oozie_mgr.get_counts(jobid)

                    # Derive the count of processed records
                    mr_processed_records = int(counts["map_input_records"]) \
                                           - int(counts["skipped"])
                    logkv(logger, {"msg": "Successfully parsed data"}, "info")
                    logkv(logger, counts, "info")
                except OozieManagerException, oe:
                    logkv(logger, {"msg": "Oozie job failed"}, "error")
                    raise LoadHandlerException()

                # Create a Hive partition at partition_path (computed earlier in this function)
                logkv(logger, {"msg": "Creating new Hive partition"}, "info")
                try:
                    # Create new hive partition
                    self.hive_mgr.create_partition(ptn_path)
                    logkv(logger, {"msg": "Added Hive partition",
                                   "partition": ptn_path}, "info")
                except HiveManagerException as ex:
                    logkv(logger, {"msg": "Error creating Hive partition"},
                          "error", ex)
                    raise LoadHandlerException()

                # Grant read/execute permissions on the newly created partition
                logkv(logger, {"msg": "Granting read/execute permissions on partition"},
                      "info")
                try:
                    self.hdfs_mgr.grantall("rx", self.get_config("target_root"))
                    logkv(logger, {"msg": "Granted read/execute permissions",
                                   "path": self.get_config("target_root")}, "info")
                except HdfsManagerException as ex:
                    logkv(logger,
                          {"msg": "Error granting permissions on HDFS path"},
                          "error", ex)
                    raise LoadHandlerException()

                # Save Hive end timestamp
                hive_end_ts = iso_format(datetime.now())

                # Update the HIVE portion of the metadata. We choose to update metadata
                # separately for Hive and Vertica steps because of potential for situations
                # in which Hive loads succeed, but Vertica ones fail. The Vertica process in
                # the next run can then look for all instances of successful Hive loads where
                # Vertica load wasnt triggered and loop through them.
                logkv(logger, {"msg": "Adding last processed directory to metadata",
                               "last_processed_dir": mr_input_dirs[-1]}, "info")

                try:
                    hive_load_metadata = {
                        "load_id": self.load_id,
                        "load_type": self.load_type,
                        "dataset_name": dataset_name,
                        "hive_db": self.get_config("hive_db"),
                        "hive_table": self.get_config("hive_table"),
                        "hive_start_ts": hive_start_ts,
                        "hive_end_ts": hive_end_ts,
                        "last_load_folder": mr_input_dirs[-1],
                        "hive_last_partition": "/".join(parse_partition(ptn_path)),
                        "hadoop_records_processed": counts["map_input_records"],
                        "hive_rows_loaded": counts["map_output_records"]
                    }

                    self.metadata_mgr.insert(hive_load_metadata, mdtype="load")
                    logkv(logger, {"msg": "Successfully updated metadata"}, "info")
                except MetadataManagerException as ex:
                    logkv(logger, {"msg": "Error updating Hive metadata"},
                          "error", ex)
                    raise LoadHandlerException()

                # Exit if vertica load is not requested
                if self.get_config("vertica_load").lower() != "true":
                    logkv(logger, {"msg": "Vertica load not requested",
                                   "dataset": dataset_name}, "info")
                    continue

                # Get unprocessed Hive partitions
                pending_ptn_data = self.metadata_mgr.get_unprocessed_partitions(
                    self.get_config("hive_db"),
                    self.get_config("hive_table"))

                logkv(logger, {"msg": "Pending partition to be loaded to Vertica",
                               "partitions": [p[1] for p in pending_ptn_data]}, "info")

                # Loop through Hive partitions that are currently not loaded in Vertica
                for load_id, hiveptn, hive_rows, mr_input_records in pending_ptn_data:
                    try:
                        # Get vertica_start_ts for this partition
                        vertica_start_ts = iso_format(datetime.now())

                        # Load the data
                        vertica_rows = self.vload_copy(hiveptn, mode="direct")

                        # Get vertica_end_ts for this partition
                        vertica_end_ts = iso_format(datetime.now())

                        # Update Vertica metadata if requested
                        try:
                            # Compose Vertica metadata to update for load of this partition
                            vertica_metadata = {
                                "vertica_db": self.get_config("vertica_db"),
                                "vertica_schema": self.get_config("vertica_schema"),
                                "vertica_table": self.get_config("vertica_table"),
                                "vertica_start_ts": vertica_start_ts,
                                "vertica_end_ts": vertica_end_ts,
                                "vertica_last_partition": hiveptn,
                                "vertica_rows_loaded": vertica_rows,
                                "status": "SUCCESS"
                            }

                            # Update metadata table with generated metadata above
                            self.metadata_mgr.update((load_id, hiveptn),
                                                     vertica_metadata,
                                                     mdtype="load")
                            logkv(logger, {"msg": "Loaded hive partition",
                                           "partition": hiveptn}, "info")
                        except MetadataManagerException as ex:
                            logkv(logger,
                                  {"msg": "Error updating Vertica metadata"},
                                  "error", ex)
                            raise LoadHandlerException()

                        # try:
                        #     # Get start and end times for load
                        #     logkv(logger, {"msg": "Getting start and end times for newly loaded dirs"}, "info")
                        #     source_start_ts = dirname_to_dto(mr_input_dirs[0])
                        #     source_end_ts = dirname_to_dto(mr_input_dirs[-1]) + CAMUS_FOLDER_FREQ
                        #
                        #     # Get counts for Jetty and Kafka
                        #     logkv(logger, {"msg": "Getting Jetty and Kafka counts"}, "info")
                        #     jetty_events = self.newrelic_mgr.get_count("jetty", source_start_ts, source_end_ts)
                        #     kafka_events = self.newrelic_mgr.get_count("kafka", source_start_ts, source_end_ts)
                        # except NewRelicManagerException as ex:
                        #     jetty_events = kafka_events = ""
                        #     logkv(logger, {"msg": "Error querying NewRelic"},
                        #           "warning", ex)

                        # Log the load summary, will be consumed by Splunk
                        logkv(logger, {
                            "msg": "load summary",
                            # "jetty_events": jetty_events,
                            # "kafka_events": kafka_events,
                            "mr_input_records": mr_input_records,
                            "mr_processed_records": mr_processed_records,
                            "hive_rows_loaded": hive_rows,
                            "vertica_rows_loaded": vertica_rows,
                            "percent_loss_mr": percentdiff(mr_processed_records, mr_input_records),
                            "percent_loss_hv": percentdiff(vertica_rows, hive_rows)
                        }, "info")

                    except VerticaManagerException as ex:
                        logkv(logger, {"msg": "Vertica load failed"},
                              "error", ex)
                        raise LoadHandlerException()
        except ThriveBaseException as ex:
            logkv(logger, {"msg": "Thrive load failed"}, "error", ex)
            raise LoadHandlerException()
        except BaseException as bex:
            logkv(logger, {"msg": "Thrive load failed because of system exception",
                           "exception": bex}, "error")
            raise LoadHandlerException()
        finally:
            if self.locked:
                logkv(logger, {"msg": "Releasing lock"}, "info")
                self.metadata_mgr.release(dataset_name)
                logkv(logger, {"msg": "Ending load", "dataset": dataset_name}, "info")

