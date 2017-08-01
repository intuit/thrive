# What is Thrive?
Thrive is an ETL framework that runs single-row transformations on HDFS data and makes the data available in
relational databases (Hive and Vertica).  This transformation is key for handling problems which require joining event 
data and relational data (e.g. user behavior from clickstream + transaction data).  We hope that this architecture will 
be a useful blueprint for teams looking to build a similar pipeline for their own Hadoop cluster.

# Why Thrive?
Thrive allows for separation of client code (which captures business logic specific to a given dataset) and framework 
 code.  This insulates the user from having to know which tech stack is being used to execute their transformation 
 (e.g. Hadoop Streaming vs Spark Streaming).  In addition, Thrive also provides:  
* Metadata management (will track / reprocess previous loads if resources unavailable) 
* Dashboards / Alerts to monitor the status of ETL process
* Checking of transformation artifacts on sample data

# Requirements
- Only state-less transformations (transforming a row does not depend on the values in any other row)
- Transformation expressible in Python
- Rows are delimited using newline character
- Source data should be written to HDFS by Camus, or at least should follow the Camus directory naming convention. Under
 this naming convention, the directory names are follow the pattern `d_[0-9]{8}-[0-9]{4}`. For example, 
 `d_20170606-1630`. This makes Thrive tightly coupled with the Kafka-Camus ecosystem. 

Hadoop cluster with following components installed:
* Camus
* Oozie
* Hive
* Hadoop Streaming jars

Separate services (can be accessed from Hadoop cluster):
* MySQL 
* Splunk (optional)
* NewRelic (optional)
* Vertica (optional)

# Quickstart
This example shows how Thrive executes transformations on sample data.  Here, data is only loaded to Hive (not Vertica)
and does not run any additional monitoring or dashboarding via Splunk/NewRelic. 

*Prerequisites*: Docker CLI should be installed.

Download the git repo, and navigate to the thrive/example folder.  From here, run:

    ./setup_env.sh

This should launch two Docker containers - one container running a MySQL instance (for metadata)
and one which extends Cloudera's single node Hadoop container by installing the appropriate packages.

Once you see the prompt, from inside the docker container, run:

    ./run_thrive_example.sh

This will copy some sample data to HDFS, run the cleanup / prepare / setup / load phases of Thrive,
and output the data from the resulting Hive table to the screen.

# Using Thrive

1. Ensure that your data is in the format specified in the Requirements section above (newline-delimited, in directories 
 matching Camus naming conventions).
2. Create a new dataset-specific config file in the config/ directory.  You can copy the existing one and modify the 
following fields:
    * dataset_name: give it a sensible name (no spaces)
    * source_root: absolute path to root folder for source data in HDFS
    * nfs_root: absolute path to directory containing Thrive code on local filesystem
    * hive_db: name of Hive schema where output table will be written to
    * hdfs_user: username for account that will run Thrive code on Hadoop
    * hdfs_root: Root folder in HDFS where transformation files (mapper, DDL, workflow) will be written
    * webhdfs_root, namenode, jobtracker: change these values to point to correct paths for your Hadoop configuration
3. Edit the environment config file to reflect your system's configuration:
    * dbhost, dbport, dbuser, dbpass, dbname: these should reflect your MySQL configuration
4. Create a new folder in onboarding/ that will contain your transformation artifacts. 
5. Add files mapper.py and hive_columns.csv to the folder you just created.  You can use the ones in the example/ folder
as a template.  The transformation specified in mapper.py (Python code) will be applied to each line in the data in your
HDFS source folder.  The column names / datatypes (separated by a single space) that will be output in your final table 
should be specified in hive_columns.csv.
6. Run the cleanup, prepare, setup and load phases as described in the next section.  After the load phase has 
completed, you should have the processed data available in Hive.
  
# Thrive command-line API
The Thrive command-line API is summarized below:

    Usage: python runthrive.py  --phase=[cleanup | setup | load | rollback | replay]
                                    --data-config=<path/to/data_config_file>
                                    --env-config=<path/to/env_config_file>
                                    --resources=<path/to/resources_file>
                                    --partitions=<path/to/partitions_file>
                                    --replaydirs=<path/to/replaydirs_file>
    
    
    Options:
      -h, --help            show this help message and exit
      --phase=PHASE         [required] Specify the thrive workflow phase
      --data-config=DATACFG_FILE
                            [required] Path to dataset-specific config file
      --env-config=ENVCFG_FILE
                            [required] Path to global environment config file
      --resources=RESOURCES_FILE
                            [only if phase=setup] Path to resources file
      --partitions=PARTITIONS_FILE
                            [only if phase=rollback] Path to partitions file
      --replaydirs=REPLAYDIRS_FILE
                            [only if phase=replay] Path to replaydirs file
   
During its lifecycle in Thrive, every onboarded dataset proceeds through
multiple steps called 'phases'. The execution of each phase is managed by a
single config file. Examples of config files can be found [here]
(https://github.intuit.com/idea/thrive/tree/develop/config). For a given dataset
all phases share the same config file. The various phases are described below.

Execution of each phase is triggered by


    python runthrive.py --phase=[cleanup | setup | load | replay | rollback]
                        --data-config=</path/to/data_config_file.cfg>
                        --env-config=</path/to/env_config_file.cfg>
                       [--resources=</path/to/resources_file.zip>]
                       [--partitions=<absolute/path/to/partitions_file.txt>]
                       [--replaydirs=<absolute/path/to/replaydirs_file.txt>]

The `resources-file` is needed only during the `setup` phase. Its creation and use is 
detailed in the 'Setup phase' section below. The `partition_file` is needed only during
the `rollback` phase and its structure is described in the 'Rollback phase' secion 
below. 
 
## Cleanup phase
__Cleanup phase is run once during initial load__

Setting up a dataset requires creating files/directories on HDFS and local file system,
databases/tables in Hive and Vertica and Before setting up a given dataset it needs to be 
ensured that the database and table names specified in the config file are all 
available. If a re-setup is needed, all previous dirs/files/dbs/tables should be nuked 
and fresh ones created. Performing this cleanup is the job of the cleanup phase. 
Cleanup phase is performed manually. 

The 'cleanup' phase is triggered as

    python runthrive.py --phase=cleanup 
                        --data-config=</path/to/data_config_file.cfg>
                        --env-config=</path/to/env_config_file.cfg>
   
## Setup phase
__Setup phase is run once during initial load__

The responsibility of the 'setup' phase is to have everything setup for the 'load' phase.
The things that need to be setup include:

 1. Unzipping the .zip artifact and placing the individual files on the local file system
 2. Generation of table-creation SQL for Hive and Vertica from `columns.csv`
 3. Execution of SQL generated in step 2
 4. Generation of Oozie XML file from template
 5. Place `mapper.py` and `workflow.xml` into HDFS to be used in the load phase
 6. Assigning appropriate permissions to the files on local and Hadoop file systems for
    run-time users
 
The 'setup' phase is manually triggered, one time, during initial load as 
    
    python runthrive.py --phase=setup 
                        --data-config=</path/to/data_config_file.cfg>
                        --env-config=</path/to/env_config_file.cfg>
                        --resources=</path/to/resources_file.zip>

## Load phase
__Load phase is run during initial loading of historical data and then scheduled
to run hourly__

The execution of the 'load' phase needs the mappers, the Oozie workflow files, 
the Hive schema, and the Vertica schema created by the 'setup' phase previously. The 
'load' phase execution instructs Thrive framework to update Hive and Vertica tables.   
Concretely, the load phase performs the following steps:

  1. Fetch HDFS directories to process in this load
  2. Parse data in each of these directories
  3. Create directory for files containing the parser output data
  4. Create new Hive hourly partition and point it to data output directory
  5. Load the data in latest Hive partition to Vertica
  6. Assign access permissions to Hive and Vertica tables so users can query them
  7. Update MySQL metadata 

The load phase is triggered manually, one time, to perform load of historical data. It 
is also scheduled to run periodically (currently at an hourly frequency).

The load phase is triggered as 

    python runthrive.py --phase=load 
                        --data-config=</path/to/data_config_file.cfg>
                        --env-config=</path/to/env_config_file.cfg>
                        
 Although the above command-line API specifies a direct means to run the Thrive 'load' phase, invoking the API directly 
 may prove cumbersome because of the many parameters involved. Thrive has made it possible to invoke the 'load' phase 
 using a single top-level script. Besides convenience, another important reason to invoke the 'load' phase through these
  scripts is that these scripts are the same ones that are run by the scheduler.

## Monitor phase 
__Monitor phase generates dashboards and alerts, should be run after load phase__

The monitor phase manages Splunk dashboards and email alerts that assist various 
stakeholders (Client, Ops, Thrive engineers) with seeing the status of each hourly load 
and informing them when corrective actions need to be taken.  To manage these, it communicates 
with the Splunk server which stores the logs from each hourly load via Splunk's REST API.  The 
dashboards give visibility into load status and record counts at various stages of the 
pipeline, both on an hourly basis for the last 24 hours and on a daily basis for the 
last 14 days.  The alerts are scheduled to run at 0 minutes after every hour and notify the 
appropriate email list if the logs for that hour contain any of the predefined error messages.

Execution of the 'monitor' phase requires the email lists (emails.txt) created by the 'setup' phase previously. 
Concretely, the monitor phase performs the following steps:

  1. Clean up preexisting alerts and dashboards for this dataset, if any
  2. Substitute dataset-specific variables into an XML template
  3. Generate the dashboard from this template and grant appropriate permissions to users
  4. Setup email alerts for the different groups (from dataset-specific emails.txt and resources/internal_emails.txt)
  5. Create alerts and grant appropriate permissions to users

The monitor phase is triggered manually, one time, after initial load as 

    python runthrive.py --phase=monitor 
                        --data-config=</path/to/data_config_file.cfg>
                        --env-config=</path/to/env_config_file.cfg>

## Rollback phase 
__Rollback is expensive to run and irreversible (know what you're doing)__

Errors in the source data or incorrect parsing may require an occasional cleanup of 
parts of data from various sources. This process is known as rollback. Thrive supports 
rollbacks at the granularity of Hive partitions (which, by default, 
are hourly). To execute the `rollback` phase, one needs to supply a simple text file 
specifying the partitions to delete. Example lines from a `partitions.txt` file look 
like the following:

    ...
    2015/10/03/19
    2015/10/03/20
    2015/10/03/22
    2015/10/03/23
    2015/10/04/01
    ...

In other words, `partitions.txt` simply lists the partitions to remove. The partitions 
dont need to be in any order, be contiguous or even exist. If the partitions dont exist, 
they will be ignored. 

Note that each instance of a Thrive load modifies the state of multiple resources 
(HDFS, Hive, MySQL, Vertica ...). A rollback operation must carefully clean up each of 
these resources. 
  
Vertica cleanup is particularly expensive. Rows in Vertica have no mapping to the 
corresponding Hive partitions. As such, cleaning up Vertica rows corresponding to the 
requested Hive partition requires a copy of the Hive partition data to a temporary 
Vertica table, followed by a `delete` operation containing a subquery on this temp table. 
As such, rolling back numerous partitions, or large partitions is an expensive operation.
 
Rollback is also irreversible. The deleted Hive partitions and the corresponding parsed
HDFS data are trashed. They cannot be recovered back. So the necessity and logistics 
of a rollback operation must be carefully planned. Typically we recommend presence of a
client engineer during this operation.

The syntax for triggering the 'rollback' phase is as follows:
  
    python runthrive.py --phase=rollback 
                        --data-config=</path/to/data_config_file.cfg>
                        --env-config=</path/to/env_config_file.cfg>
                        --partitions=<absolute/path/to/partitions.txt>

## Replay phase
__Replay phase is run to re-process any missing data__

Multiple reasons can require an ingestion framework to re-process past data. For
example, source can post incomplete data to HDFS. After the source process is
fixed, Thrive would need to reprocess the data from specific date(s) to fill in
the missing/incomplete data. This process is known as replay and is handled by
the `replay` phase in Thrive. Thrive supports replays at the granularity of HDFS
directories (which, by default, are created at 10 minute intervals). The
`replay` phase, is executed in the following steps:

  1. Identify the folders which need to be reprocessed (i.e. a list containing
  folders in the `d_yyyymmdd-HHMM` format).
  2. Copy these folders a `replaydirs.txt` file. The folders in this list would
  be re-processed. For example the following Bash command takes creates a
  `replaydirs.txt` file containing all folders in August 2016

        hadoop fs -ls /data/ds_ctg/trinity/thrive_test/ | \
        grep "d_201608" | \
        sed -nE 's:^.*(d_[0-9]{8}\-[0-9]{4})$:\1:p' \
        > ~/replaydirs.txt
  3. Run the replay phase to reprocess the identified directories:

        python runthrive.py --phase=replay
                            --data-config=</path/to/data_config_file.cfg>
                            --env-config=</path/to/env_config_file.cfg>
                            --replay-dirs=<absolute/path/to/replaydirs.txt>                       
 

# FAQ

  1. __If Thrive loads JSON data to Vertica, why doesn't it simply use Vertica Flex tables?__
  
    Multiple reasons:
        
      1.1 Data in input JSONs typically needs additional processing before loading (e.g.,
      splicing values in an array, 1 JSON to multiple rows etc).
            
      1.2 For production data, the virtual columns need to be promoted to real columns. 
      Without that the query performance suffers. However, real columns lead to duplicated
      data (data in original JSON and in the real column) and during the `COPY` command
      (which matches each key in input JSON to names of real columns). As a result, bulk
      `COPY` becomes slow.
      
      1.3 `fjsonparser()` is not modifiable. As a result you have to write `COPY UDFilter` to
      parse complex JSON. So you're not spared the work of writing parsers anyway.
       
      1.4 We're seeing increasing number of non-JSON dataset (XML, CSV) requests for Thrive.
      Vertica Flex tables cannot handle these.
     
      1.5 Flex tables cannot be partitioned on virtual columns.
      
      1.6 Rejected rows cannot be specified on `COPY`
       

# Appendix

## Thrive risks and mitigation measures
 1. __Risk: Dependency on and independent failures of external resources__:
 Thrive essentially manages data flow between different external resources. In a 
 typical load session, it connects to bash shell, MySQL server (for metadata), HDFS, 
 Oozie, Hive and Vertica. Each of these resources can fail independently and completely
 outside of Thrive's control.
 
    __Mitigation measures__: Thrive is designed to exit gracefully and not leave these 
   resources in an inconsistent state. Following are the risk mitigation measures. 
   Thrive updates metadata only twice: (1) when the Hive load is successful and (2) 
   when the load till Vertica is successful. That way, metadata is not left in an 
   inconsistent or incomplete state when the loads fail mid-way. Thrive anticipates 
   that Hive and Vertica can fail independently. During each load, 
   Thrive checks for earliest Hive partitions that have yet not been loaded to Vertica.
   That way, each load tried to keep Hive and Vertica in sync, 
   reducing need for manual intervention when Vertica fails. When primary resources 
   such  as MySQL, HDFS and Hive are unavailable, the data loads don't reach up to the 
   Vertica stage. In these cases Thrive will exit gracefully with an error message 
   that  will be present in the alert email.

 2. __Risk: Loads lasting over an hour and race conditions between consecutive loads__:
 Jobs for Thrive are typically expected to be on an hourly schedule. Our load 
 testing so far indicates that an hour is adequate for completing load of up to 25 to 
 50  million rows depending on the payload size. However, 
 cluster slowdown or unexpectedly high data volumes (for example, 
 during season peaks)   can require more than an hour to complete load. In this case 
 does  the load for next hour trigger and overwrite the in-progress load for the 
 previous  hour? No.

    __Mitigation measures__: Thrive has implemented dataset locks to prevent race 
    conditions  between two instances of a load process for the same dataset. A load 
    process for any given dataset needs to acquire a lock in order to proceed. Because 
    of locks, a second load instance for the same dataset cannot be triggered either 
    manually or via a scheduler. 
 
 3. __Risk: Dataset locked permanently due to a failed load__: The dataset acquires a 
 lock,  starts a load, fails and exits without releasing the lock. Subsequent loads are
 not  allowed to start.
  
    __Mitigation measures__: First, Thrive design prevents any load process from 
    exiting without  releasing the lock. However, this cannot be prevented entirely 
    because  a privileged kernel process can still send a kill signal and halt the 
    Thrive  process without allowing it to execute the exception code. In that case, 
    the next  load instance will increment the number of unsuccessful release attempts.
    Thrive  can then force unlock after a configurable number of release attempts have
    been  reached. What that number should be is under evaluation. Currently though, 
    we're  alerting for every unsuccessful lock acquire attempt. Currently, the dataset 
    needs to be manually unlocked in Prod, while auto-unlock is enabled in Preprod after 
    3 failed release attempts.
 
 4. __Risk: Unexpected data volumes__: Thrive does not scale due to unexpected data volume

    __Mitigation measures__: Experience teams were instructed to test data load at 150%
     to 200%  of anticipated peak volume. So the datasets with high expected data 
     volumes have  been stress tested. For example FDS transactions datasets has been 
     tested  with up to 77 million rows per load.

 5. __Risk: Missing data in Hive to Vertica load (unequal row counts)__: Rows in Hive 
 rejected by Vertica resulting in unequal row counts. Relational databases are strict 
 about  requiring the exact datatype expected by the schema. For example if the schema 
 specifies  an INT then a 1 received as 1.0 might be rejected. 
 
    __Mitigation measures__:  (1) Thrive engineering and the client team will receive an 
    email alert with  the percentage data loss. (2) The missing row data with an 
    exception (reason for  rejection) will be placed in a separate  Vertica table accessible to 
    the clients. For their debugging, client teams should use this as the first checkpoint 
    when experiencing data loss from Hive to Vertica.
 
 6. __Risk: Missing data in HDFS to Hive load (data rejected during parsing stage)__: 
 Source data in HDFS is rejected during the parsing stage due to unexpected JSON schema or 
 malformed JSON.
 
    __Mitigation measures__: (1) We advise the clients repeatedly to perform QA on their 
    schema. (2) We alert on any HDFS records rejected by the parser. (3) Client can 
    optionally request to dump  rejected rows the 'FAIL' column in Hive. However, 
    this leads to Vertica table bloat,  since every row will now need to have a large,
    mostly empty column capable of holding the entire payload. 
 
 
## Thrive architecture
   
Thrive is a simple data pipeline and has a relatively straightforward architecture. 
There are parts that the core code uses repeatedly and it pays well to invest some 
thought in abstracting these reusable components. The following diagram depicts 
Thrive architecture.  

![Thrive arch diagram](https://github.intuit.com/storage/user/1548/files/edf8d568-7a91-11e6-8576-97f793d3bee5)

Thrive core has two families of classes: `Managers` and `Handlers`. `Managers` are 
wrapper layers on various resources (Hadoop, Hive, Vertica, Oozie, Graphite, 
...). The `Handlers` are responsible for possible workflows that the pipeline may be 
called to serve. 
 
`Handler`s use the services of `Manager`s to manage various tasks in workflows. Each 
possible Thrive workflow is what we referred above as a Thrive `phase`. 
Correspondingly, the `Handler` classes align closely with the `phase`s and are named 
accordingly.  
   
The `Manager` classes abstract the various resources that Thrive needs to access to 
complete various steps in its workflow. Thrive has built abstractions for HDFS, 
Hive, Vertica, Graphite, MySQL and so on. For each new resource, 
we plugin a new abstractions with required operations. This design strives to follow 
the [Open/Closed principle](https://en.wikipedia.org/wiki/Open/closed_principle). 
Sometimes we succeed, sometimes we dont. But its a useful general guiding principle in 
our design efforts.  

The key difference between `Manager` and `Handler` classes is that Thrive's state (what
 phase has been requested, what step is the workflow at etc) resides only within the 
 `Handler` classes. The `Manager` classes provide only the requested services. They 
 know nothing about the state of the workflow.  
 
# Community
## Core Developers

Rohan D. Kekatpure

Sheel Dandekar

## Contributors

Shradha Cripe (Product Management)

Anand Mistry

Swathi Nimmagadda

Sambaiah Kilaru
 

## Technical Reviewers

Cynan de Leon

Kevin Sebastian

Todd Fast



