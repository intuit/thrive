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

import pyodbc
import logging
from thrive.utils import logkv
from thrive.exceptions import MetadataManagerException

logger = logging.getLogger(__name__)


class MetadataManager(object):
    """
    Class for managing metadata for the Thrive setup and load process. This is
    really only a CRUD layer + business logic on top of the MySQL metadata
    database.

    Primary responsibilities of this class include supplying a metadata
    manager object capable of communicating with the metadata DB and updating
    the metadata after every load event.
    """
    def __init__(self, credentials):
        """
        Initializes the connection object by establishing connection with
        the metadata DB

        @type credentials: dict
        @param credentials: parameters required for connection

        @rtype: None
        @return: None
        """
        self.credentials = credentials

        try:
            self.connection = pyodbc.connect(
                "DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DB=%s"
                % (self.credentials["dbtype"],
                   self.credentials["dbhost"],
                   self.credentials["dbport"],
                   self.credentials["dbuser"],
                   self.credentials["dbpass"],
                   self.credentials["dbname"])
            )
        except Exception as ex:
            logkv(logger, {"msg": "Could not connect to database",
                           "credentials": credentials}, "error")
            raise MetadataManagerException()

    def execute(self, qry):
        """
        Function for executing queries which dont return results.
        Sends SQL query "qry" to metadata database.

        @type qry: str
        @param qry: SQL query string

        @rtype: None
        @return: None
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(qry)
            self.connection.commit()
            cursor.close()
        except Exception:
            logkv(logger, {"msg": "SQL execution failed",
                           "query": qry}, "error")
            raise MetadataManagerException()

    def execute_return(self, qry):
        """
        Function for executing queries which return results.
        Sends SQL query "qry" to metadata database.

        @type qry: str
        @param qry: SQL query string

        @rtype: list
        @return: List of tuples
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(qry)
            self.connection.commit()
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception:
            logkv(logger, {"msg": "SQL execution failed",
                           "query": qry}, "error")
            raise MetadataManagerException()

    def insert(self, data, mdtype=None):
        """
        Inserts data from key-value pairs in "data" into "table"

        @type mdtype: str
        @param mdtype: Type of insert. Possible values "load" or "setup"

        @type data: dict
        @param data: key-value pairs with keys = column name

        @rtype: None
        @return: None
        """

        if mdtype == "setup":
            mdtable = "thrive_setup"
        elif mdtype == "load":
            mdtable = "thrive_load_metadata"
        elif mdtype == "lock":
            mdtable = "thrive_dataset_lock"
        else:
            logkv(logger, {"msg": "Invalid metadata type",
                           "mdtype": mdtype}, "error")
            raise MetadataManagerException()

        columns = ",".join(data.keys())
        values = ",".join("'%s'" % v for v in data.values())

        insert_qry = "insert into %s (%s) values (%s);" \
                     % (mdtable, columns, values)

        try:
            self.execute(insert_qry)
        except pyodbc.IntegrityError, ie:
            logkv(logger, {"msg": "Duplicate primary key insertion",
                           "query": insert_qry,
                           "error": ie}, "error")
            raise MetadataManagerException()
        except pyodbc.Error, poe:
            logkv(logger, {"msg": "Could not insert data",
                           "query": insert_qry,
                           "error": poe}, "error")
            raise MetadataManagerException()

    def update(self, pk, data, mdtype=None):
        """
        Updates a row with id 'id' with 'data'. If mdtype='setup', thrive_setup
        table updated. If mdtype='load', thrive_load_metadata table is updated.

        @type pk: tuple
        @param pk: multi-column primary key tuple for the table

        @type data: dict
        @param data: data to be updated as key-value pair

        @type mdtype: str
        @param mdtype: 'load' or 'setup'

        @rtype: None
        @return: None
        """

        if mdtype == "setup":
            mdtable = "thrive_setup"
            filter_condition = "dataset_id = '%s'" % pk[0]
        elif mdtype == "load":
            mdtable = "thrive_load_metadata"
            filter_condition = "(load_id, hive_last_partition) = ('%s', '%s')" % pk
        else:
            logkv(logger, {"msg": "Invalid metadata type",
                           "mdtype": mdtype}, "error")
            raise MetadataManagerException()

        updates = ",".join('%s="%s"' % (key, val) for key, val in data.items())
        update_qry = '''update %s
                        set %s
                        where %s;
                     ''' % (mdtable, updates, filter_condition)
        try:
            self.execute(update_qry)
        except Exception as ex:
            logkv(logger, {"msg": "Could not update data",
                           "query": update_qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def get_lastdir(self, dataset_name, hive_table, load_type):
        """
        Returns the last directory processed for "topic" by querying "table"

        @type dataset_name: str
        @param dataset_name: dataset being loaded

        @rtype: str
        @return: Last Camus directory loaded
        """

        qry = '''
                 select last_load_folder
                 from thrive_load_metadata
                 where dataset_name = '%s'
                 and hive_table = '%s'
                 and load_type = '%s'
                 order by hive_end_ts desc
                 limit 1;
              ''' % (dataset_name, hive_table, load_type)

        try:
            return self.execute_return(qry)[0][0]
        except Exception as ex:
            logkv(logger, {"msg": "Failed to get last dir for dataset",
                           "dataset": dataset_name,
                           "table": hive_table,
                           "query": qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def purge(self, dataset_name):
        """
        Purges metadata entries for 'topic' in 'thrive_setup' and
        'thrive_load_metadata' tables

        @type dataset_name: str
        @param dataset_name: topic name

        @rtype: None
        @return: None
        """
        thrive_tables = ("thrive_setup",
                         "thrive_load_metadata",
                         "thrive_dataset_lock")
        try:
            for md_table in thrive_tables:
                purge_setup_qry = "delete from %s where dataset_name = '%s';" \
                                  % (md_table, dataset_name)
                self.execute(purge_setup_qry)
        except Exception as ex:
            logkv(logger, {"msg": "Purge failed for dataset",
                           "dataset": dataset_name,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def get_unprocessed_partitions(self, hive_db, hive_table):
        """
        Get partitions for 'hive_table' in 'hive_db' that are not yet loaded into Vertica

        @type hive_db: str
        @param hive_db: Hive database name for this dataset

        @type hive_table: str
        @param hive_table: Hive table name for this dataset

        @rtype: list
        @return: List of partitions
        """
        qry = '''
                  SELECT load_id,
                         hive_last_partition,
                         hive_rows_loaded,
                         hadoop_records_processed
                  from thrive_load_metadata
                  where hive_db = '%s'
                  and hive_table = '%s'
                  and hive_last_partition <> ''
                  and vertica_last_partition is NULL;
              ''' % (hive_db, hive_table)
        try:
            return self.execute_return(qry)
        except Exception as ex:
            logkv(logger, {"msg": "Failed to get unprocessed partitions of table",
                           "db": hive_db,
                           "table": hive_table,
                           "query": qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def lock(self, dataset_name):
        """
        Locks the specified dataset. Each instance of the load process checks for the
        lock state. The lock state must be 0 for the load process to proceed.

        @type dataset_name: str
        @param dataset_name: Name of the dataset to be locked

        @rtype: None
        @return: None
        """
        lock_qry = '''
                      update thrive_dataset_lock
                      set locked = TRUE, release_attempts = 0
                      where  dataset_name = '%s';
                   ''' % dataset_name
        try:
            self.execute(lock_qry)
        except Exception as ex:
            logkv(logger, {"msg": "Failed to set dataset lock",
                           "dataset": dataset_name,
                           "query": lock_qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def release(self, dataset_name):
        """
        Releases lock on a dataset.

        @type dataset_name: str
        @param dataset_name:

        @rtype: None
        @return: None
        """
        release_qry = '''
                         update thrive_dataset_lock
                         set locked = FALSE
                         where dataset_name='%s';
                      ''' % dataset_name
        try:
            self.execute(release_qry)
        except Exception as ex:
            logkv(logger, {"msg": "Failed to release dataset lock",
                           "dataset": dataset_name,
                           "query": release_qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def get_lock_status(self, dataset_name):
        """
        Queries lock status of a dataset.

        @type dataset_name: str
        @param dataset_name:

        @rtype: tuple
        @return: Lock status and lock release attempts on the dataset
        """
        qry = '''
                  select locked, release_attempts
                  from thrive_dataset_lock
                  where dataset_name = '%s';
              ''' % dataset_name
        try:
            obj = self.execute_return(qry)
            return obj[0][0], obj[0][1]
        except IndexError as ie:
            logkv(logger, {"msg": "Query yielded zero results. Check if the dataset is setup.",
                           "dataset": dataset_name,
                           "query": qry,
                           "error": ie}, "error")
            raise MetadataManagerException()
        except Exception as ex:
            logkv(logger, {"msg": "Failed to query lock status",
                           "dataset": dataset_name,
                           "query": qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def increment_release_attempt(self, dataset_name):
        """
        Increments release attempt

        @type dataset_name: str
        @param dataset_name: Dataset name

        @rtype: None
        @return: None
        """
        qry = '''
                 update thrive_dataset_lock
                 set release_attempts = release_attempts + 1
                 where dataset_name = '%s';
              ''' % dataset_name

        try:
            self.execute(qry)
        except Exception as ex:
            logkv(logger, {"msg": "Failed to increment release attempts",
                           "dataset": dataset_name,
                           "query": qry,
                           "error": ex}, "error")
            raise MetadataManagerException()

    def delete(self, dataset, mdcolname=None, mdcolvalue=None):
        """
        Deletes row in 'thrive_load_metadata' where 'mdcolname' has value 'mdcolvalue'

        @type dataset: str
        @param dataset: Dataset for which the metadata row is to be deleted

        @type mdcolname: str
        @param mdcolname: Name of the column which to be matched

        @type mdcolvalue: str
        @param mdcolvalue: Value of the column that is matched

        @rtype: None
        @return: None
        """
        delqry = '''
                    delete from thrive_load_metadata
                    where dataset_name = '%s'
                    and %s = '%s';
                 ''' % (dataset, mdcolname, mdcolvalue)
        try:
            self.execute(delqry)
            logkv(logger, {"msg": "Removed partition information from metadata",
                           "dataset_name": dataset,
                           "colname": mdcolname,
                           "colvalue": mdcolvalue}, "info")
        except Exception:
            logkv(logger, {"msg": "Error removing partition information from metadata",
                           "dataset_name": dataset,
                           "colname": mdcolname,
                           "colvalue": mdcolvalue,
                           "query": delqry}, "error")
            raise MetadataManagerException()

    def close(self):
        self.connection.close()
