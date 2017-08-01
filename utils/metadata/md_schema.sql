-- # Copyright 2016 Intuit
-- #
-- # Licensed under the Apache License, Version 2.0 (the "License");
-- # you may not use this file except in compliance with the License.
-- # You may obtain a copy of the License at
-- #
-- #     http://www.apache.org/licenses/LICENSE-2.0
-- #
-- # Unless required by applicable law or agreed to in writing, software
-- # distributed under the License is distributed on an "AS IS" BASIS,
-- # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- # See the License for the specific language governing permissions and
-- # limitations under the License.

-- Run this file from MySQL shell as
-- "source md_schema.sql;"

drop table if exists thrive_setup;

create table thrive_setup (
  dataset_id integer primary key auto_increment,
  dataset_name varchar(500),
  hive_db varchar(500) not null,
  hive_table varchar(500) not null,
  hive_ddl varchar(500) not null,
  vertica_db varchar(500) not null,
  vertica_schema varchar(500) not null,
  vertica_table varchar(500) not null,
  vertica_ddl varchar(500) not null,
  mapper varchar(500) not null,
  self_onboarding enum('yes', 'no')
);

drop table if exists thrive_load_metadata;

create table thrive_load_metadata (
  load_id varchar(40),
  load_type enum('scheduled', 'replay'),
  dataset_name varchar(500),
  hive_db varchar(500) not null,
  hive_table varchar(500) not null,
  hive_start_ts timestamp null,
  hive_end_ts timestamp null,
  last_load_folder varchar (500),
  hive_last_partition varchar(500),
  hive_rows_loaded bigint default null,
  hadoop_records_processed bigint default null,
  vertica_db varchar(500),
  vertica_schema varchar(500),
  vertica_table varchar(500),
  vertica_start_ts timestamp null,
  vertica_end_ts timestamp null,
  vertica_last_partition varchar(500),
  vertica_rows_loaded bigint default null,
  status varchar(10) default null,
  PRIMARY KEY (load_id, hive_last_partition)
);

drop table if exists thrive_dataset_lock;

create table thrive_dataset_lock (
dataset_name varchar(500),
locked bool default FALSE,
release_attempts int default 0
);

