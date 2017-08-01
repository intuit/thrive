#!/bin/bash

ROOT_DIR="$(dirname "$PWD")"

cd docker/
docker build --no-cache -t thrive-cloudera .
docker run --name some-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=thrive-metadata -e MYSQL_USER=thrive -e MYSQL_PASSWORD=test123 -d mysql:5.6
docker run --hostname=quickstart.cloudera --privileged=true -t -i -v $ROOT_DIR:/thrive --link some-mysql:mysql thrive-cloudera /usr/bin/docker-quickstart

