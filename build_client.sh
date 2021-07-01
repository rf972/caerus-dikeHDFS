#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e               # exit on error

pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
source ${ROOT_DIR}/config.sh

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
mkdir -p ${ROOT_DIR}/build/.m2
mkdir -p ${ROOT_DIR}/build/.gnupg

# Creation of example
# CMD="mvn -B archetype:generate -DarchetypeGroupId=org.apache.maven.archetypes -DgroupId=org.dike-hdfs -DartifactId=dikeclient"
#
# Compilation
# CMD="mvn package"
#
# Test
# //java -classpath target/dikeclient-1.0.jar org.dike.hdfs.DikeClient
# java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt
#
# //java -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=y -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt
# java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt

# export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"

CMD="/bin/bash"

docker run --rm=true -it \
  -v "${ROOT_DIR}/data:/data" \
  -v "${ROOT_DIR}/client:${DOCKER_HOME_DIR}/client" \
  -v "${ROOT_DIR}/config:${DOCKER_HOME_DIR}/config" \
  -w "${DOCKER_HOME_DIR}/client" \
  -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
  -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
  -e "AWS_ACCESS_KEY_ID=${USER_NAME}" \
  -e "AWS_SECRET_ACCESS_KEY=admin123" \
  -e "AWS_EC2_METADATA_DISABLED=true" \
  -u "${USER_ID}" \
  --network dike-net \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" ${CMD}

popd

# -p 8000:8000 \