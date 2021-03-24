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

source config.sh

cd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
DOCKER_DIR=docker
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

# If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}
CMD="/bin/bash"

if [ "$#" -ge 1 ] && [ $1 = "-d" ]; then
  echo "Entering debug mode"
  echo "bin/hdfs --help"  
  CMD="/bin/bash"
fi

HADOOP_PATH=/opt/hadoop/hadoop-${HADOOP_VERSION}

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/dikeHDFS:${DOCKER_HOME_DIR}/dikeHDFS" \
  -v "${ROOT_DIR}/hadoop/etc/hadoop/core-site.xml:${HADOOP_PATH}/etc/hadoop/core-site.xml" \
  -v "${ROOT_DIR}/hadoop/etc/hadoop/hdfs-site.xml:${HADOOP_PATH}/etc/hadoop/hdfs-site.xml" \
  -w "${HADOOP_PATH}" \
  -u "${USER_ID}" \
  --network dike-net \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" ${CMD}
