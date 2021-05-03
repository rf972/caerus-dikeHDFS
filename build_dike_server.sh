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
source ./config.sh

pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

#If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

mkdir -p ${ROOT_DIR}/build/dikeHDFS

BUILD_TYPE=Release

if [ "$#" -ge 1 ] && [ $1 = "Debug" ]; then
  BUILD_TYPE=Debug
fi

CMD="                                              \
mkdir -p build/${BUILD_TYPE}                    && \
cd build/${BUILD_TYPE}                          && \
cmake -D CMAKE_BUILD_TYPE=${BUILD_TYPE} ../..   && \
make                                            && \
cd ../../                                          \
"

if [ "$#" -ge 1 ] && [ $1 = "-d" ]; then
  echo "Entering debug mode"
  echo ${CMD}
  CMD="/bin/bash"
fi

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/external/poco:${DOCKER_HOME_DIR}/dikeHDFS/external/poco" \
  -v "${ROOT_DIR}/dikeHDFS:${DOCKER_HOME_DIR}/dikeHDFS" \
  -w "${DOCKER_HOME_DIR}/dikeHDFS" \
  -v "${ROOT_DIR}/build/dikeHDFS:${DOCKER_HOME_DIR}/dikeHDFS/build" \
  -u "${USER_ID}" \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" ${CMD}

cp ${ROOT_DIR}/build/dikeHDFS/${BUILD_TYPE}/dikeHDFS ${ROOT_DIR}/server/
popd


