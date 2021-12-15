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

set -e # exit on error
source ./config.sh
pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Can be used to transfer data to HDFS
mkdir -p ${ROOT_DIR}/data

CMD="/pydike/build_venv.sh"
RUNNING_MODE="interactive"
DOCKER_IT="-i -t"

if [[ "$1" == "-d" ]]; then
  DOCKER_NAME="pydike_build_debug"
  shift
  CMD="$*"
  DOCKER_RUN="docker run --rm=true ${DOCKER_IT} --name pydike-build-venv-debug \
    --network dike-net \
    --mount type=bind,source="$(pwd)"/pydike,target=/pydike \
    --entrypoint /bin/bash -w /pydike \
    -u ${USER_ID} \
    ${DIKE_DOCKER}-${USER_NAME} ${CMD}"
else
  DOCKER_RUN="docker run --rm=true ${DOCKER_IT} --name pydike-build-venv \
    --network dike-net \
    --mount type=bind,source="$(pwd)"/pydike,target=/pydike \
    --entrypoint /bin/bash -w /pydike \
    -u ${USER_ID} \
    ${DIKE_DOCKER}-${USER_NAME} ${CMD}"
fi
eval "${DOCKER_RUN}"
