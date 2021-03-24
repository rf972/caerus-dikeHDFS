#!/usr/bin/env bash

set -e               # exit on error
source config.sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <DATA_DIR>"
    exit 1
fi

DATA_DIR="$(cd "$1"; pwd)"

if [ ! -d $DATA_DIR ]; then
  echo "$DATA_DIR does NOT exist"
  exit 1
fi

echo "Mounting $DATA_DIR as /data"

cd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
DOCKER_DIR=docker
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

#If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

# bin/hdfs dfs -put /data/lineitem.tbl /lineitem.tbl

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/external/hadoop:${DOCKER_HOME_DIR}/hadoop" \
  -v "${ROOT_DIR}/server:${DOCKER_HOME_DIR}/server" \
  -v "${ROOT_DIR}/scripts:${DOCKER_HOME_DIR}/scripts" \
  -v "${DATA_DIR}:/data" \
  -v "${ROOT_DIR}/opt/volume:/opt/volume" \
  -w "${DOCKER_HOME_DIR}/server/hadoop/hadoop" \
  -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
  -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
  -u "${USER_ID}" \
  --network dike-net \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" "~/scripts/init_tpch.sh"

