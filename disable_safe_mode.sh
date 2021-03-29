#!/usr/bin/env bash

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

#If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

HADOOP_PATH=/opt/hadoop/hadoop-${HADOOP_VERSION}

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/scripts:${DOCKER_HOME_DIR}/scripts" \
  -v "${DATA_DIR}:/data" \
  -v "${ROOT_DIR}/hadoop/etc/hadoop/core-site.xml:${HADOOP_PATH}/etc/hadoop/core-site.xml" \
  -v "${ROOT_DIR}/hadoop/etc/hadoop/hdfs-site.xml:${HADOOP_PATH}/etc/hadoop/hdfs-site.xml" \
  -w "${HADOOP_PATH}" \
  -u "${USER_ID}" \
  --network dike-net \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" "~/scripts/disable_safe_mode.sh"

