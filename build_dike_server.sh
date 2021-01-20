#!/bin/bash


set -e               # exit on error

pushd "$(dirname "$0")" # connect to root

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

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
mkdir -p ${ROOT_DIR}/build/.m2
mkdir -p ${ROOT_DIR}/build/.gnupg
mkdir -p ${ROOT_DIR}/build/dikeHDFS
mkdir -p ${ROOT_DIR}/opt/volume

#CMD="BUILD_TYPE=Release && cmake -D CMAKE_BUILD_TYPE=${BUILD_TYPE} -S . -B build/dikeHDFS/${BUILD_TYPE} && cmake --build ./build/dikeHDFS/${BUILD_TYPE}"

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
  -v "${ROOT_DIR}/external/hadoop:${DOCKER_HOME_DIR}/hadoop" \
  -v "${ROOT_DIR}/external/poco:${DOCKER_HOME_DIR}/dikeHDFS/external/poco" \
  -v "${ROOT_DIR}/server:${DOCKER_HOME_DIR}/server" \
  -v "${ROOT_DIR}/dikeHDFS:${DOCKER_HOME_DIR}/dikeHDFS" \
  -v "${ROOT_DIR}/config:${DOCKER_HOME_DIR}/config" \
  -v "${ROOT_DIR}/opt/volume:/opt/volume" \
  -w "${DOCKER_HOME_DIR}/dikeHDFS" \
  -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
  -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
  -v "${ROOT_DIR}/build/dikeHDFS:${DOCKER_HOME_DIR}/dikeHDFS/build" \
  -u "${USER_ID}" \
  "dike-hdfs-build-${USER_NAME}" ${CMD}

popd


