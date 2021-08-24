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

ROOT_DIR=$(pwd)

BUILD_TYPE=$1

BUILD_PATH=${ROOT_DIR}/../build/${BUILD_TYPE}
EXTERNAL_SRC_PATH=${ROOT_DIR}/external

mkdir -p ${BUILD_PATH}

<< 'DISABLE_AWS_BUILD'
for awslib in aws-c-common aws-checksums aws-lc s2n-tls aws-c-cal aws-c-io aws-c-io aws-c-event-stream
do 
    echo "Building ${awslib}"
    mkdir -p ${EXTERNAL_SRC_PATH}/${awslib}/build 
    pushd ${EXTERNAL_SRC_PATH}/${awslib}/build 
    cmake -D CMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DCMAKE_PREFIX_PATH=${BUILD_PATH} -DCMAKE_INSTALL_PREFIX=${BUILD_PATH} .. 
    make -j 4 && make install 
    popd 
done 
DISABLE_AWS_BUILD

pushd  ${BUILD_PATH}                           
cmake -D CMAKE_BUILD_TYPE=${BUILD_TYPE} ${ROOT_DIR}
make
                                          
