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

git submodule init
git submodule update --recursive --progress

source ./config.sh

./docker/build.sh || (echo "*** hadoop/docker/build.sh failed with $?" ; exit 1)
./build_dike_server.sh || (echo "*** dike server build failed with $?" ; exit 1)
./build_ndp_client.sh || (echo "*** ndp client build failed with $?" ; exit 1)
