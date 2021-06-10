#!/bin/bash

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

rm -f /opt/volume/status/HADOOP_STATE

if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    "${HADOOP_HOME}/bin/hdfs" namenode -format
fi

echo "Starting Name Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start namenode
echo "Starting Data Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start datanode

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
export CLASSPATH=$(bin/hadoop classpath) 

sleep 1
/server/dikeHDFS &

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi