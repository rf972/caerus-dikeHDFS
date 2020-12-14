#!/usr/bin/env bash

bin/hdfs dfs -mkdir /tpch-test

echo "Prepare to copy ..."
ls /data/tpch-test

bin/hdfs dfs -put /data/tpch-test/* /tpch-test
echo "Done"