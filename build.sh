#!/bin/bash
set -e

git submodule init
git submodule update --recursive --progress

#cd docker
#./build-docker.sh || (echo "*** dikeHDFS build-docker failed with $?" ; exit 1)
#cd ..

hadoop/docker/build.sh || (echo "*** hadoop/docker/build.sh failed with $?" ; exit 1)
#./build_hdfs_server.sh ./build.sh || (echo "*** hdfs server build failed with $?" ; exit 1)
./build_dike_server.sh || (echo "*** dike server build failed with $?" ; exit 1)
#./build_ndp_client.sh || (echo "*** ndp client build failed with $?" ; exit 1)


