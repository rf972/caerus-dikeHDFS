#!/bin/bash
set -e

docker/build.sh || (echo "*** dikeHDFS hadoop/docker/build.sh failed with $?" ; exit 1)
