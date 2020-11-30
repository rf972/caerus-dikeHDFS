#!/bin/bash

set -e               # exit on error

pushd ~/hadoop
mvn package --no-snapshot-updates  -Pdist,native -DskipTests -Dtar

tar -xzf hadoop-dist/target/hadoop-3.4.0-SNAPSHOT.tar.gz --directory ~/server/hadoop

rm ~/server/hadoop/hadoop
ln -s ~/server/hadoop/hadoop-3.4.0-SNAPSHOT ~/server/hadoop/hadoop

# export JAVA_HOME=
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

pushd ~/server/hadoop/hadoop
sed -i '/# export JAVA_HOME=/c\export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' etc/hadoop/hadoop-env.sh
cp ~/config/core-site.xml etc/hadoop/
cp ~/config/hdfs-site.xml etc/hadoop/

mkdir -p /opt/volume/datanode
mkdir -p /opt/volume/namenode

bin/hdfs namenode -format -force
popd
popd