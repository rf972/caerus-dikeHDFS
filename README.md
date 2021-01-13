setup
=====

```bash
git clone https://github.com/peterpuhov-github/dikeHDFS.git
cd dikeHDFS
git submodule init
git submodule update --recursive
# docker network create dike-net

cd docker
./build-docker.sh
cd ..

./build_hdfs_server.sh ./build.sh
./build_dike_server.sh
./start_server.sh

# In separate window
# Make sure that your path is correct
DATA=../dike/minio/data/
./run_init_tpch.sh ${DATA}

# Usage example can be found in
# client/dikeclient/src/main/java/org/dike/hdfs/DikeClient.java
# Important lines are:
# Configuration conf = new Configuration();
# Path hdfsCoreSitePath = new Path("~/config/core-site.xml");
# Path hdfsHDFSSitePath = new Path("~/config/hdfs-site.xml");
 

```

