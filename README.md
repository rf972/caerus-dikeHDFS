setup
=====

```bash
git clone https://github.com/peterpuhov-github/dikeHDFS.git
git submodule init
git submodule update --recursive
# docker network create dike-net

cd dikeHDFS/docker
./build-docker.sh
cd ..

./build_server.sh
./start_server.sh

# In separate window
./run_init_tpch.sh

# Usage example can be found in
# client/dikeclient/src/main/java/org/dike/hdfs/DikeClient.java
# Important lines are:
# Configuration conf = new Configuration();
# Path hdfsCoreSitePath = new Path("~/config/core-site.xml");
# Path hdfsHDFSSitePath = new Path("~/config/hdfs-site.xml");
 

```

