#!/bin/bash

BUILD_TYPE=Debug

if [ ! -d external/build-aws ]; then
  cd  external
  ./build_aws.sh
  cd ..
fi

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -S . -B build/${BUILD_TYPE}
cmake --build ./build/${BUILD_TYPE}

