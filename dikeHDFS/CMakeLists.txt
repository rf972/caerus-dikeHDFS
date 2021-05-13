cmake_minimum_required(VERSION 3.10)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries")

# add_subdirectory(external/aws-c-common)
# https://github.com/awslabs/aws-c-event-stream/issues/21
# add_subdirectory(external/aws-checksums)
# add_subdirectory(external/aws-c-event-stream)

add_subdirectory(external/poco)
add_subdirectory(dikeSQL)
add_subdirectory(dikeS3)

include_directories(dikeSQL dikeS3)

project(dikeHDFS)
add_executable(dikeHDFS src/dikeHDFS.cpp)

target_include_directories(dikeHDFS PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/external/poco/Foundation/include 
    ${CMAKE_SOURCE_DIR}/external/poco/Util/include
    ${CMAKE_SOURCE_DIR}/external/poco/Net/include 
    ${CMAKE_SOURCE_DIR}/external/build-aws/include
    )

target_link_libraries(dikeHDFS PUBLIC 
    -static-libgcc -static-libstdc++
    Poco::Foundation
    Poco::Util
    Poco::Net
    libdikeSQL
    libdikeS3
    "-lpthread -lm"
    "-L${CMAKE_SOURCE_DIR}/../build/Release/lib -laws-c-event-stream  -laws-checksums -laws-c-common -laws-c-io -laws-c-cal -ls2n -lssl -lcrypto -laws-c-common"
    )
