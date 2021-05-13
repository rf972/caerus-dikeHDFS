cmake_minimum_required(VERSION 3.0.0)
project(dikeS3 VERSION 0.1.0)


add_library(libdikeS3 STATIC 
    SelectObjectContent.cpp
)

include_directories(
    .
    ../src
    ${CMAKE_SOURCE_DIR}/external/poco/Foundation/include 
    ${CMAKE_SOURCE_DIR}/external/poco/Util/include
    ${CMAKE_SOURCE_DIR}/external/poco/Net/include
    ${CMAKE_SOURCE_DIR}/external/poco/XML/include
    ${CMAKE_SOURCE_DIR}/external/aws-c-event-stream/include
    ${CMAKE_SOURCE_DIR}/external/aws-c-common/include
    ${CMAKE_SOURCE_DIR}/external/aws-c-common/build/generated/include
    ${CMAKE_SOURCE_DIR}/external/aws-checksums/include
)

target_compile_options(libdikeS3 PRIVATE 

)

#    "-lpthread -lm"
#    "-L${CMAKE_SOURCE_DIR}/external/build-aws/lib -laws-c-event-stream  -laws-checksums -laws-c-common"
