cmake_minimum_required(VERSION 3.10)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries")
add_subdirectory(external/poco)
add_subdirectory(dikeSQL)

include_directories(dikeSQL)

project(dikeHDFS)
add_executable(dikeHDFS src/dikeHDFS.cpp)

target_include_directories(dikeHDFS PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/external/poco/Foundation/include 
    ${CMAKE_SOURCE_DIR}/external/poco/Util/include
    ${CMAKE_SOURCE_DIR}/external/poco/Net/include 
#    ${CMAKE_SOURCE_DIR}/external/build-aws/include
    )

target_link_libraries(dikeHDFS PUBLIC 
#    -static
    -static-libgcc -static-libstdc++
    Poco::Foundation
    Poco::Util
    Poco::Net
    libsqlite
    "-lpthread -lm"
#    "-L${CMAKE_SOURCE_DIR}/external/build-aws/lib -laws-c-event-stream  -laws-checksums -laws-c-common"
    )

add_custom_command(TARGET dikeHDFS POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:dikeHDFS> ${CMAKE_SOURCE_DIR}/../server/
)