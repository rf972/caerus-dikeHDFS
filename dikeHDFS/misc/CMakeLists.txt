cmake_minimum_required(VERSION 3.10)
#project(solver LANGUAGES CXX)

find_package(OpenMP REQUIRED)
add_executable(omp_test omp_test.cpp)
target_link_libraries(omp_test PRIVATE OpenMP::OpenMP_CXX)