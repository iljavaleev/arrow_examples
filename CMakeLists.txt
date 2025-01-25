cmake_minimum_required(VERSION 3.5)

project(arrow)

set(CMAKE_CXX_COMPILER "/usr/bin/g++")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ArrowDataset REQUIRED)


include_directories(/opt/homebrew/include)
add_executable(arrow main.cc)
target_link_libraries(arrow PRIVATE Arrow::arrow_shared Parquet::parquet_shared ArrowDataset::arrow_dataset_shared Threads::Threads)


 