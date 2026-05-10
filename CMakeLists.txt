cmake_minimum_required(VERSION 3.16)
project(mTorrentServer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(server)
add_subdirectory(storage_node)
