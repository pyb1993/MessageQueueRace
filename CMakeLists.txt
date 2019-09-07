cmake_minimum_required(VERSION 3.14)
project(QueueStore)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# 这个是必须的
link_directories(/usr/local/lib)

aux_source_directory(. sourceList)
add_executable(QueueStore ${sourceList} Storage.cpp Storage.h Cache.cpp Cache.h BlockingQueue.h Condition.h QueueStore.h IO.cpp IO.h Queue.cpp Queue.h)

target_link_libraries(QueueStore tcmalloc)
