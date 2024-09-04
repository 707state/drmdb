file(GLOB_RECURSE SOURCES *.cpp)
add_library(raft STATIC ${SOURCES})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include_directories(${Boost_INCLUDE_DIRS})

link_directories(${BOOST_LIBRARY_DIRS})
target_link_libraries(raft fmt)
