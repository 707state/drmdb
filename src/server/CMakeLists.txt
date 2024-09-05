file(GLOB_RECURSE SOURCES *.cpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include_directories(${Boost_INCLUDE_DIRS})

add_library(server STATIC ${SOURCES})
link_directories(${BOOST_LIBRARY_DIRS})
target_link_libraries(server raft storage parser)
