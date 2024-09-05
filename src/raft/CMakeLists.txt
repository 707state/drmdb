file(GLOB_RECURSE SOURCES *.cxx)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include_directories(${Boost_INCLUDE_DIRS})

add_library(raft STATIC ${SOURCES})
link_directories(${BOOST_LIBRARY_DIRS})
target_link_libraries(raft)
