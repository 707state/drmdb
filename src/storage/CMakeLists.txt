include_directories(${CMAKE_CURRENT_SOURCE_DIR})
file(GLOB_RECURSE SOURCES *.cpp)
add_library(storage STATIC ${SOURCES})
include_directories(${Boost_INCLUDE_DIRS})

link_directories(${BOOST_LIBRARY_DIRS})
target_link_libraries(storage Boost::filesystem)
