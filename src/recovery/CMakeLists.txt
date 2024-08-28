file(GLOB_RECURSE SOURCES *.cpp)
add_library(recovery STATIC ${SOURCES})
target_link_libraries(recovery system)
