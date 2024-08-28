file(GLOB_RECURSE SOURCES *.cpp)
add_library(system STATIC ${SOURCES})
target_link_libraries(system index record parser)
