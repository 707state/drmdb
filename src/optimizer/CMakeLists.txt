file(GLOB_RECURSE SOURCES *.cpp)

add_library(planner STATIC ${SOURCES})
