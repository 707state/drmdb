file(GLOB_RECURSE SOURCES *.cpp)

add_library(optimizer STATIC ${SOURCES})
