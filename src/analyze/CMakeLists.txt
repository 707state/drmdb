file(GLOB_RECURSE SOURCES *.cpp)

add_library(analyze STATIC ${SOURCES})
target_link_libraries(analyze parser system)
