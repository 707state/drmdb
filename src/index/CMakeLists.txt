file(GLOB_RECURSE SOURCES *.cpp)
add_library(index STATIC ${SOURCES})
target_link_libraries(index storage)
