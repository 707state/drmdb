file(GLOB_RECURSE SOURCES *.cpp)

add_library(record STATIC ${SOURCES})

target_link_libraries(record)
