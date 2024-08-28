file(GLOB_RECURSE SOURCES *.cpp)

add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner)
