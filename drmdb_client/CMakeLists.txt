find_package(Threads REQUIRED)

add_executable(drmdb_client main.cpp)

target_link_libraries(drmdb_client pthread readline)
