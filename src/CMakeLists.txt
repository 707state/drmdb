include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${Boost_INCLUDE_DIRS})

link_directories(${BOOST_LIBRARY_DIRS})

add_subdirectory(common)
add_subdirectory(parser)
add_subdirectory(raft)
add_subdirectory(storage)
add_subdirectory(system)
add_subdirectory(transaction)
add_subdirectory(record)
add_subdirectory(execution)
add_subdirectory(index)
add_subdirectory(optimizer)
add_subdirectory(recovery)
add_subdirectory(analyze)
add_subdirectory(server)
add_executable(drmdb server.cpp)
target_link_libraries(
  drmdb
  raft
  storage
  system
  transaction
  record
  execution
  analyze
  index
  recovery
  record
  optimizer
  server
  OpenSSL::SSL
  OpenSSL::Crypto
  Boost::thread
  fmt::fmt
  Boost::system)
