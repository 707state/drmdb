#pragma once
#include <raft/utils.h>
#include <string>
namespace raft {

enum srv_role { follower = 0x1, candidate = 0x2, leader = 0x3 };

static std::string ATTR_UNUSED srv_role_to_string(srv_role _role) {
  switch (_role) {
  case follower:
    return "follower";
  case candidate:
    return "candidate";
  case leader:
    return "leader";
  default:
    return "UNKNOWN";
  }
  return "UNKNOWN";
}

} // namespace raft
