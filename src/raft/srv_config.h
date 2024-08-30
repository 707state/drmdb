#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/buffer_serializer.h"
#include "raft/ptr.h"
#include "raft/utils.h"
#include <string>
namespace raft {
class srv_config {
public:
  const static int32 INIT_PRIORITY = 1;

private:
  int32 id_;             // 当前服务器的id
  int32 dc_id_;          // 数据中心id, 没有则为0
  std::string endpoint_; // address+port
  std::string aux_;      // 不可以存放'\0'
  bool learner_;         // 如果当前节点为学习者则为true
  int32 priority_;       // 当前节点的优先级
public:
  srv_config(int32 id, int32 dc_id, std::string endpoint, std::string aux, bool learner, int32 priority)
      : id_(id), dc_id_(dc_id), endpoint_(std::move(endpoint)), aux_(std::move(aux)), learner_(learner),
        priority_(priority) {}
  __nocopy__(srv_config);

public:
  static ptr<srv_config> deserialize(buffer &buf);
  static ptr<srv_config> deserialize(buffer_serializer &bs);
  int32 get_id() const { return id_; }
  int32 get_dc_id() const { return dc_id_; }
  const std::string &get_endpoint() const { return endpoint_; }
  const std::string &get_aux() const { return aux_; }
  bool is_learner() const { return learner_; }
  int32 get_priority() const { return priority_; }
  void set_priority(const int32 new_val) { priority_ = new_val; }
  ptr<buffer> serialize() const;
};
} // namespace raft
