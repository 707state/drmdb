#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/buffer_serializer.h"
#include "raft/srv_config.h"
#include "raft/utils.h"
#include <cstddef>
#include <list>
#include <raft/ptr.h>
#include <string>
namespace raft {
class cluster_config {
public:
  explicit cluster_config(ulong log_idx = 0L, ulong prev_log_idx = 0L, bool async_replication = false)
      : log_idx_(log_idx), prev_log_idx_(prev_log_idx), async_replication_(async_replication) {}
  ~cluster_config() = default;
  __nocopy__(cluster_config);

public:
  using srv_iter = std::list<ptr<srv_config>>::iterator;
  using const_srv_iter = std::list<ptr<srv_config>>::const_iterator;
  static ptr<cluster_config> deserialize(buffer &buf);
  static ptr<cluster_config> deserialize(buffer_serializer &buf);
  ulong get_log_idx() const { return log_idx_; }
  ulong get_prev_log_idx() const { return prev_log_idx_; }
  void set_log_idx(ulong log_idx) {
    prev_log_idx_ = log_idx_;
    log_idx_ = log_idx;
  }
  std::list<ptr<srv_config>> &get_servers() { return servers_; }
  ptr<srv_config> get_server(int id) const {
    for (auto &entry : servers_) {
      const auto &srv = entry;
      if (srv->get_id() == id) {
        return srv;
      }
    }
    return {};
  }
  bool is_async_replication() const { return async_replication_; }
  void set_async_replication(bool flag) { async_replication_ = flag; }
  std::string get_user_ctx() const { return user_ctx_; }
  void set_user_ctx(const std::string &src) { user_ctx_ = src; }
  ptr<buffer> serialize() const;

private:
  ulong log_idx_;          // 当前配置的日志索引
  ulong prev_log_idx_;     // 上一个配置的日志索引
  bool async_replication_; // 设置为异步复制
  std::string user_ctx_;
  std::list<ptr<srv_config>> servers_;
};
} // namespace raft
