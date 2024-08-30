#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>
namespace raft {
struct asio_service_meta_cb_params {
  asio_service_meta_cb_params(int msg_type, int src_id, int dst_id, uint64_t term, uint64_t log_term, uint64_t log_idx,
                              uint64_t commit_idx)
      : msg_type_(msg_type), src_id_(src_id), dst_id_(dst_id), term_(term), log_term_(log_term), log_idx_(log_idx),
        commit_idx_(commit_idx) {}
  // 请求类型
  int msg_type_;
  // 发送请求的Server ID
  int src_id_;
  // 请求发送到的Server ID
  int dst_id_;
  // 任期
  uint64_t term_;
  // 日志条目涉及的任期号
  uint64_t log_term_;
  // 日志索引号
  uint64_t log_idx_;
  // 最后一个commit的日志编号
  uint64_t commit_idx_;
};
// 响应回调函数
using asio_service_custom_resolver_response =
    std::function<void(const std::string &, const std::string &, std::error_code)>;
struct asio_service_options {
  // ASIO worker threads数量
  size_t thread_pool_size_;
  // worker thread start
  std::function<void(uint32_t)> worker_start_;
  // worker thread stop
  std::function<void(uint32_t)> worker_stop_;
  bool enable_ssl_;
  bool skip_verification_;
  // 指定了服务器证书、私钥和根证书文件的路径。当启用 SSL 时，这些文件用于建立安全连接
  std::string server_cert_file_;
  std::string server_key_file_;
  std::string root_vert_file_;
  // 处理RPC Request元数据的回调函数
  std::function<std::string(const asio_service_meta_cb_params &)> write_req_meta_;
  // 读取并验证RPC request元数据，返回false就抛弃请求
  std::function<bool(const asio_service_meta_cb_params &, const std::string &)> read_req_meta_;
  // True则出发read_req_meta_，即便获取的元数据是空的
  bool invoke_req_cb_on_empty_meta_;
  std::function<std::string(const asio_service_meta_cb_params &)> write_resp_meta_; // 写入响应的元数据
  std::function<bool(const asio_service_meta_cb_params &, const std::string &)> read_resp_meta_; // 读取响应的元数据
  bool invoke_resp_cb_on_empty_meta_;                                                            // 同上
  std::function<bool(const std::string &)> verify_sn_; // 验证整数的主题名
  std::function<void(const std::string &, const std::string &, asio_service_custom_resolver_response)>
      custom_resolver_; // 自定义IP地址解析
  bool
      replicate_log_timestamp_; // 如果设置为 true，则每个日志条目将包含它生成时的时间戳，Leader
                                // 会将这些时间戳复制到所有 Follower。Follower
                                // 会看到相同的时间戳，用于保持一致性。启用这个特性时，日志存储实现需要支持恢复这些时间戳。
};
} // namespace raft
