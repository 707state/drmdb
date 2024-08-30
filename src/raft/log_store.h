#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/ptr.h"
#include <cstdint>
#include <raft/log_entry.h>
#include <raft/utils.h>
#include <sys/types.h>
#include <vector>
namespace raft {
/*
log_store 类主要负责管理 Raft 日志条目的存储与读取，具体功能如下：

    日志条目的管理：
        next_slot()：返回下一个可用的日志槽位编号。
        start_index()：返回日志存储的起始索引，最初通常为 1，但在压缩操作后可能大于 1。
        last_entry()：返回存储中最后一个日志条目。
        append()：将新的日志条目追加到存储中。
        write_at()：覆盖指定索引处的日志条目，同时截断该索引后的所有日志条目。

    批量操作：
        end_of_append_batch()：在一批日志条目作为单个 append_entries 请求的一部分写入后调用。
        log_entries()：获取指定范围内的日志条目。
        log_entries_ext()：在返回日志条目的同时，限制返回日志条目的总大小。

    日志条目检索：
        entry_at()：获取指定索引处的日志条目。
        term_at()：获取指定索引处的日志条目的任期号。

    日志打包和应用：
        pack()：将从指定索引开始的日志条目打包编码。
        apply_pack()：将打包的日志条目应用到当前日志存储中。

    日志压缩：
        compact()：通过删除到指定日志索引的所有日志条目来压缩日志存储。

    持久化操作：
        flush()：将所有日志条目同步刷新到底层存储，确保在进程崩溃时日志条目是持久的。
        last_durable_index()：返回最后一个持久化的日志索引。
*/
class log_store {
  __interface_body(log_store);

public:
  virtual ulong next_slot() const = 0;
  virtual ulong start_index() const = 0;
  virtual ptr<log_entry> last_entry() const = 0;
  virtual ulong append(ptr<log_entry> &entry) = 0;
  virtual void write_at(ulong index, ptr<log_entry> &entry) = 0;
  virtual void end_of_append_batch(ulong start, ulong cnt) {}
  virtual ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) = 0;
  virtual ptr<std::vector<ptr<log_entry>>> log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) {
    return log_entries(start, end);
  }
  virtual ptr<log_entry> entry_at(ulong index) = 0;
  virtual ulong term_at(ulong index) = 0;
  virtual ptr<buffer> pack(ulong index, int32 cnt) = 0;
  virtual void apply_pack(ulong index, buffer &pack) = 0;
  virtual bool compact(ulong last_log_index) = 0;
  virtual bool flush() = 0;
  virtual ulong last_durable_index() { return next_slot() + 1; }
};
} // namespace raft
