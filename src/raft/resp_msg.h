#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/msg_base.h"
#include "raft/ptr.h"
#include <sys/types.h>
namespace raft {
    class resp_msg:public msg_base{

        private:
        ulong next_idx_;//下一个需要同步的日志索引
        int64 next_batch_size_hint_in_bytes_;//下一批数据的大小，可以优化批处理
        bool accepted_;
        ptr<buffer>
    };
}
