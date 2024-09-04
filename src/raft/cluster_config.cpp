#include "raft/cluster_config.h"
#include <vector>
namespace raft {
ptr<buffer> cluster_config::serialize() const {
    size_t sz = 2 * size_ulong + size_int + size_byte;
    std::vector<ptr<buffer>> srv_buffs;
    for (cluster_config::const_srv_iter it = servers_.begin(); it != servers_.end();
         ++it) {
        ptr<buffer> buf = (*it)->serialize();
        srv_buffs.push_back(buf);
        sz += buf->size();
    }
    // For aux string.
    sz += size_int;
    sz += user_ctx_.size();

    ptr<buffer> result = buffer::alloc(sz);
    result->put(log_idx_);
    result->put(prev_log_idx_);
    result->put(static_cast<byte>(async_replication_ ? 1 : 0));
    result->put((byte*)user_ctx_.data(), user_ctx_.size());
    result->put(static_cast<int32>(servers_.size()));
    for (size_t i = 0; i < srv_buffs.size(); ++i) {
        result->put(*srv_buffs[i]);
    }

    result->pos(0);
    return result;
}

ptr<cluster_config> cluster_config::deserialize(buffer& buf) {
    buffer_serializer bs(buf);
    return deserialize(bs);
}

ptr<cluster_config> cluster_config::deserialize(buffer_serializer& bs) {
    ulong log_idx = bs.get_u64();
    ulong prev_log_idx = bs.get_u64();

    byte ec_byte = bs.get_u8();
    bool ec = ec_byte ? true : false;

    size_t ctx_len = 0;
    const byte* ctx_data = static_cast<const byte*>(bs.get_bytes(ctx_len));
    std::string user_ctx = std::string(reinterpret_cast<const char*>(ctx_data), ctx_len);

    int32 cnt = bs.get_i32();
    ptr<cluster_config> conf = new_ptr<cluster_config>(log_idx, prev_log_idx, ec);
    while (cnt-- > 0) {
        conf->get_servers().push_back(srv_config::deserialize(bs));
    }

    conf->set_user_ctx(user_ctx);

    return conf;
}

} // namespace raft
