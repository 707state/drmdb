
#include "snapshot.hxx"

#include "cluster_config.hxx"

namespace nuraft {

ptr<snapshot> snapshot::deserialize(buffer& buf) {
    buffer_serializer bs(buf);
    return deserialize(bs);
}

ptr<snapshot> snapshot::deserialize(buffer_serializer& bs) {
    type snp_type = static_cast<type>(bs.get_u8());
    ulong last_log_idx = bs.get_u64();
    ulong last_log_term = bs.get_u64();
    ulong size = bs.get_u64();
    ptr<cluster_config> conf(cluster_config::deserialize(bs));
    return new_ptr<snapshot>(last_log_idx, last_log_term, conf, size, snp_type);
}

ptr<buffer> snapshot::serialize() {
    ptr<buffer> conf_buf = last_config_->serialize();
    ptr<buffer> buf = buffer::alloc(conf_buf->size() + sz_ulong * 3 + sz_byte);
    buf->put((byte)type_);
    buf->put(last_log_idx_);
    buf->put(last_log_term_);
    buf->put(size_);
    buf->put(*conf_buf);
    buf->pos(0);
    return buf;
}

} // namespace nuraft
