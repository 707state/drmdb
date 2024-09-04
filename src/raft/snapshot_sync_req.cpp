#include "snapshot.h"
#include "utils.h"
#include <raft/snapshot_sync_req.h>
namespace raft {
ptr<snapshot_sync_req> snapshot_sync_req::deserialize(buffer& buf) {
    buffer_serializer bs(buf);
    return deserialize(bs);
}

ptr<snapshot_sync_req> snapshot_sync_req::deserialize(buffer_serializer& bs) {
    ptr<snapshot> snp(snapshot::deserialize(bs));
    ulong offset = bs.get_u64();
    bool done = bs.get_u8() == 1;
    byte* src = static_cast<byte*>(bs.data());
    ptr<buffer> b;
    if (bs.pos() < bs.size()) {
        size_t sz = bs.size() - bs.pos();
        b = buffer::alloc(sz);
        ::memcpy(b->data(), src, sz);
    } else {
        b = buffer::alloc(0);
    }

    return new_ptr<snapshot_sync_req>(snp, offset, b, done);
}

ptr<buffer> snapshot_sync_req::serialize() {
    ptr<buffer> snp_buf = snapshot_->serialize();
    ptr<buffer> buf = buffer::alloc(snp_buf->size() + size_ulong + size_byte
                                    + (data_->size() - data_->pos()));
    buf->put(*snp_buf);
    buf->put(offset_);
    buf->put(done_ ? static_cast<byte>(1) : static_cast<byte>(0));
    buf->put(*data_);
    buf->pos(0);
    return buf;
}
} // namespace raft
