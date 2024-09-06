
#ifndef _SNAPSHOT_SYNC_REQ_HXX_
#define _SNAPSHOT_SYNC_REQ_HXX_

#include "buffer.hxx"
#include "buffer_serializer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"
#include "snapshot.hxx"

namespace nuraft {

class snapshot;
class snapshot_sync_req {
public:
    snapshot_sync_req(const ptr<snapshot>& s,
                      ulong offset,
                      const ptr<buffer>& buf,
                      bool done)
        : snapshot_(s), offset_(offset), data_(buf), done_(done) {}

    __nocopy__(snapshot_sync_req);

public:
    static ptr<snapshot_sync_req> deserialize(buffer& buf);

    static ptr<snapshot_sync_req> deserialize(buffer_serializer& bs);

    snapshot& get_snapshot() const {
        return *snapshot_;
    }

    ulong get_offset() const { return offset_; }
    void set_offset(const ulong src) { offset_ = src; }

    buffer& get_data() const { return *data_; }

    bool is_done() const { return done_; }

    ptr<buffer> serialize();
private:
    ptr<snapshot> snapshot_;
    ulong offset_;
    ptr<buffer> data_;
    bool done_;
};

}

#endif //_SNAPSHOT_SYNC_REQ_HXX_
