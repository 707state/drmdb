
#ifndef _SNAPSHOT_HXX_
#define _SNAPSHOT_HXX_

#include "basic_types.hxx"
#include "buffer.hxx"
#include "buffer_serializer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"

namespace nuraft {

class cluster_config;
class snapshot {
public:
    enum type : uint8_t {
        // Offset-based file-style snapshot (deprecated).
        raw_binary = 0x1,

        // Object-based logical snapshot.
        logical_object = 0x2,
    };

    snapshot(ulong last_log_idx,
             ulong last_log_term,
             const ptr<cluster_config>& last_config,
             ulong size = 0,
             type _type = logical_object)
        : last_log_idx_(last_log_idx)
        , last_log_term_(last_log_term)
        , size_(size)
        , last_config_(last_config)
        , type_(_type)
        {}

    __nocopy__(snapshot);

public:
    ulong get_last_log_idx() const {
        return last_log_idx_;
    }

    ulong get_last_log_term() const {
        return last_log_term_;
    }

    ulong size() const {
        return size_;
    }

    void set_size(ulong size) {
        size_ = size;
    }

    type get_type() const {
        return type_;
    }

    void set_type(type src) {
        type_ = src;
    }

    const ptr<cluster_config>& get_last_config() const {
        return last_config_;
    }

    static ptr<snapshot> deserialize(buffer& buf);

    static ptr<snapshot> deserialize(buffer_serializer& bs);

    ptr<buffer> serialize();

private:
    ulong last_log_idx_;
    ulong last_log_term_;
    ulong size_;
    ptr<cluster_config> last_config_;
    type type_;
};

}

#endif
