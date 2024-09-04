#include "raft/asio_service.h"
#include "boost/asio/ssl.hpp"
#include "raft_server_handler.h"
#include "rpc_listener.h"
#include "tracer.h"
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <list>
#include <raft/logger.h>
using namespace boost;
using ssl_socket = asio::ssl::stream<asio::ip::tcp::socket&>;
using ssl_context = asio::ssl::context;

// Note: both req & resp header structures have been modified by Jung-Sang Ahn.
//       They MUST NOT be combined with the original code.

// request header:
//     byte         marker (req = 0x0)  (1),
//     msg_type     type                (1),
//     int32        src                 (4),
//     int32        dst                 (4),
//     ulong        term                (8),
//     ulong        last_log_term       (8),
//     ulong        last_log_idx        (8),
//     ulong        commit_idx          (8),
//     int32        log data size       (4),
//     ulong        flags + CRC32       (8),
//     -------------------------------------
//                  total               (54)
#define RPC_REQ_HEADER_SIZE (4 * 3 + 8 * 5 + 1 * 2)

// response header:
//     byte         marker (resp = 0x1) (1),
//     msg_type     type                (1),
//     int32        src                 (4),
//     int32        dst                 (4),
//     ulong        term                (8),
//     ulong        next_idx            (8),
//     bool         accepted            (1),
//     int32        ctx data dize       (4),
//     ulong        flags + CRC32       (8),
//     -------------------------------------
//                  total               (39)
#define RPC_RESP_HEADER_SIZE (4 * 3 + 8 * 3 + 1 * 3)

#define DATA_SIZE_LEN (4)
#define CRC_FLAGS_LEN (8)

// === RPC Flags =========

// If set, RPC message includes custom meta given by user.
#define INCLUDE_META (0x1)

// If set, RPC message (response) includes additional hints.
#define INCLUDE_HINT (0x2)

// If set, each log entry will contain timestamp.
#define INCLUDE_LOG_TIMESTAMP (0x4)

// =======================
