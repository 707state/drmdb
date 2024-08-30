#pragma once
namespace raft {
enum log_val_type { app_log = 1, conf = 2, cluster_server = 3, log_pack = 4, snp_sync_req = 5, custom = 999 };
}
