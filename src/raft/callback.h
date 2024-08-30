#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
namespace raft {
class cb_func {
public:
  enum Type {
    // 从客户端/节点获取请求
    ProcessReq = 1,
    // 从其他节点获取到的请求
    GotAppendEntryRespFromPeer = 2,
    // 追加日志，执行pre-commit
    AppendLogs = 3,
    HeartBeat = 4,
    JoinedCluster = 5,
    BecomeLeader = 6,
    RequestAppendEntries = 7,
    SaveSnapShot = 8,
    NewConfig = 9,
    RemovedFromCluster = 10,
    BecomeFollower = 11,
    BecomeFresh = 12,
    BecomeStale = 13,
    GotAppendEntryReqFromLeader = 14,
    OutOfLogRangeWarning = 15,
    ConnectionOpened = 16,
    ConnectionClosed = 17,
    NewSessionFromLeader = 18,
    StateMachineExecution = 19,
  };
  struct Param {
    explicit Param(int32_t myId = -1, int32_t leaderId = -1, int32_t peerId = -1, void *ctx = nullptr)
        : myId(myId), leaderId(leaderId), peerId(peerId), ctx(ctx) {}
    int32_t myId;
    int32_t leaderId;
    int32_t peerId;
    void *ctx;
  };
  enum ReturnCode {
    Ok = 0,
    ReturnNull = -1,
  };
  struct OutOfLogRangeWarningArgs {
    explicit OutOfLogRangeWarningArgs(uint64_t startIdxOfLeader = 0) : startIdxOfLeader(startIdxOfLeader) {}
    uint64_t startIdxOfLeader;
  };
  struct ConnectionArgs {

    ConnectionArgs(uint64_t sessionId, std::string address, uint32_t port, int32_t srvId, bool isLeader)
        : sessionId(sessionId), address(std::move(address)), port(port), srvId(srvId), isLeader(isLeader) {}
    uint64_t sessionId;
    std::string address;
    uint32_t port;
    int32_t srvId;
    bool isLeader;
  };
  using func_type = std::function<ReturnCode(Type, Param *)>;
  cb_func() : func(nullptr) {}
  explicit cb_func(func_type _func) : func(std::move(_func)) {}
  ReturnCode call(Type type, Param *param) {
    if (func) {
      return func(type, param);
    }
    return Ok;
  }

private:
  func_type func;
};
} // namespace raft
