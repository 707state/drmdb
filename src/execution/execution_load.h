#pragma once
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
#include "system/sm_manager.h"
#include <memory>
class LoadExecutor : public AbstractExecutor, public ConditionDependedExecutor {
public:
  LoadExecutor(std::shared_ptr<SmManager> sm_manager, const std::string &tab_name, std::vector<Value> values,
               std::shared_ptr<Context> context);

  auto getType() -> std::string override;

  auto Next() -> std::unique_ptr<RmRecord> override;

  auto rid() -> Rid & override { return rid_; };

  static pthread_mutex_t *load_pool_access_lock;

  static std::unordered_map<std::string, pthread_t> load_pool;

private:
  Rid rid_;
  std::string file_path_; // 需加载数据的路径
  RmFileHandle *fh_;      // 表的数据文件句柄

  using load_task_param = struct {
    std::shared_ptr<SmManager> sm_manager;
    std::string tab_name;
    std::shared_ptr<Context> context;
    std::string file_path;
    RmFileHandle *fh;
    int record_size;
  };

  static auto split(std::vector<std::string_view> &split_buf, std::string &origin_str, char delim) -> void;

  static void *load_task(void *param);
};
