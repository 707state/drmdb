#pragma once
#include "common/defs.h"
#include "index/ix_index_handle.h"
#include "index/ix_manager.h"
#include "record/rm_manager.h"
#include "record/rm_scan.h"
#include <string>
#include <utility>
class Context;

struct ColDef {
    std::string name; // Column name
    ColType type;     // Type of column
    int len;          // Length of column
};
class SmManager {
public:
    DbMeta db_; // 当前打开的数据库的元数据
    boost::unordered_map<std::string, std::shared_ptr<RmFileHandle>>
        fhs_; // file name -> record file handle, 当前数据库中每张表的数据文件
    boost::unordered_map<std::string, std::shared_ptr<IxIndexHandle>>
        ihs_; // file name -> index file handle, 当前数据库中每个索引的文件
    bool enable_output_ = true; // 管理是否允许输出到output.txt中, true为允许输出,
                                // false为不允许输出, 即执行结果不会出现在output.txt中
private:
    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<BufferPoolManager> buffer_pool_manager_;
    std::shared_ptr<RmManager> rm_manager_;
    std::shared_ptr<IxManager> ix_manager_;

public:
    SmManager(std::shared_ptr<DiskManager> disk_manager,
              std::shared_ptr<BufferPoolManager> buffer_pool_manager,
              std::shared_ptr<RmManager> rm_manager,
              std::shared_ptr<IxManager> ix_manager)
        : disk_manager_(std::move(disk_manager))
        , buffer_pool_manager_(std::move(buffer_pool_manager))
        , rm_manager_(std::move(rm_manager))
        , ix_manager_(std::move(ix_manager)) {}

    ~SmManager() {}

    auto get_bpm() -> std::shared_ptr<BufferPoolManager> { return buffer_pool_manager_; }

    std::shared_ptr<RmManager> get_rm_manager() { return rm_manager_; }

    auto get_ix_manager() -> std::shared_ptr<IxManager> { return ix_manager_; }

    auto get_disk_manager() -> std::shared_ptr<DiskManager> { return disk_manager_; }

    auto is_dir(const std::string& db_name) -> bool;

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context* context);

    void desc_table(const std::string& tab_name, Context* context);

    void create_table(const std::string& tab_name,
                      const std::vector<ColDef>& col_defs,
                      Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    void create_index(const std::string& tab_name,
                      const std::vector<std::string>& col_names,
                      Context* context);

    void show_index(const std::string& tab_name, Context* context);

    void drop_index(const std::string& tab_name,
                    const std::vector<std::string>& col_names,
                    Context* context);

    void drop_index(const std::string& tab_name,
                    const std::vector<ColMeta>& col_names,
                    Context* context);

    void reset_index(const std::string& tab_name,
                     const std::vector<ColMeta>& col_names,
                     Context* context);
};
