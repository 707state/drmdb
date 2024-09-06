#include <cstdlib>
#include <fstream>
#include <regex>
#include <string>
#include <vector>

#include "execution_load.h"
#include "record/rm_scan.h"

pthread_mutex_t* LoadExecutor::load_pool_access_lock;
std::unordered_map<std::string, pthread_t> LoadExecutor::load_pool;

LoadExecutor::LoadExecutor(SmManager* sm_manager,
                           const std::string& tab_name,
                           std::vector<Value> values,
                           Context* context) {
    sm_manager_ = sm_manager;
    file_path_ = values.at(0).str_val;
    tab_name_ = tab_name;
    fh_ = sm_manager->fhs_.at(tab_name).get();
    context_ = context;
}

auto LoadExecutor::getType() -> std::string { return "LoadExecutor"; }

auto LoadExecutor::Next() -> std::unique_ptr<RmRecord> {
    pthread_t* thread_id = new pthread_t;
    auto task_param = new load_task_param{.sm_manager = sm_manager_,
                                          .tab_name = tab_name_,
                                          .context = context_,
                                          .file_path = file_path_,
                                          .fh = fh_,
                                          .record_size = fh_->get_file_hdr().record_size};
    pthread_create(thread_id, NULL, load_task, (void*)task_param);
    pthread_mutex_lock(load_pool_access_lock);
    load_pool.insert(std::make_pair(tab_name_, *thread_id));
    pthread_mutex_unlock(load_pool_access_lock);
    printf("[Internal] Table \'%s\' Loading.\n", tab_name_.c_str());
    return nullptr;
}

auto LoadExecutor::split(std::vector<std::string_view>& split_buf,
                         std::string& origin_str,
                         char delim) -> void {
    split_buf.clear();
    int last_index = 0;
    for (int i = 0; i < origin_str.length(); ++i) {
        if (origin_str[i] == delim) {
            split_buf.emplace_back(origin_str.data() + last_index, i - last_index);
            last_index = i + 1;
        }
    }
    split_buf.emplace_back(origin_str.data() + last_index,
                           origin_str.length() - last_index);
}

void* LoadExecutor::load_task(void* param) {
    auto param_ = (load_task_param*)param;
    // 如果有索引就先对源文件排序，排序后直接顺序读入建立索引即可
    auto indexes = param_->sm_manager->db_.get_table(param_->tab_name).indexes;
    if (!indexes.empty()) {
        // 调用sort命令排序，我真是个天才
        std::string sort_prog =
            "LC_ALL=C sort -n -t , --parallel=8 -o " + param_->tab_name + "_sorted.csv";
        // 所有索引都要重建
        for (auto& index: indexes) {
            // 先找到索引列的下标
            auto table_cols = param_->sm_manager->db_.get_table(param_->tab_name).cols;
            std::vector<int> index_cols;
            for (const auto& col: index.cols) {
                for (int i = 0; i < table_cols.size(); ++i) {
                    if (col.name == table_cols[i].name) {
                        index_cols.emplace_back(i + 1);
                    }
                }
            }
            // 然后根据索引列构造sort排序参数
            for (const auto& col: index_cols) {
                sort_prog += " -k ";
                sort_prog += std::to_string(col);
            }
            sort_prog += " ";
            sort_prog.append(param_->file_path);
            // 执行排序
            //      std::cout << sort_prog << std::endl;
            system(sort_prog.c_str());
            // 排序完后顺序读入
            // 需要用到的必要变量
            RmFileHdr rm_file_hdr = param_->fh->get_file_hdr();
            auto tab_cols = param_->sm_manager->db_.get_table(param_->tab_name).cols;
            // 以下是用于转换的缓冲
            const int kPageBufferSize = 1024;

            char* page_buf = new char[kPageBufferSize * PAGE_SIZE];
            // Clear buf
            std::memset(page_buf, 0, kPageBufferSize * PAGE_SIZE);

            char* key_buf = new char[index.col_tot_len];

            std::vector<std::string_view> split_buf;
            split_buf.reserve(tab_cols.size());

            int val_int_buf;
            float val_float_buf;
            std::string val_str_buf;
            DateTime val_datetime_buf;
            uint64_t val_datetime_raw_buf;

            auto index_handle =
                param_->sm_manager->ihs_
                    .at(param_->sm_manager->get_ix_manager()->get_index_name(
                        param_->tab_name, index.cols))
                    .get();

            std::ifstream csv_file_input(param_->tab_name + "_sorted.csv");
            if (!csv_file_input.is_open()) {
                throw UnixError();
            }

            // 用于直接表示Rid，构建物理记录的同时插入索引
            int rm_page_index = 1;
            // 这个是在缓冲中的页的位置，与上面的下标是不同的含义
            int page_buffer_index = 0;
            int rm_slot_index = 0;

            auto this_page_addr = page_buf + page_buffer_index * PAGE_SIZE;
            auto this_bitmap_addr = this_page_addr + sizeof(RmPageHdr);
            auto this_rm_page_header = reinterpret_cast<RmPageHdr*>(this_page_addr);

            std::string line_buf;
            std::getline(csv_file_input, line_buf);
            while (!csv_file_input.eof()) {
                std::getline(csv_file_input, line_buf);
                if (line_buf.empty()) break;

                auto this_record_addr = this_bitmap_addr + rm_file_hdr.bitmap_size
                                        + rm_slot_index * rm_file_hdr.record_size;

                split(split_buf, line_buf, ',');
                for (int i = 0; i < tab_cols.size(); ++i) {
                    auto& col = tab_cols[i];
                    switch (col.type) {
                    case TYPE_INT:
                        val_str_buf = split_buf[i];
                        val_int_buf = std::stoi(val_str_buf);
                        std::memcpy(this_record_addr + col.offset, &val_int_buf, col.len);
                        break;
                    case TYPE_STRING:
                        std::memcpy(this_record_addr + col.offset,
                                    split_buf[i].data(),
                                    split_buf[i].size());
                        break;
                    case TYPE_FLOAT:
                        val_str_buf = split_buf[i];
                        val_float_buf = std::stof(val_str_buf);
                        std::memcpy(
                            this_record_addr + col.offset, &val_float_buf, col.len);
                        break;
                    case TYPE_DATETIME:
                        val_str_buf = split_buf[i];
                        val_datetime_buf.decode_from_string(val_str_buf);
                        val_datetime_raw_buf = val_datetime_buf.encode();
                        std::memcpy(this_record_addr + col.offset,
                                    &val_datetime_raw_buf,
                                    col.len);
                        break;
                    }
                }

                Rid cur_rid = {rm_page_index, rm_slot_index};
                ix_make_key(key_buf, this_record_addr, index);
                index_handle->rebuild_index_from_load(key_buf, cur_rid);

                ++rm_slot_index;

                if (rm_slot_index == rm_file_hdr.num_records_per_page) {
                    this_rm_page_header->num_records = rm_file_hdr.num_records_per_page;
                    std::memset(this_bitmap_addr, 0b11111111, rm_file_hdr.bitmap_size);
                    // 当前页元数据已写入缓冲，更新元数据
                    ++rm_page_index;
                    rm_slot_index = 0;
                    ++page_buffer_index;
                    if (page_buffer_index == kPageBufferSize) {
                        // 页缓冲满了, flush
                        param_->sm_manager->get_disk_manager()->write_full_pages(
                            param_->fh->GetFd(),
                            rm_page_index - kPageBufferSize,
                            page_buf,
                            kPageBufferSize);
                        page_buffer_index = 0;
                        param_->fh->get_file_hdr_ptr()->num_pages += kPageBufferSize;
                    }
                    this_page_addr = page_buf + page_buffer_index * PAGE_SIZE;
                    this_bitmap_addr = this_page_addr + sizeof(RmPageHdr);
                    this_rm_page_header = reinterpret_cast<RmPageHdr*>(this_page_addr);
                }
            }

            if (page_buffer_index != 0 || rm_slot_index != 0) {
                this_rm_page_header->num_records = rm_slot_index;
                Bitmap::init(this_bitmap_addr, rm_file_hdr.bitmap_size);
                for (int i = 0; i < rm_slot_index; ++i) {
                    Bitmap::set(this_bitmap_addr, i);
                }
                param_->sm_manager->get_disk_manager()->write_full_pages(
                    param_->fh->GetFd(),
                    (rm_page_index - 1) / kPageBufferSize * kPageBufferSize + 1,
                    page_buf,
                    page_buffer_index + 1);
                param_->fh->get_file_hdr_ptr()->num_pages += (page_buffer_index + 1);
                if (rm_slot_index != 0) {
                    param_->fh->get_file_hdr_ptr()->first_free_page_no = rm_page_index;
                }
            }

            csv_file_input.close();

            delete[] page_buf;
            delete[] key_buf;
        }
    } else {
        RmFileHdr rm_file_hdr = param_->fh->get_file_hdr();
        // 无索引的，直接转并存储
        auto tab_cols = param_->sm_manager->db_.get_table(param_->tab_name).cols;
        // 以下是用于转换的缓冲
        const int kPageBufferSize = 1024;
        char* page_buf = new char[kPageBufferSize * PAGE_SIZE];
        // Clear buf
        std::memset(page_buf, 0, kPageBufferSize * PAGE_SIZE);

        std::vector<std::string_view> split_buf;
        split_buf.reserve(tab_cols.size());

        int val_int_buf;
        float val_float_buf;
        std::string val_str_buf;
        DateTime val_datetime_buf;
        uint64_t val_datetime_raw_buf;

        std::ifstream csv_file_input(param_->file_path);
        if (!csv_file_input.is_open()) {
            throw UnixError();
        }

        // 用于直接表示Rid，构建物理记录的同时插入索引
        int rm_page_index = 1;
        // 这个是在缓冲中的页的位置，与上面的下标是不同的含义
        int page_buffer_index = 0;
        int rm_slot_index = 0;

        auto this_page_addr = page_buf + page_buffer_index * PAGE_SIZE;
        auto this_bitmap_addr = this_page_addr + sizeof(RmPageHdr);
        auto this_rm_page_header = reinterpret_cast<RmPageHdr*>(this_page_addr);

        std::string line_buf;
        std::getline(csv_file_input, line_buf);
        while (!csv_file_input.eof()) {
            std::getline(csv_file_input, line_buf);
            if (line_buf.empty()) break;

            auto this_record_addr = this_bitmap_addr + rm_file_hdr.bitmap_size
                                    + rm_slot_index * rm_file_hdr.record_size;

            split(split_buf, line_buf, ',');
            for (int i = 0; i < tab_cols.size(); ++i) {
                auto& col = tab_cols[i];
                switch (col.type) {
                case TYPE_INT:
                    val_str_buf = split_buf[i];
                    val_int_buf = std::stoi(val_str_buf);
                    std::memcpy(this_record_addr + col.offset, &val_int_buf, col.len);
                    break;
                case TYPE_STRING:
                    std::memcpy(this_record_addr + col.offset,
                                split_buf[i].data(),
                                split_buf[i].size());
                    break;
                case TYPE_FLOAT:
                    val_str_buf = split_buf[i];
                    val_float_buf = std::stof(val_str_buf);
                    std::memcpy(this_record_addr + col.offset, &val_float_buf, col.len);
                    break;
                case TYPE_DATETIME:
                    val_str_buf = split_buf[i];
                    val_datetime_buf.decode_from_string(val_str_buf);
                    val_datetime_raw_buf = val_datetime_buf.encode();
                    std::memcpy(
                        this_record_addr + col.offset, &val_datetime_raw_buf, col.len);
                    break;
                }
            }

            ++rm_slot_index;

            if (rm_slot_index == rm_file_hdr.num_records_per_page) {
                this_rm_page_header->num_records = rm_file_hdr.num_records_per_page;
                std::memset(this_bitmap_addr, 0b11111111, rm_file_hdr.bitmap_size);
                // 当前页元数据已写入缓冲，更新元数据
                ++rm_page_index;
                rm_slot_index = 0;
                ++page_buffer_index;
                if (page_buffer_index == kPageBufferSize) {
                    // 页缓冲满了, flush
                    param_->sm_manager->get_disk_manager()->write_full_pages(
                        param_->fh->GetFd(),
                        rm_page_index - kPageBufferSize,
                        page_buf,
                        kPageBufferSize);
                    param_->fh->get_file_hdr_ptr()->num_pages += kPageBufferSize;
                    page_buffer_index = 0;
                }
                this_page_addr = page_buf + page_buffer_index * PAGE_SIZE;
                this_bitmap_addr = this_page_addr + sizeof(RmPageHdr);
                this_rm_page_header = reinterpret_cast<RmPageHdr*>(this_page_addr);
            }
        }

        if (page_buffer_index != 0 || rm_slot_index != 0) {
            this_rm_page_header->num_records = rm_slot_index;
            for (int i = 0; i < rm_slot_index; ++i) {
                Bitmap::set(this_bitmap_addr, i);
            }
            param_->sm_manager->get_disk_manager()->write_full_pages(
                param_->fh->GetFd(),
                (rm_page_index - 1) / kPageBufferSize * kPageBufferSize + 1,
                page_buf,
                page_buffer_index + 1);
            param_->fh->get_file_hdr_ptr()->num_pages += (page_buffer_index + 1);
        }

        csv_file_input.close();

        delete[] page_buf;
    }

    printf("[Internal] Table \'%s\' Loaded.\n", param_->tab_name.c_str());

    pthread_mutex_lock(load_pool_access_lock);
    load_pool.erase(param_->tab_name);
    pthread_mutex_unlock(load_pool_access_lock);

    return nullptr;
}
