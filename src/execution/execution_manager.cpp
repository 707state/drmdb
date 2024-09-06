/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "common/config.h"
#include "common/record_printer.h"
#include "executor_projection.h"
#include <fstream>
#include <iomanip>
const char* help_info =
    "Supported SQL syntax:\n"
    "  command ;\n"
    "command:\n"
    "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
    "  DROP TABLE table_name\n"
    "  CREATE INDEX table_name (column_name)\n"
    "  DROP INDEX table_name (column_name)\n"
    "  INSERT INTO table_name VALUES (value [, value ...])\n"
    "  DELETE FROM table_name [WHERE where_clause]\n"
    "  UPDATE table_name SET column_name = value [, column_name = value ...] "
    "[WHERE where_clause]\n"
    "  SELECT selector FROM table_name [WHERE where_clause]\n"
    "type:\n"
    "  {INT | FLOAT | CHAR(n)}\n"
    "where_clause:\n"
    "  condition [AND condition ...]\n"
    "condition:\n"
    "  column op {column | value}\n"
    "column:\n"
    "  [table_name.]column_name\n"
    "op:\n"
    "  {= | <> | < | > | <= | >=}\n"
    "selector:\n"
    "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context* context) {
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch (x->tag) {
        case T_CreateTable: {
            sm_manager_->create_table(x->tab_name_, x->cols_, context);
            break;
        }
        case T_DropTable: {
            sm_manager_->drop_table(x->tab_name_, context);
            break;
        }
        case T_CreateIndex: {
            sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
            break;
        }
        case T_DropIndex: {
            sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
            break;
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan,
                                txn_id_t* txn_id,
                                Context* context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->tag) {
        case T_Help: {
            memcpy(
                context->data_send_ + *(context->offset_), help_info, strlen(help_info));
            *(context->offset_) = strlen(help_info);
            break;
        }
        case T_SetOutputFileOff: {
            sm_manager_->enable_output_ = false; // 不输出到output.txt
            break;
        }
        case T_ShowTable: {
            sm_manager_->show_tables(context);
            break;
        }
        case T_DescTable: {
            sm_manager_->desc_table(x->tab_name_, context);
            break;
        }
        case T_ShowIndex: {
            sm_manager_->show_index(x->tab_name_, context);
            break;
        }
        case T_Transaction_begin: {
            // 显示开启一个事务
            context->txn_->set_txn_mode(true);
            break;
        }
        case T_Transaction_commit: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            txn_mgr_->commit(context->txn_, context->log_mgr_);
            break;
        }
        case T_Transaction_rollback: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            txn_mgr_->abort(context->txn_, context->log_mgr_);
            break;
        }
        case T_Transaction_abort: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            txn_mgr_->abort(context->txn_, context->log_mgr_);
            break;
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }
    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                            std::vector<TabCol> sel_cols,
                            Context* context) {
    std::cout << "[Internal] Normal Select" << std::endl;
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto& sel_col: sel_cols) {
        captions.emplace_back(sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    if (sm_manager_->enable_output_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto& caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }

    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end();
         executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        auto rid = executorTreeRoot->rid();
        APPEND_TO_FILE(rid.page_no, rid.slot_no, "READ");
        std::vector<std::string> columns;
        for (auto& col: executorTreeRoot->cols()) {
            std::string col_str;
            char* rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int*)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float*)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char*)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            } else if (col.type == TYPE_DATETIME) {
                uint64_t raw;
                memcpy((char*)&raw, rec_buf, sizeof(raw));
                DateTime dt;
                dt.decode(raw);
                col_str = dt.encode_to_string();
            } else {
                throw InvalidTypeError();
            }
            columns.emplace_back(col_str);
        }
        // print record into buffer
        rec_printer.print_record(columns, context);
        // print record into file
        if (sm_manager_->enable_output_) {
            outfile << "|";
            for (const auto& column: columns) {
                outfile << " " << column << " |";
            }
            outfile << "\n";
        }
        num_rec++;
    }
    if (sm_manager_->enable_output_) {
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

void QlManager::aggregate_select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                                      std::vector<TabCol> sel_cols,
                                      Context* context,
                                      std::vector<Condition> having_clauses,
                                      std::vector<TabCol> group_by_cols) {
    std::cout << "[Internal] Aggregate Select" << std::endl;
    for (auto& having_clause: having_clauses) {
        if (having_clause.lhs_col.col_name.empty()
            && having_clause.lhs_col.ag_type == ast::SV_AGGRE_COUNT) {
            // count(*) 的情况
            // 让其col_name等价于count(sel_cols[0])
            having_clause.lhs_col.col_name = group_by_cols[0].col_name;
        }
    }

    // 用来存储聚合结果的struct
    struct AggResult {
        std::vector<std::string> values; // 把结果保存为string 方便int和float转换
        // 如果这里把类型限制为float会导致输出上有问题 本该是int的输出带小数点
        int count; // Count for COUNT(*)
    };

    // 聚合结果
    std::map<std::vector<std::string>, AggResult> agg_values;

    // 处理表头
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto& sel_col: sel_cols) {
        // 首先处理表头
        // 如果是普通类型就输出col_name, 如果是聚合类型就输出as_name
        captions.emplace_back(sel_col.ag_type == ast::SV_AGGRE_NONE ? sel_col.col_name
                                                                    : sel_col.as_name);
    }

    // 遍历所有的tuple
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end();
         executorTreeRoot->nextTuple()) {
        int sel_cols_add_num = 0; // sel_cols中添加的having_clause数量

        auto rid = dynamic_cast<ProjectionExecutor*>(executorTreeRoot.get())->rid();
        APPEND_TO_FILE(rid.page_no, rid.slot_no, "READ");

        for (auto& having_clause: having_clauses) {
            // 把having子句中的Col加入sel_cols中，方便后边使用
            // 等到输出结果之前再pop掉即可，不影响正常输出
            TabCol col{.tab_name = having_clause.lhs_col.tab_name,
                       .col_name = having_clause.lhs_col.col_name,
                       .ag_type = having_clause.lhs_col.ag_type,
                       .as_name = having_clause.lhs_col.as_name};
            auto it = std::find_if(
                sel_cols.begin(), sel_cols.end(), [&](const TabCol& existing_col) {
                    return existing_col.col_name == col.col_name
                           && existing_col.ag_type == col.ag_type;
                });
            if (it == sel_cols.end()) {
                sel_cols.emplace_back(col);
                sel_cols_add_num++;
            }
        }
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        std::vector<std::string> group_by_values;
        std::vector<std::string> agg_results;

        // 这里循环出的col是每个tuple对应的所有列
        // 例如select id, max(score), min(score);
        // 这里的col = id, score, score
        for (auto& col: executorTreeRoot->cols()) {
            auto rid = executorTreeRoot->rid();
            // APPEND_TO_FILE(rid.page_no, rid.slot_no, "READ");
            char* rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                int val = *(int*)rec_buf;
                columns.emplace_back(std::to_string(val));
                if (std::find_if(group_by_cols.begin(),
                                 group_by_cols.end(),
                                 [&col](const TabCol& g_col) {
                                     return col.name == g_col.col_name;
                                 })
                    != group_by_cols.end()) {
                    group_by_values.emplace_back(std::to_string(val));
                }
                agg_results.emplace_back(std::to_string(val));
            } else if (col.type == TYPE_FLOAT) {
                float val = *(float*)rec_buf;
                columns.emplace_back(std::to_string(val));
                if (std::find_if(group_by_cols.begin(),
                                 group_by_cols.end(),
                                 [&col](const TabCol& g_col) {
                                     return col.name == g_col.col_name;
                                 })
                    != group_by_cols.end()) {
                    group_by_values.emplace_back(std::to_string(val));
                }
                agg_results.emplace_back(std::to_string(val));
            } else if (col.type == TYPE_STRING) {
                std::string str_val = std::string((char*)rec_buf, col.len);
                str_val.resize(strlen(str_val.c_str()));
                columns.emplace_back(str_val);
                if (std::find_if(group_by_cols.begin(),
                                 group_by_cols.end(),
                                 [&col](const TabCol& g_col) {
                                     return col.name == g_col.col_name;
                                 })
                    != group_by_cols.end()) {
                    group_by_values.emplace_back(str_val);
                }
                agg_results.emplace_back(str_val);
            } else {
                throw InternalError("Unsupported column type.");
            }
        }

        // 对值进行更新
        // 比如max min等操作需要每次循环对之前的值都进行一次更新
        auto it = agg_values.find(group_by_values);
        if (it == agg_values.end()) {
            // 如果不存在 就初始化
            AggResult new_result;
            new_result.count = 1; // Initialize COUNT(*)
            for (size_t i = 0; i < sel_cols.size(); ++i) {
                if (sel_cols[i].ag_type == ast::SV_AGGRE_COUNT) {
                    // 默认count是1
                    new_result.values.emplace_back("1");
                } else {
                    new_result.values.emplace_back(agg_results[i]);
                }
            }
            agg_values[group_by_values] = new_result;
        } else {
            // 如果已经存在
            AggResult& result = it->second;
            result.count++; // COUNT(*)
            for (size_t i = 0; i < sel_cols.size(); ++i) {
                try {
                    // max和min无需判断是什么类型，因为都转换成float后比较不影响原数
                    if (sel_cols[i].ag_type == ast::SV_AGGRE_MAX) {
                        if (std::stof(agg_results[i]) > std::stof(result.values[i])) {
                            result.values[i] = agg_results[i];
                        }
                    } else if (sel_cols[i].ag_type == ast::SV_AGGRE_MIN) {
                        if (std::stof(agg_results[i]) < std::stof(result.values[i])) {
                            result.values[i] = agg_results[i];
                        }
                    } else if (sel_cols[i].ag_type == ast::SV_AGGRE_SUM) {
                        // 检查是否有小数点来判断是什么数
                        bool isFloatResult =
                            result.values[i].find('.') != std::string::npos;
                        bool isFloatNew = agg_results[i].find('.') != std::string::npos;
                        if (!isFloatResult && !isFloatNew) {
                            // 如果没有小数点 就都是int
                            int intValueResult = std::stoi(result.values[i]);
                            int intValueNew = std::stoi(agg_results[i]);
                            result.values[i] =
                                std::to_string(intValueResult + intValueNew);
                        } else {
                            // 反之同理
                            float floatValueResult = std::stof(result.values[i]);
                            float floatValueNew = std::stof(agg_results[i]);
                            result.values[i] =
                                std::to_string(floatValueResult + floatValueNew);
                        }
                    } else if (sel_cols[i].ag_type == ast::SV_AGGRE_COUNT) {
                        // count的无所谓
                        result.values[i] =
                            std::to_string(std::stoi(result.values[i]) + 1);
                    } else if (sel_cols[i].ag_type == ast::SV_AGGRE_NONE) {
                        // do nothing
                        continue;
                    }
                } catch (std::invalid_argument const& e) {
                    throw InternalError("char类型只支持COUNT聚合操作");
                } catch (std::out_of_range const& e) {
                    throw InternalError("超出大小限制");
                }
            }
        }
        for (int i = 0; i < sel_cols_add_num; i++) {
            // 把刚刚加入的全部pop_back()掉，使得输出正确
            sel_cols.pop_back();
        }
    }
    // 用having子句过滤结果
    std::vector<std::vector<std::string>> filtered_results;
    for (const auto& [group, result]: agg_values) {
        int sel_cols_add_num = 0; // sel_cols中添加的having_clause数量
        for (auto& having_clause: having_clauses) {
            // 与之前操作同理
            TabCol col{.tab_name = having_clause.lhs_col.tab_name,
                       .col_name = having_clause.lhs_col.col_name,
                       .ag_type = having_clause.lhs_col.ag_type,
                       .as_name = having_clause.lhs_col.as_name};
            auto it = std::find_if(
                sel_cols.begin(), sel_cols.end(), [&](const TabCol& existing_col) {
                    return existing_col.col_name == col.col_name
                           && existing_col.ag_type == col.ag_type;
                });
            if (it == sel_cols.end()) {
                sel_cols.emplace_back(col);
                sel_cols_add_num++;
            }
        }
        bool pass_having = true; // 检查是否通过having子句
        for (const auto& having_clause: having_clauses) {
            // Determine the index of the column we're comparing against
            size_t index = std::distance(
                sel_cols.begin(),
                std::find_if(sel_cols.begin(),
                             sel_cols.end(),
                             [&having_clause](const TabCol& s_col) {
                                 return s_col.col_name == having_clause.lhs_col.col_name
                                        && s_col.ag_type == having_clause.lhs_col.ag_type;
                             }));
            if (index == sel_cols.size()) {
                // 如果在select list中没有找到，就在agg_result中找
                index = std::distance(
                    result.values.begin(),
                    std::find_if(
                        result.values.begin(),
                        result.values.end(),
                        [&having_clause, &sel_cols, &result](const std::string& val) {
                            // 这里说实话不用判断这么麻烦，因为从语法解析上已经杜绝了Error的发生
                            // 但已经写了就不删了
                            size_t potential_index = std::distance(
                                sel_cols.begin(),
                                std::find_if(sel_cols.begin(),
                                             sel_cols.end(),
                                             [&having_clause](const TabCol& s_col) {
                                                 return s_col.col_name
                                                        == having_clause.lhs_col.col_name;
                                             }));
                            return potential_index != sel_cols.size()
                                   && potential_index < result.values.size();
                        }));
                if (index == result.values.size()) {
                    throw InternalError("该列未在select list或者聚合结果中找到");
                }
            }

            // 根据聚合类型获取对应的结果
            double agg_value;
            if (sel_cols[index].ag_type == ast::SV_AGGRE_NONE) {
                agg_value = std::stod(group[index]); // 非聚合列
            } else {
                agg_value = std::stod(result.values[index]); // 聚合列
            }

            // having子句中的比较
            if (having_clause.is_rhs_val) {
                double rhs_val;
                if (having_clause.rhs_val.type == TYPE_INT) {
                    rhs_val = static_cast<double>(having_clause.rhs_val.int_val);
                } else if (having_clause.rhs_val.type == TYPE_FLOAT) {
                    rhs_val = having_clause.rhs_val.float_val;
                } else {
                    throw InternalError("不支持的值类型");
                }

                switch (having_clause.op) {
                case OP_EQ:
                    pass_having &= agg_value == rhs_val;
                    break;
                case OP_NE:
                    pass_having &= agg_value != rhs_val;
                    break;
                case OP_LT:
                    pass_having &= agg_value < rhs_val;
                    break;
                case OP_GT:
                    pass_having &= agg_value > rhs_val;
                    break;
                case OP_LE:
                    pass_having &= agg_value <= rhs_val;
                    break;
                case OP_GE:
                    pass_having &= agg_value >= rhs_val;
                    break;
                default:
                    throw InternalError("不支持的比较操作符");
                }
            } else {
                // Compare with another column
                size_t other_index = std::distance(
                    sel_cols.begin(),
                    std::find_if(sel_cols.begin(),
                                 sel_cols.end(),
                                 [&having_clause](const TabCol& s_col) {
                                     return s_col.col_name
                                            == having_clause.rhs_col.col_name;
                                 }));
                if (other_index == sel_cols.size()) {
                    throw InternalError("左侧的列未在select list中找到");
                }
                double other_agg_value = std::stod(result.values[other_index]);

                switch (having_clause.op) {
                case OP_EQ:
                    pass_having &= agg_value == other_agg_value;
                    break;
                case OP_NE:
                    pass_having &= agg_value != other_agg_value;
                    break;
                case OP_LT:
                    pass_having &= agg_value < other_agg_value;
                    break;
                case OP_GT:
                    pass_having &= agg_value > other_agg_value;
                    break;
                case OP_LE:
                    pass_having &= agg_value <= other_agg_value;
                    break;
                case OP_GE:
                    pass_having &= agg_value >= other_agg_value;
                    break;
                default:
                    throw InternalError("不支持的比较操作符");
                }
            }
        }

        // 不管pass_having T or F
        // 先把sel_cols pop了 不然后边会出错
        for (int i = 0; i < sel_cols_add_num; i++) {
            // 把刚刚加入的全部pop_back()掉，使得输出正确
            sel_cols.pop_back();
        }

        if (pass_having) {
            // 最后的检查
            std::vector<std::string> formatted_group;
            formatted_group.reserve(sel_cols.size());
            for (const auto& sel_col: sel_cols) {
                if (sel_col.ag_type == ast::SV_AGGRE_NONE) {
                    // 如果是普通列，emplace_back普通结果
                    auto it = std::find_if(group_by_cols.begin(),
                                           group_by_cols.end(),
                                           [&sel_col](const TabCol& g_col) {
                                               return g_col.col_name == sel_col.col_name;
                                           });
                    if (it != group_by_cols.end()) {
                        size_t index = std::distance(group_by_cols.begin(), it);
                        formatted_group.emplace_back(group[index]);
                    } else {
                        throw InternalError("Group by column not found.");
                    }
                } else if (sel_col.ag_type == ast::SV_AGGRE_COUNT) {
                    // 如果聚合类型是count，特殊处理
                    size_t index = std::distance(
                        sel_cols.begin(),
                        std::find_if(sel_cols.begin(),
                                     sel_cols.end(),
                                     [&sel_col](const TabCol& s_col) {
                                         return s_col.col_name == sel_col.col_name
                                                && s_col.ag_type == ast::SV_AGGRE_COUNT;
                                     }));
                    if (index == sel_cols.size()) {
                        throw InternalError("Column not found in the SELECT list.");
                    }
                    formatted_group.emplace_back(result.values[index]);
                } else {
                    // 如果是非count聚合函数，emplace_back聚合结果
                    size_t index = std::distance(
                        sel_cols.begin(),
                        std::find_if(sel_cols.begin(),
                                     sel_cols.end(),
                                     [&sel_col](const TabCol& s_col) {
                                         return s_col == sel_col; // 使用自定义的比较
                                     }));
                    formatted_group.emplace_back(result.values[index]);
                }
            }
            // 保证下边print不会有问题，print出问题会段错误导致程序无法继续运行
            if (formatted_group.size() != sel_cols.size()) {
                throw InternalError("Mismatch between the number of columns in the "
                                    "select list and the formatted group.");
            }
            filtered_results.emplace_back(formatted_group);
        }
    }

    // 输出前的准备
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);

    // 输出表头
    std::fstream outfile;
    if (sm_manager_->enable_output_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto& caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }

    // 输出筛选后的结果
    for (const auto& result: filtered_results) {
        rec_printer.print_record(result, context);
        if (sm_manager_) {
            outfile << "|";
            for (const auto& i: result) {
                outfile << " " << i << " |";
            }
            outfile << "\n";
        }
    }

    // 对于count的情况，如果select只有count，而且是空表，需要输出0
    bool zero_pass = true;
    for (const auto& col: sel_cols) {
        if (col.ag_type != ast::SV_AGGRE_COUNT) {
            zero_pass = false;
            break;
        }
    }
    if (zero_pass && filtered_results.empty()) {
        std::vector<std::string> result;
        for (int i = 0; i < sel_cols.size(); i++) {
            result.emplace_back("0");
        }
        rec_printer.print_record(result, context);
        if (sm_manager_->enable_output_) {
            outfile << "|";
            for (const auto& i: result) {
                outfile << " " << i << " |";
            }
            outfile << "\n";
        }
    }

    // print footer into buffer
    rec_printer.print_separator(context);
    // print record nums into buffer
    RecordPrinter::print_record_count(filtered_results.size(), context);
    if (sm_manager_->enable_output_) {
        outfile.close();
    }
}

void QlManager::aggregate_select_from_with_index(
    std::unique_ptr<AbstractExecutor> executorTreeRoot,
    std::vector<TabCol> sel_cols,
    Context* context) {
    std::cout << "[Internal] Aggregate Select With Index" << std::endl;
    // 处理表头
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto& sel_col: sel_cols) {
        // 首先处理表头
        // 如果是普通类型就输出col_name, 如果是聚合类型就输出as_name
        captions.emplace_back(sel_col.ag_type == ast::SV_AGGRE_NONE ? sel_col.col_name
                                                                    : sel_col.as_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    if (sm_manager_->enable_output_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto& caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }
    // Print records
    size_t num_rec = 0;
    std::vector<std::string> columns;
    if (sel_cols[0].ag_type == ast::SV_AGGRE_COUNT) {
        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end();
             executorTreeRoot->nextTuple()) {
            ++num_rec;
        }
        columns.emplace_back(std::to_string(num_rec));
    } else {
        std::string max_num = "-2147483648", min_num = "2147483647", sum_num = "0";
        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end();
             executorTreeRoot->nextTuple()) {
            auto Tuple = executorTreeRoot->Next();
            auto rid = executorTreeRoot->rid();
            APPEND_TO_FILE(rid.page_no, rid.slot_no, "READ");

            for (auto& col: executorTreeRoot->cols()) {

                if (col.name != sel_cols[0].col_name) {
                    // 如果不是要找的列直接跳过
                    continue;
                }

                const char* rec_buf = Tuple->data + col.offset;
                if (col.type == TYPE_INT) {
                    int val = *reinterpret_cast<const int*>(rec_buf);
                    max_num = std::to_string(std::max(std::stoi(max_num), val));
                    min_num = std::to_string(std::min(std::stoi(min_num), val));
                    sum_num = std::to_string(std::stoi(sum_num) + val);
                } else if (col.type == TYPE_FLOAT) {
                    float val = *reinterpret_cast<const float*>(rec_buf);
                    max_num = std::to_string(std::max(std::stof(max_num), val));
                    min_num = std::to_string(std::min(std::stof(min_num), val));
                    sum_num = std::to_string(std::stof(sum_num) + val);
                } else {
                    throw InternalError("char类型只支持COUNT聚合操作");
                }
            }
            ++num_rec;
        }
        if (sel_cols[0].ag_type == ast::SV_AGGRE_MAX && num_rec != 0) {
            columns.emplace_back(max_num);
        } else if (sel_cols[0].ag_type == ast::SV_AGGRE_MIN && num_rec != 0) {
            columns.emplace_back(min_num);
        } else if (sel_cols[0].ag_type == ast::SV_AGGRE_SUM && num_rec != 0) {
            columns.emplace_back(sum_num);
        }
    }
    if (!columns.empty()) {
        // print record into buffer
        rec_printer.print_record(columns, context);
        // print record into file
        if (sm_manager_->enable_output_) {
            outfile << "|";
            for (const auto& column: columns) {
                outfile << " " << column << " |";
            }
            outfile << "\n";
        }
    }
    if (sm_manager_->enable_output_) {
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

void QlManager::fast_count_aggre(std::vector<TabCol> sel_cols, Context* context) {
    while (true) {
        pthread_mutex_lock(LoadExecutor::load_pool_access_lock);
        if (LoadExecutor::load_pool.count(sel_cols[0].tab_name) == 0) {
            pthread_mutex_unlock(LoadExecutor::load_pool_access_lock);
            break;
        }
        pthread_mutex_unlock(LoadExecutor::load_pool_access_lock);
    }
    // 处理表头
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto& sel_col: sel_cols) {
        // 首先处理表头
        // 如果是普通类型就输出col_name, 如果是聚合类型就输出as_name
        captions.emplace_back(sel_col.ag_type == ast::SV_AGGRE_NONE ? sel_col.col_name
                                                                    : sel_col.as_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    if (sm_manager_->enable_output_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto& caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }
    size_t num_rec = 0;
    std::vector<std::string> columns;
    auto rm_handle = sm_manager_->fhs_.at(sel_cols[0].tab_name).get();
    auto page_count = rm_handle->get_file_hdr().num_pages;
    printf("[Internal] Fast_Count_Aggregate, Total Pages: %d\n", page_count);
    for (int i = 1; i < page_count; ++i) {
        auto page_handle = rm_handle->fetch_page_handle(i);
        num_rec += page_handle.page_hdr->num_records;
        sm_manager_->get_bpm()->unpin_page({rm_handle->GetFd(), i}, false);
    }
    columns.emplace_back(std::to_string(num_rec));
    rec_printer.print_record(columns, context);
    // print record into file
    if (sm_manager_->enable_output_) {
        outfile << "|";
        for (const auto& column: columns) {
            outfile << " " << column << " |";
        }
        outfile << "\n";
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

void QlManager::fast_aggre_with_index(std::vector<TabCol> sel_cols,
                                      IxIndexHandle* ix_index_handle,
                                      IndexMeta index_meta,
                                      std::vector<Condition> conds,
                                      RmFileHandle* rm_file_handle,
                                      Context* context) {
    // 处理表头
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto& sel_col: sel_cols) {
        // 首先处理表头
        // 如果是普通类型就输出col_name, 如果是聚合类型就输出as_name
        captions.emplace_back(sel_col.ag_type == ast::SV_AGGRE_NONE ? sel_col.col_name
                                                                    : sel_col.as_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    if (sm_manager_->enable_output_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto& caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }
    std::vector<std::string> columns;
    // 确定上下界
    char* lower_key = new char[index_meta.col_tot_len];
    char* upper_key = new char[index_meta.col_tot_len];
    size_t off = 0;
    for (auto& col: index_meta.cols) {
        Value upper_value, lower_value;
        int col_len = 0;
        // 初始化上下界
        switch (col.type) {
        case TYPE_INT: {
            upper_value.set_int(INT32_MAX);
            lower_value.set_int(INT32_MIN);
            break;
        }
        case TYPE_FLOAT: {
            upper_value.set_float(__FLT_MAX__);
            lower_value.set_float(-__FLT_MAX__);
            break;
        }
        case TYPE_STRING: {
            upper_value.set_string(std::string(col.len, 255));
            lower_value.set_string(std::string(col.len, 0));
            break;
        }
        default: {
            throw InvalidTypeError();
        }
        }

        // 根据条件调整上下界
        for (auto& cond: conds) {
            if (cond.lhs_col.col_name == col.name && cond.is_rhs_val) {
                switch (cond.op) {
                case OP_EQ: {
                    if (ConditionDependedExecutor::check_cond(
                            cond.rhs_val, lower_value, OP_GT)) {
                        lower_value = cond.rhs_val;
                    }
                    if (ConditionDependedExecutor::check_cond(
                            cond.rhs_val, upper_value, OP_LT)) {
                        upper_value = cond.rhs_val;
                    }
                    break;
                }
                case OP_GT: {
                }
                case OP_GE: {
                    if (ConditionDependedExecutor::check_cond(
                            cond.rhs_val, lower_value, OP_GT)) {
                        lower_value = cond.rhs_val;
                    }
                    break;
                }
                case OP_LT: {
                }
                case OP_LE: {
                    if (ConditionDependedExecutor::check_cond(
                            cond.rhs_val, upper_value, OP_LT)) {
                        upper_value = cond.rhs_val;
                    }
                    break;
                }
                case OP_NE: {
                    break;
                }
                default: {
                    throw InternalError("Unexpected CompOp field type");
                }
                }
                break;
            }
        }

        // 生成原始值
        upper_value.init_raw(col.len);
        lower_value.init_raw(col.len);

        memcpy(upper_key + off, upper_value.raw->data, col.len);
        memcpy(lower_key + off, lower_value.raw->data, col.len);
        off += col.len;
    }
    if (sel_cols[0].ag_type == ast::SV_AGGRE_COUNT) {
        int total_count = 0;
        auto lower_bound = ix_index_handle->lower_bound(lower_key);
        APPEND_IID_TO_FILE(lower_bound.page_no, lower_bound.slot_no, "LOW");
        auto upper_bound = ix_index_handle->upper_bound(upper_key);
        APPEND_IID_TO_FILE(upper_bound.page_no, upper_bound.slot_no, "UP");
        if (lower_bound.page_no == upper_bound.page_no) {
            total_count = upper_bound.slot_no - lower_bound.slot_no;
        } else {
            auto lower_node = ix_index_handle->fetch_node(lower_bound.page_no);
            total_count += (lower_node->get_size() - lower_bound.slot_no);
            auto upper_node = ix_index_handle->fetch_node(upper_bound.page_no);
            total_count += (upper_node->get_size() - upper_bound.slot_no);
            auto cur_node = lower_node;
            while (cur_node->get_next_leaf() != upper_node->get_page_no()) {
                ix_index_handle->release_node(cur_node, false);
                cur_node = ix_index_handle->fetch_node(cur_node->get_next_leaf());
                total_count += cur_node->get_size();
            }
            ix_index_handle->release_node(cur_node, false);
            ix_index_handle->release_node(upper_node, false);
        }
        columns.emplace_back(std::to_string(total_count));
    } else if (sel_cols[0].ag_type == ast::SV_AGGRE_MIN) {
        auto lower_bound = ix_index_handle->lower_bound(lower_key);
        APPEND_IID_TO_FILE(lower_bound.page_no, lower_bound.slot_no, "LOW");
        auto rid = ix_index_handle->get_rid(lower_bound);
        auto record = rm_file_handle->get_record(rid, context);
        auto col_def = *(sm_manager_->db_.get_table(sel_cols[0].tab_name)
                             .get_col(sel_cols[0].col_name));
        auto ret = ConditionDependedExecutor::get_record_value(record, col_def);
        switch (ret.type) {
        case TYPE_INT:
            columns.emplace_back(std::to_string(ret.int_val));
            break;
        case TYPE_FLOAT:
            columns.emplace_back(std::to_string(ret.float_val));
            break;
        case TYPE_STRING:
            columns.emplace_back(ret.str_val);
            break;
        case TYPE_DATETIME:
            columns.emplace_back(ret.datetime_val.encode_to_string());
            break;
        }
    } else if (sel_cols[0].ag_type == ast::SV_AGGRE_MAX) {
        auto upper_bound = ix_index_handle->upper_bound(upper_key);
        APPEND_IID_TO_FILE(upper_bound.page_no, upper_bound.slot_no, "UP");
        auto rid = ix_index_handle->get_rid(upper_bound);
        auto record = rm_file_handle->get_record(rid, context);
        auto col_def = *(sm_manager_->db_.get_table(sel_cols[0].tab_name)
                             .get_col(sel_cols[0].col_name));
        auto ret = ConditionDependedExecutor::get_record_value(record, col_def);
        switch (ret.type) {
        case TYPE_INT:
            columns.emplace_back(std::to_string(ret.int_val));
            break;
        case TYPE_FLOAT:
            columns.emplace_back(std::to_string(ret.float_val));
            break;
        case TYPE_STRING:
            columns.emplace_back(ret.str_val);
            break;
        case TYPE_DATETIME:
            columns.emplace_back(ret.datetime_val.encode_to_string());
            break;
        }
    }

    delete lower_key;
    delete upper_key;

    rec_printer.print_record(columns, context);
    // print record into file
    if (sm_manager_->enable_output_) {
        outfile << "|";
        for (const auto& column: columns) {
            outfile << " " << column << " |";
        }
        outfile << "\n";
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(1, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) {
    exec->beginTuple();
    exec->Next();
}
