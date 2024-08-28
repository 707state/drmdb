#pragma once
#include "common/defs.h"
#include "common/errors.h"
#include <parser/ast.h>
#include <string>
struct ColMeta {
  std::string tab_name; // 字段所属表名称
  std::string name;     // 字段名称
  ColType type;         // 字段类型
  int len;              // 字段长度
  int offset;           // 字段位于记录中的偏移量
  bool index;           /** unused */

  // 这里必须定义在最下边，不然会影响上边变量的顺序
  // 导致编译不能通过
  ast::SvAggreType ag_type; // 聚合类型
  std::string as_name;      // 别名

  friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
    // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
    return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
              << col.index;
  }

  friend std::istream &operator>>(std::istream &is, ColMeta &col) {
    return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
  }
};

/* 索引元数据 */
struct IndexMeta {
  std::string tab_name;      // 索引所属表名称
  int col_tot_len;           // 索引字段长度总和
  int col_num;               // 索引字段数量
  std::vector<ColMeta> cols; // 索引包含的字段

  friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
    os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
    for (auto &col : index.cols) {
      os << "\n" << col;
    }
    return os;
  }

  friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
    is >> index.tab_name >> index.col_tot_len >> index.col_num;
    for (int i = 0; i < index.col_num; ++i) {
      ColMeta col;
      is >> col;
      index.cols.push_back(col);
    }
    return is;
  }
};

/* 表元数据 */
struct TabMeta {
  std::string name;               // 表名称
  std::vector<ColMeta> cols;      // 表包含的字段
  std::vector<IndexMeta> indexes; // 表上建立的索引

  TabMeta() {}

  TabMeta(const TabMeta &other) {
    name = other.name;
    for (auto col : other.cols) {
      cols.push_back(col);
    }
  }

  /* 判断当前表中是否存在名为col_name的字段 */
  bool is_col(const std::string &col_name) const {
    auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
    return pos != cols.end();
  }
  // 2024.8.6修改：如果没有最佳匹配，则返回找到的第一个索引，该索引包含传入的字段之一即可
  std::pair<bool, IndexMeta> is_index(const std::vector<std::string> &col_names) const {
    // 先统计cols
    boost::unordered_map<std::string, bool> col2bool;
    for (auto col_name : col_names) {
      col2bool[col_name] = true;
    }

    // 目前：支持最左匹配，且会自动调换顺序

    // 找最匹配的index_meta
    int min_not_match_cols = INT32_MAX;
    IndexMeta const *most_match_index = nullptr;

    for (auto &index : indexes) {
      // 检查是否符合index需求
      // 1. 统计有多少连续的列用到了index
      size_t i = 0;
      int not_match_cols = 0;
      // 最左匹配，如果最左侧的没有找到则直接跳过
      while (i < index.col_num) {
        if (col2bool[index.cols[i].name]) {
          i++;
        } else {
          break;
        }
      }
      // 2. 如果没有发现，那么该索引不匹配
      if (i == 0) {
        continue;
      }
      // 3. 检查之后的所有列是否都没有在索引中
      while (i < index.col_num) {
        if (!col2bool[index.cols[i].name]) {
          i++;
          not_match_cols++;
        } else {
          break;
        }
      }
      // 4. 如果i ==
      // col_num那么说明之后的所有列都没有用到该索引，否则则说明用到了，该索引不符合最左匹配
      if (i == index.col_num && not_match_cols < min_not_match_cols) {
        most_match_index = &index;
        min_not_match_cols = not_match_cols;
      } else {
        continue;
      }
    }
    if (most_match_index != nullptr) {
      return {true, *most_match_index};
    } else {
      // 如果没有找到任意一个匹配的索引，就不使用索引
      return {false, IndexMeta()};
    }
  }

  /* 根据字段名称集合获取索引元数据 */
  std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string> &col_names) {
    for (auto index = indexes.begin(); index != indexes.end(); ++index) {
      if ((*index).col_num != col_names.size()) {
        continue;
      }
      auto &index_cols = (*index).cols;
      size_t i = 0;
      for (; i < col_names.size(); ++i) {
        if (index_cols[i].name.compare(col_names[i]) != 0) {
          break;
        }
      }
      if (i == col_names.size()) {
        return index;
      }
    }
    throw IndexNotFoundError(name, col_names);
  }

  /* 根据字段名称获取字段元数据 */
  std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
    auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
    if (pos == cols.end()) {
      throw ColumnNotFoundError(col_name);
    }
    return pos;
  }

  bool is_col_to_index(const std::string &col_name) {
    for (auto &index : indexes) {
      auto &cols = index.cols;
      for (auto it = cols.begin(); it != cols.end(); ++it) {
        if (it->name == col_name) {
          return true;
        }
      }
    }
    return false;
  }

  friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
    os << tab.name << '\n' << tab.cols.size() << '\n';
    for (auto &col : tab.cols) {
      os << col << '\n'; // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
    }
    os << tab.indexes.size() << "\n";
    for (auto &index : tab.indexes) {
      os << index << "\n";
    }
    return os;
  }

  friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
    size_t n;
    is >> tab.name >> n;
    for (size_t i = 0; i < n; i++) {
      ColMeta col;
      is >> col;
      tab.cols.push_back(col);
    }
    is >> n;
    for (size_t i = 0; i < n; ++i) {
      IndexMeta index;
      is >> index;
      tab.indexes.push_back(index);
    }
    return is;
  }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和
// >>
/* 数据库元数据 */
class DbMeta {
  friend class SmManager;

private:
  std::string name_;                    // 数据库名称
  std::map<std::string, TabMeta> tabs_; // 数据库中包含的表

public:
  /* 判断数据库中是否存在指定名称的表 */
  bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

  void SetTabMeta(const std::string &tab_name, const TabMeta &meta) { tabs_[tab_name] = meta; }

  /* 获取指定名称表的元数据 */
  TabMeta &get_table(const std::string &tab_name) {
    auto pos = tabs_.find(tab_name);
    if (pos == tabs_.end()) {
      throw FileNotFoundError(tab_name);
    }

    return pos->second;
  }

  // 重载操作符 <<
  friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
    os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
    for (auto &entry : db_meta.tabs_) {
      os << entry.second << '\n';
    }
    return os;
  }

  friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
    size_t n;
    is >> db_meta.name_ >> n;
    for (size_t i = 0; i < n; i++) {
      TabMeta tab;
      is >> tab;
      db_meta.tabs_[tab.name] = tab;
    }
    return is;
  }
};
