#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

enum JoinType { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN };
namespace ast {

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_DATETIME };

enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum SvAggreType { SV_AGGRE_COUNT, SV_AGGRE_SUM, SV_AGGRE_MAX, SV_AGGRE_MIN, SV_AGGRE_NONE };

// Base class for tree nodes
struct TreeNode {
  virtual ~TreeNode() = default; // enable polymorphism
};

struct Help : public TreeNode {};

struct SetOutputFileOff : public TreeNode {};

struct ShowTables : public TreeNode {};

struct TxnBegin : public TreeNode {};

struct TxnCommit : public TreeNode {};

struct TxnAbort : public TreeNode {};

struct TxnRollback : public TreeNode {};

struct TypeLen : public TreeNode {
  SvType type;
  int len;

  TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
};

struct Field : public TreeNode {};

struct ColDef : public Field {
  std::string col_name;
  std::shared_ptr<TypeLen> type_len;

  ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_)
      : col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
};

struct CreateTable : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<Field>> fields;

  CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_)
      : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
  std::string tab_name;

  explicit DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
  std::string tab_name;

  explicit DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
  std::string tab_name;
  std::vector<std::string> col_names;

  CreateIndex(std::string tab_name_, std::vector<std::string> col_names_)
      : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct ShowIndex : public TreeNode {
  std::string tab_name;

  explicit ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct DropIndex : public TreeNode {
  std::string tab_name;
  std::vector<std::string> col_names;

  DropIndex(std::string tab_name_, std::vector<std::string> col_names_)
      : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct Expr : public TreeNode {};

struct Value : public Expr {};

struct IntLit : public Value {
  int val;

  explicit IntLit(int val_) : val(val_) {}
};

struct FloatLit : public Value {
  float val;

  explicit FloatLit(float val_) : val(val_) {}
};

struct StringLit : public Value {
  std::string val;

  explicit StringLit(std::string val_) : val(std::move(val_)) {}
};

// add DATETIMELit
struct DateTimeLit : public Value {
  std::string val;

  explicit DateTimeLit(std::string val_) : val(std::move(val_)) {}
};

struct Col : public Expr {
  std::string tab_name;
  std::string col_name;
  SvAggreType ag_type;
  std::string as_name;

  Col(std::string tab_name_, std::string col_name_, ast::SvAggreType ag_type_ = SV_AGGRE_NONE,
      std::string as_name_ = "")
      : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)), ag_type(ag_type_),
        as_name(std::move(as_name_)) {}
};

struct SetClause : public TreeNode {
  std::string col_name;
  std::shared_ptr<Value> val;
  bool is_plus_value = false; // 为了支持 update student set score=score+5.5 where id<3; 这种update
  // 默认为false，即普通的update
  SetClause(std::string col_name_, std::shared_ptr<Value> val_)
      : col_name(std::move(col_name_)), val(std::move(val_)) {}
  SetClause(std::string col_name_, std::shared_ptr<Value> val_, bool is_plus_value_)
      : col_name(std::move(col_name_)), val(std::move(val_)), is_plus_value(is_plus_value_) {}
};

struct BinaryExpr : public TreeNode {
  std::shared_ptr<Col> lhs;
  SvCompOp op;
  std::shared_ptr<Expr> rhs;

  BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_)
      : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderBy : public TreeNode {
  std::shared_ptr<Col> cols;
  OrderByDir orderby_dir;
  OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_)
      : cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
};

struct InsertStmt : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<Value>> vals;

  InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_)
      : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct LoadStmt : public TreeNode {
  std::string file_path_;
  std::string tab_name_;

  LoadStmt(std::string file_path, std::string tab_name)
      : file_path_(std::move(file_path)), tab_name_(std::move(tab_name)) {}
};

struct DeleteStmt : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<SetClause>> set_clauses;
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  UpdateStmt(std::string tab_name_, std::vector<std::shared_ptr<SetClause>> set_clauses_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
  std::string left;
  std::string right;
  std::vector<std::shared_ptr<BinaryExpr>> conds;
  JoinType type;

  JoinExpr(std::string left_, std::string right_, std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_)
      : left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
};

struct SelectStmt : public TreeNode {
  std::vector<std::shared_ptr<Col>> cols;
  std::vector<std::string> tabs;
  std::vector<std::shared_ptr<BinaryExpr>> conds;
  std::vector<std::shared_ptr<JoinExpr>> jointree;

  bool has_sort;
  std::vector<std::shared_ptr<OrderBy>> orders;
  int limit;

  std::vector<std::shared_ptr<Col>> group_by;      // 添加group_by 是一个colList
  std::vector<std::shared_ptr<BinaryExpr>> having; // 添加having 是一个表达式，和where很像
  // 其实这个having和where语句差不多 直接把表达式复用一下就可以

  SelectStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::string> tabs_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_, std::vector<std::shared_ptr<Col>> group_by_,
             std::vector<std::shared_ptr<BinaryExpr>> having_,
             std::pair<std::vector<std::shared_ptr<OrderBy>>, int> orders_)
      : cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)), orders(std::move(orders_.first)),
        limit(orders_.second), group_by(std::move(group_by_)), having(std::move(having_)) {
    has_sort = !orders.empty();
  }
};

//// 聚合元信息
// struct AggreCol : public Expr {
//   SvAggreType ag_type;
//   std::string col_name;
//   std::string as_name;    //别名
//   AggreCol(SvAggreType ag_type_, std::string col_name_, std::string as_name_) :
//       ag_type(ag_type_), col_name(col_name_), as_name(std::move(as_name_)) {
//
//   }
// };
//
// struct AggreStmt : public TreeNode {
//   std::shared_ptr<AggreCol> aggre_col;
//   std::string tab_name;
//   std::vector<std::shared_ptr<BinaryExpr>> conds;
//   std::vector<std::shared_ptr<Col>> group_by;     // 添加group_by
//   std::vector<std::shared_ptr<BinaryExpr>> having; // 添加having
//
//   AggreStmt(std::shared_ptr<AggreCol> aggre_col_,
//             std::string tab_name_,
//             std::vector<std::shared_ptr<BinaryExpr>> conds_,
//             std::vector<std::shared_ptr<Col>> group_by_,
//             std::vector<std::shared_ptr<BinaryExpr>> having_)
//       : aggre_col(std::move(aggre_col_)),
//         tab_name(std::move(tab_name_)),
//         conds(std::move(conds_)),
//         group_by(std::move(group_by_)),
//         having(std::move(having_)) {
//   }
// };

// Semantic value
struct SemValue {
  int sv_int;
  float sv_float;
  std::string sv_str;
  OrderByDir sv_orderby_dir;
  std::vector<std::string> sv_strs;

  std::shared_ptr<TreeNode> sv_node;

  SvCompOp sv_comp_op;
  SvAggreType sv_ag_type; // 聚合type

  std::shared_ptr<TypeLen> sv_type_len;

  std::shared_ptr<Field> sv_field;
  std::vector<std::shared_ptr<Field>> sv_fields;

  std::shared_ptr<Expr> sv_expr;

  std::shared_ptr<Value> sv_val;
  std::vector<std::shared_ptr<Value>> sv_vals;

  std::shared_ptr<Col> sv_col;
  std::vector<std::shared_ptr<Col>> sv_cols;

  std::shared_ptr<SetClause> sv_set_clause;
  std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

  std::shared_ptr<BinaryExpr> sv_cond;
  std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

  std::shared_ptr<OrderBy> sv_orderby;
  std::vector<std::shared_ptr<OrderBy>> sv_orderbys;
  std::pair<std::vector<std::shared_ptr<OrderBy>>, int> op_sv_orderbys;

  // std::shared_ptr<AggreCol> sv_aggre_col; //聚合col
};

extern std::shared_ptr<ast::TreeNode> parse_tree;

} // namespace ast

#define YYSTYPE ast::SemValue
