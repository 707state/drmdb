#pragma once

int yyparse();

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *str);

void yy_delete_buffer(YY_BUFFER_STATE buffer);
namespace ast {

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

} // namespace ast
