/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>
#include <stx/option.h>
#include <csql/svalue.h>
#include <csql/qtree/TableExpressionNode.h>
#include <csql/qtree/ValueExpressionNode.h>
#include <csql/qtree/SelectListNode.h>
#include <csql/qtree/JoinCondition.h>

using namespace stx;

namespace csql {

enum class JoinType {
  CARTESIAN, INNER, LEFT, RIGHT
};

class JoinNode : public TableExpressionNode {
public:

  JoinNode(
      JoinType join_type,
      RefPtr<QueryTreeNode> base_table,
      RefPtr<QueryTreeNode> joined_table,
      Vector<RefPtr<SelectListNode>> select_list,
      Option<RefPtr<ValueExpressionNode>> where_expr,
      Option<RefPtr<JoinCondition>> join_cond);

  JoinNode(const JoinNode& other);

  JoinType joinType() const;

  RefPtr<QueryTreeNode> baseTable() const;
  RefPtr<QueryTreeNode> joinedTable() const;

  Vector<RefPtr<SelectListNode>> selectList() const;
  Vector<String> outputColumns() const override;

  size_t getColumnIndex(
      const String& column_name,
      bool allow_add = false) override;

  Option<RefPtr<ValueExpressionNode>> whereExpression() const;
  Option<RefPtr<JoinCondition>> joinCondition() const;

  RefPtr<QueryTreeNode> deepCopy() const override;

  String toString() const override;

protected:
  JoinType join_type_;
  RefPtr<QueryTreeNode> base_table_;
  RefPtr<QueryTreeNode> joined_table_;
  Vector<String> column_names_;
  Vector<RefPtr<SelectListNode>> select_list_;
  Option<RefPtr<ValueExpressionNode>> where_expr_;
  Option<RefPtr<JoinCondition>> join_cond_;
};

} // namespace csql
