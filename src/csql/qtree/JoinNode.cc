/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/JoinNode.h>
#include <csql/qtree/ColumnReferenceNode.h>

using namespace stx;

namespace csql {

JoinNode::JoinNode(
    RefPtr<QueryTreeNode> base_table,
    RefPtr<QueryTreeNode> joined_table,
    Vector<RefPtr<SelectListNode>> select_list,
    Option<RefPtr<ValueExpressionNode>> where_expr,
    Option<RefPtr<JoinCondition>> join_cond) :
    base_table_(base_table),
    joined_table_(joined_table),
    select_list_(select_list),
    where_expr_(where_expr),
    join_cond_(join_cond) {
  for (const auto& sl : select_list_) {
    column_names_.emplace_back(sl->columnName());
  }

  addChild(&base_table_);
  addChild(&joined_table_);
}

JoinNode::JoinNode(
    const JoinNode& other) :
    base_table_(other.base_table_->deepCopy()),
    joined_table_(other.joined_table_->deepCopy()),
    column_names_(other.column_names_),
    join_cond_(other.join_cond_) {
  for (const auto& e : other.select_list_) {
    select_list_.emplace_back(e->deepCopyAs<SelectListNode>());
  }

  if (!other.where_expr_.isEmpty()) {
    where_expr_ = Some(
        other.where_expr_.get()->deepCopyAs<ValueExpressionNode>());
  }

  addChild(&base_table_);
  addChild(&joined_table_);
}

Vector<RefPtr<SelectListNode>> JoinNode::selectList() const {
  return select_list_;
}

Vector<String> JoinNode::outputColumns() const {
  return column_names_;
}

size_t JoinNode::getColumnIndex(
    const String& column_name,
    bool allow_add /* = false */) {
  auto col = column_name;
  for (int i = 0; i < column_names_.size(); ++i) {
    if (column_names_[i] == col || column_names_[i] == column_name) {
      return i;
    }
  }

  return -1; // FIXME
}

Option<RefPtr<ValueExpressionNode>> JoinNode::whereExpression() const {
  return where_expr_;
}

Option<RefPtr<JoinCondition>> JoinNode::joinCondition() const {
  return join_cond_;
}

RefPtr<QueryTreeNode> JoinNode::deepCopy() const {
  return new JoinNode(*this);
}

String JoinNode::toString() const {
  auto str = StringUtil::format(
      "(join (base-table $0) (joined-table $0) (select-list",
      base_table_->toString(),
      joined_table_->toString());

  for (const auto& e : select_list_) {
    str += " " + e->toString();
  }
  str += ")";

  if (!where_expr_.isEmpty()) {
    str += StringUtil::format(" (where $0)", where_expr_.get()->toString());
  }

  str += ")";
  return str;
};

} // namespace csql

