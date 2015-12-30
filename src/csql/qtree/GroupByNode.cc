/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/GroupByNode.h>

using namespace stx;

namespace csql {

GroupByNode::GroupByNode(
    Vector<RefPtr<SelectListNode>> select_list,
    Vector<RefPtr<ValueExpressionNode>> group_exprs,
    RefPtr<QueryTreeNode> table) :
    select_list_(select_list),
    group_exprs_(group_exprs),
    table_(table) {
  addChild(&table_);

  for (const auto& sl : select_list_) {
    column_names_.emplace_back(sl->columnName());
  }
}

GroupByNode::GroupByNode(
    const GroupByNode& other) :
    column_names_(other.column_names_),
    table_(table_->deepCopyAs<QueryTreeNode>()) {
  for (const auto& e : other.select_list_) {
    select_list_.emplace_back(e->deepCopyAs<SelectListNode>());
  }

  for (const auto& e : other.group_exprs_) {
    group_exprs_.emplace_back(e->deepCopyAs<ValueExpressionNode>());
  }
}

Vector<RefPtr<SelectListNode>> GroupByNode::selectList() const {
  return select_list_;
}

Vector<String> GroupByNode::outputColumns() const {
  return column_names_;
}

size_t GroupByNode::getColumnIndex(const String& column_name) {
  for (int i = 0; i < column_names_.size(); ++i) {
    if (column_names_[i] == column_name) {
      return i;
    }
  }

  return -1;
}

Vector<RefPtr<ValueExpressionNode>> GroupByNode::groupExpressions() const {
  return group_exprs_;
}

RefPtr<QueryTreeNode> GroupByNode::inputTable() const {
  return table_;
}

RefPtr<QueryTreeNode> GroupByNode::deepCopy() const {
  return new GroupByNode(*this);
}

String GroupByNode::toString() const {
  String str = "(group-by (select-list";

  for (const auto& e : select_list_) {
    str += " " + e->toString();
  }

  str += ") (group-list";
  for (const auto& e : group_exprs_) {
    str += " " + e->toString();
  }

  str += ") (subexpr " + table_->toString() + "))";

  return str;
}

} // namespace csql
