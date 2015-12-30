/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/SubqueryNode.h>

using namespace stx;

namespace csql {

SubqueryNode::SubqueryNode(
    RefPtr<QueryTreeNode> subquery,
    Vector<RefPtr<SelectListNode>> select_list,
    Option<RefPtr<ValueExpressionNode>> where_expr) :
    subquery_(subquery),
    select_list_(select_list),
    where_expr_(where_expr) {
  for (const auto& sl : select_list_) {
    column_names_.emplace_back(sl->columnName());
  }

  addChild(&subquery_);
}

SubqueryNode::SubqueryNode(
    const SubqueryNode& other) :
    subquery_(other.subquery_->deepCopy()) {
  for (const auto& e : other.select_list_) {
    select_list_.emplace_back(e->deepCopyAs<SelectListNode>());
  }

  if (!other.where_expr_.isEmpty()) {
    where_expr_ = Some(
        other.where_expr_.get()->deepCopyAs<ValueExpressionNode>());
  }

  for (const auto& sl : select_list_) {
    column_names_.emplace_back(sl->columnName());
  }

  addChild(&subquery_);
}

RefPtr<QueryTreeNode> SubqueryNode::subquery() const {
  return subquery_;
}

Vector<RefPtr<SelectListNode>> SubqueryNode::selectList() const {
  return select_list_;
}

Vector<String> SubqueryNode::columnNames() const {
  return column_names_;
}

Option<RefPtr<ValueExpressionNode>> SubqueryNode::whereExpression() const {
  return where_expr_;
}

RefPtr<QueryTreeNode> SubqueryNode::deepCopy() const {
  return new SubqueryNode(*this);
}

String SubqueryNode::toString() const {
  auto str = StringUtil::format(
      "(select (subquery $0) (select-list", subquery_->toString());
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
