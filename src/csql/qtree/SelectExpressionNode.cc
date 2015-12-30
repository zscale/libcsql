/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/SelectExpressionNode.h>

using namespace stx;

namespace csql {

SelectExpressionNode::SelectExpressionNode(
    Vector<RefPtr<SelectListNode>> select_list) :
    select_list_(select_list) {
  for (const auto& sl : select_list_) {
    column_names_.emplace_back(sl->columnName());
  }
}

Vector<RefPtr<SelectListNode>> SelectExpressionNode::selectList()
    const {
  return select_list_;
}

Vector<String> SelectExpressionNode::columnNames() const {
  return column_names_;
}

RefPtr<QueryTreeNode> SelectExpressionNode::deepCopy() const {
  Vector<RefPtr<SelectListNode>> args;
  for (const auto& arg : select_list_) {
    args.emplace_back(arg->deepCopyAs<SelectListNode>());
  }

  return new SelectExpressionNode(args);
}

String SelectExpressionNode::toString() const {
  String str = "(select-expr";

  for (const auto& e : select_list_) {
    str += " " + e->toString();
  }

  str += ")";
  return str;
}

} // namespace csql
