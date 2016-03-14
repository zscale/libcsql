/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/tasks/select.h>

namespace csql {

Select::Select(
    Transaction* txn,
    Vector<ValueExpression> select_expressions) :
    txn_(txn),
    select_exprs_(std::move(select_expressions)),
    pos_(0) {}

//void Select::onInputsReady() {
//  Vector<SValue> out_row(select_exprs_.size(), SValue{});
//
//  for (int i = 0; i < select_exprs_.size(); ++i) {
//    VM::evaluate(txn_, select_exprs_[i].program(), 0, nullptr,  &out_row[i]);
//  }
//
//  input_(out_row.data(), out_row.size());
//}

bool Select::nextRow(SValue* out, int out_len) {
  if (pos_ == 0) {
    return -1;
  } else {
    return -1;
  }
}

SelectFactory::SelectFactory(
    Vector<RefPtr<SelectListNode>> select_exprs) :
    select_exprs_(select_exprs) {}

RefPtr<Task> SelectFactory::build(
    Transaction* txn,
    HashMap<TaskID, ScopedPtr<ResultCursor>> input) const {
  Vector<ValueExpression> select_expressions;
  auto qbuilder = txn->getRuntime()->queryBuilder();
  for (const auto& slnode : select_exprs_) {
    select_expressions.emplace_back(
        qbuilder->buildValueExpression(txn, slnode->expression()));
  }

  return new Select(txn, std::move(select_expressions));
}

}
