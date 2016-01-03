/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/NestedLoopJoin.h>

namespace csql {

NestedLoopJoin::NestedLoopJoin(
    Transaction* txn,
    JoinType join_type,
    ScopedPtr<TableExpression> base_tbl,
    ScopedPtr<TableExpression> joined_tbl,
    const Vector<String>& column_names,
    Vector<ValueExpression> select_expressions) :
    txn_(txn),
    join_type_(join_type),
    base_table_(std::move(base_tbl)),
    joined_table_(std::move(joined_tbl)),
    column_names_(column_names),
    select_exprs_(std::move(select_expressions)) {}

void NestedLoopJoin::prepare(ExecutionContext* context) {
  context->incrNumSubtasksTotal(1);
  base_table_->prepare(context);
  joined_table_->prepare(context);
}

void NestedLoopJoin::execute(
    ExecutionContext* context,
    Function<bool (int argc, const SValue* argv)> fn) {
  RAISE(kNotYetImplementedError);
  switch (join_type_) {
    case JoinType::CARTESIAN:
      executeCartesianJoin(fn);
      break;
    default:
      RAISE(kNotYetImplementedError);
  }

  context->incrNumSubtasksCompleted(1);
}

Vector<String> NestedLoopJoin::columnNames() const {
  return column_names_;
}

size_t NestedLoopJoin::numColumns() const {
  return column_names_.size();
}

}
