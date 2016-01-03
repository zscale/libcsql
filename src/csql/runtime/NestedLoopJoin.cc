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
    const Vector<JoinNode::InputColumnRef>& input_map,
    const Vector<String>& column_names,
    Vector<ValueExpression> select_expressions) :
    txn_(txn),
    join_type_(join_type),
    base_table_(std::move(base_tbl)),
    joined_table_(std::move(joined_tbl)),
    input_map_(input_map),
    column_names_(column_names),
    select_exprs_(std::move(select_expressions)) {}

void NestedLoopJoin::prepare(ExecutionContext* context) {
  context->incrNumSubtasksTotal(3);
  base_table_->prepare(context);
  joined_table_->prepare(context);
}

void NestedLoopJoin::execute(
    ExecutionContext* context,
    Function<bool (int argc, const SValue* argv)> fn) {
  List<Vector<SValue>> base_tbl_data;
  base_table_->execute(
      context,
      [&base_tbl_data] (int argc, const SValue* argv) -> bool {
    base_tbl_data.emplace_back(argv, argv + argc);
    return true;
  });
  context->incrNumSubtasksCompleted(1);

  List<Vector<SValue>> joined_tbl_data;
  joined_table_->execute(
      context,
      [&joined_tbl_data] (int argc, const SValue* argv) -> bool {
    joined_tbl_data.emplace_back(argv, argv + argc);
    return true;
  });
  context->incrNumSubtasksCompleted(1);

  switch (join_type_) {
    case JoinType::CARTESIAN:
      executeCartesianJoin(
          context,
          fn,
          base_tbl_data,
          joined_tbl_data);
      break;
    default:
      RAISE(kNotYetImplementedError);
  }

  context->incrNumSubtasksCompleted(1);
}

// FIXME max rows
void NestedLoopJoin::executeCartesianJoin(
    ExecutionContext* context,
    Function<bool (int argc, const SValue* argv)> fn,
    const List<Vector<SValue>>& t1,
    const List<Vector<SValue>>& t2) {
  Vector<SValue> outbuf(select_exprs_.size(), SValue{});
  Vector<SValue> inbuf(input_map_.size(), SValue{});

  for (const auto& r1 : t1) {
    for (const auto& r2 : t2) {

      for (size_t i = 0; i < input_map_.size(); ++i) {
        const auto& m = input_map_[i];

        switch (m.table_idx) {
          case 0:
            inbuf[i] = r1[m.column_idx];
            break;
          case 1:
            inbuf[i] = r2[m.column_idx];
            break;
          default:
            RAISE(kRuntimeError, "invalid table index");
        }
      }

      for (int i = 0; i < select_exprs_.size(); ++i) {
        VM::evaluate(
            txn_,
            select_exprs_[i].program(),
            inbuf.size(),
            inbuf.data(),
            &outbuf[i]);
      }

      if (!fn(outbuf.size(), outbuf.data()))  {
        return;
      }
    }
  }
}

Vector<String> NestedLoopJoin::columnNames() const {
  return column_names_;
}

size_t NestedLoopJoin::numColumns() const {
  return column_names_.size();
}

}
