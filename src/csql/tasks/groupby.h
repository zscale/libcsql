/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>
#include <stx/SHA1.h>
#include <csql/tasks/Task.h>
#include <csql/runtime/defaultruntime.h>

namespace csql {

class GroupBy : public Task {
public:

  GroupBy(
      Transaction* txn,
      Vector<ValueExpression> select_expressions,
      Vector<ValueExpression> group_expressions,
      RowSinkFn output,
      SHA1Hash qtree_fingerprint);

  bool onInputRow(
      const TaskID& input_id,
      const SValue* row,
      int row_len) override;

  void onInputsReady() override;

protected:

  void freeResult();

  Transaction* txn_;
  Vector<ValueExpression> select_exprs_;
  Vector<ValueExpression> group_exprs_;
  RowSinkFn output_;
  SHA1Hash qtree_fingerprint_;
  HashMap<String, Vector<VM::Instance>> groups_;
  ScratchMemory scratch_;
};

//class RemoteGroupBy : public GroupByExpression {
//public:
//  typedef
//      Function<ScopedPtr<InputStream> (const RemoteAggregateParams& params)>
//      RemoteExecuteFn;
//
//  RemoteGroupBy(
//      Transaction* txn,
//      const Vector<String>& column_names,
//      Vector<ValueExpression> select_expressions,
//      const RemoteAggregateParams& params,
//      RemoteExecuteFn execute_fn);
//
//  void accumulate(
//      HashMap<String, Vector<VM::Instance >>* groups,
//      ScratchMemory* scratch,
//      ExecutionContext* context) override;
//
//protected:
//  RemoteAggregateParams params_;
//  RemoteExecuteFn execute_fn_;
//};
//
//class GroupByMerge : public Task {
//public:
//
//  GroupByMerge(Vector<ScopedPtr<GroupByExpression>> sources);
//
//  Vector<String> columnNames() const override;
//
//  size_t numColumns() const override;
//
//protected:
//  Vector<ScopedPtr<GroupByExpression>> sources_;
//};

}
