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
#include <csql/tasks/Task.h>
#include <csql/runtime/defaultruntime.h>

namespace csql {

class Select : public Task {
public:

  Select(
      Transaction* txn,
      Vector<ValueExpression> select_expressions,
      RowSinkFn output);

  void onInputsReady() override;

protected:
  Transaction* txn_;
  Vector<ValueExpression> select_exprs_;
  RowSinkFn output_;
};

class SelectFactory : public TaskFactory {
public:

  SelectFactory(
      Vector<RefPtr<SelectListNode>> select_exprs);

  RefPtr<Task> build(
      Transaction* txn,
      RowSinkFn output) const override;

protected:
  Vector<RefPtr<SelectListNode>> select_exprs_;
};

}
