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
#include <stx/autoref.h>
#include <csql/runtime/tablerepository.h>
#include <csql/runtime/charts/drawstatement.h>

namespace csql {
class Runtime;

class QueryPlan : public RefCounted  {
public:

  QueryPlan(
      Vector<RefPtr<QueryTreeNode>> qtrees,
      Vector<ScopedPtr<Statement>> statements);

  size_t numStatements() const;

  Statement* getStatement(size_t stmt_idx) const;
  RefPtr<QueryTreeNode> getStatementQTree(size_t stmt_idx) const;

protected:
  Vector<RefPtr<QueryTreeNode>> qtrees_;
  Vector<ScopedPtr<Statement>> statements_;
};

class ExecutionPlan : public RefCounted {
public:

  virtual ~ExecutionPlan() {}
  virtual void execute(Function<bool (int argc, const SValue* argv)> fn) = 0;

};

}
