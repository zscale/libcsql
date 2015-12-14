/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#ifndef _FNORDMETRIC_SQL_RUNTIME_H
#define _FNORDMETRIC_SQL_RUNTIME_H
#include <stdlib.h>
#include <string>
#include <vector>
#include <memory>
#include <stx/thread/threadpool.h>
#include <csql/parser/parser.h>
#include <csql/qtree/RemoteAggregateParams.pb.h>
#include <csql/runtime/queryplan.h>
#include <csql/runtime/queryplanbuilder.h>
#include <csql/runtime/QueryBuilder.h>
#include <csql/runtime/symboltable.h>
#include <csql/runtime/ResultFormat.h>
#include <csql/runtime/ExecutionStrategy.h>
#include <csql/runtime/resultlist.h>

namespace csql {

class Runtime : public RefCounted {
public:

  static RefPtr<Runtime> getDefaultRuntime();

  // FIXPAUL: make parser configurable via parserfactory
  Runtime(
      stx::thread::ThreadPoolOptions tpool_opts,
      RefPtr<SymbolTable> symbol_table,
      RefPtr<QueryBuilder> query_builder,
      RefPtr<QueryPlanBuilder> query_plan_builder);

  RefPtr<QueryPlan> buildQueryPlan(
      const String& query,
      RefPtr<ExecutionStrategy> execution_strategy);

  RefPtr<QueryPlan> buildQueryPlan(
      Vector<RefPtr<csql::QueryTreeNode>> statements,
      RefPtr<ExecutionStrategy> execution_strategy);

  void executeQuery(
      const String& query,
      RefPtr<ExecutionStrategy> execution_strategy,
      RefPtr<ResultFormat> result_format);

  void executeQuery(
      RefPtr<QueryPlan> query_plan,
      RefPtr<ResultFormat> result_format);

  void executeStatement(
      Statement* statement,
      ResultList* result);

  void executeStatement(
      TableExpression* statement,
      Function<bool (int argc, const SValue* argv)> fn);

  SValue evaluateScalarExpression(
      const String& expr,
      int argc,
      const SValue* argv);

  SValue evaluateScalarExpression(
      ASTNode* expr,
      int argc,
      const SValue* argv);

  SValue evaluateScalarExpression(
      RefPtr<ValueExpressionNode> expr,
      int argc,
      const SValue* argv);

  SValue evaluateScalarExpression(
      const ValueExpression& expr,
      int argc,
      const SValue* argv);

  SValue evaluateConstExpression(const String& expr);
  SValue evaluateConstExpression(ASTNode* expr);
  SValue evaluateConstExpression(RefPtr<ValueExpressionNode> expr);
  SValue evaluateConstExpression(const ValueExpression& expr);

  void executeAggregate(
      const RemoteAggregateParams& query,
      RefPtr<ExecutionStrategy> execution_strategy,
      OutputStream* os);

  Option<String> cacheDir() const;
  void setCacheDir(const String& cachedir);

  RefPtr<QueryBuilder> queryBuilder() const;
  RefPtr<QueryPlanBuilder> queryPlanBuilder() const;

  TaskScheduler* scheduler();

protected:
  thread::ThreadPool tpool_;
  RefPtr<SymbolTable> symbol_table_;
  RefPtr<QueryBuilder> query_builder_;
  RefPtr<QueryPlanBuilder> query_plan_builder_;
  Option<String> cachedir_;
};

}
#endif
