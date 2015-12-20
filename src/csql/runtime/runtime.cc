/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/runtime.h>
#include <csql/runtime/groupby.h>
#include <csql/defaults.h>

namespace csql {

ScopedPtr<Transaction> Runtime::newTransaction() {
  return mkScoped(new Transaction());
}

RefPtr<Runtime> Runtime::getDefaultRuntime() {
  auto symbols = mkRef(new SymbolTable());
  installDefaultSymbols(symbols.get());

  return new Runtime(
      stx::thread::ThreadPoolOptions{},
      symbols,
      new QueryBuilder(
          new ValueExpressionBuilder(symbols.get()),
          new TableExpressionBuilder()),
      new QueryPlanBuilder(
          QueryPlanBuilderOptions{},
          symbols.get()));
}

Runtime::Runtime(
    stx::thread::ThreadPoolOptions tpool_opts,
    RefPtr<SymbolTable> symbol_table,
    RefPtr<QueryBuilder> query_builder,
    RefPtr<QueryPlanBuilder> query_plan_builder) :
    tpool_(tpool_opts),
    symbol_table_(symbol_table),
    query_builder_(query_builder),
    query_plan_builder_(query_plan_builder) {}

RefPtr<QueryPlan> Runtime::buildQueryPlan(
    Transaction* ctx,
    const String& query,
    RefPtr<ExecutionStrategy> execution_strategy) {
  /* parse query */
  csql::Parser parser;
  parser.parse(query.data(), query.size());

  /* build query plan */
  auto statements = query_plan_builder_->build(
      parser.getStatements(),
      execution_strategy->tableProvider());

  return buildQueryPlan(
      ctx,
      statements,
      execution_strategy);
}

RefPtr<QueryPlan> Runtime::buildQueryPlan(
    Transaction* ctx,
    Vector<RefPtr<QueryTreeNode>> statements,
    RefPtr<ExecutionStrategy> execution_strategy) {
  for (auto& stmt : statements) {
    stmt = execution_strategy->rewriteQueryTree(stmt);
  }

  Vector<ScopedPtr<Statement>> stmt_exprs;
  for (const auto& stmt : statements) {
    if (dynamic_cast<TableExpressionNode*>(stmt.get())) {
      stmt_exprs.emplace_back(query_builder_->buildTableExpression(
          ctx,
          stmt.asInstanceOf<TableExpressionNode>(),
          execution_strategy->tableProvider(),
          this));

      continue;
    }

    if (dynamic_cast<ChartStatementNode*>(stmt.get())) {
      stmt_exprs.emplace_back(query_builder_->buildChartStatement(
          ctx,
          stmt.asInstanceOf<ChartStatementNode>(),
          execution_strategy->tableProvider(),
          this));

      continue;
    }

    RAISE(
        kRuntimeError,
        "cannot figure out how to execute this query plan");

  }

  return mkRef(new QueryPlan(statements, std::move(stmt_exprs)));
}

void Runtime::executeQuery(
    Transaction* ctx,
    const String& query,
    RefPtr<ExecutionStrategy> execution_strategy,
    RefPtr<ResultFormat> result_format) {
  executeQuery(
      ctx,
      buildQueryPlan(ctx, query, execution_strategy),
      result_format);
}

void Runtime::executeQuery(
    Transaction* ctx,
    RefPtr<QueryPlan> query_plan,
    RefPtr<ResultFormat> result_format) {
  /* execute query and format results */
  csql::ExecutionContext context(&tpool_);
  if (!cachedir_.isEmpty()) {
    context.setCacheDir(cachedir_.get());
  }

  for (int i = 0; i < query_plan->numStatements(); ++i) {
    query_plan->getStatement(i)->prepare(&context);
  }

  result_format->formatResults(query_plan, &context);
}

void Runtime::executeStatement(
    Transaction* ctx,
    Statement* statement,
    ResultList* result) {
  auto table_expr = dynamic_cast<TableExpression*>(statement);
  if (!table_expr) {
    RAISE(kRuntimeError, "statement must be a table expression");
  }

  result->addHeader(table_expr->columnNames());
  executeStatement(
      ctx,
      table_expr,
      [result] (int argc, const csql::SValue* argv) -> bool {
    result->addRow(argv, argc);
    return true;
  });
}

void Runtime::executeStatement(
    Transaction* ctx,
    TableExpression* statement,
    Function<bool (int argc, const SValue* argv)> fn) {
  csql::ExecutionContext context(&tpool_);
  if (!cachedir_.isEmpty()) {
    context.setCacheDir(cachedir_.get());
  }

  statement->prepare(&context);
  statement->execute(&context, fn);
}

void Runtime::executeAggregate(
    Transaction* ctx,
    const RemoteAggregateParams& query,
    RefPtr<ExecutionStrategy> execution_strategy,
    OutputStream* os) {
  Vector<RefPtr<SelectListNode>> outer_select_list;
  for (const auto& e : query.aggregate_expression_list()) {
    csql::Parser parser;
    parser.parseValueExpression(
        e.expression().data(),
        e.expression().size());

    auto stmts = parser.getStatements();
    if (stmts.size() != 1) {
      RAISE(kIllegalArgumentError);
    }

    auto slnode = mkRef(
        new SelectListNode(
            query_plan_builder_->buildValueExpression(stmts[0])));

    if (e.has_alias()) {
      slnode->setAlias(e.alias());
    }

    outer_select_list.emplace_back(slnode);
  }

  Vector<RefPtr<SelectListNode>> inner_select_list;
  for (const auto& e : query.select_expression_list()) {
    csql::Parser parser;
    parser.parseValueExpression(
        e.expression().data(),
        e.expression().size());

    auto stmts = parser.getStatements();
    if (stmts.size() != 1) {
      RAISE(kIllegalArgumentError);
    }

    auto slnode = mkRef(
        new SelectListNode(
            query_plan_builder_->buildValueExpression(stmts[0])));

    if (e.has_alias()) {
      slnode->setAlias(e.alias());
    }

    inner_select_list.emplace_back(slnode);
  }

  Vector<RefPtr<ValueExpressionNode>> group_exprs;
  for (const auto& e : query.group_expression_list()) {
    csql::Parser parser;
    parser.parseValueExpression(e.data(), e.size());

    auto stmts = parser.getStatements();
    if (stmts.size() != 1) {
      RAISE(kIllegalArgumentError);
    }

    group_exprs.emplace_back(
        query_plan_builder_->buildValueExpression(stmts[0]));
  }

  Option<RefPtr<ValueExpressionNode>> where_expr;
  if (query.has_where_expression()) {
    csql::Parser parser;
    parser.parseValueExpression(
        query.where_expression().data(),
        query.where_expression().size());

    auto stmts = parser.getStatements();
    if (stmts.size() != 1) {
      RAISE(kIllegalArgumentError);
    }

    where_expr = Some(query_plan_builder_->buildValueExpression(stmts[0]));
  }

  auto qtree = mkRef(
      new GroupByNode(
          outer_select_list,
          group_exprs,
          new SequentialScanNode(
                query.table_name(),
                inner_select_list,
                where_expr,
                (AggregationStrategy) query.aggregation_strategy())));

  auto expr = query_builder_->buildTableExpression(
      ctx,
      qtree.get(),
      execution_strategy->tableProvider(),
      this);

  auto group_expr = dynamic_cast<GroupByExpression*>(expr.get());
  if (!group_expr) {
    RAISE(kIllegalStateError);
  }

  csql::ExecutionContext context(&tpool_);
  if (!cachedir_.isEmpty()) {
    context.setCacheDir(cachedir_.get());
  }

  group_expr->executeRemote(&context, os);
}

SValue Runtime::evaluateScalarExpression(
    Transaction* ctx,
    ASTNode* expr,
    int argc,
    const SValue* argv) {
  auto val_expr = query_plan_builder_->buildValueExpression(expr);
  auto compiled = query_builder_->buildValueExpression(ctx, val_expr);

  SValue out;
  VM::evaluate(ctx, compiled.program(), argc, argv, &out);
  return out;
}

SValue Runtime::evaluateScalarExpression(
    Transaction* ctx,
    const String& expr,
    int argc,
    const SValue* argv) {
  csql::Parser parser;
  parser.parseValueExpression(expr.data(), expr.size());

  auto stmts = parser.getStatements();
  if (stmts.size() != 1) {
    RAISE(
        kParseError,
        "static expression must consist of exactly one statement");
  }

  auto val_expr = query_plan_builder_->buildValueExpression(stmts[0]);
  auto compiled = query_builder_->buildValueExpression(ctx, val_expr);

  SValue out;
  VM::evaluate(ctx, compiled.program(), argc, argv, &out);
  return out;
}

SValue Runtime::evaluateScalarExpression(
    Transaction* ctx,
    RefPtr<ValueExpressionNode> expr,
    int argc,
    const SValue* argv) {
  auto compiled = query_builder_->buildValueExpression(ctx, expr);

  SValue out;
  VM::evaluate(ctx, compiled.program(), argc, argv, &out);
  return out;
}

SValue Runtime::evaluateScalarExpression(
    Transaction* ctx,
    const ValueExpression& expr,
    int argc,
    const SValue* argv) {
  SValue out;
  VM::evaluate(ctx, expr.program(), argc, argv, &out);
  return out;
}

SValue Runtime::evaluateConstExpression(Transaction* ctx, ASTNode* expr) {
  auto val_expr = query_plan_builder_->buildValueExpression(expr);
  auto compiled = query_builder_->buildValueExpression(ctx, val_expr);

  SValue out;
  VM::evaluate(ctx, compiled.program(), 0, nullptr, &out);
  return out;
}

SValue Runtime::evaluateConstExpression(Transaction* ctx, const String& expr) {
  csql::Parser parser;
  parser.parseValueExpression(expr.data(), expr.size());

  auto stmts = parser.getStatements();
  if (stmts.size() != 1) {
    RAISE(
        kParseError,
        "static expression must consist of exactly one statement");
  }

  auto val_expr = query_plan_builder_->buildValueExpression(stmts[0]);
  auto compiled = query_builder_->buildValueExpression(ctx, val_expr);

  SValue out;
  VM::evaluate(ctx, compiled.program(), 0, nullptr, &out);
  return out;
}

SValue Runtime::evaluateConstExpression(
    Transaction* ctx,
    RefPtr<ValueExpressionNode> expr) {
  auto compiled = query_builder_->buildValueExpression(ctx, expr);

  SValue out;
  VM::evaluate(ctx, compiled.program(), 0, nullptr, &out);
  return out;
}

SValue Runtime::evaluateConstExpression(
    Transaction* ctx,
    const ValueExpression& expr) {
  SValue out;
  VM::evaluate(ctx, expr.program(), 0, nullptr, &out);
  return out;
}

Option<String> Runtime::cacheDir() const {
  return cachedir_;
}

void Runtime::setCacheDir(const String& cachedir) {
  cachedir_ = Some(cachedir);
}

RefPtr<QueryBuilder> Runtime::queryBuilder() const {
  return query_builder_;
}

RefPtr<QueryPlanBuilder> Runtime::queryPlanBuilder() const {
  return query_plan_builder_;
}

TaskScheduler* Runtime::scheduler() {
  return &tpool_;
}


}
