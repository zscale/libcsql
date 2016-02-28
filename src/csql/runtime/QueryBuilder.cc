/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/QueryBuilder.h>
#include <csql/runtime/charts/drawstatement.h>

using namespace stx;

namespace csql {

QueryBuilder::QueryBuilder(
    RefPtr<ValueExpressionBuilder> scalar_exp_builder,
    RefPtr<TableExpressionBuilder> table_exp_builder) :
    scalar_exp_builder_(scalar_exp_builder),
    table_exp_builder_(table_exp_builder) {}

ValueExpression QueryBuilder::buildValueExpression(
    Transaction* ctx,
    RefPtr<ValueExpressionNode> node) {
  return scalar_exp_builder_->compile(ctx, node);
}

ScopedPtr<TableExpression> QueryBuilder::buildTableExpression(
    Transaction* ctx,
    RefPtr<TableExpressionNode> node,
    RefPtr<TableProvider> tables,
    Runtime* runtime) {
  return table_exp_builder_->build(ctx, node.get(), this, tables.get());
}

ScopedPtr<ChartStatement> QueryBuilder::buildChartStatement(
    Transaction* ctx,
    RefPtr<ChartStatementNode> node,
    RefPtr<TableProvider> tables,
    Runtime* runtime) {
  Vector<ScopedPtr<DrawStatement>> draw_statements;

  for (size_t i = 0; i < node->numChildren(); ++i) {
    Vector<ScopedPtr<TableExpression>> union_tables;

    auto draw_stmt_node = node->child(i).asInstanceOf<DrawStatementNode>();
    for (const auto& table : draw_stmt_node->inputTables()) {
      union_tables.emplace_back(
          table_exp_builder_->build(ctx, table, this, tables.get()));
    }

    draw_statements.emplace_back(
        new DrawStatement(ctx, draw_stmt_node, std::move(union_tables), runtime));
  }

  return mkScoped(new ChartStatement(std::move(draw_statements)));
}

} // namespace csql
