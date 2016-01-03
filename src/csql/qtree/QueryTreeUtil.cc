/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth, zScale Technology GmbH
 *
 * libcsql is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/runtime.h>
#include <csql/qtree/QueryTreeUtil.h>
#include <csql/qtree/ColumnReferenceNode.h>
#include <stx/logging.h>

using namespace stx;

namespace csql {

void QueryTreeUtil::resolveColumns(
    RefPtr<ValueExpressionNode> expr,
    Function<size_t (const String&)> resolver) {
  auto colref = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (colref && !colref->fieldName().empty()) {
    auto idx = resolver(colref->fieldName());
    if (idx == size_t(-1)) {
      RAISEF(kRuntimeError, "column not found: '$0'", colref->fieldName());
    }

    colref->setColumnIndex(idx);
  }

  for (auto& arg : expr->arguments()) {
    resolveColumns(arg, resolver);
  }
}

void QueryTreeUtil::findColumns(
    RefPtr<ValueExpressionNode> expr,
    Function<void (const RefPtr<ColumnReferenceNode>&)> fn) {
  auto colref = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (colref) {
    fn(colref);
  }

  for (auto& arg : expr->arguments()) {
    findColumns(arg, fn);
  }
}

RefPtr<ValueExpressionNode> QueryTreeUtil::foldConstants(
    Transaction* txn,
    RefPtr<ValueExpressionNode> expr) {
  if (isConstantExpression(txn, expr)) {
    auto runtime = txn->getRuntime();
    auto const_val = runtime->evaluateConstExpression(txn, expr);

    return new LiteralExpressionNode(const_val);
  } else {
    return expr;
  }
}

bool QueryTreeUtil::isConstantExpression(
    Transaction* txn,
    RefPtr<ValueExpressionNode> expr) {
  if (dynamic_cast<ColumnReferenceNode*>(expr.get())) {
    return false;
  }

  auto call_expr = dynamic_cast<CallExpressionNode*>(expr.get());
  if (call_expr) {
    auto symbol = txn->getSymbolTable()->lookup(call_expr->symbol());
    if (symbol.isAggregate()) {
      return false;
    }
  }

  for (const auto& arg : expr->arguments()) {
    if (!isConstantExpression(txn, arg)) {
      return false;
    }
  }

  return true;
}

RefPtr<ValueExpressionNode> QueryTreeUtil::prunePredicateExpression(
    RefPtr<ValueExpressionNode> expr,
    const Set<String>& column_whitelist) {
  auto call_expr = dynamic_cast<CallExpressionNode*>(expr.get());
  if (call_expr && call_expr->symbol() == "logical_and") {
    return new CallExpressionNode(
        "logical_and",
        Vector<RefPtr<ValueExpressionNode>> {
          prunePredicateExpression(call_expr->arguments()[0], column_whitelist),
          prunePredicateExpression(call_expr->arguments()[1], column_whitelist),
        });
  }

  bool is_invalid = false;
  findColumns(expr, [&] (const RefPtr<ColumnReferenceNode>& col) {
    const auto& col_name = col->columnName();
    if (!col_name.empty() && column_whitelist.count(col_name) == 0) {
      is_invalid = true;
    }
  });

  if (is_invalid) {
    return new LiteralExpressionNode(SValue(SValue::BoolType(true)));
  } else {
    return expr;
  }
}

} // namespace csql
