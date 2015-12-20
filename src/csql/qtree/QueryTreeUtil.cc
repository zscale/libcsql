/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth, zScale Technology GmbH
 *
 * libcsql is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/QueryTreeUtil.h>
#include <csql/qtree/ColumnReferenceNode.h>
#include <stx/logging.h>

using namespace stx;

namespace csql {

void QueryTreeUtil::resolveColumns(
    RefPtr<ValueExpressionNode> expr,
    Function<size_t (const String&)> resolver) {
  auto colref = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (colref) {
    colref->setColumnIndex(resolver(colref->fieldName()));
  }

  for (auto& arg : expr->arguments()) {
    resolveColumns(arg, resolver);
  }
}

RefPtr<ValueExpressionNode> QueryTreeUtil::foldConstants(
    RefPtr<ValueExpressionNode> expr) {
  return expr;
}

} // namespace csql
