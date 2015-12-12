/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth, zScale Technology GmbH
 *
 * libcsql is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>
#include <stx/autoref.h>
#include <csql/qtree/ValueExpressionNode.h>

using namespace stx;

namespace csql {

class QueryTreeUtil {
public:

  /**
   * Walks the provided value expression and calls the provided resolver
   * function for each unresolved column name. The resolver must return a column
   * index for each column name
   *
   * This method will modify the provided expression in place
   */
  static void resolveColumns(
      RefPtr<ValueExpressionNode> expr,
      Function<size_t (const String&)> resolver);

};


} // namespace csql
