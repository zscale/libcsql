/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <stdlib.h>
#include <csql/runtime/compiler.h>
#include <csql/runtime/ValueExpressionBuilder.h>

namespace csql {

ValueExpressionBuilder::ValueExpressionBuilder(
    SymbolTable* symbol_table) :
    symbol_table_(symbol_table) {}

ValueExpression ValueExpressionBuilder::compile(
    Transaction* ctx,
    RefPtr<ValueExpressionNode> node) {
  return ValueExpression(Compiler::compile(ctx, node, symbol_table_));
}

}
