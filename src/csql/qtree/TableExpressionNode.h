/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>
#include <csql/qtree/QueryTreeNode.h>

using namespace stx;

namespace csql {

class TableExpressionNode : public QueryTreeNode {
public:

  virtual Vector<String> columnNames() const = 0;

  size_t numColumns() const;

  size_t getColumnIndex(const String& column_name) const;

};

} // namespace csql
