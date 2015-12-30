/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/TableExpressionNode.h>

using namespace stx;

namespace csql {

size_t TableExpressionNode::numColumns() const {
  return columnNames().size();
}

size_t TableExpressionNode::getColumnIndex(const String& column_name) const {
  auto cols = columnNames();

  for (int i = 0; i < cols.size(); ++i) {
    if (cols[i] == column_name) {
      return i;
    }
  }

  return -1;
}

} // namespace csql
