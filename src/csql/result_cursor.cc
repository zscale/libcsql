/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2016 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/result_cursor.h>

using namespace stx;

namespace csql {

ResultCursorList::ResultCursorList(
    Vector<ScopedPtr<ResultCursor>> cursors) :
    cursors_(std::move(cursors)) {}

bool ResultCursorList::next(SValue* row, int row_len) {
  return false;
}

}
