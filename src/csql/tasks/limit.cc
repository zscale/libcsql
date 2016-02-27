/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/tasks/limit.h>

namespace csql {

Limit::Limit(
    size_t limit,
    size_t offset,
    RowSinkFn output) :
    limit_(limit),
    offset_(offset),
    output_(output),
    counter_(0) {}

bool Limit::onInputRow(
    const TaskID& input_id,
    const SValue* row,
    int row_len) {
  if (counter_++ < offset_) {
    return true;
  }

  if (counter_ > (offset_ + limit_)) {
    return false;
  }

  return output_(row, row_len);
}

LimitFactory::LimitFactory(
    size_t limit,
    size_t offset) :
    limit_(limit),
    offset_(offset) {}

RefPtr<Task> LimitFactory::build(
    Transaction* txn,
    RowSinkFn output) const {
  return new Limit(limit_, offset_, output);
}

}
