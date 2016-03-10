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
#include <stx/autoref.h>
#include <stx/SHA1.h>
#include <csql/svalue.h>
#include <csql/runtime/ExecutionContext.h>
#include <csql/runtime/Statement.h>
#include <csql/runtime/rowsink.h>
#include <csql/tasks/TaskID.h>

using namespace stx;

namespace csql {

typedef Function<bool (const SValue* argv, int argc)> RowSinkFn;

class ResultCursor {
public:

  virtual void open() = 0;
  virtual int fetch(SValue* row, int row_len) = 0;
  virtual void close() = 0;

  /**
   * Returns true if a call to fetch will not block and false if such a call
   * would block
   */
  virtual bool poll() {
    return true;
  }

  /**
   * Wait for the next row. Calls the provided callback exactly one time once the
   * new row becomes available. The callback may be executed in the current
   * or any other thread.
   */
  virtual void wait(Function<void ()> callback) {
    callback();
  }

};

}
