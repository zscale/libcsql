/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>

using namespace stx;

extern "C" {

typedef struct {
  uint64_t now_unixmicros;
} sql_ctx;

}

namespace csql {

struct SContext {

  static inline sql_ctx* get(SContext* ctx) {
    return (sql_ctx*)((char*) ctx + offsetof(SContext, ctx_));
  }

protected:
  sql_ctx ctx_;
};

} // namespace csql
