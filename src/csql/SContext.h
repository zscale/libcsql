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
#include <csql/csql.h>

using namespace stx;

namespace csql {

class SContext {
public:

  static inline sql_ctx* get(SContext* ctx) {
    return (sql_ctx*)((char*) ctx + offsetof(SContext, ctx_));
  }

protected:
  sql_ctx ctx_;
};

static_assert(
    sizeof(SContext) == sizeof(sql_ctx),
    "libcsql requires the c++ compiler to produce classes without overhead");

} // namespace csql
