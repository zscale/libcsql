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
#include <stdlib.h>
#include <stdint.h>

extern "C" {

enum sql_type : uint8_t {
  SQL_NULL = 0,
  SQL_STRING = 1,
  SQL_FLOAT = 2,
  SQL_INTEGER = 3,
  SQL_BOOL = 4,
  SQL_TIMESTAMP = 5,
};

struct sql_txn__ { int unused; };
typedef struct sql_txn__* sql_txn;

struct sql_val__ { int unused; };
typedef struct sql_val__* sql_val;

struct sql_args__ { int unused; };
typedef struct sql_val__* sql_args;

sql_val* sql_getarg(sql_args* args, size_t idx);

bool sql_getint(sql_val* in, int64_t* out);
bool sql_getfloat(sql_val* in, double* out);
bool sql_getstring(sql_val* in, const char** data, size_t* size);
bool sql_getbool(sql_val* in, bool* out);

}

#ifdef __cplusplus
namespace csql {

static const uint32_t kVersionMajor = 0;
static const uint32_t kVersionMinor = 2;
static const uint32_t kVersionPatch = 0;
static const std::string kVersionString = "v0.2.0-dev";

} // namespace csql
#endif
