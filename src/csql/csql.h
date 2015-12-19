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

typedef struct {
  uint64_t now_unixmicros_;
} sql_txn;

typedef struct {
  sql_type type;
  union {
    int64_t t_integer;
    double t_float;
    bool t_bool;
    uint64_t t_timestamp;
    struct {
      char* ptr;
      uint32_t len;
    } t_string;
  } u;
} sql_val;

}

#ifdef __cplusplus
namespace csql {

static const uint32_t kVersionMajor = 0;
static const uint32_t kVersionMinor = 2;
static const uint32_t kVersionPatch = 0;
static const std::string kVersionString = "v0.2.0-dev";

} // namespace csql
#endif
