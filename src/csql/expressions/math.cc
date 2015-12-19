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
#include <assert.h>
#include <math.h>
#include <string.h>
#include <csql/expressions/math.h>
#include <csql/Transaction.h>

namespace csql {
namespace expressions {

void addExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  if (argc != 2) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for add. expected: 2, got: %i", argc);
  }

  SValue* lhs = argv;
  SValue* rhs = argv + 1;

  switch(lhs->testTypeWithNumericConversion()) {
    case SQL_INTEGER:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((int64_t) (lhs->getInteger() + rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getInteger() + rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_FLOAT:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((double) (lhs->getFloat() + rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getFloat() + rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_NULL:
      *out = SValue();
      return;
    default:
      break;
  }

  *out = SValue(lhs->toString() + rhs->toString());
}

void subExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  if (argc != 2) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for sub. expected: 2, got: %i", argc);
  }

  SValue* lhs = argv;
  SValue* rhs = argv + 1;

  switch(lhs->testTypeWithNumericConversion()) {
    case SQL_INTEGER:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((int64_t) (lhs->getInteger() - rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getInteger() - rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_FLOAT:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((double) (lhs->getFloat() - rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getFloat() - rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_NULL:
      *out = SValue();
      return;
    default:
      break;
  }

  RAISE(kRuntimeError, "can't subtract %s and %s",
      lhs->getTypeName(),
      rhs->getTypeName());
}

void mulExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  if (argc != 2) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for mul. expected: 2, got: %i", argc);
  }

  SValue* lhs = argv;
  SValue* rhs = argv + 1;

  switch(lhs->testTypeWithNumericConversion()) {
    case SQL_INTEGER:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((int64_t) (lhs->getInteger() * rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getInteger() * rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_FLOAT:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((double) (lhs->getFloat() * rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getFloat() * rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_NULL:
      *out = SValue();
      return;
    default:
      break;
  }

  RAISE(kRuntimeError, "can't multiply %s and %s",
      lhs->getTypeName(),
      rhs->getTypeName());
}

void divExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  if (argc != 2) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for div. expected: 2, got: %i", argc);
  }

  SValue* lhs = argv;
  SValue* rhs = argv + 1;

  switch(lhs->testTypeWithNumericConversion()) {
    case SQL_INTEGER:
    case SQL_FLOAT:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
        case SQL_FLOAT:
          *out = SValue((double) (lhs->getFloat() / rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_NULL:
      *out = SValue();
      return;
    default:
      break;
  }

  RAISE(kRuntimeError, "can't divide %s and %s",
      lhs->getTypeName(),
      rhs->getTypeName());
}

void modExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  if (argc != 2) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for mod. expected: 2, got: %i", argc);
  }

  SValue* lhs = argv;
  SValue* rhs = argv + 1;

  switch(lhs->testTypeWithNumericConversion()) {
    case SQL_INTEGER:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((int64_t) (lhs->getInteger() % rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue(fmod(lhs->getInteger(), rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_FLOAT:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue(fmod(lhs->getFloat(), rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue(fmod(lhs->getFloat(), rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_NULL:
      *out = SValue();
      return;
    default:
      break;
  }

  RAISE(kRuntimeError, "can't modulo %s and %s",
      lhs->getTypeName(),
      rhs->getTypeName());
}

void powExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  if (argc != 2) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for pow. expected: 2, got: %i", argc);
  }

  SValue* lhs = argv;
  SValue* rhs = argv + 1;

  switch(lhs->testTypeWithNumericConversion()) {
    case SQL_INTEGER:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((int64_t) pow(lhs->getInteger(), rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) pow(lhs->getInteger(), rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_FLOAT:
      switch(rhs->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
          *out = SValue((double) pow(lhs->getFloat(), rhs->getInteger()));
          return;
        case SQL_FLOAT:
          *out = SValue((double) pow(lhs->getFloat(), rhs->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          break;
      }
      break;
    case SQL_NULL:
      *out = SValue();
      return;
    default:
      break;
  }

  RAISE(kRuntimeError, "can't pow %s and %s",
      lhs->getTypeName(),
      rhs->getTypeName());
}

void roundExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  switch (argc) {

    // round to integer
    case 1:
      RAISE(kNotYetImplementedError);

    // round to arbitrary precision float
    case 2:
      RAISE(kNotYetImplementedError);

    default:
      RAISE(
          kRuntimeError,
          "wrong number of arguments for ROUND. expected: 1 or 2, got: %i", argc);
  }
}

void truncateExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  switch (argc) {

    // truncate to integer
    case 1: {
      SValue* val = argv;
      switch(val->testTypeWithNumericConversion()) {
        case SQL_INTEGER:
        case SQL_FLOAT:
          *out = SValue(SValue::IntegerType(val->getFloat()));
          return;
        case SQL_NULL:
          *out = SValue();
          return;
        default:
          RAISE(kRuntimeError, "can't TRUNCATE %s", val->getTypeName());
      }
    }

    // truncate to specified number of decimal places
    case 2:
      RAISE(kNotYetImplementedError);

    default:
      RAISE(
          kRuntimeError,
          "wrong number of arguments for TRUNCATE. expected: 1 or 2, got: %i", argc);
  }
}


}
}
