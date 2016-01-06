/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *   Copyright (c) 2015 Laura Schlimmer
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
#include <stx/inspect.h>
#include <stx/wallclock.h>
#include <csql/expressions/datetime.h>
#include <csql/svalue.h>

namespace csql {
namespace expressions {

static void checkArgs(const char* symbol, int argc, int argc_expected) {
  if (argc != argc_expected) {
    RAISE(
        kRuntimeError,
        "wrong number of arguments for %s. expected: %i, got: %i",
        symbol,
        argc_expected,
        argc);
  }
}

void fromTimestamp(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  checkArgs("FROM_TIMESTAMP", argc, 1);

  SValue tmp = *argv;
  tmp.tryTimeConversion();
  *out = SValue(tmp.getTimestamp());
}

void dateTruncExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  checkArgs("DATE_TRUNC", argc, 2);

  SValue val = argv[1];
  val.tryTimeConversion();

  auto tmp = val.getTimestamp();
  auto time_suffix = argv[0].toString();
  unsigned long long dur = 1;

  if (StringUtil::isNumber(time_suffix.substr(0, 1))) {
    size_t sz;
    dur = std::stoull(time_suffix, &sz);
    time_suffix = time_suffix.substr(sz);
  }

  if (time_suffix == "ms" ||
      time_suffix == "msec" ||
      time_suffix == "msecs" ||
      time_suffix == "millisecond" ||
      time_suffix == "milliseconds") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerMilli * dur)) * kMicrosPerMilli * dur));
    return;
  }

  if (time_suffix == "s" ||
      time_suffix == "sec" ||
      time_suffix == "secs" ||
      time_suffix == "second" ||
      time_suffix == "seconds") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerSecond * dur)) * kMicrosPerSecond * dur));
    return;
  }

  if (time_suffix == "m" ||
      time_suffix == "min" ||
      time_suffix == "mins" ||
      time_suffix == "minute" ||
      time_suffix == "minutes") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerMinute * dur)) * kMicrosPerMinute * dur));
    return;
  }

  if (time_suffix == "h" ||
      time_suffix == "hour" ||
      time_suffix == "hours") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerHour * dur)) * kMicrosPerHour * dur));
    return;
  }

  if (time_suffix == "d" ||
      time_suffix == "day" ||
      time_suffix == "days") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerDay * dur)) * kMicrosPerDay * dur));
    return;
  }

  if (time_suffix == "w" ||
      time_suffix == "week" ||
      time_suffix == "weeks") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerWeek * dur)) * kMicrosPerWeek * dur));
    return;
  }

  if (time_suffix == "m" ||
      time_suffix == "month" ||
      time_suffix == "months") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerDay * 31 * dur)) * kMicrosPerDay * 31 * dur));
    return;
  }

  if (time_suffix == "y" ||
      time_suffix == "year" ||
      time_suffix == "years") {
    *out = SValue(SValue::TimeType(
        (uint64_t(tmp) / (kMicrosPerYear * dur)) * kMicrosPerYear * dur));
    return;
  }

  RAISE(
      kRuntimeError,
      "unknown time precision %s",
      time_suffix.c_str());
}

void dateAddExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  checkArgs("DATE_ADD", argc, 3);

  SValue val = argv[0];
  val.tryTimeConversion();

  auto date = val.getTimestamp();
  auto unit = argv[2].toString();
  StringUtil::toLower(&unit);

  if (unit == "second") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerSecond)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerSecond)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "minute") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerMinute)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerMinute)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "hour") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerHour)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerHour)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "day") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerDay)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerDay)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "week") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerWeek)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerWeek)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "month") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerDay * 31)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerDay * 31)));
          return;

        default:
          break;
      }
    }
    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "year") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getInteger() * kMicrosPerYear)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) + (argv[1].getFloat() * kMicrosPerYear)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  auto expr = argv[1].toString();
  if (unit == "minute_second") {
    auto values = StringUtil::split(expr, ":");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) +
            (std::stoull(values[0]) * kMicrosPerMinute) +
            (std::stoull(values[1]) * kMicrosPerSecond)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "hour_second") {
    auto values = StringUtil::split(expr, ":");
    if (values.size() == 3 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1]) &&
        StringUtil::isNumber(values[2])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) +
            (std::stoull(values[0]) * kMicrosPerHour) +
            (std::stoull(values[1]) * kMicrosPerMinute) +
            (std::stoull(values[2]) * kMicrosPerSecond)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "hour_minute") {
    auto values = StringUtil::split(expr, ":");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) +
            (std::stoull(values[0]) * kMicrosPerHour) +
            (std::stoull(values[1]) * kMicrosPerMinute)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "day_second") {
    auto values = StringUtil::split(expr, " ");
    if (values.size() == 2 && StringUtil::isNumber(values[0])) {

      auto time_values = StringUtil::split(values[1], ":");
      if (time_values.size() == 3 &&
          StringUtil::isNumber(time_values[0]) &&
          StringUtil::isNumber(time_values[1]) &&
          StringUtil::isNumber(time_values[2])) {

        try {
          *out = SValue(SValue::TimeType(
              uint64_t(date) +
              (std::stoull(values[0]) * kMicrosPerDay) +
              (std::stoull(time_values[0]) * kMicrosPerHour) +
              (std::stoull(time_values[1]) * kMicrosPerMinute) +
              (std::stoull(time_values[2]) * kMicrosPerSecond)));
          return;
        } catch (std::invalid_argument e) {
          /* fallthrough */
        }
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "day_minute") {
    auto values = StringUtil::split(expr, " ");
    if (values.size() == 2 && StringUtil::isNumber(values[0])) {

      auto time_values = StringUtil::split(values[1], ":");
      if (time_values.size() == 2 &&
          StringUtil::isNumber(time_values[0]) &&
          StringUtil::isNumber(time_values[1])) {

        try {
          *out = SValue(SValue::TimeType(
              uint64_t(date) +
              (std::stoull(values[0]) * kMicrosPerDay) +
              (std::stoull(time_values[0]) * kMicrosPerHour) +
              (std::stoull(time_values[1]) * kMicrosPerMinute)));
          return;
        } catch (std::invalid_argument e) {
          /* fallthrough */
        }
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "day_hour") {
    auto values = StringUtil::split(expr, " ");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) +
            (std::stoull(values[0]) * kMicrosPerDay) +
            (std::stoull(values[1]) * kMicrosPerHour)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "year_month") {
    auto values = StringUtil::split(expr, "-");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) +
            (std::stoull(values[0]) * kMicrosPerYear) +
            (std::stoull(values[1]) * kMicrosPerDay * 31)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_ADD: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  RAISEF(
      kRuntimeError,
      "DATE_ADD: invalid unit $0",
      argv[2].toString());
}

void dateSubExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  checkArgs("DATE_SUB", argc, 3);

  SValue val = argv[0];
  val.tryTimeConversion();

  auto date = val.getTimestamp();
  auto unit = argv[2].toString();
  StringUtil::toLower(&unit);

  if (unit == "second") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerSecond)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerSecond)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "minute") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerMinute)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerMinute)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "hour") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerHour)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerHour)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "day") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerDay)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerDay)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "week") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerWeek)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerWeek)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "month") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerDay * 31)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerDay * 31)));
          return;

        default:
          break;
      }
    }
    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  if (unit == "year") {
    if (argv[1].tryNumericConversion()) {
      switch (argv[1].getType()) {
        case SQL_INTEGER:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getInteger() * kMicrosPerYear)));
          return;

        case SQL_FLOAT:
          *out = SValue(SValue::TimeType(
              uint64_t(date) - (argv[1].getFloat() * kMicrosPerYear)));
          return;

        default:
          break;
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        argv[1].toString(),
        argv[2].toString());
  }

  auto expr = argv[1].toString();
  if (unit == "minute_second") {
    auto values = StringUtil::split(expr, ":");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) -
            (std::stoull(values[0]) * kMicrosPerMinute) +
            (std::stoull(values[1]) * kMicrosPerSecond)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "hour_second") {
    auto values = StringUtil::split(expr, ":");
    if (values.size() == 3 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1]) &&
        StringUtil::isNumber(values[2])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) -
            (std::stoull(values[0]) * kMicrosPerHour) +
            (std::stoull(values[1]) * kMicrosPerMinute) +
            (std::stoull(values[2]) * kMicrosPerSecond)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "hour_minute") {
    auto values = StringUtil::split(expr, ":");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) -
            (std::stoull(values[0]) * kMicrosPerHour) +
            (std::stoull(values[1]) * kMicrosPerMinute)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "day_second") {
    auto values = StringUtil::split(expr, " ");
    if (values.size() == 2 && StringUtil::isNumber(values[0])) {

      auto time_values = StringUtil::split(values[1], ":");
      if (time_values.size() == 3 &&
          StringUtil::isNumber(time_values[0]) &&
          StringUtil::isNumber(time_values[1]) &&
          StringUtil::isNumber(time_values[2])) {

        try {
          *out = SValue(SValue::TimeType(
              uint64_t(date) -
              (std::stoull(values[0]) * kMicrosPerDay) +
              (std::stoull(time_values[0]) * kMicrosPerHour) +
              (std::stoull(time_values[1]) * kMicrosPerMinute) +
              (std::stoull(time_values[2]) * kMicrosPerSecond)));
          return;
        } catch (std::invalid_argument e) {
          /* fallthrough */
        }
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "day_minute") {
    auto values = StringUtil::split(expr, " ");
    if (values.size() == 2 && StringUtil::isNumber(values[0])) {

      auto time_values = StringUtil::split(values[1], ":");
      if (time_values.size() == 2 &&
          StringUtil::isNumber(time_values[0]) &&
          StringUtil::isNumber(time_values[1])) {

        try {
          *out = SValue(SValue::TimeType(
              uint64_t(date) -
              (std::stoull(values[0]) * kMicrosPerDay) +
              (std::stoull(time_values[0]) * kMicrosPerHour) +
              (std::stoull(time_values[1]) * kMicrosPerMinute)));
          return;
        } catch (std::invalid_argument e) {
          /* fallthrough */
        }
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "day_hour") {
    auto values = StringUtil::split(expr, " ");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) -
            (std::stoull(values[0]) * kMicrosPerDay) +
            (std::stoull(values[1]) * kMicrosPerHour)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  if (unit == "year_month") {
    auto values = StringUtil::split(expr, "-");
    if (values.size() == 2 &&
        StringUtil::isNumber(values[0]) &&
        StringUtil::isNumber(values[1])) {

      try {
        *out = SValue(SValue::TimeType(
            uint64_t(date) -
            (std::stoull(values[0]) * kMicrosPerYear) +
            (std::stoull(values[1]) * kMicrosPerDay * 31)));
        return;
      } catch (std::invalid_argument e) {
        /* fallthrough */
      }
    }

    RAISEF(
        kRuntimeError,
        "DATE_SUB: invalid expression $0 for unit $1",
        expr,
        argv[2].toString());
  }

  RAISEF(
      kRuntimeError,
      "DATE_SUB: invalid unit $0",
      argv[2].toString());
}

void timeAtExpr(sql_txn* ctx, int argc, SValue* argv, SValue* out) {
  checkArgs("TIME_AT", argc, 1);

  SValue time_val = argv[0];
  try {
    if (time_val.tryTimeConversion()) {
      *out = SValue(time_val.getTimestamp());
      return;
    }
  } catch (...) {
    /* fallthrough */
  }

  String time_interval = time_val.toString();
  StringUtil::toLower(&time_interval);

  if (time_interval == "now") {
    *out = SValue(SValue::TimeType(WallClock::now()));
    return;
  }

  if (StringUtil::beginsWith(time_interval, "-")) {
    try {
      Option<uint64_t> time_val = timeFromNow(time_interval.substr(1));
      if (!time_val.isEmpty()) {
        *out = SValue(SValue::TimeType(time_val.get()));
        return;
      }

    } catch (...) {
      RAISEF(
        kRuntimeError,
        "TIME_AT: invalid argument $0",
        time_interval);
    }
  }

  if (StringUtil::endsWith(time_interval, "ago")) {
    try {
      Option<uint64_t> time_val = timeFromNow(
          time_interval.substr(0, time_interval.length() - 4));
      if (!time_val.isEmpty()) {
        *out = SValue(SValue::TimeType(time_val.get()));
        return;
      }

    } catch (...) {
      RAISEF(
        kRuntimeError,
        "TIME_AT: invalid argument $0",
        time_interval);
    }
  }

  RAISEF(
      kRuntimeError,
      "TIME_AT: invalid argument $0",
      time_interval);
}

Option<uint64_t> timeFromNow(String time_interval) {
  unsigned long long num;
  String unit;

  try {
    size_t sz;
    num = std::stoull(time_interval, &sz);
    unit = time_interval.substr(sz);
    StringUtil::toLower(&unit);

  } catch (std::invalid_argument e) {
    RAISEF(
      kRuntimeError,
      "TIME_AT: invalid argument $0",
      time_interval);
  }


  auto now = uint64_t(WallClock::now());
  if (unit == "sec" ||
      unit == "secs" ||
      unit == "second" ||
      unit == "seconds") {
    return Some(now - num * kMicrosPerSecond);
  }

  if (unit == "min" ||
      unit == "mins" ||
      unit == "minute" ||
      unit == "minutes") {
    return Some(now - num * kMicrosPerMinute);
  }

  if (unit == "h" ||
      unit == "hour" ||
      unit == "hours") {
    return Some(now - num * kMicrosPerHour);
  }

  if (unit == "d" ||
      unit == "day" ||
      unit == "days") {
    return Some(now - num * kMicrosPerDay);
  }

  if (unit == "w" ||
      unit == "week" ||
      unit == "weeks") {
    return Some(now - num * kMicrosPerWeek);
  }

  if (unit == "month" ||
      unit == "months") {
    return Some(now - num * kMicrosPerDay * 31);
  }

  if (unit == "y" ||
      unit == "year" ||
      unit == "years") {
    return Some(now - num * kMicrosPerYear);
  }

  return None<uint64_t>();
}


}
}
