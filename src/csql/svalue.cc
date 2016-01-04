/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdlib.h>
#include <string>
#include <ctime>
#include <stdint.h>
#include <stx/inspect.h>
#include <stx/human.h>
#include <csql/svalue.h>
#include <csql/format.h>
#include <csql/parser/token.h>

namespace csql {

SValue::SValue() {
  memset(&data_, 0, sizeof(data_));
  data_.type = SQL_NULL;
}

SValue::~SValue() {
  switch (data_.type) {

    case SQL_STRING:
      free(data_.u.t_string.ptr);
      break;

    default:
      break;

  }
}

SValue::SValue(const SValue::StringType& string_value) {
  data_.type = SQL_STRING;
  data_.u.t_string.len = string_value.size();
  data_.u.t_string.ptr = static_cast<char *>(malloc(data_.u.t_string.len));

  if (data_.u.t_string.ptr == nullptr) {
    RAISE(kRuntimeError, "could not allocate SValue");
  }

  memcpy(
      data_.u.t_string.ptr,
      string_value.data(),
      data_.u.t_string.len);
}

SValue::SValue(
    char const* string_value) :
    SValue(std::string(string_value)) {}

SValue::SValue(SValue::IntegerType integer_value) {
  data_.type = SQL_INTEGER;
  data_.u.t_integer = integer_value;
}

SValue::SValue(SValue::FloatType float_value) {
  data_.type = SQL_FLOAT;
  data_.u.t_float = float_value;
}

SValue::SValue(SValue::BoolType bool_value) {
  data_.type = SQL_BOOL;
  data_.u.t_bool = bool_value;
}

SValue::SValue(SValue::TimeType time_value) {
  data_.type = SQL_TIMESTAMP;
  data_.u.t_timestamp = static_cast<uint64_t>(time_value) / kMicrosPerSecond;
}

SValue::SValue(const SValue& copy) {
  switch (copy.data_.type) {

    case SQL_STRING:
      data_.type = SQL_STRING;
      data_.u.t_string.len = copy.data_.u.t_string.len;
      data_.u.t_string.ptr = static_cast<char *>(malloc(data_.u.t_string.len));

      if (data_.u.t_string.ptr == nullptr) {
        RAISE(kRuntimeError, "could not allocate SValue");
      }

      memcpy(
          data_.u.t_string.ptr,
          copy.data_.u.t_string.ptr,
          data_.u.t_string.len);
      break;

    default:
      memcpy(&data_, &copy.data_, sizeof(data_));
      break;

  }

}

SValue& SValue::operator=(const SValue& copy) {
  switch (data_.type) {

    case SQL_STRING:
      free(data_.u.t_string.ptr);
      break;

    default:
      break;

  }

  switch (copy.data_.type) {

    case SQL_STRING:
      data_.type = SQL_STRING;
      data_.u.t_string.len = copy.data_.u.t_string.len;
      data_.u.t_string.ptr = static_cast<char *>(malloc(data_.u.t_string.len));

      if (data_.u.t_string.ptr == nullptr) {
        RAISE(kRuntimeError, "could not allocate SValue");
      }

      memcpy(
          data_.u.t_string.ptr,
          copy.data_.u.t_string.ptr,
          data_.u.t_string.len);
      break;

    default:
      memcpy(&data_, &copy.data_, sizeof(data_));
      break;

  }

  return *this;
}

bool SValue::operator==(const SValue& other) const {
  switch (data_.type) {

    case SQL_INTEGER: {
      return getInteger() == other.getInteger();
    }

    case SQL_TIMESTAMP: {
      return getInteger() == other.getInteger();
    }

    case SQL_FLOAT: {
      return getFloat() == other.getFloat();
    }

    case SQL_BOOL: {
      return getBool() == other.getBool();
    }

    case SQL_STRING: {
      return memcmp(
          data_.u.t_string.ptr,
          other.data_.u.t_string.ptr,
          data_.u.t_string.len) == 0;
    }

    case SQL_NULL: {
      return other.getInteger() == 0;
    }

  }
}

sql_type SValue::getType() const {
  return data_.type;
}

SValue::IntegerType SValue::getInteger() const {
  switch (data_.type) {

    case SQL_INTEGER:
      return data_.u.t_integer;

    case SQL_TIMESTAMP:
      return data_.u.t_timestamp;

    case SQL_FLOAT:
      return data_.u.t_float;

    case SQL_BOOL:
      return data_.u.t_bool;

    case SQL_NULL:
      return 0;

    case SQL_STRING:
      try {
        return std::stol(getString());
      } catch (std::exception e) {
        /* fallthrough */
      }

    default:
      RAISE(
          kTypeError,
          "can't convert %s '%s' to Integer",
          SValue::getTypeName(data_.type),
          toString().c_str());

  }

  return 0;
}

SValue::IntegerType SValue::toInteger() const {
  return getInteger();
}

SValue::FloatType SValue::getFloat() const {
  switch (data_.type) {

    case SQL_INTEGER:
      return data_.u.t_integer;

    case SQL_TIMESTAMP:
      return data_.u.t_timestamp;

    case SQL_FLOAT:
      return data_.u.t_float;

    case SQL_BOOL:
      return data_.u.t_bool;

    case SQL_NULL:
      return 0;

    case SQL_STRING:
      try {
        return std::stod(getString());
      } catch (std::exception e) {
        /* fallthrough */
      }

    default:
      RAISE(
          kTypeError,
          "can't convert %s '%s' to Float",
          SValue::getTypeName(data_.type),
          toString().c_str());

  }

  return 0;
}

SValue::FloatType SValue::toFloat() const {
  return getFloat();
}

SValue::BoolType SValue::getBool() const {
  switch (data_.type) {

    case SQL_INTEGER:
      return getInteger() > 0;

    case SQL_FLOAT:
      return getFloat() > 0;

    case SQL_BOOL:
      return data_.u.t_bool;

    case SQL_STRING:
      return true;

    case SQL_NULL:
      return false;

    default:
      RAISEF(
         kTypeError,
          "can't convert $0 '$1' to Boolean",
          SValue::getTypeName(data_.type),
          toString());

  }
}

SValue::BoolType SValue::toBool() const {
  return getBool();
}

SValue::TimeType SValue::getTimestamp() const {
  switch (getType()) {

    case SQL_TIMESTAMP:
      return data_.u.t_timestamp * kMicrosPerSecond;

    case SQL_NULL:
      return 0;

    default:
      RAISE(
         kTypeError,
          "can't convert %s '%s' to DateTime",
          SValue::getTypeName(data_.type),
          toString().c_str());

  }
}

SValue::StringType SValue::getString() const {
  if (data_.type == SQL_STRING) {
    return std::string(data_.u.t_string.ptr, data_.u.t_string.len);
  } else {
    return toString();
  }
}

std::string SValue::makeUniqueKey(SValue* arr, size_t len) {
  std::string key;

  for (int i = 0; i < len; ++i) {
    key.append(arr[i].toString());
    key.append("\x00");
  }

  return key;
}

std::string SValue::toString() const {
  char buf[512];
  const char* str;
  size_t len;

  switch (data_.type) {

    case SQL_INTEGER: {
      len = snprintf(buf, sizeof(buf), "%" PRId64, getInteger());
      str = buf;
      break;
    }

    case SQL_TIMESTAMP: {
      return getTimestamp().toString("%Y-%m-%d %H:%M:%S");
    }

    case SQL_FLOAT: {
      len = snprintf(buf, sizeof(buf), "%f", getFloat());
      str = buf;
      break;
    }

    case SQL_BOOL: {
      static const auto true_str = "true";
      static const auto false_str = "false";
      if (getBool()) {
        str = true_str;
        len = strlen(true_str);
      } else {
        str = false_str;
        len = strlen(false_str);
      }
      break;
    }

    case SQL_STRING: {
      return getString();
    }

    case SQL_NULL: {
      static const char undef_str[] = "NULL";
      str = undef_str;
      len = sizeof(undef_str) - 1;
    }

  }

  return std::string(str, len);
}

String SValue::toSQL() const {
  switch (data_.type) {

    case SQL_INTEGER: {
      return toString();
    }

    case SQL_TIMESTAMP: {
      return StringUtil::format("\"$0\"", toString());
    }

    case SQL_FLOAT: {
      return toString();
    }

    case SQL_BOOL: {
      return toString();
    }

    case SQL_STRING: {
      auto str = sql_escape(getString());
      return StringUtil::format("\"$0\"", str);
    }

    case SQL_NULL: {
      return "NULL";
    }

  }
}

const char* SValue::getTypeName(sql_type type) {
  switch (type) {
    case SQL_STRING:
      return "String";
    case SQL_FLOAT:
      return "Float";
    case SQL_INTEGER:
      return "Integer";
    case SQL_BOOL:
      return "Boolean";
    case SQL_TIMESTAMP:
      return "Timestamp";
    case SQL_NULL:
      return "NULL";
  }
}

const char* SValue::getTypeName() const {
  return SValue::getTypeName(data_.type);
}

template <> SValue::BoolType SValue::getValue<SValue::BoolType>() const {
  return getBool();
}

template <> SValue::IntegerType SValue::getValue<SValue::IntegerType>() const {
  return getInteger();
}

template <> SValue::FloatType SValue::getValue<SValue::FloatType>() const {
  return getFloat();
}

template <> SValue::StringType SValue::getValue<SValue::StringType>() const {
  return toString();
}

template <> SValue::TimeType SValue::getValue<SValue::TimeType>() const {
  return getTimestamp();
}

// FIXPAUL: smarter type detection
template <> bool SValue::testType<SValue::BoolType>() const {
  return data_.type == SQL_BOOL;
}

template <> bool SValue::testType<SValue::TimeType>() const {
  return data_.type == SQL_TIMESTAMP;
}

template <> bool SValue::testType<SValue::IntegerType>() const {
  if (data_.type == SQL_INTEGER) {
    return true;
  }

  auto str = toString();
  const char* cur = str.c_str();
  const char* end = cur + str.size();

  if (*cur == '-') {
    ++cur;
  }

  if (cur == end) {
    return false;
  }

  for (; cur < end; ++cur) {
    if (*cur < '0' || *cur > '9') {
      return false;
    }
  }

  return true;
}

template <> bool SValue::testType<SValue::FloatType>() const {
  if (data_.type == SQL_FLOAT) {
    return true;
  }

  auto str = toString();
  bool dot = false;
  const char* c = str.c_str();

  if (*c == '-') {
    ++c;
  }

  for (; *c != 0; ++c) {
    if (*c >= '0' && *c <= '9') {
      continue;
    }

    if (*c == '.' || *c == ',') {
      if (dot) {
        return false;
      } else {
        dot = true;
      }
      continue;
    }

    return false;
  }

  return true;
}

template <> bool SValue::testType<std::string>() const {
  return true;
}

sql_type SValue::testTypeWithNumericConversion() const {
  if (testType<SValue::IntegerType>()) return SQL_INTEGER;
  if (testType<SValue::FloatType>()) return SQL_FLOAT;
  return getType();
}

bool SValue::tryNumericConversion() {
  if (testType<SValue::IntegerType>()) {
    SValue::IntegerType val = getValue<SValue::IntegerType>();
    data_.type = SQL_INTEGER;
    data_.u.t_integer = val;
    return true;
  }

  if (testType<SValue::FloatType>()) {
    SValue::FloatType val = getValue<SValue::FloatType>();
    data_.type = SQL_FLOAT;
    data_.u.t_float = val;
    return true;
  }

  return false;
}

bool SValue::tryTimeConversion() {
  uint64_t ts;

  switch (data_.type) {
    case SQL_TIMESTAMP:
      return true;
    case SQL_INTEGER:
      ts = getInteger();
      break;
    case SQL_FLOAT:
      ts = getFloat();
      break;
    default: {
      auto time_opt = stx::Human::parseTime(getString());
      if (time_opt.isEmpty()) {
        RAISEF(
           kTypeError,
            "can't convert $0 '$1' to DateTime",
            SValue::getTypeName(data_.type),
            toString());
      } else {
        ts = time_opt.get().unixMicros() / kMicrosPerSecond;
      }
      break;
    }
  }

  data_.type = SQL_TIMESTAMP;
  // FIXPAUL take a smart guess if this is milli, micro, etc
  data_.u.t_timestamp = ts;
  return true;
}

void SValue::encode(OutputStream* os) const {
  os->appendUInt8(data_.type);

  switch (data_.type) {
    case SQL_STRING:
      os->appendLenencString(data_.u.t_string.ptr, data_.u.t_string.len);
      return;
    case SQL_FLOAT:
      os->appendDouble(data_.u.t_float);
      return;
    case SQL_INTEGER:
      os->appendUInt64(data_.u.t_integer);
      return;
    case SQL_BOOL:
      os->appendUInt8(data_.u.t_bool ? 1 : 0);
      return;
    case SQL_TIMESTAMP:
      os->appendUInt64(data_.u.t_timestamp);
      return;
    case SQL_NULL:
      return;
  }
}

void SValue::decode(InputStream* is) {
  auto type = is->readUInt8();

  switch (type) {
    case SQL_STRING:
      *this = SValue(is->readLenencString());
      return;
    case SQL_FLOAT:
      *this = SValue(SValue::FloatType(is->readDouble()));
      return;
    case SQL_INTEGER:
      *this = SValue(SValue::IntegerType(is->readUInt64()));
      return;
    case SQL_BOOL:
      *this = SValue(SValue::BoolType(is->readUInt8() == 1));
      return;
    case SQL_TIMESTAMP: {
      *this = SValue(SValue::TimeType(is->readUInt64() * kMicrosPerSecond));
      return;
    }
    case SQL_NULL:
      *this = SValue();
      return;
  }
}

String sql_escape(const String& orig_str) {
  auto str = orig_str;
  StringUtil::replaceAll(&str, "\\", "\\\\");
  StringUtil::replaceAll(&str, "'", "\\'");
  StringUtil::replaceAll(&str, "\"", "\\\"");
  return str;
}

}

namespace stx {

template <>
std::string inspect<sql_type>(
    const sql_type& type) {
  return csql::SValue::getTypeName(type);
}

template <>
std::string inspect<csql::SValue>(
    const csql::SValue& sval) {
  return sval.toString();
}

}

namespace std {

size_t hash<csql::SValue>::operator()(const csql::SValue& sval) const {
  return hash<std::string>()(sval.toString()); // FIXPAUL
}

}
