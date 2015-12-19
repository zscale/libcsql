/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stdlib.h>
#include <string>
#include <string.h>
#include <vector>
#include <stx/io/inputstream.h>
#include <stx/io/outputstream.h>
#include <stx/stdtypes.h>
#include <stx/UnixTime.h>
#include <stx/exception.h>
#include <csql/csql.h>

using namespace stx;

namespace csql {
class Token;

class SValue {
public:
  typedef String StringType;
  typedef double FloatType;
  typedef int64_t IntegerType;
  typedef bool BoolType;
  typedef stx::UnixTime TimeType;

  static const char* getTypeName(sql_type type);
  const char* getTypeName() const;

  explicit SValue();
  explicit SValue(const StringType& string_value);
  explicit SValue(char const* string_value); // FIXPAUL HACK!!!
  explicit SValue(IntegerType integer_value);
  explicit SValue(FloatType float_value);
  explicit SValue(BoolType bool_value);
  explicit SValue(TimeType time_value);

  SValue(const SValue& copy);
  SValue& operator=(const SValue& copy);
  bool operator==(const SValue& other) const;
  ~SValue();

  static std::string makeUniqueKey(SValue* arr, size_t len);

  template <typename T> T getValue() const;
  template <typename T> bool testType() const;
  sql_type getType() const;
  sql_type testTypeWithNumericConversion() const;
  IntegerType getInteger() const;
  FloatType getFloat() const;
  BoolType getBool() const;
  BoolType toBool() const;
  TimeType getTimestamp() const;
  StringType getString() const;
  std::string toString() const;
  bool tryNumericConversion();
  bool tryTimeConversion();

  void encode(OutputStream* os) const;
  void decode(InputStream* is);

  String toSQL() const;

  sql_val data_;
};

static_assert(
    sizeof(SValue) == sizeof(sql_val),
    "libcsql requires the C++ compiler to produce classes without overhead");

String sql_escape(const String& str);

}

namespace std {

template <>
struct hash<csql::SValue> {
  size_t operator()(const csql::SValue& sval) const;
};

}
