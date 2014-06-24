/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * Licensed under the MIT license (see LICENSE).
 */
#ifndef _FNORDMETRIC_QUERY_SYMBOLTABLE_H
#define _FNORDMETRIC_QUERY_SYMBOLTABLE_H
#include <stdlib.h>
#include <string>
#include <vector>

namespace fnordmetric {
namespace query {
class SymbolTableEntry;
class SValue;

void registerSymbol(const std::string& symbol, const SymbolTableEntry* entry);
const SymbolTableEntry* lookupSymbol(const std::string& symbol);

class SymbolTableEntry {
public:

  SymbolTableEntry(
      const std::string& symbol,
      void (*method)(void*, int, SValue*, SValue*));

  SymbolTableEntry(
      const std::string& symbol,
      void (*method)(void*, int, SValue*, SValue*),
      size_t scratchpad_size);

  inline void call(void* scratchpad, int argc, SValue* argv, SValue* out) const {
    call_(scratchpad, argc, argv, out);
  }

  bool isAggregate() const;
  void (*getFnPtr() const)(void*, int, SValue*, SValue*);
  size_t getScratchpadSize() const;

protected:
  void (*call_)(void*, int, SValue*, SValue*);
  const size_t scratchpad_size_;
};

}
}
#endif
