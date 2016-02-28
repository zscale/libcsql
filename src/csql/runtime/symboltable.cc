/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <algorithm>
#include <stdlib.h>
#include <assert.h>
#include <string>
#include "symboltable.h"
#include <stx/exception.h>

namespace csql {

void SymbolTable::registerFunction(
    const String& symbol,
    void (*fn)(sql_txn*, int, SValue*, SValue*)) {
  PureFunction sym;
  sym.call = fn;
  registerFunction(symbol, sym);
}

void SymbolTable::registerFunction(
    const String& symbol,
    AggregateFunction fn) {
  AggregateFunction sym;
  sym.scratch_size = fn.scratch_size;
  sym.accumulate = fn.accumulate;
  sym.get = fn.get;
  sym.reset = fn.reset;
  sym.init = fn.init;
  sym.free = fn.free;
  sym.merge = fn.merge;
  sym.loadstate = fn.loadstate;
  sym.savestate = fn.savestate;
  registerFunction(symbol, SFunction(sym));
}

void SymbolTable::registerFunction(const String& symbol, SFunction fn) {
  std::string symbol_downcase = symbol;
  StringUtil::toLower(&symbol_downcase);

  syms_.emplace(symbol_downcase, fn);
}

void SymbolTable::registerSymbol(
    const std::string& symbol,
    void (*method)(sql_txn*, void*, int, SValue*, SValue*)) {
  std::string symbol_downcase = symbol;
  std::transform(
      symbol_downcase.begin(),
      symbol_downcase.end(),
      symbol_downcase.begin(),
      ::tolower);

  symbols_.emplace(
      std::make_pair(
          symbol_downcase,
          SymbolTableEntry(symbol_downcase, method)));
}

void SymbolTable::registerSymbol(
    const std::string& symbol,
    void (*method)(sql_txn* ctx, void*, int, SValue*, SValue*),
    size_t scratchpad_size,
    void (*free_method)(sql_txn* ctx, void*)) {
  std::string symbol_downcase = symbol;
  StringUtil::toLower(&symbol_downcase);

  symbols_.emplace(
      std::make_pair(
          symbol_downcase,
          SymbolTableEntry(
              symbol_downcase,
              method,
              scratchpad_size,
              free_method)));
}

SymbolTableEntry const* SymbolTable::lookupSymbol(const std::string& symbol)
    const {
  std::string symbol_downcase = symbol;
  StringUtil::toLower(&symbol_downcase);

  auto iter = symbols_.find(symbol_downcase);

  if (iter == symbols_.end()) {
    RAISE(kRuntimeError, "symbol not found: %s", symbol.c_str());
    return nullptr;
  } else {
    return &iter->second;
  }
}

SFunction SymbolTable::lookup(const String& symbol) const {
  std::string symbol_downcase = symbol;
  StringUtil::toLower(&symbol_downcase);

  auto iter = syms_.find(symbol_downcase);
  if (iter == syms_.end()) {
    RAISEF(kRuntimeError, "symbol not found: $0", symbol);
  }

  return iter->second;
}

bool SymbolTable::isAggregateFunction(const String& symbol) const {
  auto sf = lookup(symbol);

  switch (sf.type) {
    case FN_AGGREGATE:
      return true;
    case FN_PURE:
      return false;
  }
}

SymbolTableEntry::SymbolTableEntry(
    const std::string& symbol,
    void (*method)(sql_txn*, void*, int, SValue*, SValue*),
    size_t scratchpad_size,
    void (*free_method)(sql_txn*, void*)) :
    call_(method),
    scratchpad_size_(scratchpad_size) {}

SymbolTableEntry::SymbolTableEntry(
    const std::string& symbol,
    void (*method)(sql_txn*, void*, int, SValue*, SValue*)) :
    SymbolTableEntry(symbol, method, 0, nullptr) {}

bool SymbolTableEntry::isAggregate() const {
  return scratchpad_size_ > 0;
}

void (*SymbolTableEntry::getFnPtr() const)(sql_txn* ctx, void*, int, SValue*, SValue*) {
  return call_;
}

size_t SymbolTableEntry::getScratchpadSize() const {
  return scratchpad_size_;
}

}
