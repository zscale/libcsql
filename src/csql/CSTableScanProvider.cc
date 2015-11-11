/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/CSTableScanProvider.h>
#include <csql/CSTableScan.h>
#include <csql/qtree/SequentialScanNode.h>

using namespace stx;

namespace csql {

CSTableScanProvider::CSTableScanProvider(
    const String& table_name,
    const String& cstable_file) :
    table_name_(table_name),
    cstable_file_(cstable_file) {}

Option<ScopedPtr<TableExpression>>
    CSTableScanProvider::buildSequentialScan(
        RefPtr<SequentialScanNode> node,
        QueryBuilder* runtime) const {
  return Option<ScopedPtr<TableExpression>>(
      mkScoped(
          new CSTableScan(
              node,
              cstable_file_,
              runtime)));
}

void CSTableScanProvider::listTables(
    Function<void (const csql::TableInfo& table)> fn) const {
  fn(tableInfo());
}

Option<csql::TableInfo> CSTableScanProvider::describe(
    const String& table_name) const {
  if (table_name == table_name_) {
    return Some(tableInfo());
  } else {
    return None<csql::TableInfo>();
  }
}

csql::TableInfo CSTableScanProvider::tableInfo() const {
  auto cstable = cstable::CSTableReader::openFile(cstable_file_);

  csql::TableInfo ti;
  ti.table_name = table_name_;

  for (const auto& col : cstable->columns()) {
    csql::ColumnInfo ci;
    ci.column_name = col;
    ci.type_size = 0;
    ci.is_nullable = true;

    switch (cstable->getColumnType(col)) {
      case cstable::ColumnType::BOOLEAN:
        ci.type = "bool";
        break;

      case cstable::ColumnType::UINT32_BITPACKED:
      case cstable::ColumnType::UINT32_PLAIN:
        ci.type = "uint32";
        break;

      case cstable::ColumnType::UINT64_PLAIN:
      case cstable::ColumnType::UINT64_LEB128:
        ci.type = "uint64";
        break;

      case cstable::ColumnType::DOUBLE:
        ci.type = "double";
        break;

      case cstable::ColumnType::STRING_PLAIN:
        ci.type = "string";
        break;

    }

    ti.columns.emplace_back(ci);
  }

  return ti;
}

} // namespace csql
