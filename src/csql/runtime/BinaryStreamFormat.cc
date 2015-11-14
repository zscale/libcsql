/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *   Copyright (c) 2015 Laura Schlimmer
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/BinaryStreamFormat.h>
#include <stx/logging.h>

namespace csql {

BinaryStreamFormat::BinaryStreamFormat(
    RefPtr<http::HTTPResponseStream> output) :
    output_(output) {}

void BinaryStreamFormat::formatResults(
    RefPtr<QueryPlan> query,
    ExecutionContext* context) {

  auto writer = stx::util::BinaryMessageWriter();
  try {
    context->onStatusChange([this, &writer, context] (const csql::ExecutionStatus& status) {
      auto progress = status.progress();

      if (output_->isClosed()) {
        stx::logDebug("sql", "Aborting Query...");
        context->cancel();
        return;
      }

      //FIXME check event id type
      writer.appendUInt8(2);
      writer.appendDouble(progress);
      output_->writeBodyChunk(writer.data(), writer.size());
    });

    for (int i = 0; i < query->numStatements(); ++i) {
      auto stmt = query->getStatement(i);

      auto table_expr = dynamic_cast<TableExpression*>(stmt);
      if (table_expr) {
        renderTable(table_expr, context, &writer);
        output_->writeBodyChunk(writer.data(), writer.size());
        continue;
      }

      RAISE(kRuntimeError, "can't render statement in BinaryFormat")
    }

  } catch (const StandardException& e) {
    stx::logError("sql", e, "SQL execution failed");

    //FIXME check event id type
    writer.appendUInt8(3);
    writer.appendLenencString(e.what());
    output_->writeBodyChunk(writer.data(), writer.size());
  }
}

void BinaryStreamFormat::renderTable(
    TableExpression* stmt,
    ExecutionContext* context,
    stx::util::BinaryMessageWriter* writer) {

  writer->appendUInt8(1);

  auto columns = stmt->columnNames();
  writer->appendUInt32(columns.size());
  for (int n = 0; n < columns.size(); ++n) {
    writer->appendUInt16(columns[n].size());
    writer->appendString(columns[n]);
  }

  //rows
  size_t j = 0;
  stmt->execute(
      context,
      [this, writer] (int argc, const csql::SValue* argv) -> bool {

    //check me (row event id)
    writer->appendUInt8(1);
    writer->appendUInt32(argc);

    for (int n = 0; n < argc; ++n) {
      switch (argv[n].getType()) {
        case SValue::T_STRING:
          writer->appendLenencString(argv[n].getString());
          break;
        case SValue::T_FLOAT:
          writer->appendDouble(argv[n].getFloat());
          break;
        case SValue::T_INTEGER:
          writer->appendUInt64(argv[n].getInteger());
          break;
        case SValue::T_BOOL:
          writer->appendUInt8(argv[n].getBool() ? 1 : 0);
          break;
        case SValue::T_TIMESTAMP:
          writer->appendUInt64(argv[n].getInteger());
          break;
        case SValue::T_NULL:
          break;
      }
    }

    return true;
  });


}

}
