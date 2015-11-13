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
#pragma once
#include <stx/stdtypes.h>
#include <csql/runtime/ResultFormat.h>
#include <stx/http/HTTPResponseStream.h>
#include <stx/util/binarymessagewriter.h>

namespace csql {

class BinaryStreamFormat : public ResultFormat {
public:

  BinaryStreamFormat(RefPtr<http::HTTPResponseStream> output);

  void formatResults(
      RefPtr<QueryPlan> query,
      ExecutionContext* context) override;

protected:

  void renderTable(
      TableExpression* stmt,
      ExecutionContext* context,
      stx::util::BinaryMessageWriter* writer);

  RefPtr<http::HTTPResponseStream> output_;
};

}
