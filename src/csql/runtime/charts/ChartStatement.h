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
#include <stx/stdtypes.h>
#include <csql/runtime/Statement.h>
#include <csql/runtime/charts/drawstatement.h>

namespace csql {
class Runtime;

class ChartStatement : public Statement {
public:

  ChartStatement(Vector<ScopedPtr<DrawStatement>> draw_statements);

  void prepare(ExecutionContext* context) override;

  void execute(
      ExecutionContext* context,
      stx::chart::RenderTarget* target);

protected:
  Vector<ScopedPtr<DrawStatement>> draw_statements_;
};

}
