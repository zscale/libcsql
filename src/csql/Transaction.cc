/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/Transaction.h>
#include <stx/wallclock.h>

using namespace stx;

namespace csql {

Transaction::Transaction() : now_(WallClock::now()) {}

UnixTime Transaction::now() const {
  return now_;
}

} // namespace csql
