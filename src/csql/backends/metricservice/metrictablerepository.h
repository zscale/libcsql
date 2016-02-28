/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#ifndef _FNORDMETRIC_METRICDB_METRICTABLEREPOSITORY_H
#define _FNORDMETRIC_METRICDB_METRICTABLEREPOSITORY_H
#include <metricd/metricrepository.h>
#include <csql/runtime/tablerepository.h>
#include <memory>
#include <mutex>
#include <vector>

namespace csql {

class MetricTableRepository : public TableRepository {
public:

  MetricTableRepository(stx::metric_service::IMetricRepository* metric_repo);
  csql::TableRef* getTableRef(const std::string& table_name) const override;

protected:
  stx::metric_service::IMetricRepository* metric_repo_;
};

}
#endif
