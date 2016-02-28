/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/backends/metricservice/metrictableref.h>
#include <csql/runtime/tablescan.h>
#include <csql/svalue.h>

namespace csql {

MetricTableRef::MetricTableRef(
    stx::metric_service::IMetric* metric) :
    metric_(metric) {}

int MetricTableRef::getColumnIndex(const std::string& name) {
  if (name == "time") {
    return 0;
  }

  if (name == "value") {
    return 1;
  }

  if (metric_->hasLabel(name)) {
    fields_.emplace_back(name);
    return fields_.size() + 1;
  }

  return -1;
}

std::string MetricTableRef::getColumnName(int index) {
  if (index == 0) {
    return "time";
  }

  if (index == 1) {
    return "value";
  }

  if (index - 2 >= fields_.size()) {
    RAISE(kIndexError, "no such column");
  }

  return fields_[index - 2];
}

std::vector<std::string> MetricTableRef::columns() {
  auto columns = fields_;
  columns.emplace_back("value");
  columns.emplace_back("time");
  return columns;
}

void MetricTableRef::executeScan(csql::TableScan* scan) {
  auto begin = stx::UnixTime::epoch();
  auto limit = stx::UnixTime::now();

  metric_->scanSamples(
      begin,
      limit,
      [this, scan] (stx::metric_service::Sample* sample) -> bool {
        std::vector<csql::SValue> row;
        row.emplace_back(sample->time());
        row.emplace_back(sample->value());

        // FIXPAUL slow!
        for (const auto& field : fields_) {
          bool found = false;

          for (const auto& label : sample->labels()) {
            if (label.first == field) {
              found = true;
              row.emplace_back(label.second);
              break;
            }
          }

          if (!found) {
            row.emplace_back();
          }
        }

        return scan->nextRow(row.data(), row.size());
      });
}

}
