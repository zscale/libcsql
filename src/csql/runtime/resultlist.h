/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#ifndef _FNORDMETRIC_QUERY_RESULTLIST_H
#define _FNORDMETRIC_QUERY_RESULTLIST_H
#include <stdlib.h>
#include <string>
#include <vector>
#include <memory>
#include <csql/runtime/rowsink.h>
#include <csql/svalue.h>

namespace csql {

class ResultList {
public:

  ResultList() {}
  ResultList(const ResultList& copy) = delete;
  ResultList& operator=(const ResultList& copy) = delete;

  ResultList(ResultList&& move) :
      columns_(std::move(move.columns_)),
      rows_(std::move(move.rows_)) {}

  size_t getNumColumns() const {
    return columns_.size();
  }

  size_t getNumRows() const {
    return rows_.size();
  }

  const std::vector<std::string>& getRow(size_t index) const {
    if (index >= rows_.size()) {
      RAISE(kRuntimeError, "invalid index");
    }

    return rows_[index];
  }

  const std::vector<std::string>& getColumns() const {
    return columns_;
  }

  int getColumnIndex(const std::string& column_name) const {
    for (int i = 0; i < columns_.size(); ++i) {
      if (columns_[i] == column_name) {
        return i;
      }
    }

    return -1;
  }

  void addHeader(const std::vector<std::string>& columns) {
    columns_ = columns;
  }

  void addRow(const csql::SValue* row, int row_len) {
    if (row_len > columns_.size()) {
      row_len = columns_.size();
    }

    Vector<String> str_row;
    for (int i = 0; i < row_len; ++i) {
      str_row.emplace_back(row[i].getString());
    }

    rows_.emplace_back(str_row);
  }

  void debugPrint() const {
    std::vector<int> col_widths;
    int total_width = 0;

    for (size_t i = 0; i < columns_.size(); ++i) {
      col_widths.push_back(20);
      total_width += 20;
    }

    auto print_hsep = [&col_widths] () {
      for (auto w : col_widths) {
        for (int i = 0; i < w; ++i) {
          char c = (i == 0 || i == w - 1) ? '+' : '-';
          printf("%c", c);
        }
      }
      printf("\n");
    };

    auto print_row = [this, &col_widths] (const std::vector<std::string>& row) {
      for (int n = 0; n < row.size(); ++n) {
        const auto& val = row[n];
        printf("| %s", val.c_str());
        for (int i = col_widths[n] - val.size() - 3; i > 0; --i) {
          printf(" ");
        }
      }
      printf("|\n");
    };

    print_hsep();
    print_row(columns_);
    print_hsep();
    for (const auto& row : rows_) {
      print_row(row);
    }
    print_hsep();
  }

protected:
  std::vector<std::string> columns_;
  std::vector<std::vector<std::string>> rows_;
};

}
#endif
