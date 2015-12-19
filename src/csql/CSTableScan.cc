/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/CSTableScan.h>
#include <csql/qtree/ColumnReferenceNode.h>
#include <csql/runtime/defaultruntime.h>
#include <csql/runtime/compiler.h>
#include <stx/ieee754.h>
#include <stx/logging.h>

using namespace stx;

namespace csql {

CSTableScan::CSTableScan(
    Transaction* ctx,
    RefPtr<SequentialScanNode> stmt,
    const String& cstable_filename,
    QueryBuilder* runtime) :
    ctx_(ctx),
    stmt_(stmt),
    cstable_filename_(cstable_filename),
    runtime_(runtime),
    colindex_(0),
    aggr_strategy_(stmt_->aggregationStrategy()),
    rows_scanned_(0) {
  for (const auto& slnode : stmt_->selectList()) {
    column_names_.emplace_back(slnode->columnName());
  }
}

void CSTableScan::prepare(ExecutionContext* context) {
  context->incrNumSubtasksTotal(1);
}

void CSTableScan::execute(
    ExecutionContext* context,
    Function<bool (int argc, const SValue* argv)> fn) {
  auto cstable = cstable::CSTableReader::openFile(cstable_filename_);
  execute(cstable.get(), context, fn);
}

void CSTableScan::execute(
    cstable::CSTableReader* cstable,
    ExecutionContext* context,
    Function<bool (int argc, const SValue* argv)> fn) {
  logTrace("sql", "Scanning cstable: $0", cstable_filename_);

  Set<String> column_names;
  for (const auto& slnode : stmt_->selectList()) {
    findColumns(slnode->expression(), &column_names);
  }

  auto where_expr = stmt_->whereExpression();
  if (!where_expr.isEmpty()) {
    findColumns(where_expr.get(), &column_names);
  }

  for (const auto& col : column_names) {
    if (cstable->hasColumn(col)) {
      columns_.emplace(
          col,
          ColumnRef(cstable->getColumnReader(col), colindex_++));
    }
  }

  for (auto& slnode : stmt_->selectList()) {
    resolveColumns(slnode->expression());
  }

  for (const auto& slnode : stmt_->selectList()) {
    select_list_.emplace_back(
        ctx_,
        findMaxRepetitionLevel(slnode->expression()),
        runtime_->buildValueExpression(ctx_, slnode->expression()),
        &scratch_);
  }

  if (!where_expr.isEmpty()) {
    resolveColumns(where_expr.get());
    where_expr_ = runtime_->buildValueExpression(ctx_, where_expr.get());
  }

  if (columns_.empty()) {
    scanWithoutColumns(cstable, fn);
  } else {
    scan(cstable, fn);
  }

  context->incrNumSubtasksCompleted(1);
}

void CSTableScan::scan(
    cstable::CSTableReader* cstable,
    Function<bool (int argc, const SValue* argv)> fn) {
  uint64_t select_level = 0;
  uint64_t fetch_level = 0;
  bool filter_pred = true;

  Vector<SValue> in_row(colindex_, SValue{});
  Vector<SValue> out_row(select_list_.size(), SValue{});

  size_t num_records = 0;
  size_t total_records = cstable->numRecords();
  while (num_records < total_records) {
    ++rows_scanned_;
    uint64_t next_level = 0;

    if (fetch_level == 0) {
      if (num_records < total_records && filter_fn_) {
        filter_pred = filter_fn_();
      }
    }

    for (auto& col : columns_) {
      auto nextr = col.second.reader->nextRepetitionLevel();

      if (nextr >= fetch_level) {
        auto& reader = col.second.reader;

        uint64_t r;
        uint64_t d;

        switch (col.second.reader->type()) {

          case cstable::ColumnType::STRING: {
            String v;
            reader->readString(&r, &d, &v);

            if (d < reader->maxDefinitionLevel()) {
              in_row[col.second.index] = SValue();
            } else {
              in_row[col.second.index] = SValue(v);
            }
            break;
          }

          case cstable::ColumnType::UNSIGNED_INT: {
            uint64_t v = 0;
            reader->readUnsignedInt(&r, &d, &v);

            if (d < reader->maxDefinitionLevel()) {
              in_row[col.second.index] = SValue();
            } else {
              in_row[col.second.index] = SValue(SValue::IntegerType(v));
            }
            break;
          }

          case cstable::ColumnType::SIGNED_INT: {
            int64_t v = 0;
            reader->readSignedInt(&r, &d, &v);

            if (d < reader->maxDefinitionLevel()) {
              in_row[col.second.index] = SValue();
            } else {
              in_row[col.second.index] = SValue(SValue::IntegerType(v));
            }
            break;
          }

          case cstable::ColumnType::BOOLEAN: {
            bool v = 0;
            reader->readBoolean(&r, &d, &v);

            if (d < reader->maxDefinitionLevel()) {
              in_row[col.second.index] = SValue();
            } else {
              in_row[col.second.index] = SValue(SValue::BoolType(v));
            }
            break;
          }

          case cstable::ColumnType::FLOAT: {
            double v = 0;
            reader->readFloat(&r, &d, &v);

            if (d < reader->maxDefinitionLevel()) {
              in_row[col.second.index] = SValue();
            } else {
              in_row[col.second.index] = SValue(SValue::FloatType(v));
            }
            break;
          }

          case cstable::ColumnType::DATETIME: {
            UnixTime v;
            reader->readDateTime(&r, &d, &v);

            if (d < reader->maxDefinitionLevel()) {
              in_row[col.second.index] = SValue();
            } else {
              in_row[col.second.index] = SValue(SValue::TimeType(v));
            }
            break;
          }

          case cstable::ColumnType::SUBRECORD:
            RAISE(kIllegalStateError);

        }
      }

      next_level = std::max(
          next_level,
          col.second.reader->nextRepetitionLevel());
    }

    fetch_level = next_level;
    if (fetch_level == 0) {
      ++num_records;
    }

    bool where_pred = filter_pred;
    if (where_pred && where_expr_.program() != nullptr) {
      SValue where_tmp;
      VM::evaluate(
          ctx_,
          where_expr_.program(),
          in_row.size(),
          in_row.data(),
          &where_tmp);

      where_pred = where_tmp.toBool();
    }

    if (where_pred) {
      for (int i = 0; i < select_list_.size(); ++i) {
        if (select_list_[i].rep_level >= select_level) {
          VM::accumulate(
              ctx_,
              select_list_[i].compiled.program(),
              &select_list_[i].instance,
              in_row.size(),
              in_row.data());
        }
      }

      switch (aggr_strategy_) {

        case AggregationStrategy::AGGREGATE_ALL:
          break;

        case AggregationStrategy::AGGREGATE_WITHIN_RECORD_FLAT:
          if (next_level != 0) {
            break;
          }

        case AggregationStrategy::AGGREGATE_WITHIN_RECORD_DEEP:
          for (int i = 0; i < select_list_.size(); ++i) {
            VM::result(
                ctx_,
                select_list_[i].compiled.program(),
                &select_list_[i].instance,
                &out_row[i]);

            VM::reset(
                ctx_,
                select_list_[i].compiled.program(),
                &select_list_[i].instance);
          }

          if (!fn(out_row.size(), out_row.data())) {
            return;
          }

          break;

        case AggregationStrategy::NO_AGGREGATION:
          for (int i = 0; i < select_list_.size(); ++i) {
            VM::evaluate(
                ctx_,
                select_list_[i].compiled.program(),
                in_row.size(),
                in_row.data(),
                &out_row[i]);
          }

          if (!fn(out_row.size(), out_row.data())) {
            return;
          }

          break;

      }

      select_level = fetch_level;
    } else {
      select_level = std::min(select_level, fetch_level);
    }

    for (const auto& col : columns_) {
      if (col.second.reader->maxRepetitionLevel() >= select_level) {
        in_row[col.second.index] = SValue();
      }
    }
  }

  switch (aggr_strategy_) {
    case AggregationStrategy::AGGREGATE_ALL:
      for (int i = 0; i < select_list_.size(); ++i) {
        VM::result(
            ctx_,
            select_list_[i].compiled.program(),
            &select_list_[i].instance,
            &out_row[i]);
      }

      fn(out_row.size(), out_row.data());
      break;

    default:
      break;

  }
}

void CSTableScan::scanWithoutColumns(
    cstable::CSTableReader* cstable,
    Function<bool (int argc, const SValue* argv)> fn) {
  Vector<SValue> out_row(select_list_.size(), SValue{});

  size_t total_records = cstable->numRecords();
  for (size_t i = 0; i < total_records; ++i) {
    bool where_pred = true;
    if (where_expr_.program() != nullptr) {
      SValue where_tmp;
      VM::evaluate(ctx_, where_expr_.program(), 0, nullptr, &where_tmp);
      where_pred = where_tmp.toBool();
    }

    if (where_pred) {
      switch (aggr_strategy_) {

        case AggregationStrategy::AGGREGATE_ALL:
          for (int i = 0; i < select_list_.size(); ++i) {
            VM::accumulate(
                ctx_,
                select_list_[i].compiled.program(),
                &select_list_[i].instance,
                0,
                nullptr);
          }
          break;

        case AggregationStrategy::AGGREGATE_WITHIN_RECORD_DEEP:
        case AggregationStrategy::AGGREGATE_WITHIN_RECORD_FLAT:
        case AggregationStrategy::NO_AGGREGATION:
          for (int i = 0; i < select_list_.size(); ++i) {
            VM::evaluate(
                ctx_,
                select_list_[i].compiled.program(),
                0,
                nullptr,
                &out_row[i]);
          }

          if (!fn(out_row.size(), out_row.data())) {
            return;
          }
          break;
      }
    }
  }

  switch (aggr_strategy_) {
    case AggregationStrategy::AGGREGATE_ALL:
      for (int i = 0; i < select_list_.size(); ++i) {
        VM::result(
            ctx_,
            select_list_[i].compiled.program(),
            &select_list_[i].instance,
            &out_row[i]);
      }

      fn(out_row.size(), out_row.data());
      break;

    default:
      break;

  }
}

void CSTableScan::findColumns(
    RefPtr<ValueExpressionNode> expr,
    Set<String>* column_names) const {
  auto fieldref = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (fieldref != nullptr) {
    column_names->emplace(fieldref->fieldName());
  }

  for (const auto& e : expr->arguments()) {
    findColumns(e, column_names);
  }
}

void CSTableScan::resolveColumns(RefPtr<ValueExpressionNode> expr) const {
  auto fieldref = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (fieldref != nullptr) {
    auto col = columns_.find(fieldref->fieldName());
    if (col == columns_.end()) {
      fieldref->setColumnIndex(size_t(-1));
    } else {
      fieldref->setColumnIndex(col->second.index);
    }
  }

  for (const auto& e : expr->arguments()) {
    resolveColumns(e);
  }
}

uint64_t CSTableScan::findMaxRepetitionLevel(
    RefPtr<ValueExpressionNode> expr) const {
  uint64_t max_level = 0;

  auto fieldref = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (fieldref != nullptr) {
    auto col = columns_.find(fieldref->fieldName());
      if (col != columns_.end()) {
      auto col_level = col->second.reader->maxRepetitionLevel();
      if (col_level > max_level) {
        max_level = col_level;
      }
    }
  }

  for (const auto& e : expr->arguments()) {
    auto e_level = findMaxRepetitionLevel(e);
    if (e_level > max_level) {
      max_level = e_level;
    }
  }

  return max_level;
}

Vector<String> CSTableScan::columnNames() const {
  return column_names_;
}

size_t CSTableScan::numColumns() const {
  return column_names_.size();
}

Option<SHA1Hash> CSTableScan::cacheKey() const {
  return cache_key_;
}

void CSTableScan::setCacheKey(const SHA1Hash& key) {
  cache_key_ = Some(key);
}

size_t CSTableScan::rowsScanned() const {
  return rows_scanned_;
}

void CSTableScan::setFilter(Function<bool ()> filter_fn) {
  filter_fn_ = filter_fn;
}

CSTableScan::ColumnRef::ColumnRef(
    RefPtr<cstable::ColumnReader> r,
    size_t i) :
    reader(r),
    index(i) {}

CSTableScan::ExpressionRef::ExpressionRef(
    Transaction* _ctx,
    size_t _rep_level,
    ValueExpression _compiled,
    ScratchMemory* smem) :
    ctx(_ctx),
    rep_level(_rep_level),
    compiled(std::move(_compiled)),
    instance(VM::allocInstance(ctx, compiled.program(), smem)) {}

CSTableScan::ExpressionRef::ExpressionRef(
    ExpressionRef&& other) :
    rep_level(other.rep_level),
    compiled(std::move(other.compiled)),
    instance(other.instance) {
  other.instance.scratch = nullptr;
}

CSTableScan::ExpressionRef::~ExpressionRef() {
  if (instance.scratch) {
    VM::freeInstance(ctx, compiled.program(), &instance);
  }
}

} // namespace csql
