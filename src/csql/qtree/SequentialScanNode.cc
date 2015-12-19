/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/qtree/SequentialScanNode.h>
#include <csql/qtree/ColumnReferenceNode.h>
#include <csql/qtree/CallExpressionNode.h>
#include <csql/qtree/LiteralExpressionNode.h>

using namespace stx;

namespace csql {

SequentialScanNode::SequentialScanNode(
    const String& table_name,
    Vector<RefPtr<SelectListNode>> select_list,
    Option<RefPtr<ValueExpressionNode>> where_expr) :
    SequentialScanNode(
        table_name,
        select_list,
        where_expr,
        AggregationStrategy::NO_AGGREGATION) {}

SequentialScanNode::SequentialScanNode(
    const String& table_name,
    Vector<RefPtr<SelectListNode>> select_list,
    Option<RefPtr<ValueExpressionNode>> where_expr,
    AggregationStrategy aggr_strategy) :
    table_name_(table_name),
    select_list_(select_list),
    where_expr_(where_expr),
    aggr_strategy_(aggr_strategy) {
  if (!where_expr_.isEmpty()) {
    findConstraints(where_expr_.get());
  }
}

SequentialScanNode::SequentialScanNode(
    const SequentialScanNode& other) :
    table_name_(other.table_name_),
    aggr_strategy_(other.aggr_strategy_) {
  for (const auto& e : other.select_list_) {
    select_list_.emplace_back(e->deepCopyAs<SelectListNode>());
  }

  if (!other.where_expr_.isEmpty()) {
    where_expr_ = Some(
        other.where_expr_.get()->deepCopyAs<ValueExpressionNode>());
  }
}

Option<RefPtr<ValueExpressionNode>> SequentialScanNode::whereExpression() const {
  return where_expr_;
}

const String& SequentialScanNode::tableName() const {
  return table_name_;
}

void SequentialScanNode::setTableName(const String& table_name) {
  table_name_ = table_name;
}

Vector<RefPtr<SelectListNode>> SequentialScanNode::selectList()
    const {
  return select_list_;
}

static void findSelectedColumnNames(
    RefPtr<ValueExpressionNode> expr,
    Set<String>* columns) {
  auto colname = dynamic_cast<ColumnReferenceNode*>(expr.get());
  if (colname) {
    columns->emplace(colname->fieldName());
  }

  for (const auto& a : expr->arguments()) {
    findSelectedColumnNames(a, columns);
  }
}

Set<String> SequentialScanNode::selectedColumns() const {
  Set<String> columns;

  for (const auto& sl : select_list_) {
    findSelectedColumnNames(sl->expression(), &columns);
  }

  return columns;
}

Vector<String> SequentialScanNode::columnNames() const {
  Vector<String> columns;

  for (const auto& sl : select_list_) {
    columns.emplace_back(sl->columnName());
  }

  return columns;
}

AggregationStrategy SequentialScanNode::aggregationStrategy() const {
  return aggr_strategy_;
}

void SequentialScanNode::setAggregationStrategy(AggregationStrategy strategy) {
  aggr_strategy_ = strategy;
}

RefPtr<QueryTreeNode> SequentialScanNode::deepCopy() const {
  return new SequentialScanNode(*this);
}

String SequentialScanNode::toString() const {
  String aggr;
  switch (aggr_strategy_) {

    case AggregationStrategy::NO_AGGREGATION:
      aggr = "NO_AGGREGATION";
      break;

    case AggregationStrategy::AGGREGATE_WITHIN_RECORD_FLAT:
      aggr = "AGGREGATE_WITHIN_RECORD_FLAT";
      break;

    case AggregationStrategy::AGGREGATE_WITHIN_RECORD_DEEP:
      aggr = "AGGREGATE_WITHIN_RECORD_DEEP";
      break;

    case AggregationStrategy::AGGREGATE_ALL:
      aggr = "AGGREGATE_ALL";
      break;

  };

  auto str = StringUtil::format(
      "(seqscan (table $0) (aggregate $1) (select-list",
      table_name_,
      aggr);

  for (const auto& e : select_list_) {
    str += " " + e->toString();
  }
  str += ")";

  if (!where_expr_.isEmpty()) {
    str += StringUtil::format(" (where $0)", where_expr_.get()->toString());
  }

  str += ")";
  return str;
}

const Vector<ScanConstraint>& SequentialScanNode::constraints() const {
  return constraints_;
}

void SequentialScanNode::findConstraints(RefPtr<ValueExpressionNode> expr) {
  auto call_expr = dynamic_cast<CallExpressionNode*>(expr.get());

  // logical ands allow chaining multiple constraints
  if (call_expr && call_expr->symbol() == "logical_and") {
    for (const auto& arg : call_expr->arguments()) {
      findConstraints(arg);
    }

    return;
  }

  // extract constraints of format "column <OP> value"
  {
    bool found_type = false;
    ScanConstraintType type;
    bool found_literal = false;
    RefPtr<LiteralExpressionNode> literal;
    bool found_column = false;
    RefPtr<ColumnReferenceNode> column;

    if (call_expr && call_expr->symbol() == "eq") {
      type = ScanConstraintType::EQUAL_TO;
      found_type = true;
    }

    auto expr_args = expr->arguments();
    if (expr_args.size() == 2) {
      for (const auto& arg : expr_args) {
        auto literal_expr = dynamic_cast<LiteralExpressionNode*>(arg.get());
        if (literal_expr) {
          literal = mkRef(literal_expr);
          found_literal = true;
        }
        auto colref_expr = dynamic_cast<ColumnReferenceNode*>(arg.get());
        if (colref_expr) {
          column = mkRef(colref_expr);
          found_column = true;
        }
      }
    }

    if (found_type && found_literal && found_column) {
      ScanConstraint constraint;
      constraint.column_name = column->fieldName();
      constraint.type = type;
      constraint.value = literal->value();
      constraints_.emplace_back(constraint);
    }
  }
}

} // namespace csql
