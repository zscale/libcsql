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
    aggr_strategy_(other.aggr_strategy_),
    constraints_(other.constraints_) {
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
    RefPtr<LiteralExpressionNode> literal;
    RefPtr<ColumnReferenceNode> column;
    bool reverse_expr = false;
    auto args = expr->arguments();
    if (args.size() == 2) {
      for (size_t i = 0; i < args.size(); ++i) {
        auto literal_expr = dynamic_cast<LiteralExpressionNode*>(args[i].get());
        if (literal_expr) {
          literal = mkRef(literal_expr);
        }
        auto colref_expr = dynamic_cast<ColumnReferenceNode*>(args[i].get());
        if (colref_expr) {
          column = mkRef(colref_expr);
          reverse_expr = i > 0;
        }
      }
    }

    if (literal.get() != nullptr && column.get() != nullptr) {

      // EQUAL_TO
      if (call_expr && call_expr->symbol() == "eq") {
        ScanConstraint constraint;
        constraint.column_name = column->fieldName();
        constraint.type = ScanConstraintType::EQUAL_TO;
        constraint.value = literal->value();
        constraints_.emplace_back(constraint);
      }

      // NOT_EQUAL_TO
      if (call_expr && call_expr->symbol() == "neq") {
        ScanConstraint constraint;
        constraint.column_name = column->fieldName();
        constraint.type = ScanConstraintType::NOT_EQUAL_TO;
        constraint.value = literal->value();
        constraints_.emplace_back(constraint);
      }

      // LESS_THAN
      if (call_expr && call_expr->symbol() == "lt") {
        ScanConstraint constraint;
        constraint.column_name = column->fieldName();
        constraint.type = reverse_expr ?
            ScanConstraintType::GREATER_THAN :
            ScanConstraintType::LESS_THAN;
        constraint.value = literal->value();
        constraints_.emplace_back(constraint);
      }

      // LESS_THAN_OR_EQUALS
      if (call_expr && call_expr->symbol() == "lte") {
        ScanConstraint constraint;
        constraint.column_name = column->fieldName();
        constraint.type = reverse_expr ?
            ScanConstraintType::GREATER_THAN_OR_EQUAL_TO :
            ScanConstraintType::LESS_THAN_OR_EQUAL_TO;
        constraint.value = literal->value();
        constraints_.emplace_back(constraint);
      }

      // GREATER_THAN
      if (call_expr && call_expr->symbol() == "gt") {
        ScanConstraint constraint;
        constraint.column_name = column->fieldName();
        constraint.type = reverse_expr ?
            ScanConstraintType::LESS_THAN :
            ScanConstraintType::GREATER_THAN;
        constraint.value = literal->value();
        constraints_.emplace_back(constraint);
      }

      // GREATER_THAN_OR_EQUAL_TO
      if (call_expr && call_expr->symbol() == "gte") {
        ScanConstraint constraint;
        constraint.column_name = column->fieldName();
        constraint.type = reverse_expr ?
            ScanConstraintType::LESS_THAN_OR_EQUAL_TO :
            ScanConstraintType::GREATER_THAN_OR_EQUAL_TO;
        constraint.value = literal->value();
        constraints_.emplace_back(constraint);
      }

    }
  }
}

} // namespace csql
