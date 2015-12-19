/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>
#include <stx/autoref.h>
#include <stx/option.h>
#include <csql/qtree/QueryTreeNode.h>
#include <csql/qtree/GroupByNode.h>
#include <csql/qtree/GroupByMergeNode.h>
#include <csql/qtree/UnionNode.h>
#include <csql/qtree/LimitNode.h>
#include <csql/qtree/OrderByNode.h>
#include <csql/qtree/SequentialScanNode.h>
#include <csql/qtree/DrawStatementNode.h>
#include <csql/qtree/SelectExpressionNode.h>
#include <csql/qtree/DescribeTableNode.h>
#include <csql/qtree/RemoteAggregateNode.h>
#include <csql/runtime/TableExpression.h>
#include <csql/runtime/tablerepository.h>
#include <csql/svalue.h>

using namespace stx;

namespace csql {
class QueryBuilder;

class TableExpressionBuilder : public RefCounted {
public:

  ScopedPtr<TableExpression> build(
      SContext* ctx,
      RefPtr<QueryTreeNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

protected:

  ScopedPtr<TableExpression> buildGroupBy(
      SContext* ctx,
      RefPtr<GroupByNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildGroupMerge(
      SContext* ctx,
      RefPtr<GroupByMergeNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildUnion(
      SContext* ctx,
      RefPtr<UnionNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildLimit(
      SContext* ctx,
      RefPtr<LimitNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildOrderBy(
      SContext* ctx,
      RefPtr<OrderByNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildSequentialScan(
      SContext* ctx,
      RefPtr<SequentialScanNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildSelectExpression(
      SContext* ctx,
      RefPtr<SelectExpressionNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildDescribeTableStatment(
      SContext* ctx,
      RefPtr<DescribeTableNode> node,
      QueryBuilder* runtime,
      TableProvider* tables);

  ScopedPtr<TableExpression> buildRemoteAggregate(
      SContext* ctx,
      RefPtr<RemoteAggregateNode> node,
      QueryBuilder* runtime);

};

} // namespace csql
