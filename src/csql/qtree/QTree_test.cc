/**
 * This file is part of the "libcsql" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <stx/stdtypes.h>
#include <stx/exception.h>
#include <stx/wallclock.h>
#include <stx/test/unittest.h>
#include "csql/svalue.h"
#include "csql/runtime/defaultruntime.h"
#include "csql/qtree/SequentialScanNode.h"
#include "csql/qtree/ColumnReferenceNode.h"
#include "csql/qtree/CallExpressionNode.h"
#include "csql/qtree/LiteralExpressionNode.h"
#include "csql/CSTableScanProvider.h"

using namespace stx;
using namespace csql;

UNIT_TEST(QTreeTest);

TEST_CASE(QTreeTest, TestExtractEqualsConstraint, [] () {
  auto runtime = Runtime::getDefaultRuntime();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  Vector<String> queries;
  queries.push_back("select 1 from testtable where time = 1234;");
  queries.push_back("select 1 from testtable where 1234 = time;");
  for (const auto& query : queries) {
    csql::Parser parser;
    parser.parse(query.data(), query.size());

    auto qtree_builder = runtime->queryPlanBuilder();
    auto qtrees = qtree_builder->build(
        parser.getStatements(),
        estrat->tableProvider());

    EXPECT_EQ(qtrees.size(), 1);
    auto qtree = qtrees[0];
    EXPECT_TRUE(dynamic_cast<SequentialScanNode*>(qtree.get()) != nullptr);
    auto seqscan = qtree.asInstanceOf<SequentialScanNode>();
    EXPECT_EQ(seqscan->tableName(), "testtable");

    auto constraints = seqscan->constraints();
    EXPECT_EQ(constraints.size(), 1);

    auto constraint = constraints[0];
    EXPECT_EQ(constraint.column_name, "time");
    EXPECT_TRUE(constraint.type == ScanConstraintType::EQUAL_TO);
    EXPECT_EQ(constraint.value.toString(), "1234");
  }
});

TEST_CASE(QTreeTest, TestExtractNotEqualsConstraint, [] () {
  auto runtime = Runtime::getDefaultRuntime();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  Vector<String> queries;
  queries.push_back("select 1 from testtable where time != 1234;");
  queries.push_back("select 1 from testtable where 1234 != time;");
  for (const auto& query : queries) {
    csql::Parser parser;
    parser.parse(query.data(), query.size());

    auto qtree_builder = runtime->queryPlanBuilder();
    auto qtrees = qtree_builder->build(
        parser.getStatements(),
        estrat->tableProvider());

    EXPECT_EQ(qtrees.size(), 1);
    auto qtree = qtrees[0];
    EXPECT_TRUE(dynamic_cast<SequentialScanNode*>(qtree.get()) != nullptr);
    auto seqscan = qtree.asInstanceOf<SequentialScanNode>();
    EXPECT_EQ(seqscan->tableName(), "testtable");

    auto constraints = seqscan->constraints();
    EXPECT_EQ(constraints.size(), 1);

    auto constraint = constraints[0];
    EXPECT_EQ(constraint.column_name, "time");
    EXPECT_TRUE(constraint.type == ScanConstraintType::NOT_EQUAL_TO);
    EXPECT_EQ(constraint.value.toString(), "1234");
  }
});

TEST_CASE(QTreeTest, TestExtractLessThanConstraint, [] () {
  auto runtime = Runtime::getDefaultRuntime();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  Vector<String> queries;
  queries.push_back("select 1 from testtable where time < 1234;");
  queries.push_back("select 1 from testtable where 1234 > time;");
  for (const auto& query : queries) {
    csql::Parser parser;
    parser.parse(query.data(), query.size());

    auto qtree_builder = runtime->queryPlanBuilder();
    auto qtrees = qtree_builder->build(
        parser.getStatements(),
        estrat->tableProvider());

    EXPECT_EQ(qtrees.size(), 1);
    auto qtree = qtrees[0];
    EXPECT_TRUE(dynamic_cast<SequentialScanNode*>(qtree.get()) != nullptr);
    auto seqscan = qtree.asInstanceOf<SequentialScanNode>();
    EXPECT_EQ(seqscan->tableName(), "testtable");

    auto constraints = seqscan->constraints();
    EXPECT_EQ(constraints.size(), 1);

    auto constraint = constraints[0];
    EXPECT_EQ(constraint.column_name, "time");
    EXPECT_TRUE(constraint.type == ScanConstraintType::LESS_THAN);
    EXPECT_EQ(constraint.value.toString(), "1234");
  }
});

TEST_CASE(QTreeTest, TestExtractLessThanOrEqualToConstraint, [] () {
  auto runtime = Runtime::getDefaultRuntime();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  Vector<String> queries;
  queries.push_back("select 1 from testtable where time <= 1234;");
  queries.push_back("select 1 from testtable where 1234 >= time;");
  for (const auto& query : queries) {
    csql::Parser parser;
    parser.parse(query.data(), query.size());

    auto qtree_builder = runtime->queryPlanBuilder();
    auto qtrees = qtree_builder->build(
        parser.getStatements(),
        estrat->tableProvider());

    EXPECT_EQ(qtrees.size(), 1);
    auto qtree = qtrees[0];
    EXPECT_TRUE(dynamic_cast<SequentialScanNode*>(qtree.get()) != nullptr);
    auto seqscan = qtree.asInstanceOf<SequentialScanNode>();
    EXPECT_EQ(seqscan->tableName(), "testtable");

    auto constraints = seqscan->constraints();
    EXPECT_EQ(constraints.size(), 1);

    auto constraint = constraints[0];
    EXPECT_EQ(constraint.column_name, "time");
    EXPECT_TRUE(constraint.type == ScanConstraintType::LESS_THAN_OR_EQUAL_TO);
    EXPECT_EQ(constraint.value.toString(), "1234");
  }
});

TEST_CASE(QTreeTest, TestExtractGreaterThanConstraint, [] () {
  auto runtime = Runtime::getDefaultRuntime();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  Vector<String> queries;
  queries.push_back("select 1 from testtable where time > 1234;");
  queries.push_back("select 1 from testtable where 1234 < time;");
  for (const auto& query : queries) {
    csql::Parser parser;
    parser.parse(query.data(), query.size());

    auto qtree_builder = runtime->queryPlanBuilder();
    auto qtrees = qtree_builder->build(
        parser.getStatements(),
        estrat->tableProvider());

    EXPECT_EQ(qtrees.size(), 1);
    auto qtree = qtrees[0];
    EXPECT_TRUE(dynamic_cast<SequentialScanNode*>(qtree.get()) != nullptr);
    auto seqscan = qtree.asInstanceOf<SequentialScanNode>();
    EXPECT_EQ(seqscan->tableName(), "testtable");

    auto constraints = seqscan->constraints();
    EXPECT_EQ(constraints.size(), 1);

    auto constraint = constraints[0];
    EXPECT_EQ(constraint.column_name, "time");
    EXPECT_TRUE(constraint.type == ScanConstraintType::GREATER_THAN);
    EXPECT_EQ(constraint.value.toString(), "1234");
  }
});

TEST_CASE(QTreeTest, TestExtractGreaterThanOrEqualToConstraint, [] () {
  auto runtime = Runtime::getDefaultRuntime();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  Vector<String> queries;
  queries.push_back("select 1 from testtable where time >= 1234;");
  queries.push_back("select 1 from testtable where 1234 <= time;");
  for (const auto& query : queries) {
    csql::Parser parser;
    parser.parse(query.data(), query.size());

    auto qtree_builder = runtime->queryPlanBuilder();
    auto qtrees = qtree_builder->build(
        parser.getStatements(),
        estrat->tableProvider());

    EXPECT_EQ(qtrees.size(), 1);
    auto qtree = qtrees[0];
    EXPECT_TRUE(dynamic_cast<SequentialScanNode*>(qtree.get()) != nullptr);
    auto seqscan = qtree.asInstanceOf<SequentialScanNode>();
    EXPECT_EQ(seqscan->tableName(), "testtable");

    auto constraints = seqscan->constraints();
    EXPECT_EQ(constraints.size(), 1);

    auto constraint = constraints[0];
    EXPECT_EQ(constraint.column_name, "time");
    EXPECT_TRUE(constraint.type == ScanConstraintType::GREATER_THAN_OR_EQUAL_TO);
    EXPECT_EQ(constraint.value.toString(), "1234");
  }
});
