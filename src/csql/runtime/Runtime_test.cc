/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *   Copyright (c) 2015 Laura Schlimmer
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
#include "csql/runtime/defaultruntime.h"
#include "csql/qtree/SequentialScanNode.h"
#include "csql/qtree/ColumnReferenceNode.h"
#include "csql/qtree/CallExpressionNode.h"
#include "csql/qtree/LiteralExpressionNode.h"
#include "csql/CSTableScanProvider.h"
#include "csql/backends/csv/CSVTableProvider.h"

using namespace stx;
using namespace csql;

UNIT_TEST(RuntimeTest);

TEST_CASE(RuntimeTest, TestStaticExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto expr = mkRef(
      new csql::CallExpressionNode(
          "add",
          {
            new csql::LiteralExpressionNode(SValue(SValue::IntegerType(1))),
            new csql::LiteralExpressionNode(SValue(SValue::IntegerType(2))),
          }));

  auto t0 = WallClock::unixMicros();
  SValue out;
  for (int i = 0; i < 1000; ++i) {
    out = runtime->evaluateConstExpression(ctx.get(), expr.get());
  }
  auto t1 = WallClock::unixMicros();

  EXPECT_EQ(out.getInteger(), 3);
});

TEST_CASE(RuntimeTest, TestComparisons, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("true = true"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("false = false"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("false = true"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("true = false"));
    EXPECT_EQ(v.toString(), "false");
  }
});

TEST_CASE(RuntimeTest, TestExecuteIfStatement, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto out_a = runtime->evaluateConstExpression(
      ctx.get(),
      String("if(1 = 1, 42, 23)"));
  EXPECT_EQ(out_a.getInteger(), 42);

  auto out_b = runtime->evaluateConstExpression(
      ctx.get(),
      String("if(1 = 2, 42, 23)"));
  EXPECT_EQ(out_b.getInteger(), 23);

  {
    auto v = runtime->evaluateConstExpression(
      ctx.get(),
      String("if(1 = 1, 'fnord', 'blah')"));
    EXPECT_EQ(v.toString(), "fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
      ctx.get(),
      String("if(1 = 2, 'fnord', 'blah')"));
    EXPECT_EQ(v.toString(), "blah");
  }

  {
    auto v = runtime->evaluateConstExpression(
      ctx.get(),
      String("if('fnord' = 'blah', 1, 2)"));
    EXPECT_EQ(v.toString(), "2");
  }

  {
    auto v = runtime->evaluateConstExpression(
      ctx.get(),
      String("if('fnord' = 'fnord', 1, 2)"));
    EXPECT_EQ(v.toString(), "1");
  }

  {
    auto v = runtime->evaluateConstExpression(
      ctx.get(),
      String("if('fnord' = '', 1, 2)"));
    EXPECT_EQ(v.toString(), "2");
  }
});

TEST_CASE(RuntimeTest, TestColumnReferenceWithTableNamePrefix, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select testtable.time from testtable;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 213);
  }

  {
    ResultList result;
    auto query = R"(select t1.time from testtable t1;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 213);
  }
});


TEST_CASE(RuntimeTest, TestSimpleCSTableAggregate, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  ResultList result;
  auto query = R"(select count(1) from testtable;)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 1);
  EXPECT_EQ(result.getNumRows(), 1);
  EXPECT_EQ(result.getRow(0)[0], "213");
});

TEST_CASE(RuntimeTest, TestNestedCSTableAggregate, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  ResultList result;
  auto query = R"(select count(event.search_query.time) from testtable;)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 1);
  EXPECT_EQ(result.getNumRows(), 1);
  EXPECT_EQ(result.getRow(0)[0], "704");
});

TEST_CASE(RuntimeTest, TestWithinRecordCSTableAggregate, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select sum(event.search_query.num_result_items) from testtable;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "24793");
  }

  {
    ResultList result;
    auto query = R"(select sum(count(event.search_query.result_items.position) WITHIN RECORD) from testtable;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "24793");
  }

  ResultList result;
  auto query = R"(
      select
        sum(event.search_query.num_result_items) WITHIN RECORD,
        count(event.search_query.result_items.position) WITHIN RECORD
      from testtable;)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 2);
  EXPECT_EQ(result.getNumRows(), 213);

  size_t s = 0;
  for (size_t i = 0; i < result.getNumRows(); ++i) {
    auto r1 = result.getRow(i)[0];
    if (r1 == "NULL") {
      r1 = "0";
    }

    auto r2 = result.getRow(i)[1];
    if (r2 == "NULL") {
      r2 = "0";
    }

    EXPECT_EQ(r1, r2);
    s += std::stoull(r1);
  }

  EXPECT_EQ(s, 24793);
});

TEST_CASE(RuntimeTest, TestMultiLevelNestedCSTableAggregate, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(
        select
          1,
          event.search_query.time,
          event.search_query.num_result_items,
          event.search_query.result_items.position
        from testtable;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumRows(), 24866);
  }

  {
    ResultList result;
    auto query = R"(
        select
          count(time),
          sum(count(event.search_query.time) WITHIN RECORD),
          sum(sum(event.search_query.num_result_items) WITHIN RECORD),
          sum(count(event.search_query.result_items.position) WITHIN RECORD)
        from testtable;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);

    EXPECT_EQ(result.getNumColumns(), 4);
    auto cols = result.getColumns();
    EXPECT_EQ(cols[0], "count(time)");
    EXPECT_EQ(cols[1], "sum(count(event.search_query.time) WITHIN RECORD)");
    EXPECT_EQ(cols[2], "sum(sum(event.search_query.num_result_items) WITHIN RECORD)");
    EXPECT_EQ(
        cols[3],
        "sum(count(event.search_query.result_items.position) WITHIN RECORD)");

    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "213");
    EXPECT_EQ(result.getRow(0)[1], "704");
    EXPECT_EQ(result.getRow(0)[2], "24793");
    EXPECT_EQ(result.getRow(0)[3], "24793");
  }

  {
    ResultList result;
    auto query = R"(
        select
          count(time),
          sum(count(event.search_query.time) WITHIN RECORD),
          sum(sum(event.search_query.num_result_items) WITHIN RECORD),
          sum(count(event.search_query.result_items.position) WITHIN RECORD),
          (
            count(time) +
            sum(count(event.search_query.time) WITHIN RECORD) +
            sum(sum(event.search_query.num_result_items) WITHIN RECORD) +
            sum(count(event.search_query.result_items.position) WITHIN RECORD)
          )
        from testtable
        group by 1;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 5);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "213");
    EXPECT_EQ(result.getRow(0)[1], "704");
    EXPECT_EQ(result.getRow(0)[2], "24793");
    EXPECT_EQ(result.getRow(0)[3], "24793");
    EXPECT_EQ(result.getRow(0)[4], "50503");
  }
});

TEST_CASE(RuntimeTest, TestMultiLevelNestedCSTableAggrgateWithGroup, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(
        select
          count(1) as num_items,
          sum(if(s.c, 1, 0)) as clicks
        from (
            select
                event.search_query.result_items.position as p,
                event.search_query.result_items.clicked as c
            from testtable) as s
        where s.p = 6;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 2);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "688");
    EXPECT_EQ(result.getRow(0)[1], "2");
  }

  //{
  //  ResultList result;
  //  auto query = R"(
  //      select
  //        sum(count(event.search_query.result_items.position) WITHIN RECORD),
  //        sum(sum(if(event.search_query.result_items.clicked, 1, 0)) WITHIN RECORD)
  //      from testtable
  //      where event.search_query.result_items.position = 9;)";
  //  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  //  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  //  EXPECT_EQ(result.getNumColumns(), 2);
  //  EXPECT_EQ(result.getNumRows(), 1);
  //  EXPECT_EQ(result.getRow(0)[0], "679");
  //  EXPECT_EQ(result.getRow(0)[1], "4");
  //}

  //{
  //  ResultList result;
  //  auto query = R"(
  //      select
  //        event.search_query.result_items.position,
  //        sum(count(event.search_query.result_items.position) WITHIN RECORD),
  //        sum(sum(if(event.search_query.result_items.clicked, 1, 0)) WITHIN RECORD)
  //      from testtable
  //      group by event.search_query.result_items.position
  //      order by event.search_query.result_items.position ASC
  //      LIMIT 10;)";
  //  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  //  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);

  //  EXPECT_EQ(result.getNumColumns(), 3);
  //  auto cols = result.getColumns();
  //  EXPECT_EQ(cols[0], "event.search_query.result_items.position");
  //  EXPECT_EQ(cols[1], "sum(count(event.search_query.result_items.position) WITHIN RECORD)");
  //  EXPECT_EQ(cols[2], "sum(sum(if(event.search_query.result_items.clicked, 1, 0)) WITHIN RECORD)");

  //  EXPECT_EQ(result.getNumRows(), 10);
  //  EXPECT_EQ(result.getRow(6)[0], "6");
  //  EXPECT_EQ(result.getRow(6)[1], "688");
  //  EXPECT_EQ(result.getRow(6)[2], "2");
  //}
});

TEST_CASE(RuntimeTest, TestMultiLevelNestedCSTableAggrgateWithMultiLevelGroup, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(
        select
          FROM_TIMESTAMP(TRUNCATE(time / 86400000000) *  86400),
          sum(sum(if(event.cart_items.checkout_step = 1, event.cart_items.price_cents, 0)) WITHIN RECORD) / 100.0 as gmv_eur,
          sum(if(event.cart_items.checkout_step = 1, event.cart_items.price_cents, 0)) / sum(count(event.page_view.item_id) WITHIN RECORD) as fu
          from testtable
          group by TRUNCATE(time / 86400000000)
          order by time desc;)";

    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 3);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "2015-07-28 00:00:00");
    EXPECT_EQ(result.getRow(0)[1], "28.150000");
  }
});

TEST_CASE(RuntimeTest, TestMultiLevelNestedCSTableAggrgateWithWhere, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  //{
  //  ResultList result;
  //  auto query = R"(
  //    select
  //      FROM_TIMESTAMP(TRUNCATE(event.search_query.time / 86400000000) * 86400) as time,
  //      event.search_query.result_items.position,
  //      sum(count(event.search_query.result_items.position) WITHIN RECORD) as number_impressions,
  //      sum(sum(if(event.search_query.result_items.clicked, 1, 0)) WITHIN RECORD) as number_result_clicked,
  //      (
  //        sum(sum(if(event.search_query.result_items.clicked, 1, 0)) WITHIN RECORD) / 
  //        sum(count(event.search_query.result_items.position) WITHIN RECORD)
  //      ) as ctr
  //    from testtable
  //    where event.search_query.result_items.position = 9
  //    group by TRUNCATE(event.search_query.time / 86400000000);
  //  )";

  //  auto qplan = runtime->buildQueryPlan(query, estrat.get());
  //  runtime->executeStatement(qplan->getStatement(0), &result);
  //  EXPECT_EQ(result.getNumColumns(), 5);
  //  result.debugPrint();
  //  EXPECT_EQ(result.getNumRows(), 1);
  //  EXPECT_EQ(result.getRow(0)[0], "2015-07-28 00:00:00");
  //  EXPECT_EQ(result.getRow(0)[1], "9");
  //  EXPECT_EQ(result.getRow(0)[2], "679");
  //  EXPECT_EQ(result.getRow(0)[3], "4");
  //  EXPECT_EQ(result.getRow(0)[4], "0.005891");
  //}
});

TEST_CASE(RuntimeTest, TestTableNamesWithDots, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "test.tbl",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select count(1) from 'test.tbl';)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "213");
  }

  {
    ResultList result;
    auto query = R"(select count(1) from `test.tbl`;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "213");
  }

  {
    ResultList result;
    auto query = R"(select count(1) from test.tbl;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 1);
    EXPECT_EQ(result.getRow(0)[0], "213");
  }
});

TEST_CASE(RuntimeTest, SelectFloatIntegerDivision, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("1 / 5"));
    EXPECT_EQ(v.toString(), "0.200000");
  }
});

TEST_CASE(RuntimeTest, SelectFloatIntegerMultiplication, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10 * 5"));
    EXPECT_EQ(v.toString(), "50");
  }

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10 * 5.0"));
    EXPECT_EQ(v.toString(), "50.000000");
  }

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10.0 * 5"));
    EXPECT_EQ(v.toString(), "50.000000");
  }
});

TEST_CASE(RuntimeTest, SelectFloatIntegerAddition, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10 + 5"));
    EXPECT_EQ(v.toString(), "15");
  }

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10 + 5.0"));
    EXPECT_EQ(v.toString(), "15.000000");
  }

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10.0 + 5"));
    EXPECT_EQ(v.toString(), "15.000000");
  }
});

TEST_CASE(RuntimeTest, SelectFloatIntegerSubtraction, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10 - 5"));
    EXPECT_EQ(v.toString(), "5");
  }

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10 - 5.0"));
    EXPECT_EQ(v.toString(), "5.000000");
  }

  {
    auto v = runtime->evaluateConstExpression(ctx.get(), String("10.0 - 5"));
    EXPECT_EQ(v.toString(), "5.000000");
  }
});

TEST_CASE(RuntimeTest, TestSelectInvalidColumn, [] () {
  EXPECT_EXCEPTION("column(s) not found: fnord", [] () {
    auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

    auto estrat = mkRef(new DefaultExecutionStrategy());
    estrat->addTableProvider(
        new CSTableScanProvider(
            "test.tbl",
            "src/csql/testdata/testtbl.cst"));

    ResultList result;
    auto query = R"(select fnord from 'test.tbl';)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  });
});

TEST_CASE(RuntimeTest, TestFromTimestampExpr, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("FROM_TIMESTAMP(1441408424)"));
    EXPECT_EQ(v.toString(), "2015-09-04 23:13:44");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("FROM_TIMESTAMP(1441408424.0)"));
    EXPECT_EQ(v.toString(), "2015-09-04 23:13:44");
  }
});

TEST_CASE(RuntimeTest, TestTruncateExpr, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("TRUNCATE(23.3)"));
    EXPECT_EQ(v.toString(), "23");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("TRUNCATE(23.7)"));
    EXPECT_EQ(v.toString(), "23");
  }
});

TEST_CASE(RuntimeTest, TestWildcardSelect, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select * from testtable;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 63);
    EXPECT_EQ(result.getColumns()[0], "attr.ab_test_group");
    EXPECT_EQ(result.getColumns()[62], "user_id");
    EXPECT_EQ(result.getNumRows(), 24883);
  }
});

TEST_CASE(RuntimeTest, TestWildcardSelectWithOrderLimit, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select * from testtable order by time desc limit 10;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 63);
    EXPECT_EQ(result.getColumns()[0], "attr.ab_test_group");
    EXPECT_EQ(result.getColumns()[62], "user_id");
    EXPECT_EQ(result.getNumRows(), 10);
  }
});

TEST_CASE(RuntimeTest, TestSelectWithInternalAggrGroupColumns, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select count(1) cnt, time from testtable group by TRUNCATE(time / 60000000) order by cnt desc;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 2);
    EXPECT_EQ(result.getNumRows(), 129);
    EXPECT_EQ(result.getRow(0)[0], "6");
    EXPECT_EQ(result.getRow(0)[1], "1438055578000000");
  }
});

TEST_CASE(RuntimeTest, TestSelectWithInternalGroupColumns, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select time from testtable group by TRUNCATE(time / 60000000);)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 129);
  }
});

TEST_CASE(RuntimeTest, TestSelectWithInternalOrderColumns, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(select user_id from testtable order by time desc limit 10;)";
    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 1);
    EXPECT_EQ(result.getNumRows(), 10);
  }
});

TEST_CASE(RuntimeTest, TestStringStartsWithExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("startswith('fnordblah', 'fnord')"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("startswith('fnordblah', 'f')"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("startswith('fnordblah', 'fnordblah')"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("startswith('fnordblah', 'fnordx')"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("startswith('fnordblah', 'bar')"));
    EXPECT_EQ(v.toString(), "false");
  }
});

TEST_CASE(RuntimeTest, TestStringEndsWithExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("endswith('fnordblah', 'blah')"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("endswith('fnordblah', 'h')"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("endswith('fnordblah', 'fnordblah')"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("endswith('fnordblah', 'bar')"));
    EXPECT_EQ(v.toString(), "false");
  }
});

TEST_CASE(RuntimeTest, TestLogicalAnd, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("true AND true"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("true AND false"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("false AND true"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("false AND false"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_and(true, true)"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_and(false, true)"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_and(true, false)"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_and(false, false)"));
    EXPECT_EQ(v.toString(), "false");
  }
});

TEST_CASE(RuntimeTest, TestLogicalOr, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("true OR true"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("true OR false"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("false OR true"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("false OR false"));
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_or(true, true)"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_or(false, true)"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_or(true, false)"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("logical_or(false, false)"));
    EXPECT_EQ(v.toString(), "false");
  }
});

TEST_CASE(RuntimeTest, TestIsNull, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        "isnull('NULL')");
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        "isnull(0)");
    EXPECT_EQ(v.toString(), "false");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        "isnull(NULL)");
    EXPECT_EQ(v.toString(), "true");
  }
});

TEST_CASE(RuntimeTest, TestStringUppercaseExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("uppercase('blah')"));
    EXPECT_EQ(v.toString(), "BLAH");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("ucase('blah')"));
    EXPECT_EQ(v.toString(), "BLAH");
  }
});

TEST_CASE(RuntimeTest, TestStringLowercaseExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("lowercase('FNORD')"));
    EXPECT_EQ(v.toString(), "fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("lcase('FnOrD')"));
    EXPECT_EQ(v.toString(), "fnord");
  }
});

TEST_CASE(RuntimeTest, TestDateTimeDateTruncExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('milliseconds', 1444229262.983758)"));
    EXPECT_EQ(double(v.getTimestamp()), 1444229262000000.000000);
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('seconds', 1444229262.983758)"));
    EXPECT_EQ(double(v.getTimestamp()), 1444229262000000.000000);
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('minutes', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-07 14:47:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('30minutes', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-07 14:30:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('hours', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-07 14:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('5hours', 1444229262.598)"));
    EXPECT_EQ(v.toString(), "2015-10-07 10:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('days', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-07 00:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('7days', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-01 00:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('week', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-01 00:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('month', 1444229262)"));
    EXPECT_EQ(v.toString(), "2015-10-01 00:00:00");
  }

  {
    //date_trunc returns last day of previous month for months with 30 days
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('month', 1441836754)"));
    EXPECT_EQ(v.toString(), "2015-08-31 00:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('year', 1444229262)"));
    //returns first of year - number of leap years until now
    EXPECT_EQ(v.toString(), "2014-12-21 00:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_trunc('2years', 1444229262)"));
    //returns first of year - number of leap years until now
    EXPECT_EQ(v.toString(), "2013-12-21 00:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("'blah' REGEXP '^b'"));
    EXPECT_EQ(v.toString(), "true");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("'fubar' REGEX '^b'"));
    EXPECT_EQ(v.toString(), "false");
  }

  //{
  //  auto v = runtime->evaluateConstExpression(String("'abc' LIKE 'abc'"));
  //  EXPECT_EQ(v.toString(), "true");
  //}

  //{
  //  auto v = runtime->evaluateConstExpression(String("'abc' LIKE 'a%'"));
  //  EXPECT_EQ(v.toString(), "true");
  //}

  //{
  //  auto v = runtime->evaluateConstExpression(String("'abc' LIKE '_b_'"));
  //  EXPECT_EQ(v.toString(), "true");
  //}

  //{
  //  auto v = runtime->evaluateConstExpression(String("'abc' LIKE '%bc'"));
  //  EXPECT_EQ(v.toString(), "true");
  //}

  //{
  //  auto v = runtime->evaluateConstExpression(String("'abc' LIKE 'c'"));
  //  EXPECT_EQ(v.toString(), "false");
  //}
});

TEST_CASE(RuntimeTest, TestDateTimeDateAddExpression, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '1.0', 'SECOND')"));
    EXPECT_EQ(v.toString(), "2015-11-16 11:00:25");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '-1', 'SECOND')"));
    EXPECT_EQ(v.toString(), "2015-11-16 11:00:23");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('2015-11-16 11:00:24', '1', 'SECOND')"));
    EXPECT_EQ(v.toString(), "2015-11-16 11:00:25");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '2', 'MINUTE')"));
    EXPECT_EQ(v.toString(), "2015-11-16 11:02:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '4', 'HOUR')"));
    EXPECT_EQ(v.toString(), "2015-11-16 15:00:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '30', 'DAY')"));
    EXPECT_EQ(v.toString(), "2015-12-16 11:00:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '1', 'MONTH')"));
    EXPECT_EQ(v.toString(), "2015-12-17 11:00:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '2', 'YEAR')"));
    EXPECT_EQ(v.toString(), "2017-11-15 11:00:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '2:15', 'MINUTE_SECOND')"));
    EXPECT_EQ(v.toString(), "2015-11-16 11:02:39");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '2:15:00', 'HOUR_SECOND')"));
    EXPECT_EQ(v.toString(), "2015-11-16 13:15:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('1447671624', '2:60', 'HOUR_MINUTE')"));
    EXPECT_EQ(v.toString(), "2015-11-16 14:00:24");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('2015-01-01 00:00:00', '1 1:30:30', 'DAY_SECOND')"));
    EXPECT_EQ(v.toString(), "2015-01-02 01:30:30");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('2015-12-31 00:00:00', '1 1:30', 'DAY_MINUTE')"));
    EXPECT_EQ(v.toString(), "2016-01-01 01:30:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('2015-12-31 23:00:00', '2 2', 'DAY_HOUR')"));
    EXPECT_EQ(v.toString(), "2016-01-03 01:00:00");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String("date_add('2015-12-31 23:00:00', '2-2', 'YEAR_MONTH')"));
    EXPECT_EQ(v.toString(), "2018-03-02 23:00:00");
  }
});

TEST_CASE(RuntimeTest, TestEscaping, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String(R"( "fnord'fnord" )"));

    EXPECT_EQ(v.toString(), "fnord'fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String(R"( "fnord\'fnord" )"));

    EXPECT_EQ(v.toString(), "fnord'fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String(R"( "fnord\\'fnord" )"));

    EXPECT_EQ(v.toString(), "fnord\\'fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String(R"( "fnord\\'fn\ord" )"));

    EXPECT_EQ(v.toString(), "fnord\\'fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String(R"( "fnord\\\'fn\ord" )"));

    EXPECT_EQ(v.toString(), "fnord\\'fnord");
  }

  {
    auto v = runtime->evaluateConstExpression(
        ctx.get(),
        String(R"( "fnord\\\\'fn\ord" )"));

    EXPECT_EQ(v.toString(), "fnord\\\\'fnord");
  }
});

TEST_CASE(RuntimeTest, TestSimpleSubSelect, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());

  ResultList result;
  auto query = R"(select t1.b, a from (select 123 as a, 435 as b) as t1)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 2);
  EXPECT_EQ(result.getNumRows(), 1);
  EXPECT_EQ(result.getRow(0)[0], "435");
  EXPECT_EQ(result.getRow(0)[1], "123");
});

TEST_CASE(RuntimeTest, TestWildcardOnSubselect, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());

  ResultList result;
  auto query = R"(select * from (select 123 as a, 435 as b) as t1)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 2);
  EXPECT_EQ(result.getNumRows(), 1);
  EXPECT_EQ(result.getRow(0)[0], "123");
  EXPECT_EQ(result.getRow(0)[1], "435");
});

TEST_CASE(RuntimeTest, TestSubqueryInGroupBy, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  ResultList result;
  auto query = R"(select count(1), t1.fubar + t1.x from (select count(1) as x, 123 as fubar from testtable group by TRUNCATE(time / 2000000)) t1 GROUP BY t1.x;)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 2);
  EXPECT_EQ(result.getNumRows(), 2);
  EXPECT_EQ(result.getRow(0)[0], "1");
  EXPECT_EQ(result.getRow(0)[1], "125");
  EXPECT_EQ(result.getRow(1)[0], "211");
  EXPECT_EQ(result.getRow(1)[1], "124");
});

TEST_CASE(RuntimeTest, TestInternalOrderByWithSubquery, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  ResultList result;
  auto query = R"(select t1.x from (select count(1) as x from testtable group by TRUNCATE(time / 2000000)) t1  order by t1.x DESC LIMIT 2;)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 1);
  EXPECT_EQ(result.getNumRows(), 2);
});

TEST_CASE(RuntimeTest, TestWildcardWithGroupBy, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new backends::csv::CSVTableProvider(
          "testtable",
          "src/csql/testdata/testtbl1.csv",
          '\t'));

  ResultList result;
  auto query = R"(select * from testtable group by time;)";
  auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
  runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
  EXPECT_EQ(result.getNumColumns(), 4);
  EXPECT_EQ(result.getColumns()[0], "time");
  EXPECT_EQ(result.getColumns()[1], "value");
  EXPECT_EQ(result.getColumns()[2], "segment1");
  EXPECT_EQ(result.getColumns()[3], "segment2");
  EXPECT_EQ(result.getNumRows(), 4);
});

TEST_CASE(RuntimeTest, TestInnerJoin, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new CSTableScanProvider(
          "testtable",
          "src/csql/testdata/testtbl.cst"));

  {
    ResultList result;
    auto query = R"(
        SELECT
          t1.time, t2.time, t3.time, t1.x, t2.x, t1.x + t2.x, t1.x * 3 = t3.x, x1, x2, x3
        FROM
          (select TRUNCATE(time / 1000000) as time, count(1) as x, 123 as x1 from testtable group by TRUNCATE(time / 1200000000)) t1,
          (select TRUNCATE(time / 1000000) as time, sum(2) as x, 456 as x2 from testtable group by TRUNCATE(time / 1200000000)) AS t2,
          (select TRUNCATE(time / 1000000) as time, sum(3) as x, 789 as x3 from testtable group by TRUNCATE(time / 1200000000)) AS t3
        ORDER BY
          t1.time desc;
    )";

    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 10);
    EXPECT_EQ(result.getNumRows(), 12 * 12 * 12);
  }

  {
    ResultList result;
    auto query = R"(
        SELECT
          t1.time, t2.time, t3.time, t1.x, t2.x, t1.x + t2.x, t1.x * 3 = t3.x, x1, x2, x3
        FROM
          (select TRUNCATE(time / 1000000) as time, count(1) as x, 123 as x1 from testtable group by TRUNCATE(time / 1200000000)) t1
        JOIN
          (select TRUNCATE(time / 1000000) as time, sum(2) as x, 456 as x2 from testtable group by TRUNCATE(time / 1200000000)) AS t2
        JOIN
          (select TRUNCATE(time / 1000000) as time, sum(3) as x, 789 as x3 from testtable group by TRUNCATE(time / 1200000000)) AS t3
        ON
          t2.time = t1.time and t3.time = t2.time
        ORDER BY
          t1.time desc;
    )";

    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 10);
    EXPECT_EQ(result.getNumRows(), 12);
    EXPECT_EQ(result.getRow(0)[0], "1438055447");
    EXPECT_EQ(result.getRow(0)[1], "1438055447");
    EXPECT_EQ(result.getRow(0)[2], "1438055447");
    EXPECT_EQ(result.getRow(0)[3], "48");
    EXPECT_EQ(result.getRow(0)[4], "96");
    EXPECT_EQ(result.getRow(0)[5], "144");
    EXPECT_EQ(result.getRow(0)[6], "true");
    EXPECT_EQ(result.getRow(0)[7], "123");
    EXPECT_EQ(result.getRow(0)[8], "456");
    EXPECT_EQ(result.getRow(0)[9], "789");
    EXPECT_EQ(result.getRow(11)[0], "1438041765");
    EXPECT_EQ(result.getRow(11)[1], "1438041765");
    EXPECT_EQ(result.getRow(11)[2], "1438041765");
    EXPECT_EQ(result.getRow(11)[3], "17");
    EXPECT_EQ(result.getRow(11)[4], "34");
    EXPECT_EQ(result.getRow(11)[5], "51");
    EXPECT_EQ(result.getRow(11)[6], "true");
    EXPECT_EQ(result.getRow(11)[7], "123");
    EXPECT_EQ(result.getRow(11)[8], "456");
    EXPECT_EQ(result.getRow(11)[9], "789");
  }

  {
    ResultList result;
    auto query = R"(
        SELECT
          t1.time, t2.time, t3.time, t1.x, t2.x, t1.x + t2.x, t1.x * 3 = t3.x, x1, x2, x3
        FROM
          (select TRUNCATE(time / 1000000) as time, count(1) as x, 123 as x1 from testtable group by TRUNCATE(time / 1200000000)) t1
        JOIN
          (select TRUNCATE(time / 1000000) as time, sum(2) as x, 456 as x2 from testtable group by TRUNCATE(time / 1200000000)) AS t2
        JOIN
          (select TRUNCATE(time / 1000000) as time, sum(3) as x, 789 as x3 from testtable group by TRUNCATE(time / 1200000000)) AS t3
        WHERE
          t2.time = t1.time AND t1.time = t3.time
        ORDER BY
          t1.time desc;
    )";

    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 10);
    EXPECT_EQ(result.getNumRows(), 12);
    EXPECT_EQ(result.getRow(0)[0], "1438055447");
    EXPECT_EQ(result.getRow(0)[1], "1438055447");
    EXPECT_EQ(result.getRow(0)[2], "1438055447");
    EXPECT_EQ(result.getRow(0)[3], "48");
    EXPECT_EQ(result.getRow(0)[4], "96");
    EXPECT_EQ(result.getRow(0)[5], "144");
    EXPECT_EQ(result.getRow(0)[6], "true");
    EXPECT_EQ(result.getRow(0)[7], "123");
    EXPECT_EQ(result.getRow(0)[8], "456");
    EXPECT_EQ(result.getRow(0)[9], "789");
    EXPECT_EQ(result.getRow(11)[0], "1438041765");
    EXPECT_EQ(result.getRow(11)[1], "1438041765");
    EXPECT_EQ(result.getRow(11)[2], "1438041765");
    EXPECT_EQ(result.getRow(11)[3], "17");
    EXPECT_EQ(result.getRow(11)[4], "34");
    EXPECT_EQ(result.getRow(11)[5], "51");
    EXPECT_EQ(result.getRow(11)[6], "true");
    EXPECT_EQ(result.getRow(11)[7], "123");
    EXPECT_EQ(result.getRow(11)[8], "456");
    EXPECT_EQ(result.getRow(11)[9], "789");
  }
});

TEST_CASE(RuntimeTest, TestLeftJoin, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new backends::csv::CSVTableProvider(
          "customers",
          "src/csql/testdata/testtbl2.csv",
          '\t'));
  estrat->addTableProvider(
      new backends::csv::CSVTableProvider(
          "orders",
          "src/csql/testdata/testtbl3.csv",
          '\t'));

  {
    ResultList result;
    auto query = R"(
      SELECT customers.customername, orders.orderid
      FROM customers
      LEFT JOIN orders
      ON customers.customerid=orders.customerid
      ORDER BY customers.customername;
    )";

    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 2);
    EXPECT_EQ(result.getNumRows(), 213);
    EXPECT_EQ(result.getRow(0)[0], "Alfreds Futterkiste");
    EXPECT_EQ(result.getRow(0)[1], "NULL");
    EXPECT_EQ(result.getRow(1)[0], "Ana Trujillo Emparedados y helados");
    EXPECT_EQ(result.getRow(1)[1], "10308");
    EXPECT_EQ(result.getRow(212)[0], "Wolski");
    EXPECT_EQ(result.getRow(212)[1], "10374");
  }
});

TEST_CASE(RuntimeTest, TestRightJoin, [] () {
  auto runtime = Runtime::getDefaultRuntime();
  auto ctx = runtime->newTransaction();

  auto estrat = mkRef(new DefaultExecutionStrategy());
  estrat->addTableProvider(
      new backends::csv::CSVTableProvider(
          "employees",
          "src/csql/testdata/testtbl4.csv",
          '\t'));
  estrat->addTableProvider(
      new backends::csv::CSVTableProvider(
          "orders",
          "src/csql/testdata/testtbl3.csv",
          '\t'));

  {
    ResultList result;
    auto query = R"(
      SELECT orders.orderid, employees.firstname
      FROM orders
      RIGHT JOIN employees
      ON orders.employeeid=employees.employeeid
      ORDER BY orders.orderid;
    )";

    auto qplan = runtime->buildQueryPlan(ctx.get(), query, estrat.get());
    runtime->executeStatement(ctx.get(), qplan->getStatement(0), &result);
    EXPECT_EQ(result.getNumColumns(), 2);
    EXPECT_EQ(result.getNumRows(), 197);
    EXPECT_EQ(result.getRow(0)[0], "10248");
    EXPECT_EQ(result.getRow(0)[1], "Steven");
    EXPECT_EQ(result.getRow(1)[0], "10249");
    EXPECT_EQ(result.getRow(1)[1], "Michael");
    EXPECT_EQ(result.getRow(195)[0], "10443");
    EXPECT_EQ(result.getRow(195)[1], "Laura");
    EXPECT_EQ(result.getRow(196)[0], "NULL");
    EXPECT_EQ(result.getRow(196)[1], "Adam");
  }
});
