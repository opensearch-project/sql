/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Test;

public class CalcitePPLEventstatsEarliestLatestTest extends CalcitePPLAbstractTest {
  public CalcitePPLEventstatsEarliestLatestTest() {
    super(CalciteAssert.SchemaSpec.POST);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    schema.add(
        "LOGS",
        new CalcitePPLEarliestLatestTestUtils.LogsTable(
            CalcitePPLEarliestLatestTestUtils.createLogsTestData()));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  // After https://github.com/opensearch-project/sql/issues/5483 the visitor rewrites eventstats
  // from `Project(RexOver)` into `Join(input, Aggregate(input))` so the right-side aggregate can
  // push down to OpenSearch. The verifyLogical expectations below pin the new lowered shape;
  // the LOGS.server column is non-nullable in the POST schema so Calcite simplifies the BY-case
  // join condition from `IS NOT DISTINCT FROM` down to plain equality (`=`). The corresponding
  // `verifyPPLToSparkSQL` assertions are deferred until the SparkSqlDialect emission for the
  // join+aggregate form is observed and stabilized.

  @Test
  public void testEventstatsEarliestWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats earliest(message) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[$6])\n"
            + "  LogicalJoin(condition=[=($5, $7)], joinType=[inner])\n"
            + "    LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3],"
            + " created_at=[$4], __eventstats_join_key__=[0])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalProject(earliest_message=[$0], __eventstats_join_key__=[0])\n"
            + "      LogicalAggregate(group=[{}], earliest_message=[ARG_MIN($0, $1)])\n"
            + "        LogicalProject(message=[$2], @timestamp=[$3])\n"
            + "          LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsLatestWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats latest(message) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " latest_message=[$6])\n"
            + "  LogicalJoin(condition=[=($5, $7)], joinType=[inner])\n"
            + "    LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3],"
            + " created_at=[$4], __eventstats_join_key__=[0])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalProject(latest_message=[$0], __eventstats_join_key__=[0])\n"
            + "      LogicalAggregate(group=[{}], latest_message=[ARG_MAX($0, $1)])\n"
            + "        LogicalProject(message=[$2], @timestamp=[$3])\n"
            + "          LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsEarliestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats earliest(message) as earliest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[$6])\n"
            + "  LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalAggregate(group=[{0}], earliest_message=[ARG_MIN($1, $2)])\n"
            + "      LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "        LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsLatestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats latest(message) as latest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " latest_message=[$6])\n"
            + "  LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalAggregate(group=[{0}], latest_message=[ARG_MAX($1, $2)])\n"
            + "      LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "        LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsEarliestWithOtherAggregatesWithoutSecondArgument() {
    String ppl =
        "source=LOGS | eventstats earliest(message) as earliest_message, count() as cnt by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[$6], cnt=[$7])\n"
            + "  LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalAggregate(group=[{0}], earliest_message=[ARG_MIN($1, $2)],"
            + " cnt=[COUNT()])\n"
            + "      LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "        LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsEarliestWithExplicitTimestampField() {
    String ppl = "source=LOGS | eventstats earliest(message, created_at) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[$6])\n"
            + "  LogicalJoin(condition=[=($5, $7)], joinType=[inner])\n"
            + "    LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3],"
            + " created_at=[$4], __eventstats_join_key__=[0])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalProject(earliest_message=[$0], __eventstats_join_key__=[0])\n"
            + "      LogicalAggregate(group=[{}], earliest_message=[ARG_MIN($0, $1)])\n"
            + "        LogicalProject(message=[$2], created_at=[$4])\n"
            + "          LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsLatestWithExplicitTimestampField() {
    String ppl = "source=LOGS | eventstats latest(message, created_at) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " latest_message=[$6])\n"
            + "  LogicalJoin(condition=[=($5, $7)], joinType=[inner])\n"
            + "    LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3],"
            + " created_at=[$4], __eventstats_join_key__=[0])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalProject(latest_message=[$0], __eventstats_join_key__=[0])\n"
            + "      LogicalAggregate(group=[{}], latest_message=[ARG_MAX($0, $1)])\n"
            + "        LogicalProject(message=[$2], created_at=[$4])\n"
            + "          LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsEarliestLatestCombined() {
    String ppl =
        "source=LOGS | eventstats earliest(message) as earliest_msg, latest(message) as latest_msg"
            + " by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_msg=[$6], latest_msg=[$7])\n"
            + "  LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n"
            + "    LogicalAggregate(group=[{0}], earliest_msg=[ARG_MIN($1, $2)],"
            + " latest_msg=[ARG_MAX($1, $2)])\n"
            + "      LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "        LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);
  }
}
