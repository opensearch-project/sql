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

  @Test
  public void testEventstatsEarliestWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats earliest(message) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[ARG_MIN($2, $3) OVER ()])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MIN_BY (`message`,"
            + " `@timestamp`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `earliest_message`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsLatestWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats latest(message) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " latest_message=[ARG_MAX($2, $3) OVER ()])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MAX_BY (`message`,"
            + " `@timestamp`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `latest_message`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsEarliestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats earliest(message) as earliest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[ARG_MIN($2, $3) OVER (PARTITION BY $0)])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MIN_BY (`message`,"
            + " `@timestamp`) OVER (PARTITION BY `server` RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `earliest_message`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsLatestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | eventstats latest(message) as latest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " latest_message=[ARG_MAX($2, $3) OVER (PARTITION BY $0)])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MAX_BY (`message`,"
            + " `@timestamp`) OVER (PARTITION BY `server` RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `latest_message`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsEarliestWithOtherAggregatesWithoutSecondArgument() {
    String ppl =
        "source=LOGS | eventstats earliest(message) as earliest_message, count() as cnt by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[ARG_MIN($2, $3) OVER (PARTITION BY $0)], cnt=[COUNT() OVER"
            + " (PARTITION BY $0)])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MIN_BY (`message`,"
            + " `@timestamp`) OVER (PARTITION BY `server` RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `earliest_message`, COUNT(*) OVER (PARTITION BY `server` RANGE"
            + " BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `cnt`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsEarliestWithExplicitTimestampField() {
    String ppl = "source=LOGS | eventstats earliest(message, created_at) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_message=[ARG_MIN($2, $4) OVER ()])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MIN_BY (`message`,"
            + " `created_at`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `earliest_message`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsLatestWithExplicitTimestampField() {
    String ppl = "source=LOGS | eventstats latest(message, created_at) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " latest_message=[ARG_MAX($2, $4) OVER ()])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MAX_BY (`message`,"
            + " `created_at`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `latest_message`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsEarliestLatestCombined() {
    String ppl =
        "source=LOGS | eventstats earliest(message) as earliest_msg, latest(message) as latest_msg"
            + " by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(server=[$0], level=[$1], message=[$2], @timestamp=[$3], created_at=[$4],"
            + " earliest_msg=[ARG_MIN($2, $3) OVER (PARTITION BY $0)], latest_msg=[ARG_MAX($2, $3)"
            + " OVER (PARTITION BY $0)])\n"
            + "  LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `server`, `level`, `message`, `@timestamp`, `created_at`, MIN_BY (`message`,"
            + " `@timestamp`) OVER (PARTITION BY `server` RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `earliest_msg`, MAX_BY (`message`, `@timestamp`) OVER"
            + " (PARTITION BY `server` RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `latest_msg`\n"
            + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
