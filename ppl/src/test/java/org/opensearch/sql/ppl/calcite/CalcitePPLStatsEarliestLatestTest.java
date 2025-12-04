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

public class CalcitePPLStatsEarliestLatestTest extends CalcitePPLAbstractTest {
  public CalcitePPLStatsEarliestLatestTest() {
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
  public void testEarliestWithoutSecondArgument() {
    String ppl = "source=LOGS | stats earliest(message) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], earliest_message=[ARG_MIN($0, $1)])\n"
            + "  LogicalProject(message=[$2], @timestamp=[$3])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "earliest_message=Database connection failed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MIN_BY(`message`, `@timestamp`) `earliest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestWithoutSecondArgument() {
    String ppl = "source=LOGS | stats latest(message) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], latest_message=[ARG_MAX($0, $1)])\n"
            + "  LogicalProject(message=[$2], @timestamp=[$3])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "latest_message=Backup completed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MAX_BY(`message`, `@timestamp`) `latest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | stats earliest(message) as earliest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_message=[$1], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_message=[ARG_MIN($1, $2)])\n"
            + "    LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_message=Disk space low; server=server3\n"
            + "earliest_message=Service started; server=server2\n"
            + "earliest_message=Database connection failed; server=server1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MIN_BY(`message`, `@timestamp`) `earliest_message`, `server`\n"
            + "FROM `POST`.`LOGS`\n"
            + "GROUP BY `server`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | stats latest(message) as latest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(latest_message=[$1], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], latest_message=[ARG_MAX($1, $2)])\n"
            + "    LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "latest_message=Disk space low; server=server3\n"
            + "latest_message=Backup completed; server=server2\n"
            + "latest_message=High memory usage; server=server1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MAX_BY(`message`, `@timestamp`) `latest_message`, `server`\n"
            + "FROM `POST`.`LOGS`\n"
            + "GROUP BY `server`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestWithOtherAggregatesWithoutSecondArgument() {
    String ppl =
        "source=LOGS | stats earliest(message) as earliest_message, count() as cnt by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_message=[$1], cnt=[$2], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_message=[ARG_MIN($1, $2)], cnt=[COUNT()])\n"
            + "    LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_message=Disk space low; cnt=1; server=server3\n"
            + "earliest_message=Service started; cnt=2; server=server2\n"
            + "earliest_message=Database connection failed; cnt=2; server=server1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MIN_BY(`message`, `@timestamp`) `earliest_message`, "
            + "COUNT(*) `cnt`, `server`\n"
            + "FROM `POST`.`LOGS`\n"
            + "GROUP BY `server`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestWithExplicitTimestampField() {
    String ppl = "source=LOGS | stats earliest(message, created_at) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], earliest_message=[ARG_MIN($0, $1)])\n"
            + "  LogicalProject(message=[$2], created_at=[$4])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "earliest_message=Backup completed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MIN_BY(`message`, `created_at`) `earliest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestWithExplicitTimestampField() {
    String ppl = "source=LOGS | stats latest(message, created_at) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], latest_message=[ARG_MAX($0, $1)])\n"
            + "  LogicalProject(message=[$2], created_at=[$4])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "latest_message=Database connection failed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT MAX_BY(`message`, `created_at`) `latest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
