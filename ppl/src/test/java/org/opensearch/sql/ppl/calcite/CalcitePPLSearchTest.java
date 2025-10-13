/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Ignore;
import org.junit.Test;

public class CalcitePPLSearchTest extends CalcitePPLAbstractTest {
  public CalcitePPLSearchTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
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
  public void testSearchWithFilter() {
    String ppl = "search source=EMP DEPTNO=20";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[query_string(MAP('query', 'DEPTNO:20':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\nFROM `scott`.`EMP`\nWHERE `query_string`(MAP ('query', 'DEPTNO:20'))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Ignore("Fields used in search commands are not validated. Enable after fixing it.")
  @Test
  public void testSearchWithoutTimestampShouldThrow() {
    String ppl = "source=EMP earliest='2020-10-11'";
    Throwable t = assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(t, "field [@timestamp] not found");
  }

  @Test
  public void testSearchWithAbsoluteTimeRange() {
    String ppl = "source=LOGS earliest='2020-10-11' latest='2025-01-01'";
    RelNode root = getRelNode(ppl);
    // @timestamp is a field of type DATE here

    String expectedLogical =
        "LogicalFilter(condition=[query_string(MAP('query',"
            + " '(@timestamp:>=2020\\-10\\-11T00\\:00\\:00Z) AND"
            + " (@timestamp:<=2025\\-01\\-01T00\\:00\\:00Z)':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`LOGS`\n"
            + "WHERE `query_string`(MAP ('query', '(@timestamp:>=2020\\-10\\-11T00\\:00\\:00Z) AND"
            + " (@timestamp:<=2025\\-01\\-01T00\\:00\\:00Z)'))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSearchWithTimeSnap() {
    String ppl = "search source=LOGS earliest=@year";
    getRelNode(ppl);
  }
}
