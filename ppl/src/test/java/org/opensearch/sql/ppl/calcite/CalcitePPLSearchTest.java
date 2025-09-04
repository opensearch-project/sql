/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;

public class CalcitePPLSearchTest extends CalcitePPLAbstractTest {
  public CalcitePPLSearchTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    return configureTimestampLogSchema(schemaSpecs);
  }

  @Test
  public void testSearchWithFilter() {
    String ppl = "search source=EMP DEPTNO=20";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[=($7, 20)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`\n" + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

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
        "LogicalFilter(condition=[AND(>=($3, DATE('2020-10-11':VARCHAR)), <=($3,"
            + " DATE('2025-01-01':VARCHAR)))])\n"
            + "  LogicalTableScan(table=[[scott, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`LOGS`\n"
            + "WHERE `@timestamp` >= `DATE`('2020-10-11') AND `@timestamp` <= `DATE`('2025-01-01')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
