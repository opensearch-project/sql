/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLDateTimeFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLDateTimeFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testDateAndCurrentTimestamp() {
    String ppl = "source=EMP | eval added = DATE(CURRENT_TIMESTAMP()) | fields added";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(added=[DATE(NOW())])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `DATE`(`NOW`()) `added`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCurrentDate() {
    String ppl = "source=EMP | eval added = CURRENT_DATE() | fields added";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(added=[CURRENT_DATE()])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT CURRENT_DATE() `added`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
