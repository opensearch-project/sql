/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.time.LocalDate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLDateTimeFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLDateTimeFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testDateAndCurrentTimestamp() {
    String ppl = "source=EMP | eval added = DATE(CURRENT_TIMESTAMP()) | fields added | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(added=[POSTPROCESS(DATE(PREPROCESS(POSTPROCESS(CURRENT_TIMESTAMP, FLAG(TIMESTAMP)), FLAG(TIMESTAMP))), FLAG(DATE))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "added=" + LocalDate.now() + "\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT DATE(CURRENT_TIMESTAMP) `added`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCurrentDate() {
    String ppl = "source=EMP | eval added = CURRENT_DATE() | fields added | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(added=[POSTPROCESS(CURRENT_DATE, FLAG(DATE))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "added=" + LocalDate.now() + "\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT CURRENT_DATE `added`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
