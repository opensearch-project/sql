/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAppendPipeTest extends CalcitePPLAbstractTest {
  public CalcitePPLAppendPipeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAppendPipe() {
    String ppl = "source=EMP | appendpipe [ where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 19); // 14 original table rows + 5 filtered subquery rows

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendPipeWithMergedColumns() {
    String ppl =
        "source=EMP | fields DEPTNO | appendpipe [ fields DEPTNO | eval DEPTNO_PLUS ="
            + " DEPTNO + 10 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO_PLUS=[null:INTEGER])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO_PLUS=[+($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 28);

    String expectedSparkSql =
        "SELECT `DEPTNO`, CAST(NULL AS INTEGER) `DEPTNO_PLUS`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT `DEPTNO`, `DEPTNO` + 10 `DEPTNO_PLUS`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
