/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLMathFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLMathFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAbsWithOverriding() {
    String ppl = "source=EMP | eval SAL = abs(-30) | head 10 | fields SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SAL0=[$7])\n"
            + "  LogicalSort(fetch=[10])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " COMM=[$6], DEPTNO=[$7], SAL0=[ABS(-30)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT ABS(-30) `SAL0`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
